#!/usr/bin/env python3

import os
import shutil
import subprocess
import threading
import time
from enum import Enum
from typing import Dict, Optional, Set

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    QoSProfile,
    ReliabilityPolicy,
    DurabilityPolicy,
    HistoryPolicy,
)
from std_msgs.msg import Bool, String


class Stage1Status(str, Enum):
    INIT1 = "INIT1"
    TSP = "TSP"
    NAV2_SHELF = "NAV2_SHELF"
    INIT2 = "INIT2"
    FAILED = "FAILED"


class Stage1Master(Node):
    """
    Stage 1 전용 master.

    현재 Stage 1 조건:

      INIT1에서 원래 필요한 노드:
        - TSP
        - NAV2_SHELF
        - ARM_CART

      현재 처리:
        - TSP      : 일단 공백
        - ARM_CART : 0단계에서 이미 켜져 있다고 가정
        - NAV2_SHELF : start1 명령에서 새 터미널로 실행 요청

    사용 흐름:

      1. master_1 실행
         STAGE  = 1
         STATUS = INIT1

      2. start1 입력
         - nav2_shelf.py 새 터미널로 실행 요청
         - 실제 실행 성공 여부는 검사하지 않음
         - master 내부에서는 start1 수행 완료로 간주
         - STAGE  = 1 유지
         - STATUS = INIT1 유지

      3. shelf1 입력
         - nav2_shelf를 다시 실행하지 않음
         - /nav2_shelf_target String "shelf_1" publish
         - 5초 대기
         - STAGE  = 1
         - STATUS = NAV2_SHELF
         - /nav2_shelf_start Bool true publish

      4. shelf2 입력
         - nav2_shelf를 다시 실행하지 않음
         - /nav2_shelf_target String "shelf_2" publish
         - 5초 대기
         - STAGE  = 1
         - STATUS = NAV2_SHELF
         - /nav2_shelf_start Bool true publish

      5. /nav2_shelf_finish true 수신
         - /nav2_shelf_start Bool false publish
         - STAGE  = 2
         - STATUS = INIT2

    QoS:
      reliable + transient_local + keep_last + depth=1
    """

    def __init__(self):
        super().__init__("stage1_master")

        self.lock = threading.Lock()

        self.stage = "1"
        self.status = Stage1Status.INIT1.value
        self.last_event = "stage1_master started"

        self.target_wait_sec = 5.0

        # start1을 수행했는지 여부.
        # 실제 nav2_shelf 프로세스 생존 여부를 검사하지 않는다.
        self.stage1_started = False

        self.finish_received: Set[str] = set()
        self.processes: Dict[str, subprocess.Popen] = {}

        # 새 터미널 방식에서는 gnome-terminal/xterm parent process가 바로 끝날 수 있으므로
        # proc.poll()로 실제 노드 생존 여부를 판단하지 않는다.
        # 대신 launch 요청한 노드를 별도로 기록한다.
        self.launch_requested_nodes: Set[str] = set()

        self.qos_cmd = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
        )

        self.node_commands = self._build_node_commands()

        self.pub_nav2_shelf_target = self.create_publisher(
            String,
            "/nav2_shelf_target",
            self.qos_cmd,
        )

        self.pub_nav2_shelf_start = self.create_publisher(
            Bool,
            "/nav2_shelf_start",
            self.qos_cmd,
        )

        self.sub_nav2_shelf_finish = self.create_subscription(
            Bool,
            "/nav2_shelf_finish",
            lambda msg: self._finish_callback("nav2_shelf_finish", msg),
            self.qos_cmd,
        )

        # 오타 대응용. 정상 토픽은 /nav2_shelf_finish.
        self.sub_nav2_shelt_finish_typo = self.create_subscription(
            Bool,
            "/nav2_shelt_finish",
            lambda msg: self._finish_callback("nav2_shelt_finish_typo", msg),
            self.qos_cmd,
        )

        self.get_logger().info("Target publisher created: /nav2_shelf_target")
        self.get_logger().info("Start publisher created: /nav2_shelf_start")
        self.get_logger().info("Finish subscriber created: /nav2_shelf_finish")
        self.get_logger().info("Finish subscriber created: /nav2_shelt_finish [typo fallback]")
        self.get_logger().info("stage1_master started. Type 'help' to see commands.")

        self._print_dashboard()

        self.input_thread = threading.Thread(target=self._input_loop, daemon=True)
        self.input_thread.start()

    # ============================================================
    # Node commands
    # ============================================================

    def _build_node_commands(self) -> Dict[str, list]:
        return {
            "nav2_shelf": [
                "ros2",
                "run",
                "ffw_navigation",
                "nav2_shelf.py",
            ],

            # TSP는 현재 공백 처리.
            # 나중에 TSP 노드가 생기면 아래처럼 추가.
            #
            # "tsp": [
            #     "ros2",
            #     "run",
            #     "some_package",
            #     "tsp_node.py",
            # ],
        }

    # ============================================================
    # Dashboard
    # ============================================================

    def _set_state(
        self,
        stage: Optional[str] = None,
        status: Optional[str] = None,
        event: Optional[str] = None,
    ):
        with self.lock:
            if stage is not None:
                self.stage = stage
            if status is not None:
                self.status = status
            if event is not None:
                self.last_event = event

    def _clear_finish_received(self):
        with self.lock:
            self.finish_received.clear()

    def _dashboard_text(self) -> str:
        with self.lock:
            finishes = ", ".join(sorted(self.finish_received)) if self.finish_received else "-"
            launch_requested = (
                ", ".join(sorted(self.launch_requested_nodes))
                if self.launch_requested_nodes
                else "-"
            )

            return (
                "\n"
                "============================================================\n"
                " STAGE MASTER : STAGE 1 MASTER ONLY\n"
                f" STAGE        : {self.stage}\n"
                f" STATUS       : {self.status}\n"
                " INIT1 NODES  : NAV2_SHELF\n"
                " TSP NODE     : excluded now\n"
                " ARM_CART     : assumed already running from Stage 0\n"
                f" START1 DONE  : {self.stage1_started}\n"
                f" LAUNCH REQ   : {launch_requested}\n"
                f" FINISH RX    : {finishes}\n"
                f" EVENT        : {self.last_event}\n"
                "============================================================"
            )

    def _print_dashboard(self):
        print(self._dashboard_text(), flush=True)

    def _print_prompt(self):
        print("master1> ", end="", flush=True)

    # ============================================================
    # Input loop
    # ============================================================

    def _input_loop(self):
        while rclpy.ok():
            try:
                line = input("master1> ").strip()
            except EOFError:
                break
            except KeyboardInterrupt:
                break

            if not line:
                continue

            cmd = line.lower()
            should_print_dashboard = True

            if cmd in ["exit", "quit", "q"]:
                self._safe_clear_all_start_topics()
                self._shutdown_all_processes()
                rclpy.shutdown()
                break

            elif cmd in ["help", "h", "?"]:
                self._print_help()
                should_print_dashboard = False

            elif cmd in ["status", "dashboard"]:
                self._set_state(event="status requested")

            elif cmd == "clear":
                os.system("clear")
                should_print_dashboard = False

            elif cmd in ["start1", "init1_start", "run1"]:
                self._start_stage1_from_init()

            elif cmd == "launch":
                self._launch_stage1_nodes()
                self.stage1_started = True
                self._set_state(event="manual launch requested. stage1_started set true.")

            elif cmd == "shelf1":
                self._select_shelf_and_start_nav("shelf_1")

            elif cmd == "shelf2":
                self._select_shelf_and_start_nav("shelf_2")

            elif cmd in ["reset", "init1"]:
                self._reset_stage1()

            elif cmd == "list":
                self._print_topics_and_nodes()
                should_print_dashboard = False

            elif cmd == "kill_all":
                self._shutdown_all_processes()

            else:
                print(f"[WARN] Unknown command: {cmd}. Type 'help'.")
                self._set_state(event=f"unknown command: {cmd}")

            if should_print_dashboard:
                self._print_dashboard()

    # ============================================================
    # Stage flow
    # ============================================================

    def _start_stage1_from_init(self):
        with self.lock:
            current_stage = self.stage
            current_status = self.status

        if current_stage != "1" or current_status != Stage1Status.INIT1.value:
            print(
                f"[WARN] start1 is only allowed in STAGE=1, STATUS=INIT1. "
                f"current stage={current_stage}, status={current_status}"
            )
            self._set_state(
                event=f"start1 rejected. current stage={current_stage}, status={current_status}"
            )
            return

        self._launch_stage1_nodes()

        # 중요:
        # 실제 nav2_shelf.py가 존재하는지, 프로세스가 살아있는지는 검사하지 않는다.
        # start1 명령을 수행했으면 필요한 노드가 켜졌다고 가정한다.
        self.stage1_started = True

        self._set_state(
            stage="1",
            status=Stage1Status.INIT1.value,
            event="start1 complete. NAV2_SHELF launch requested. waiting shelf command.",
        )

        print("\n[STAGE1 READY] INIT1 nodes launch requested.")
        print("[WAITING] Type 'shelf1' or 'shelf2'.")

    def _launch_stage1_nodes(self):
        # 현재 Stage 1에서 실제로 켜는 노드는 NAV2_SHELF만.
        # TSP는 공백, ARM_CART는 Stage 0에서 이미 켜져 있다고 가정.
        self._launch_node("nav2_shelf")

    def _select_shelf_and_start_nav(self, shelf_name: str):
        with self.lock:
            current_stage = self.stage
            current_status = self.status
            stage1_started = self.stage1_started

        if current_stage != "1" or current_status != Stage1Status.INIT1.value:
            print(
                f"[WARN] shelf command is only allowed in STAGE=1, STATUS=INIT1. "
                f"current stage={current_stage}, status={current_status}"
            )
            self._set_state(
                event=f"shelf command rejected. current stage={current_stage}, status={current_status}"
            )
            return

        if shelf_name not in ["shelf_1", "shelf_2"]:
            print(f"[ERROR] invalid shelf_name: {shelf_name}")
            self._set_state(
                status=Stage1Status.FAILED.value,
                event=f"invalid shelf_name: {shelf_name}",
            )
            return

        # 중요:
        # 여기서 nav2_shelf를 다시 실행하지 않는다.
        # 실제 노드가 켜져 있는지도 검사하지 않는다.
        # start1 명령을 수행했는지만 확인한다.
        if not stage1_started:
            print("[WARN] Type 'start1' first before shelf1/shelf2.")
            self._set_state(
                event="shelf command rejected. start1 was not executed."
            )
            return

        self._clear_finish_received()

        self._publish_shelf_target(shelf_name)

        print(f"\n[TARGET SELECTED] {shelf_name}")
        print(f"[WAIT] waiting {self.target_wait_sec:.1f}s before /nav2_shelf_start true...")

        time.sleep(self.target_wait_sec)

        self._set_state(
            stage="1",
            status=Stage1Status.NAV2_SHELF.value,
            event=f"INIT1 -> NAV2_SHELF. target={shelf_name}. /nav2_shelf_start true published.",
        )

        print("\n[STATUS MOVE] INIT1 -> NAV2_SHELF")
        self._publish_nav2_shelf_start(True)

    def _finish_shelf_navigation(self, finish_key: str):
        self._publish_nav2_shelf_start(False)

        with self.lock:
            self.finish_received.add(finish_key)

        self._set_state(
            stage="2",
            status=Stage1Status.INIT2.value,
            event=f"{finish_key} received. NAV2_SHELF complete. Enter STAGE=2, STATUS=INIT2.",
        )

        print("\n[STAGE MOVE] STAGE 1 -> STAGE 2")
        print("[STATUS MOVE] NAV2_SHELF -> INIT2")

    def _reset_stage1(self):
        self._safe_clear_all_start_topics()
        self._clear_finish_received()

        self.stage1_started = False
        self.launch_requested_nodes.clear()

        self._set_state(
            stage="1",
            status=Stage1Status.INIT1.value,
            event="stage1 reset to STAGE=1, STATUS=INIT1",
        )

        print("[RESET] Stage 1 reset to STAGE=1, STATUS=INIT1.")

    # ============================================================
    # Publish
    # ============================================================

    def _publish_shelf_target(self, shelf_name: str):
        msg = String()
        msg.data = shelf_name
        self.pub_nav2_shelf_target.publish(msg)

        print(f"[PUB] /nav2_shelf_target std_msgs/String data: '{shelf_name}'")

    def _publish_nav2_shelf_start(self, value: bool):
        msg = Bool()
        msg.data = value
        self.pub_nav2_shelf_start.publish(msg)

        print(f"[PUB] /nav2_shelf_start std_msgs/Bool data: {str(value).lower()}")

    def _safe_clear_all_start_topics(self):
        self._publish_nav2_shelf_start(False)

    # ============================================================
    # Finish callback
    # ============================================================

    def _finish_callback(self, finish_key: str, msg: Bool):
        if not msg.data:
            return

        with self.lock:
            current_stage = self.stage
            current_status = self.status

        print(f"\n[FINISH RX] {finish_key} std_msgs/Bool data: true")

        if current_stage == "1" and current_status == Stage1Status.NAV2_SHELF.value:
            self._finish_shelf_navigation(finish_key)
            self._print_dashboard()
            self._print_prompt()
            return

        self._set_state(
            event=f"finish ignored in stage={current_stage}, status={current_status}: {finish_key}",
        )

        self._print_dashboard()
        self._print_prompt()

    # ============================================================
    # Launch with new terminal
    # ============================================================

    def _launch_node(self, node_key: str):
        if node_key not in self.node_commands:
            print(f"[ERROR] Unknown node key: {node_key}")
            self._set_state(
                status=Stage1Status.FAILED.value,
                event=f"unknown node key: {node_key}",
            )
            return

        # 새 터미널 방식에서는 실제 프로세스 생존 여부로 중복 실행을 판단하지 않는다.
        # start1을 여러 번 쳤을 때만 중복 실행을 막기 위해 launch_requested_nodes 기준 사용.
        if node_key in self.launch_requested_nodes:
            print(f"[INFO] {node_key} launch already requested.")
            return

        cmd = self.node_commands[node_key]
        terminal_cmd = self._make_terminal_command(cmd)

        if terminal_cmd is None:
            print(f"[WARN] No terminal found. Running without new terminal: {' '.join(cmd)}")
            proc = subprocess.Popen(cmd)
        else:
            print(f"[LAUNCH] {node_key}: {' '.join(cmd)}")
            proc = subprocess.Popen(terminal_cmd)

        self.processes[node_key] = proc
        self.launch_requested_nodes.add(node_key)

    def _make_terminal_command(self, ros_command: list):
        """
        VNC 환경에서 새 터미널을 열어 노드를 실행한다.

        우선순위:
          1. gnome-terminal
          2. xterm
          3. konsole

        workspace setup 경로:
          - ~/ros2_ws/install/setup.bash 우선
          - 없으면 ~/colcon_ws/install/setup.bash
        """

        workspace_setup = os.path.expanduser("~/ros2_ws/install/setup.bash")
        if not os.path.exists(workspace_setup):
            workspace_setup = os.path.expanduser("~/colcon_ws/install/setup.bash")

        command_str = " ".join(ros_command)

        command_inside_terminal = (
            f"source {workspace_setup}; "
            f"{command_str}; "
            f"echo ''; "
            f"echo '[{command_str}] exited. Press Ctrl+D or close terminal.'; "
            f"exec bash"
        )

        if shutil.which("gnome-terminal"):
            return [
                "gnome-terminal",
                "--",
                "bash",
                "-lc",
                command_inside_terminal,
            ]

        if shutil.which("xterm"):
            return [
                "xterm",
                "-hold",
                "-e",
                f"bash -lc \"{command_inside_terminal}\"",
            ]

        if shutil.which("konsole"):
            return [
                "konsole",
                "-e",
                "bash",
                "-lc",
                command_inside_terminal,
            ]

        return None

    # ============================================================
    # Shutdown
    # ============================================================

    def _shutdown_all_processes(self):
        for node_key, proc in list(self.processes.items()):
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass

            cmd = self.node_commands.get(node_key)
            if cmd:
                cmd_str = " ".join(cmd)

                if node_key == "nav2_shelf":
                    patterns = [
                        cmd_str,
                        "ffw_navigation.*nav2_shelf.py",
                        "nav2_shelf.py",
                    ]
                else:
                    patterns = [cmd_str]

                for pattern in patterns:
                    subprocess.run(
                        ["pkill", "-f", pattern],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )

        self.processes.clear()
        self.launch_requested_nodes.clear()
        self.stage1_started = False

        self._set_state(event="all launched processes terminated")
        print("[INFO] all launched processes terminated")

    # ============================================================
    # Print helpers
    # ============================================================

    def _print_help(self):
        print(
            """
============================================================
Stage 1 Master Commands
============================================================

[main flow]

  start1
    - INIT1에서 필요한 노드 실행 요청
    - 현재는 NAV2_SHELF만 새 터미널로 실행 요청
    - 실제 실행 성공 여부는 검사하지 않음
    - TSP는 공백
    - ARM_CART는 Stage 0에서 이미 켜져 있다고 가정
    - STAGE  = 1 유지
    - STATUS = INIT1 유지
    - 이후 shelf1 또는 shelf2 입력 대기

  shelf1
    - nav2_shelf를 새로 실행하지 않음
    - start1을 수행했다고 가정
    - /nav2_shelf_target String "shelf_1" publish
    - 5초 대기
    - STAGE  = 1
    - STATUS = NAV2_SHELF
    - /nav2_shelf_start Bool true publish

  shelf2
    - nav2_shelf를 새로 실행하지 않음
    - start1을 수행했다고 가정
    - /nav2_shelf_target String "shelf_2" publish
    - 5초 대기
    - STAGE  = 1
    - STATUS = NAV2_SHELF
    - /nav2_shelf_start Bool true publish

  /nav2_shelf_finish true 수신 시
    - /nav2_shelf_start Bool false publish
    - STAGE  = 2
    - STATUS = INIT2

[node]
  launch      : nav2_shelf 노드만 새 터미널로 실행 요청
  kill_all    : master가 실행 요청한 nav2_shelf 프로세스 종료 시도

[status]
  status
  dashboard

[reset]
  reset
  init1       : /nav2_shelf_start false clear 후 STAGE=1, STATUS=INIT1로 복귀

[list]
  list

[exit]
  exit
  quit
  q

============================================================

현재 비활성 처리:
  TSP      : excluded now
  ARM_CART : assumed already running from Stage 0

QoS:
  reliability: reliable
  durability : transient_local
  history    : keep_last
  depth      : 1

수동 테스트:

  target 확인:
    ros2 topic echo --qos-reliability reliable --qos-durability transient_local --qos-history keep_last --qos-depth 1 /nav2_shelf_target

  start 확인:
    ros2 topic echo --qos-reliability reliable --qos-durability transient_local --qos-history keep_last --qos-depth 1 /nav2_shelf_start

  finish 강제 입력:
    ros2 topic pub --once --qos-reliability reliable --qos-durability transient_local --qos-history keep_last --qos-depth 1 /nav2_shelf_finish std_msgs/Bool "data: true"

============================================================
"""
        )

    def _print_topics_and_nodes(self):
        print("\n[Node commands]")
        for key, cmd in self.node_commands.items():
            print(f"  {key:<20} -> {' '.join(cmd)}")

        print("\n[Publish topics]")
        print("  nav2_shelf_target       -> /nav2_shelf_target  std_msgs/String")
        print("  nav2_shelf_start        -> /nav2_shelf_start   std_msgs/Bool")

        print("\n[Subscribe topics]")
        print("  nav2_shelf_finish       -> /nav2_shelf_finish  std_msgs/Bool")
        print("  nav2_shelt_finish typo  -> /nav2_shelt_finish  std_msgs/Bool")


def main(args=None):
    rclpy.init(args=args)

    node = Stage1Master()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node._safe_clear_all_start_topics()
        node._shutdown_all_processes()
    finally:
        if rclpy.ok():
            node.destroy_node()
            rclpy.shutdown()


if __name__ == "__main__":
    main()