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
from std_msgs.msg import Bool


class Stage0Status(str, Enum):
    INIT0 = "INIT0"
    CART_NAV = "CART_NAV"
    CART_DOCKING = "CART_DOCKING"
    INIT1 = "INIT1"
    FAILED = "FAILED"


class Stage0Master(Node):
    """
    Stage 0 전용 master.

    현재 조건:

      INIT0에서 원래 필요한 노드:
        - ARM_CART
        - ARM_PICKING
        - ARM_PLACING
        - DETECTION_CART
        - DETECTION_HANDLE
        - NAV2_DOCKING
        - NAV2_CART

      현재 ARM 관련 노드는 아직 없으므로 master가 실행하는 노드:
        - detection_handle
        - detection_cart
        - nav2_cart
        - nav2_docking

    Stage 0 흐름:

      1. master 실행
         STATUS = INIT0

      2. start0 입력

      3. master가 4개 노드 실행
         - ros2 run cart_handle_detector purple_feature_detect ...
         - ros2 run cart_handle_detector cart_detect ...
         - ros2 run ffw_navigation nav2_cart.py
         - ros2 run ffw_navigation nav2_docking.py

      4. 2초 대기

      5. INIT0 -> CART_NAV
         /nav2_cart_start true publish

      6. /nav2_cart_finish true 수신

      7. CART_NAV -> CART_DOCKING
         /nav2_cart_start false publish
         /nav2_docking_start true publish

      8. /nav2_docking_finish true 수신

      9. CART_DOCKING -> INIT1
         /nav2_docking_start false publish

    QoS:
      reliable + transient_local + keep_last + depth=1
    """

    def __init__(self):
        super().__init__("stage0_master")

        self.lock = threading.Lock()

        self.stage = "0"
        self.status = Stage0Status.INIT0.value
        self.last_event = "stage0_master started"

        self.launch_wait_sec = 2.0

        self.finish_received: Set[str] = set()
        self.processes: Dict[str, subprocess.Popen] = {}

        self.qos_cmd = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
        )

        self.node_commands = self._build_node_commands()
        self.start_topics = self._build_start_topics()
        self.finish_topics = self._build_finish_topics()

        self.start_publishers: Dict[str, object] = {}
        self.finish_subscribers: Dict[str, object] = {}

        self._create_start_publishers()
        self._create_finish_subscribers()

        self.get_logger().info("stage0_master started. Type 'help' to see commands.")
        self._print_dashboard()

        self.input_thread = threading.Thread(target=self._input_loop, daemon=True)
        self.input_thread.start()

    # ============================================================
    # Node commands
    # ============================================================

    def _build_node_commands(self) -> Dict[str, list]:
        return {
            # --------------------------------------------------------
            # DETECTION_HANDLE
            # image -> marker pixel topics
            # --------------------------------------------------------
            "detection_handle": [
                "ros2",
                "run",
                "cart_handle_detector",
                "purple_feature_detect",
                "--ros-args",
                "-p", "color_topic:=/zedm/zed_node/rgb/image_rect_color",
                "-p", "publish_debug_image:=true",
                "-p", "debug_log:=true",
                "-p", "profile:=true",
                "-p", "purple_h_min:=122",
                "-p", "purple_h_max:=179",
                "-p", "purple_s_min:=20",
                "-p", "purple_s_max:=168",
                "-p", "purple_v_min:=87",
                "-p", "purple_v_max:=207",
                "-p", "open_kernel:=1",
                "-p", "close_kernel:=0",
                "-p", "handle_length_cm:=54.5",
                "-p", "marker_gap_cm:=11.2",
                "-p", "center_green_center_cm:=27.2",
                "-p", "purple_width_cm:=3.0",
                "-p", "green_width_cm:=3.0",
                "-p", "min_pair_dist_px:=18.0",
                "-p", "max_pair_dist_px:=260.0",
                "-p", "endpoint_margin_px:=80.0",
                "-p", "pattern_corridor_half_width_px:=28.0",
                "-p", "min_yellow_total_pixels:=70",
                "-p", "min_yellow_left_pixels:=15",
                "-p", "min_yellow_middle_pixels:=10",
                "-p", "min_yellow_right_pixels:=20",
                "-p", "min_purple_pixels:=6",
                "-p", "min_green_pixels:=6",
                "-p", "min_lr_yellow_balance:=0.15",
            ],

            # --------------------------------------------------------
            # DETECTION_CART
            # marker pixel topics + camera_info + TF -> cart pose
            # --------------------------------------------------------
            "detection_cart": [
                "ros2",
                "run",
                "cart_handle_detector",
                "cart_detect",
                "--ros-args",
                "-p", "camera_info_topic:=/zedm/zed_node/rgb/camera_info",
                "-p", "handle_length_m:=0.545",
                "-p", "end_green_center_m:=0.160",
                "-p", "center_green_center_m:=0.272",
                "-p", "handle_z_min_m:=1.010",
                "-p", "handle_z_max_m:=1.010",
                "-p", "handle_z_step_m:=0.005",
                "-p", "use_cylinder_centerline_correction:=true",
                "-p", "handle_radius_m:=0.015",
                "-p", "perp_offset_search_px:=45.0",
                "-p", "perp_offset_step_px:=2.0",
                "-p", "max_green_gap_error_m:=0.055",
                "-p", "max_handle_length_error_m:=0.160",
                "-p", "min_axis_consistency:=0.55",
                "-p", "basket_side:=left",
                "-p", "standoff_m:=0.45",
                "-p", "publish_forward_offset_m:=0.07",
                "-p", "publish_yaw_offset_deg:=3.0",
                "-p", "enable_temporal_gate:=true",
                "-p", "max_center_jump_m:=0.10",
                "-p", "max_goal_jump_m:=0.15",
                "-p", "max_yaw_jump_deg:=18.0",
                "-p", "pending_accept_count:=4",
                "-p", "pending_similarity_center_m:=0.04",
                "-p", "pending_similarity_goal_m:=0.06",
                "-p", "pending_similarity_yaw_deg:=8.0",
                "-p", "hold_previous_on_reject:=true",
                "-p", "max_hold_sec:=1.0",
                "-p", "force_accept_after_rejects:=10",
                "-p", "enable_smoothing:=true",
                "-p", "xy_alpha:=0.55",
                "-p", "yaw_alpha:=0.55",
                "-p", "publish_markers:=true",
                "-p", "debug:=true",
                "-p", "profile:=true",
            ],

            # --------------------------------------------------------
            # NAV2_CART
            # --------------------------------------------------------
            "nav2_cart": [
                "ros2",
                "run",
                "ffw_navigation",
                "nav2_cart.py",
            ],

            # --------------------------------------------------------
            # NAV2_DOCKING
            # --------------------------------------------------------
            "nav2_docking": [
                "ros2",
                "run",
                "ffw_navigation",
                "nav2_docking.py",
            ],
        }

    def _build_start_topics(self) -> Dict[str, str]:
        return {
            "nav2_cart_start": "/nav2_cart_start",
            "nav2_docking_start": "/nav2_docking_start",
        }

    def _build_finish_topics(self) -> Dict[str, str]:
        return {
            "nav2_cart_finish": "/nav2_cart_finish",
            "nav2_docking_finish": "/nav2_docking_finish",
        }

    # ============================================================
    # ROS pub/sub
    # ============================================================

    def _create_start_publishers(self):
        for key, topic in self.start_topics.items():
            self.start_publishers[key] = self.create_publisher(
                Bool,
                topic,
                self.qos_cmd,
            )
            self.get_logger().info(f"Start publisher created: {topic}")

    def _create_finish_subscribers(self):
        for key, topic in self.finish_topics.items():
            self.finish_subscribers[key] = self.create_subscription(
                Bool,
                topic,
                lambda msg, finish_key=key: self._finish_callback(finish_key, msg),
                self.qos_cmd,
            )
            self.get_logger().info(f"Finish subscriber created: {topic}")

    # ============================================================
    # Dashboard
    # ============================================================

    def _set_state(
        self,
        status: Optional[str] = None,
        event: Optional[str] = None,
    ):
        with self.lock:
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

            running_nodes = []
            dead_nodes = []

            for key, proc in self.processes.items():
                if proc.poll() is None:
                    running_nodes.append(key)
                else:
                    dead_nodes.append(key)

            running = ", ".join(sorted(running_nodes)) if running_nodes else "-"
            dead = ", ".join(sorted(dead_nodes)) if dead_nodes else "-"

            return (
                "\n"
                "============================================================\n"
                " STAGE MASTER : STAGE 0 MASTER ONLY\n"
                f" STAGE        : {self.stage}\n"
                f" STATUS       : {self.status}\n"
                " INIT0 NODES  : DETECTION_HANDLE, DETECTION_CART,\n"
                "                NAV2_CART, NAV2_DOCKING\n"
                " ARM NODES    : ARM_CART, ARM_PICKING, ARM_PLACING excluded now\n"
                f" RUNNING NODE : {running}\n"
                f" DEAD NODE    : {dead}\n"
                f" FINISH RX    : {finishes}\n"
                f" EVENT        : {self.last_event}\n"
                "============================================================"
            )

    def _print_dashboard(self):
        print(self._dashboard_text(), flush=True)

    def _print_prompt(self):
        print("master0> ", end="", flush=True)

    # ============================================================
    # Input loop
    # ============================================================

    def _input_loop(self):
        while rclpy.ok():
            try:
                line = input("master0> ").strip()
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

            elif cmd in ["start0", "init0", "run0"]:
                self._start_stage0_from_init()

            elif cmd == "launch":
                self._launch_stage0_nodes()

            elif cmd == "reset":
                self._reset_stage0()

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

    def _start_stage0_from_init(self):
        with self.lock:
            current_status = self.status

        if current_status != Stage0Status.INIT0.value:
            print(
                f"[WARN] start0 is only allowed in INIT0. "
                f"current status={current_status}"
            )
            self._set_state(event=f"start0 rejected. current status={current_status}")
            return

        self._launch_stage0_nodes()

        print(f"[WAIT] waiting {self.launch_wait_sec:.1f}s before CART_NAV...")
        time.sleep(self.launch_wait_sec)

        self._enter_cart_nav()

    def _enter_cart_nav(self):
        self._clear_finish_received()

        self._set_state(
            status=Stage0Status.CART_NAV.value,
            event="INIT0 -> CART_NAV. publish /nav2_cart_start true",
        )

        print("\n[STATUS MOVE] INIT0 -> CART_NAV")
        self._publish_start("nav2_cart_start", True)

    def _enter_cart_docking(self):
        self._clear_finish_received()

        self._set_state(
            status=Stage0Status.CART_DOCKING.value,
            event="CART_NAV -> CART_DOCKING. publish /nav2_docking_start true",
        )

        print("\n[STATUS MOVE] CART_NAV -> CART_DOCKING")
        self._publish_start("nav2_docking_start", True)

    def _enter_init1(self):
        self._clear_finish_received()

        self._set_state(
            status=Stage0Status.INIT1.value,
            event="nav2_docking_finish received. Stage 0 complete. Enter INIT1.",
        )

        print("\n[STAGE COMPLETE] STAGE 0 -> INIT1")

    def _reset_stage0(self):
        self._safe_clear_all_start_topics()
        self._clear_finish_received()

        self._set_state(
            status=Stage0Status.INIT0.value,
            event="stage0 reset to INIT0",
        )

        print("[RESET] Stage 0 reset to INIT0.")

    # ============================================================
    # Launch
    # ============================================================

    def _launch_stage0_nodes(self):
        for node_key in [
            "detection_handle",
            "detection_cart",
            "nav2_cart",
            "nav2_docking",
        ]:
            self._launch_node(node_key)

        self._set_state(event="stage0 nodes launch requested")

    def _launch_node(self, node_key: str):
        if node_key not in self.node_commands:
            print(f"[ERROR] Unknown node key: {node_key}")
            self._set_state(
                status=Stage0Status.FAILED.value,
                event=f"unknown node key: {node_key}",
            )
            return

        old = self.processes.get(node_key)
        if old is not None and old.poll() is None:
            print(f"[INFO] {node_key} already running.")
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

    def _make_terminal_command(self, ros_command: list):
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
    # Start publish
    # ============================================================

    def _publish_start(self, key: str, value: bool):
        pub = self.start_publishers.get(key)
        topic = self.start_topics.get(key)

        if pub is None or topic is None:
            print(f"[ERROR] Unknown start key: {key}")
            self._set_state(
                status=Stage0Status.FAILED.value,
                event=f"unknown start key: {key}",
            )
            return

        msg = Bool()
        msg.data = value
        pub.publish(msg)

        print(f"[PUB] {topic} std_msgs/Bool data: {str(value).lower()}")

    def _safe_clear_all_start_topics(self):
        for key in self.start_topics.keys():
            self._publish_start(key, False)

    # ============================================================
    # Finish callback
    # ============================================================

    def _finish_callback(self, finish_key: str, msg: Bool):
        if not msg.data:
            return

        with self.lock:
            current_status = self.status

        topic = self.finish_topics.get(finish_key, finish_key)
        print(f"\n[FINISH RX] {topic} std_msgs/Bool data: true")

        if current_status == Stage0Status.CART_NAV.value:
            if finish_key == "nav2_cart_finish":
                with self.lock:
                    self.finish_received.add(finish_key)

                self._publish_start("nav2_cart_start", False)

                self._enter_cart_docking()
                self._print_dashboard()
                self._print_prompt()
                return

        if current_status == Stage0Status.CART_DOCKING.value:
            if finish_key == "nav2_docking_finish":
                with self.lock:
                    self.finish_received.add(finish_key)

                self._publish_start("nav2_docking_start", False)

                self._enter_init1()
                self._print_dashboard()
                self._print_prompt()
                return

        self._set_state(
            event=f"finish ignored in status {current_status}: {finish_key}",
        )
        self._print_dashboard()
        self._print_prompt()

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
                subprocess.run(
                    ["pkill", "-f", cmd_str],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

        self.processes.clear()
        self._set_state(event="all launched processes terminated")
        print("[INFO] all launched processes terminated")

    # ============================================================
    # Print helpers
    # ============================================================

    def _print_help(self):
        print(
            """
============================================================
Stage 0 Master Commands
============================================================

[start]
  start0
  init0
  run0

동작:
  1. detection_handle 실행
  2. detection_cart 실행
  3. nav2_cart 실행
  4. nav2_docking 실행
  5. 2초 대기
  6. INIT0 -> CART_NAV
  7. /nav2_cart_start true publish
  8. /nav2_cart_finish true 수신
  9. /nav2_cart_start false publish
  10. CART_NAV -> CART_DOCKING
  11. /nav2_docking_start true publish
  12. /nav2_docking_finish true 수신
  13. /nav2_docking_start false publish
  14. STATUS = INIT1

[node]
  launch      : 0단계 노드만 실행
  kill_all    : master가 실행한 프로세스 종료

[status]
  status
  dashboard

[reset]
  reset       : start topic false clear 후 INIT0로 복귀

[list]
  list

[exit]
  exit
  quit
  q

============================================================

QoS:
  reliability: reliable
  durability : transient_local
  history    : keep_last
  depth      : 1

수동 테스트:
  ros2 topic pub --once /nav2_cart_finish std_msgs/Bool "data: true"
  ros2 topic pub --once /nav2_docking_finish std_msgs/Bool "data: true"

주의:
  transient_local true가 남으면 노드 재시작 시 자동 재실행될 수 있음.
  그래서 finish 수신 후 해당 start topic을 false로 clear함.

============================================================
"""
        )

    def _print_topics_and_nodes(self):
        print("\n[Node commands]")
        for key, cmd in self.node_commands.items():
            print(f"  {key:<20} -> {' '.join(cmd)}")

        print("\n[Start topics]")
        for key, topic in self.start_topics.items():
            print(f"  {key:<25} -> {topic}")

        print("\n[Finish topics]")
        for key, topic in self.finish_topics.items():
            print(f"  {key:<25} -> {topic}")


def main(args=None):
    rclpy.init(args=args)

    node = Stage0Master()

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