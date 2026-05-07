#!/usr/bin/env python3

import os
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

import rclpy
from rclpy.action import ActionClient
from rclpy.node import Node
from rclpy.qos import QoSProfile, DurabilityPolicy, ReliabilityPolicy
from std_msgs.msg import Bool

from master_capstone_interfaces.action import BringupNode


@dataclass
class NodeProcessConfig:
    node_key: str
    command: List[str]
    action_name: str
    timeout_sec: float = 5.0
    restart_on_bringup: bool = False


class ManualMaster(Node):
    """
    Stage 0 자동 전환 테스트용 master node.

    이번 표 기준 Stage 0 흐름:

      master> init0

      1. INIT0에서 아래 3개 노드를 action으로 bringup 확인
         - nav2_docking
         - nav2_cart
         - detection_cart

      2. 3개 노드가 모두 READY이면
         INIT0 -> CART_NAV

      3. CART_NAV 진입 시
         /nav2_cart_start true 발행

      4. /nav2_cart_finish true 수신 시
         CART_NAV -> CART_DOCKING

      5. CART_DOCKING 진입 시
         /nav2_docking_start true 발행

      6. /nav2_docking_finish true 수신 시
         Stage 0 종료
         STAGE = 1
         STATUS = INIT1

    주의:
      - NAV2는 master 실행 전에 미리 켜져 있다고 가정한다.
      - 따라서 master는 NAV2 자체를 bringup하지 않는다.
      - ARM_CART, ARM_PICKING, ARM_PLACING도 이번 INIT0 action 확인 대상에서 제외한다.
      - 실제 노드 명령어가 준비되면 _build_node_process_map()의 TODO만 교체하면 된다.
    """

    def __init__(self):
        super().__init__("manual_master")

        self.lock = threading.Lock()

        self.current_stage = "DEBUG"
        self.current_status = "IDLE"
        self.last_event = "manual_master started"

        self.ready_nodes: Set[str] = set()
        self.running_nodes: Set[str] = set()
        self.failed_nodes: Set[str] = set()
        self.finish_received: Set[str] = set()

        self.active_init_stage: Optional[str] = None

        self.start_publishers: Dict[str, object] = {}
        self.finish_subscribers: Dict[str, object] = {}
        self.action_clients: Dict[str, ActionClient] = {}
        self.processes: Dict[str, subprocess.Popen] = {}
        self.bringup_threads: Dict[str, threading.Thread] = {}

        self.qos_start = QoSProfile(
            depth=1,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            reliability=ReliabilityPolicy.RELIABLE,
        )

        self.qos_finish = QoSProfile(
            depth=10,
            durability=DurabilityPolicy.VOLATILE,
            reliability=ReliabilityPolicy.RELIABLE,
        )

        self.topic_command_map = self._build_topic_command_map()
        self.finish_topic_map = self._build_finish_topic_map()
        self.node_process_map = self._build_node_process_map()
        self.stage_required_nodes = self._build_stage_required_nodes()

        self._create_start_publishers()
        self._create_finish_subscribers()

        self.get_logger().info("manual_master started. Type 'help' to see commands.")

        self.input_thread = threading.Thread(target=self._input_loop, daemon=True)
        self.input_thread.start()

    # ============================================================
    # Start PUB command map
    # ============================================================

    def _build_topic_command_map(self) -> Dict[str, List[str]]:
        return {
            # ----------------------------------------------------
            # Stage 0
            # ----------------------------------------------------
            "nav2_cart_start": ["nav2_cart_start"],
            "nav2_docking_start": ["nav2_docking_start"],

            "cart_nav": ["nav2_cart_start"],
            "cart_docking": ["nav2_docking_start"],

            # ----------------------------------------------------
            # Stage 1 TODO
            # ----------------------------------------------------
            "tsp_start": ["tsp_start"],
            "nav2_object_start": ["nav2_object_start"],

            "tsp": ["tsp_start"],
            "nav2_object": ["nav2_object_start"],

            # ----------------------------------------------------
            # Stage 2 TODO
            # ----------------------------------------------------
            "nav2_shelf_start": ["nav2_shelf_start"],
            "cali_zed_start": ["cali_zed_start"],
            "sam3_start": ["sam3_start"],
            "anygrasp_start": ["anygrasp_start"],
            "cali_d405_start": ["cali_d405_start"],
            "gripper_start": ["gripper_start"],

            "nav2_shelf": ["nav2_shelf_start", "cali_zed_start"],

            "grasp_detection": [
                "sam3_start",
                "anygrasp_start",
                "cali_d405_start",
                "gripper_start",
            ],

            # ----------------------------------------------------
            # Stage 3 TODO
            # ----------------------------------------------------
            "nav2_side_start": ["nav2_side_start"],
            "nav2_return_start": ["nav2_return_start"],

            "nav2_side": [
                "nav2_side_start",
                "gripper_start",
            ],

            "arm_placing": [
                # TODO:
                # "arm_placing_start",
            ],

            "nav2_return": ["nav2_return_start"],

            "return_home": [
                # TODO:
                # "return_home_start",
            ],
        }

    # ============================================================
    # Finish topic map
    # ============================================================

    def _build_finish_topic_map(self) -> Dict[str, str]:
        return {
            # Stage 0
            "nav2_cart_finish": "/nav2_cart_finish",
            "nav2_docking_finish": "/nav2_docking_finish",

            # Optional. 지금 Stage 0 전환 조건에는 사용하지 않지만,
            # detection_cart가 완료 신호를 보내면 dashboard에는 기록되게 둔다.
            "detection_cart_finish": "/detection_cart_finish",

            # Stage 1 TODO
            "tsp_finish": "/tsp_finish",
            "nav2_object_finish": "/nav2_object_finish",

            # Stage 2 TODO
            "nav2_shelf_finish": "/nav2_shelf_finish",
            "cali_zed_finish": "/cali_zed_finish",
            "sam3_finish": "/sam3_finish",
            "anygrasp_finish": "/anygrasp_finish",
            "cali_d405_finish": "/cali_d405_finish",
            "gripper_finish": "/gripper_finish",

            # Stage 3 TODO
            "nav2_side_finish": "/nav2_side_finish",
            "nav2_return_finish": "/nav2_return_finish",
        }

    # ============================================================
    # Stage required nodes
    # ============================================================

    def _build_stage_required_nodes(self) -> Dict[str, List[str]]:
        return {
            "0": [
                "nav2_docking",
                "nav2_cart",
                "detection_cart",
            ],
            "1": [
                # TODO:
                # "tsp",
                # "nav2_object",
            ],
            "2": [
                # TODO:
                # "nav2_shelf",
                # "sam3",
                # "anygrasp",
                # "cali_zed",
                # "cali_d405",
                # "gripper",
            ],
            "3": [
                # TODO:
                # "detection_cart",
                # "nav2_side",
                # "arm_placing",
                # "gripper",
                # "nav2_return",
            ],
        }

    # ============================================================
    # Node process config
    # ============================================================

    def _build_node_process_map(self) -> Dict[str, NodeProcessConfig]:
        result: Dict[str, NodeProcessConfig] = {}

        # --------------------------------------------------------
        # DEBUG
        # --------------------------------------------------------
        result["dummy"] = NodeProcessConfig(
            node_key="dummy",
            command=[
                "ros2",
                "run",
                "master_capstone",
                "dummy_task_node",
            ],
            action_name="/dummy_task_node/bringup",
            timeout_sec=5.0,
            restart_on_bringup=True,
        )

        # --------------------------------------------------------
        # Stage 0 / INIT0 required nodes
        # 실제 패키지명/실행파일명 준비되면 TODO만 교체.
        # --------------------------------------------------------

        result["nav2_docking"] = NodeProcessConfig(
            node_key="nav2_docking",
            command=[
                "ros2",
                "launch",
                "TODO_nav2_docking_bringup",
                "TODO_nav2_docking.launch.py",
            ],
            action_name="/nav2_docking/bringup",
            timeout_sec=20.0,
            restart_on_bringup=False,
        )

        result["nav2_cart"] = NodeProcessConfig(
            node_key="nav2_cart",
            command=[
                "ros2",
                "launch",
                "TODO_nav2_cart_bringup",
                "TODO_nav2_cart.launch.py",
            ],
            action_name="/nav2_cart/bringup",
            timeout_sec=20.0,
            restart_on_bringup=False,
        )

        result["detection_cart"] = NodeProcessConfig(
            node_key="detection_cart",
            command=[
                "ros2",
                "run",
                "TODO_detection_cart_pkg",
                "TODO_detection_cart_node",
            ],
            action_name="/detection_cart/bringup",
            timeout_sec=10.0,
            restart_on_bringup=False,
        )

        # --------------------------------------------------------
        # Stage 1 이후는 실제 노드 준비되면 추가
        # --------------------------------------------------------
        #
        # result["tsp"] = NodeProcessConfig(
        #     node_key="tsp",
        #     command=[
        #         "ros2",
        #         "run",
        #         "TODO_tsp_pkg",
        #         "TODO_tsp_node",
        #     ],
        #     action_name="/tsp/bringup",
        #     timeout_sec=10.0,
        #     restart_on_bringup=False,
        # )

        return result

    # ============================================================
    # Publisher / subscriber setup
    # ============================================================

    def _create_start_publishers(self):
        unique_topics = set()

        for topics in self.topic_command_map.values():
            for topic in topics:
                unique_topics.add(topic)

        for topic in sorted(unique_topics):
            self.start_publishers[topic] = self.create_publisher(
                Bool,
                self._to_ros_topic(topic),
                self.qos_start,
            )

        self.get_logger().info("Start publishers created.")

    def _create_finish_subscribers(self):
        for finish_key, ros_topic in sorted(self.finish_topic_map.items()):
            self.finish_subscribers[finish_key] = self.create_subscription(
                Bool,
                ros_topic,
                lambda msg, key=finish_key: self.finish_callback(key, msg),
                self.qos_finish,
            )

        self.get_logger().info("Finish topic subscribers created.")

    @staticmethod
    def _to_ros_topic(topic_name: str) -> str:
        if topic_name.startswith("/"):
            return topic_name
        return f"/{topic_name}"

    # ============================================================
    # UI
    # ============================================================

    def _dashboard_text(self) -> str:
        with self.lock:
            ready = ", ".join(sorted(self.ready_nodes)) if self.ready_nodes else "-"
            running = ", ".join(sorted(self.running_nodes)) if self.running_nodes else "-"
            failed = ", ".join(sorted(self.failed_nodes)) if self.failed_nodes else "-"
            finishes = ", ".join(sorted(self.finish_received)) if self.finish_received else "-"

            return (
                "\n"
                "============================================================\n"
                f" STAGE   : {self.current_stage}\n"
                f" STATUS  : {self.current_status}\n"
                f" READY   : {ready}\n"
                f" RUNNING : {running}\n"
                f" FAILED  : {failed}\n"
                f" FINISH  : {finishes}\n"
                f" EVENT   : {self.last_event}\n"
                "============================================================"
            )

    def _print_dashboard(self):
        print(self._dashboard_text(), flush=True)

    def _print_prompt(self):
        print("master> ", end="", flush=True)

    def _set_state(
        self,
        stage: Optional[str] = None,
        status: Optional[str] = None,
        event: Optional[str] = None,
    ):
        with self.lock:
            if stage is not None:
                self.current_stage = stage
            if status is not None:
                self.current_status = status
            if event is not None:
                self.last_event = event

    def _mark_running(self, node_key: str):
        with self.lock:
            self.running_nodes.add(node_key)
            self.ready_nodes.discard(node_key)
            self.failed_nodes.discard(node_key)

    def _mark_ready(self, node_key: str):
        with self.lock:
            self.running_nodes.discard(node_key)
            self.ready_nodes.add(node_key)
            self.failed_nodes.discard(node_key)

    def _mark_failed(self, node_key: str):
        with self.lock:
            self.running_nodes.discard(node_key)
            self.ready_nodes.discard(node_key)
            self.failed_nodes.add(node_key)

    def _clear_finish_received(self):
        with self.lock:
            self.finish_received.clear()

    # ============================================================
    # Input loop
    # ============================================================

    def _input_loop(self):
        while rclpy.ok():
            try:
                line = input("master> ").strip()
            except EOFError:
                break
            except KeyboardInterrupt:
                break

            if not line:
                continue

            tokens = line.split()
            command = tokens[0].lower()
            should_print_dashboard = True

            if command in ["exit", "quit", "q"]:
                self.shutdown_all_processes()
                rclpy.shutdown()
                break

            elif command in ["help", "h", "?"]:
                self.print_help()
                should_print_dashboard = False

            elif command in ["list", "ls"]:
                self.print_topic_commands()
                should_print_dashboard = False

            elif command == "nodes":
                self.print_nodes()
                should_print_dashboard = False

            elif command == "status":
                self._set_state(event="status requested")

            elif command == "clear":
                os.system("clear")
                should_print_dashboard = False

            elif command in ["init0", "stage0", "start0"]:
                self.start_init0_sequence()

            elif command == "bringup":
                if len(tokens) != 2:
                    print("Usage: bringup <node_key>")
                    should_print_dashboard = False
                else:
                    self.start_bringup_thread(tokens[1].lower())

            elif command == "kill":
                if len(tokens) != 2:
                    print("Usage: kill <node_key>")
                    should_print_dashboard = False
                else:
                    self.kill_node(tokens[1].lower())

            elif command == "kill_all":
                self.shutdown_all_processes()

            elif command == "pub":
                if len(tokens) != 2:
                    print("Usage: pub <start_command>")
                    should_print_dashboard = False
                else:
                    self.publish_start_command(tokens[1].lower())

            else:
                self.publish_start_command(command)

            if should_print_dashboard:
                self._print_dashboard()

    # ============================================================
    # Stage 0 automatic sequence
    # ============================================================

    def start_init0_sequence(self):
        required_nodes = self.stage_required_nodes["0"]

        missing_configs = [
            node_key
            for node_key in required_nodes
            if node_key not in self.node_process_map
        ]

        if missing_configs:
            self._set_state(
                stage="0",
                status="INIT0_FAILED",
                event=f"missing node configs: {missing_configs}",
            )
            print(f"[ERROR] Missing node configs: {missing_configs}")
            return

        self.active_init_stage = "0"
        self._clear_finish_received()

        with self.lock:
            for node_key in required_nodes:
                self.ready_nodes.discard(node_key)
                self.failed_nodes.discard(node_key)
                self.running_nodes.discard(node_key)

        self._set_state(
            stage="0",
            status="INIT0",
            event="INIT0 started: bringup nav2_docking, nav2_cart, detection_cart",
        )

        print("[INIT0] Bringup required nodes by action.")
        print("[INIT0] Required nodes: nav2_docking, nav2_cart, detection_cart")

        for node_key in required_nodes:
            self.start_bringup_thread(node_key)

    def _check_init0_complete(self):
        if self.active_init_stage != "0":
            return

        required_nodes = set(self.stage_required_nodes["0"])

        with self.lock:
            ready = set(self.ready_nodes)
            failed = set(self.failed_nodes)

        if failed.intersection(required_nodes):
            failed_required = sorted(failed.intersection(required_nodes))
            self.active_init_stage = None
            self._set_state(
                stage="0",
                status="INIT0_FAILED",
                event=f"INIT0 failed nodes: {failed_required}",
            )
            print(f"\n[INIT0 FAILED] failed nodes: {failed_required}")
            self._print_dashboard()
            self._print_prompt()
            return

        if required_nodes.issubset(ready):
            self.active_init_stage = None
            self._enter_cart_nav()

    def _enter_cart_nav(self):
        self._clear_finish_received()

        self._set_state(
            stage="0",
            status="CART_NAV",
            event="INIT0 complete. Enter CART_NAV.",
        )

        print("\n[STATUS MOVE] INIT0 -> CART_NAV")
        self._publish_start_topics(["nav2_cart_start"], value=True)

        self._print_dashboard()
        self._print_prompt()

    def _enter_cart_docking(self):
        self._clear_finish_received()

        self._set_state(
            stage="0",
            status="CART_DOCKING",
            event="nav2_cart_finish received. Enter CART_DOCKING.",
        )

        print("\n[STATUS MOVE] CART_NAV -> CART_DOCKING")
        self._publish_start_topics(["nav2_docking_start"], value=True)

        self._print_dashboard()
        self._print_prompt()

    def _finish_stage0_enter_init1(self):
        self._clear_finish_received()

        self._set_state(
            stage="1",
            status="INIT1",
            event="nav2_docking_finish received. Stage 0 complete. Enter INIT1.",
        )

        print("\n[STAGE COMPLETE] STAGE 0 -> INIT1")
        self._print_dashboard()
        self._print_prompt()

    # ============================================================
    # Start PUB
    # ============================================================

    def publish_start_command(self, command: str):
        if command not in self.topic_command_map:
            self._set_state(event=f"unknown command: {command}")
            print(f"[WARN] Unknown command: {command}. Type 'help' or 'list'.")
            return

        topics = self.topic_command_map[command]

        if not topics:
            self._set_state(event=f"{command} has no start topic yet")
            print(f"[WARN] Command '{command}' has no start topic yet. TODO 상태.")
            return

        self._set_state(event=f"manual start pub command: {command}")
        self._publish_start_topics(topics, value=True)

    def _publish_start_topics(self, topics: List[str], value: bool = True):
        msg = Bool()
        msg.data = value

        for topic in topics:
            pub = self.start_publishers.get(topic)

            if pub is None:
                print(f"[ERROR] No publisher for /{topic}")
                continue

            pub.publish(msg)
            print(f"[PUB] /{topic} std_msgs/Bool data: {str(value).lower()}")

    # ============================================================
    # Finish topic callback
    # ============================================================

    def finish_callback(self, finish_key: str, msg: Bool):
        if not msg.data:
            return

        with self.lock:
            self.finish_received.add(finish_key)
            current_stage = self.current_stage
            current_status = self.current_status

        self._set_state(event=f"finish received: {finish_key}")

        print(f"\n[FINISH] /{finish_key} true received")

        if current_stage == "0" and current_status == "CART_NAV":
            if finish_key == "nav2_cart_finish":
                # TRANSIENT_LOCAL start topic의 stale true 방지용 clear
                self._publish_start_topics(["nav2_cart_start"], value=False)
                self._enter_cart_docking()
                return

        if current_stage == "0" and current_status == "CART_DOCKING":
            if finish_key == "nav2_docking_finish":
                # TRANSIENT_LOCAL start topic의 stale true 방지용 clear
                self._publish_start_topics(["nav2_docking_start"], value=False)
                self._finish_stage0_enter_init1()
                return

        self._print_dashboard()
        self._print_prompt()

    # ============================================================
    # Bringup
    # ============================================================

    def start_bringup_thread(self, node_key: str):
        if node_key not in self.node_process_map:
            self._set_state(event=f"unknown node_key: {node_key}")
            print(f"[WARN] Unknown node_key: {node_key}. Type 'nodes'.")
            return

        old_thread = self.bringup_threads.get(node_key)
        if old_thread is not None and old_thread.is_alive():
            self._set_state(event=f"bringup already running: {node_key}")
            print(f"[WARN] bringup already running for {node_key}")
            return

        self._mark_running(node_key)

        if self.active_init_stage == "0" and node_key in self.stage_required_nodes["0"]:
            self._set_state(
                stage="0",
                status="INIT0",
                event=f"INIT0 bringup requested: {node_key}",
            )
        else:
            self._set_state(
                stage="BRINGUP",
                status=f"BRINGUP_{node_key.upper()}",
                event=f"bringup requested: {node_key}",
            )

        worker = threading.Thread(
            target=self._bringup_worker,
            args=(node_key,),
            daemon=True,
        )
        self.bringup_threads[node_key] = worker
        worker.start()

        print(f"[BRINGUP] {node_key} requested. Running in background.")

    def _bringup_worker(self, node_key: str):
        config = self.node_process_map[node_key]

        try:
            if config.restart_on_bringup:
                self._cleanup_before_restart(node_key, config)
            else:
                if node_key in self.processes and self.processes[node_key].poll() is None:
                    self._set_state(event=f"{node_key} terminal already running")
                    self._send_bringup_goal(config)
                    return

            self._set_state(event=f"opening terminal for {node_key}")

            terminal_command = self.make_terminal_command(config.command)

            if terminal_command is None:
                self._mark_failed(node_key)
                self._set_state(event="no supported terminal found")
                print(
                    "\n[ERROR] No supported terminal found. "
                    "Install gnome-terminal, xterm, or konsole."
                )
                self._check_init0_complete()
                self._print_dashboard()
                self._print_prompt()
                return

            process = subprocess.Popen(terminal_command)
            self.processes[node_key] = process

            time.sleep(0.8)

            self._send_bringup_goal(config)

        except Exception as e:
            self._mark_failed(node_key)
            self._set_state(event=f"bringup exception: {e}")
            print(f"\n[ERROR] bringup exception for {node_key}: {e}")
            self._check_init0_complete()
            self._print_dashboard()
            self._print_prompt()

    def _cleanup_before_restart(self, node_key: str, config: NodeProcessConfig):
        self._set_state(event=f"cleanup before restart: {node_key}")

        old_process = self.processes.get(node_key)

        if old_process is not None and old_process.poll() is None:
            try:
                old_process.terminate()
                old_process.wait(timeout=1.0)
            except Exception:
                try:
                    old_process.kill()
                except Exception:
                    pass

        self.processes.pop(node_key, None)
        self.action_clients.pop(config.action_name, None)

        if node_key == "dummy":
            subprocess.run(
                ["pkill", "-f", "ros2 run master_capstone dummy_task_node"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                ["pkill", "-f", "master_capstone.*dummy_task_node"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        self._mark_running(node_key)

        time.sleep(0.5)

    def make_terminal_command(self, ros_command: List[str]) -> Optional[List[str]]:
        workspace_setup = os.path.expanduser("~/colcon_ws/install/setup.bash")
        command_str = " ".join(ros_command)

        command_inside_terminal = (
            f"source {workspace_setup}; "
            f"{command_str}; "
            f"exec bash"
        )

        if shutil.which("gnome-terminal"):
            return [
                "gnome-terminal",
                "--wait",
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
                f"bash -lc '{command_inside_terminal}'",
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

    def _send_bringup_goal(self, config: NodeProcessConfig):
        client = ActionClient(
            self,
            BringupNode,
            config.action_name,
        )
        self.action_clients[config.action_name] = client

        self._set_state(event=f"waiting action server: {config.action_name}")

        if not client.wait_for_server(timeout_sec=config.timeout_sec):
            self._mark_failed(config.node_key)
            self._set_state(event=f"action server timeout: {config.action_name}")
            print(f"\n[ERROR] Action server not available: {config.action_name}")
            self._check_init0_complete()
            self._print_dashboard()
            self._print_prompt()
            return

        goal_msg = BringupNode.Goal()
        goal_msg.node_key = config.node_key
        goal_msg.command = "start"

        self._set_state(event=f"sending bringup goal: {config.node_key}")

        future = client.send_goal_async(
            goal_msg,
            feedback_callback=self.bringup_feedback_callback,
        )
        future.add_done_callback(self.bringup_goal_response_callback)

    def bringup_goal_response_callback(self, future):
        goal_handle = future.result()

        if not goal_handle.accepted:
            self._set_state(event="bringup goal rejected")
            print("\n[ERROR] Bringup goal rejected.")
            self._print_dashboard()
            self._print_prompt()
            return

        self._set_state(event="bringup goal accepted")

        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(self.bringup_result_callback)

    def bringup_feedback_callback(self, feedback_msg):
        feedback = feedback_msg.feedback
        self._set_state(event=f"feedback: {feedback.phase} / {feedback.detail}")

    def bringup_result_callback(self, future):
        result = future.result().result

        if result.success:
            self._mark_ready(result.node_key)

            if self.active_init_stage == "0" and result.node_key in self.stage_required_nodes["0"]:
                self._set_state(
                    stage="0",
                    status="INIT0",
                    event=f"{result.node_key} ready",
                )
                print(
                    f"\n[INIT0 READY] {result.node_key} ready. "
                    f"message={result.message}"
                )
                self._check_init0_complete()
                return

            self._set_state(
                stage="BRINGUP",
                status=f"{result.node_key.upper()}_READY",
                event=result.message,
            )
            print(
                f"\n[BRINGUP READY] {result.node_key} ready. "
                f"message={result.message}"
            )

        else:
            self._mark_failed(result.node_key)

            if self.active_init_stage == "0" and result.node_key in self.stage_required_nodes["0"]:
                self._set_state(
                    stage="0",
                    status="INIT0_FAILED",
                    event=f"{result.node_key} failed: {result.message}",
                )
                print(
                    f"\n[INIT0 FAILED] {result.node_key} failed. "
                    f"message={result.message}"
                )
                self._check_init0_complete()
                return

            self._set_state(
                stage="BRINGUP",
                status=f"{result.node_key.upper()}_FAILED",
                event=result.message,
            )
            print(
                f"\n[BRINGUP FAILED] {result.node_key} failed. "
                f"message={result.message}"
            )

        self._print_dashboard()
        self._print_prompt()

    # ============================================================
    # Kill process
    # ============================================================

    def kill_node(self, node_key: str):
        process = self.processes.get(node_key)

        if process is not None and process.poll() is None:
            process.terminate()

        self.processes.pop(node_key, None)

        if node_key == "dummy":
            subprocess.run(
                ["pkill", "-f", "ros2 run master_capstone dummy_task_node"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                ["pkill", "-f", "master_capstone.*dummy_task_node"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

        with self.lock:
            self.running_nodes.discard(node_key)
            self.ready_nodes.discard(node_key)
            self.failed_nodes.discard(node_key)

        self._set_state(event=f"terminated process: {node_key}")
        print(f"[INFO] Terminated process for {node_key}")

    def shutdown_all_processes(self):
        for node_key in list(self.processes.keys()):
            self.kill_node(node_key)

        with self.lock:
            self.running_nodes.clear()
            self.ready_nodes.clear()

        self._set_state(event="all processes terminated")

    # ============================================================
    # Print helpers
    # ============================================================

    def print_help(self):
        print(
            """
============================================================
Manual Master Commands
============================================================

[Stage 0 자동 전환 테스트]
  init0
  stage0
  start0

동작:
  INIT0 required nodes action bringup
    - nav2_docking
    - nav2_cart
    - detection_cart

  모든 노드 READY 확인
  INIT0 -> CART_NAV
  /nav2_cart_start true 발행

  /nav2_cart_finish true 수신 시
  CART_NAV -> CART_DOCKING
  /nav2_docking_start true 발행

  /nav2_docking_finish true 수신 시
  STAGE 0 -> INIT1

[더미 노드 실행 + action ready 확인]
  bringup dummy

[개별 노드 action bringup]
  bringup nav2_cart
  bringup nav2_docking
  bringup detection_cart

[Start PUB 수동 발행]
  <start_command>
  pub <start_command>

예:
  nav2_cart_start
  cart_nav
  nav2_docking_start
  cart_docking

[Finish topic 테스트용 외부 명령 예시]
  ros2 topic pub /nav2_cart_finish std_msgs/msg/Bool "{data: true}" -1
  ros2 topic pub /nav2_docking_finish std_msgs/msg/Bool "{data: true}" -1

[상태 확인]
  status
  clear

[목록]
  nodes
  list

[종료]
  kill <node_key>
  kill_all
  exit

============================================================
"""
        )

    def print_topic_commands(self):
        print("\n[Start PUB commands]")
        for command in sorted(self.topic_command_map.keys()):
            topics = self.topic_command_map[command]
            if topics:
                topic_str = ", ".join([f"/{topic}" for topic in topics])
            else:
                topic_str = "(TODO: no start topic yet)"
            print(f"  {command:<25} -> {topic_str}")

        print("\n[Finish topics subscribed]")
        for key, topic in sorted(self.finish_topic_map.items()):
            print(f"  {key:<25} -> {topic}")

    def print_nodes(self):
        print("\n[Node process configs]")
        for key, config in sorted(self.node_process_map.items()):
            print(
                f"  {key:<20} "
                f"action={config.action_name:<30} "
                f"cmd={' '.join(config.command)} "
                f"restart={config.restart_on_bringup}"
            )

        print("\n[Stage required nodes]")
        for stage, nodes in sorted(self.stage_required_nodes.items()):
            node_str = ", ".join(nodes) if nodes else "-"
            print(f"  INIT{stage:<3} -> {node_str}")


def main(args=None):
    rclpy.init(args=args)

    node = ManualMaster()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.shutdown_all_processes()
    finally:
        if rclpy.ok():
            node.destroy_node()
            rclpy.shutdown()


if __name__ == "__main__":
    main()