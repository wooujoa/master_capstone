#!/usr/bin/env python3

import os
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import rclpy
from rclpy.action import ActionClient
from rclpy.node import Node
from std_msgs.msg import Bool

from master_capstone_interfaces.action import BringupNode


@dataclass
class NodeProcessConfig:
    node_key: str
    command: List[str]
    action_name: str
    timeout_sec: float = 5.0
    restart_on_bringup: bool = True


class ManualMaster(Node):
    def __init__(self):
        super().__init__("manual_master")

        self.lock = threading.Lock()

        self.current_stage = "DEBUG"
        self.current_status = "IDLE"
        self.last_event = "manual_master started"

        self.ready_nodes = set()
        self.running_nodes = set()
        self.failed_nodes = set()

        self.start_publishers: Dict[str, object] = {}
        self.action_clients: Dict[str, ActionClient] = {}
        self.processes: Dict[str, subprocess.Popen] = {}
        self.bringup_threads: Dict[str, threading.Thread] = {}

        self.topic_command_map = self._build_topic_command_map()
        self.topic_status_map = self._build_topic_status_map()
        self.node_process_map = self._build_node_process_map()

        self._create_start_publishers()

        self.get_logger().info("manual_master started. Type 'help' to see commands.")

        self.input_thread = threading.Thread(target=self._input_loop, daemon=True)
        self.input_thread.start()

    def _build_topic_command_map(self) -> Dict[str, List[str]]:
        return {
            "nav2_start": ["nav2_start"],
            "detection_cart_start": ["detection_cart_start"],
            "nav2_docking_start": ["nav2_docking_start"],

            "tsp_start": ["tsp_start"],
            "nav2_object_start": ["nav2_object_start"],

            "nav2_shelf_start": ["nav2_shelf_start"],
            "cali_zed_start": ["cali_zed_start"],
            "sam3_start": ["sam3_start"],
            "anygrasp_start": ["anygrasp_start"],
            "cali_d405_start": ["cali_d405_start"],
            "gripper_start": ["gripper_start"],

            "nav2_side_start": ["nav2_side_start"],
            "nav2_return_start": ["nav2_return_start"],

            "cart_nav": ["nav2_start"],
            "cart_docking": ["detection_cart_start", "nav2_docking_start"],

            "tsp": ["tsp_start"],
            "nav2_object": ["nav2_object_start"],

            "nav2_shelf": ["nav2_shelf_start", "cali_zed_start"],

            "grasp_detection": [
                "sam3_start",
                "anygrasp_start",
                "cali_d405_start",
                "gripper_start",
            ],

            "nav2_side": [
                "detection_cart_start",
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

    def _build_topic_status_map(self) -> Dict[str, Dict[str, str]]:
        return {
            "cart_nav": {"stage": "0", "status": "CART_NAV"},
            "nav2_start": {"stage": "0", "status": "CART_NAV"},

            "cart_docking": {"stage": "0", "status": "CART_DOCKING"},
            "detection_cart_start": {"stage": "0", "status": "CART_DOCKING"},
            "nav2_docking_start": {"stage": "0", "status": "CART_DOCKING"},

            "tsp": {"stage": "1", "status": "TSP"},
            "tsp_start": {"stage": "1", "status": "TSP"},

            "nav2_object": {"stage": "1", "status": "NAV2_OBJECT"},
            "nav2_object_start": {"stage": "1", "status": "NAV2_OBJECT"},

            "nav2_shelf": {"stage": "2", "status": "NAV2_SHELF"},
            "nav2_shelf_start": {"stage": "2", "status": "NAV2_SHELF"},
            "cali_zed_start": {"stage": "2", "status": "NAV2_SHELF"},

            "grasp_detection": {"stage": "2", "status": "GRASP_DETECTION"},
            "sam3_start": {"stage": "2", "status": "GRASP_DETECTION"},
            "anygrasp_start": {"stage": "2", "status": "GRASP_DETECTION"},
            "cali_d405_start": {"stage": "2", "status": "GRASP_DETECTION"},
            "gripper_start": {"stage": "2", "status": "GRASP_DETECTION"},

            "nav2_side": {"stage": "3", "status": "NAV2_SIDE"},
            "nav2_side_start": {"stage": "3", "status": "NAV2_SIDE"},

            "arm_placing": {"stage": "3", "status": "ARM_PLACING"},

            "nav2_return": {"stage": "3", "status": "NAV2_RETURN"},
            "nav2_return_start": {"stage": "3", "status": "NAV2_RETURN"},

            "return_home": {"stage": "3", "status": "RETURN_HOME"},
        }

    def _build_node_process_map(self) -> Dict[str, NodeProcessConfig]:
        result: Dict[str, NodeProcessConfig] = {}

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

        # TODO:
        # 실제 노드가 생기면 아래처럼 추가.
        #
        # result["nav2_docking"] = NodeProcessConfig(
        #     node_key="nav2_docking",
        #     command=[
        #         "ros2",
        #         "launch",
        #         "nav2_docking_bringup",
        #         "bringup.launch.py",
        #     ],
        #     action_name="/nav2_docking/bringup",
        #     timeout_sec=20.0,
        #     restart_on_bringup=False,
        # )

        return result

    def _create_start_publishers(self):
        unique_topics = set()

        for topics in self.topic_command_map.values():
            for topic in topics:
                unique_topics.add(topic)

        for topic in sorted(unique_topics):
            self.start_publishers[topic] = self.create_publisher(
                Bool,
                self._to_ros_topic(topic),
                10,
            )

    @staticmethod
    def _to_ros_topic(topic_name: str) -> str:
        if topic_name.startswith("/"):
            return topic_name
        return f"/{topic_name}"

    def _dashboard_text(self) -> str:
        with self.lock:
            ready = ", ".join(sorted(self.ready_nodes)) if self.ready_nodes else "-"
            running = ", ".join(sorted(self.running_nodes)) if self.running_nodes else "-"
            failed = ", ".join(sorted(self.failed_nodes)) if self.failed_nodes else "-"

            return (
                "\n"
                "============================================================\n"
                f" STAGE  : {self.current_stage}\n"
                f" STATUS : {self.current_status}\n"
                f" READY  : {ready}\n"
                f" RUNNING: {running}\n"
                f" FAILED : {failed}\n"
                f" EVENT  : {self.last_event}\n"
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

            elif command == "set_stage":
                if len(tokens) != 2:
                    print("Usage: set_stage <0|1|2|3|DEBUG>")
                    should_print_dashboard = False
                else:
                    self._set_state(
                        stage=tokens[1],
                        event=f"manual stage set to {tokens[1]}",
                    )

            elif command == "set_status":
                if len(tokens) < 2:
                    print("Usage: set_status <STATUS_NAME>")
                    should_print_dashboard = False
                else:
                    new_status = " ".join(tokens[1:]).upper()
                    self._set_state(
                        status=new_status,
                        event=f"manual status set to {new_status}",
                    )

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

    def publish_start_command(self, command: str):
        if command not in self.topic_command_map:
            self._set_state(event=f"unknown command: {command}")
            print(f"[WARN] Unknown command: {command}. Type 'help' or 'list'.")
            return

        status_info = self.topic_status_map.get(command)
        if status_info is not None:
            self._set_state(
                stage=status_info["stage"],
                status=status_info["status"],
                event=f"manual start pub command: {command}",
            )
        else:
            self._set_state(event=f"manual start pub command: {command}")

        topics = self.topic_command_map[command]

        if not topics:
            self._set_state(event=f"{command} has no start topic yet")
            print(f"[WARN] Command '{command}' has no start topic yet. TODO 상태.")
            return

        msg = Bool()
        msg.data = True

        for topic in topics:
            pub = self.start_publishers.get(topic)

            if pub is None:
                print(f"[ERROR] No publisher for /{topic}")
                continue

            pub.publish(msg)
            print(f"[PUB] /{topic} std_msgs/Bool data: true")

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

        with self.lock:
            self.ready_nodes.discard(node_key)
            self.failed_nodes.discard(node_key)
            self.running_nodes.add(node_key)

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
                self._cleanup_dummy_before_restart(node_key, config)
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
                self._print_dashboard()
                self._print_prompt()
                return

            process = subprocess.Popen(terminal_command)
            self.processes[node_key] = process

            # ROS graph에 이전 서버 제거/새 서버 등록 반영 시간을 조금 줌.
            time.sleep(0.8)

            self._send_bringup_goal(config)

        except Exception as e:
            self._mark_failed(node_key)
            self._set_state(event=f"bringup exception: {e}")
            print(f"\n[ERROR] bringup exception for {node_key}: {e}")
            self._print_dashboard()
            self._print_prompt()

    def _cleanup_dummy_before_restart(self, node_key: str, config: NodeProcessConfig):
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

        # 중요:
        # gnome-terminal을 죽여도 내부 ros2 run이 남아있는 경우가 있어서
        # dummy_task_node 프로세스를 명시적으로 정리한다.
        # 디버깅용 dummy에만 적용하는 강제 정리다.
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
            self.ready_nodes.discard(node_key)
            self.failed_nodes.discard(node_key)
            self.running_nodes.add(node_key)

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

    def print_help(self):
        print(
            """
============================================================
Manual Master Commands
============================================================

[더미 노드 실행 + action ready 확인]
  bringup dummy

[Start PUB 수동 발행]
  <start_command>
  pub <start_command>

예:
  nav2_docking_start
  cart_docking
  nav2_shelf
  grasp_detection
  nav2_return

[상태 확인/수동 변경]
  status
  set_stage <0|1|2|3|DEBUG>
  set_status <STATUS_NAME>
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

    def print_nodes(self):
        print("\n[Node process configs]")
        for key, config in sorted(self.node_process_map.items()):
            print(
                f"  {key:<20} "
                f"action={config.action_name:<25} "
                f"cmd={' '.join(config.command)} "
                f"restart={config.restart_on_bringup}"
            )


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