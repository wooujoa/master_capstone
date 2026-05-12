#!/usr/bin/env python3

import os
import shlex
import shutil
import subprocess
import threading
import time
from enum import Enum
from typing import Dict, Optional, Set

import yaml

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy
from ament_index_python.packages import get_package_share_directory
from std_msgs.msg import Bool, String, Int32


class Stage2Status(str, Enum):
    INIT2 = "INIT2"
    OBJECT_ALIGN = "OBJECT_ALIGN"
    DETECTION_GRASPING = "DETECTION_GRASPING"
    GRIPPER_ACTIVATE = "GRIPPER_ACTIVATE"
    INIT3 = "INIT3"
    FAILED = "FAILED"


class Stage2Master(Node):
    def __init__(self):
        super().__init__("stage2_master")
        self.lock = threading.Lock()

        self.stage = "2"
        self.status = Stage2Status.INIT2.value
        self.last_event = "stage2_master started"
        self.stage2_started = False

        self.current_item: Optional[dict] = None
        self.current_item_input = ""
        self.finish_received: Set[str] = set()
        self.processes: Dict[str, subprocess.Popen] = {}
        self.launch_requested_nodes: Set[str] = set()

        self.declare_parameter("config_package", "master_capstone")
        self.declare_parameter("item_database_file", "item_database.yaml")
        self.declare_parameter("item_database_path", "")

        # 실제 gripper 노드가 없으면 실행 시 -p gripper_command:="" 로 비워두면 됨.
        self.declare_parameter("gripper_command", "ros2 run gripper_control gripper_node")
        self.declare_parameter("use_new_terminal", True)

        # TRANSIENT_LOCAL start topic을 false로 clear한 뒤, 다음 true가 바로 붙어서 씹히는 것을 막기 위한 대기.
        self.declare_parameter("phase_clear_wait_sec", 0.5)
        self.declare_parameter("prompt_start_wait_sec", 0.3)
        self.declare_parameter("cmd_topic", "/master2/cmd")
        self.declare_parameter("item_cmd_topic", "/master2/item")
        self.declare_parameter("status_topic", "/master2/status")
        self.phase_clear_wait_sec = float(self.get_parameter("phase_clear_wait_sec").value)
        self.prompt_start_wait_sec = float(self.get_parameter("prompt_start_wait_sec").value)

        self.qos_cmd = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
        )

        self.item_db = self._load_item_database()
        self.node_commands = self._build_node_commands()
        self.start_topics = self._build_start_topics()
        self.finish_topics = self._build_finish_topics()

        self.start_publishers: Dict[str, object] = {}
        self.finish_subscribers: Dict[str, object] = {}
        self._create_start_publishers()
        self._create_finish_subscribers()

        self.pub_target_item_name = self.create_publisher(String, "/target_item_name", self.qos_cmd)
        self.pub_target_aruco_id = self.create_publisher(Int32, "/target_aruco_id", self.qos_cmd)
        self.pub_target_text_prompt = self.create_publisher(String, "/target_text_prompt", self.qos_cmd)
        self.pub_sam3_text_prompt = self.create_publisher(String, "/sam3_text_prompt", self.qos_cmd)
        self.pub_target_shelf_layer = self.create_publisher(Int32, "/target_shelf_layer", self.qos_cmd)

        # Topic control for testing without keyboard input.
        self.cmd_topic = self.get_parameter("cmd_topic").value
        self.item_cmd_topic = self.get_parameter("item_cmd_topic").value
        self.status_topic = self.get_parameter("status_topic").value
        self.pub_status = self.create_publisher(String, self.status_topic, self.qos_cmd)
        self.sub_cmd = self.create_subscription(String, self.cmd_topic, self._cmd_topic_callback, self.qos_cmd)
        self.sub_item_cmd = self.create_subscription(String, self.item_cmd_topic, self._item_topic_callback, self.qos_cmd)

        self.get_logger().info("stage2_master started. Type 'help' to see commands.")
        self.get_logger().info(f"Topic command enabled: {self.cmd_topic} std_msgs/String")
        self.get_logger().info(f"Topic item command enabled: {self.item_cmd_topic} std_msgs/String")
        self.get_logger().info(f"Status publisher enabled: {self.status_topic} std_msgs/String")
        self._print_dashboard()
        self.input_thread = threading.Thread(target=self._input_loop, daemon=True)
        self.input_thread.start()

    # ============================================================
    # DB
    # ============================================================
    def _resolve_database_path(self) -> str:
        explicit_path = str(self.get_parameter("item_database_path").value).strip()
        if explicit_path:
            return os.path.expanduser(explicit_path)
        pkg = self.get_parameter("config_package").value
        fname = self.get_parameter("item_database_file").value
        return os.path.join(get_package_share_directory(pkg), "config", fname)

    def _load_item_database(self) -> Dict[str, dict]:
        path = self._resolve_database_path()
        if not os.path.exists(path):
            self.get_logger().error(f"item_database.yaml not found: {path}")
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            items = data.get("items", {}) if isinstance(data, dict) else {}
            self.get_logger().info(f"Loaded item database: {path}")
            self.get_logger().info(f"Number of items: {len(items)}")
            return items
        except Exception as e:
            self.get_logger().error(f"Failed to load item database: {repr(e)}")
            return {}

    def _find_item_by_name(self, query: str) -> Optional[dict]:
        q = query.strip().lower()
        if not q:
            return None
        for item_key, info in self.item_db.items():
            candidates = [str(item_key), str(info.get("item_id", "")), str(info.get("product_name", ""))]
            candidates += [str(a) for a in info.get("aliases", [])]
            if q in [c.strip().lower() for c in candidates if c.strip()]:
                out = dict(info)
                out["item_key"] = item_key
                return out
        return None

    # ============================================================
    # Maps
    # ============================================================
    def _build_node_commands(self) -> Dict[str, list]:
        gripper_command = str(self.get_parameter("gripper_command").value).strip()
        return {"gripper": shlex.split(gripper_command)} if gripper_command else {}

    def _build_start_topics(self) -> Dict[str, str]:
        return {
            "aruco_zed_start": "/aruco_zed_start",
            "sam3_start": "/sam3_start",
            "anygrasp_start": "/anygrasp_start",
            "cali_d405_start": "/cali_d405_start",
            "arm_picking_start": "/arm_picking_start",
            "gripper_start": "/gripper_start",
        }

    def _build_finish_topics(self) -> Dict[str, str]:
        # master_2의 상태 전이 기준은 arm_picking_finish 하나.
        return {"arm_picking_finish": "/arm_picking_finish"}

    def _create_start_publishers(self):
        for key, topic in self.start_topics.items():
            self.start_publishers[key] = self.create_publisher(Bool, topic, self.qos_cmd)
            self.get_logger().info(f"Start publisher created: {topic}")

    def _create_finish_subscribers(self):
        for key, topic in self.finish_topics.items():
            self.finish_subscribers[key] = self.create_subscription(
                Bool, topic, lambda msg, finish_key=key: self._finish_callback(finish_key, msg), self.qos_cmd
            )
            self.get_logger().info(f"Finish subscriber created: {topic}")

    # ============================================================
    # Dashboard
    # ============================================================
    def _set_state(self, stage: Optional[str] = None, status: Optional[str] = None, event: Optional[str] = None):
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
            launch_req = ", ".join(sorted(self.launch_requested_nodes)) if self.launch_requested_nodes else "-"
            if self.current_item is None:
                item_text = "-"
            else:
                item_text = (
                    f"{self.current_item.get('product_name', '-')}"
                    f" / item_key={self.current_item.get('item_key', '-')}"
                    f" / aruco_id={self.current_item.get('aruco_id', '-')}"
                    f" / prompt={self.current_item.get('text_prompt', '-')}"
                    f" / shelf_layer={self.current_item.get('shelf_layer', '-')}"
                )
            return (
                "\n============================================================\n"
                " STAGE MASTER : STAGE 2 MASTER ONLY\n"
                f" STAGE        : {self.stage}\n"
                f" STATUS       : {self.status}\n"
                " STATUS FLOW  : INIT2 -> OBJECT_ALIGN -> DETECTION_GRASPING -> GRIPPER_ACTIVATE -> INIT3\n"
                " PRE-LAUNCHED : SAM3, ANYGRASP, ARUCO_ZED, CALI_ZED, CALI_D405, ARM_PICKING\n"
                " MASTER LAUNCH: GRIPPER only\n"
                f" START2 DONE  : {self.stage2_started}\n"
                f" CURRENT ITEM : {item_text}\n"
                f" LAUNCH REQ   : {launch_req}\n"
                f" FINISH RX    : {finishes}\n"
                f" EVENT        : {self.last_event}\n"
                "============================================================"
            )

    def _print_dashboard(self):
        text = self._dashboard_text()
        print(text, flush=True)
        self._publish_status_text(text)

    def _publish_status_text(self, text: str):
        try:
            msg = String()
            msg.data = text
            self.pub_status.publish(msg)
        except Exception:
            pass

    def _print_prompt(self):
        print("master2> ", end="", flush=True)

    # ============================================================
    # Topic command callbacks
    # ============================================================
    def _cmd_topic_callback(self, msg: String):
        raw = msg.data.strip()
        if not raw:
            return
        self.get_logger().info(f"[CMD TOPIC RX] {raw}")
        self._handle_external_command(raw)
        self._print_dashboard()

    def _item_topic_callback(self, msg: String):
        item_name = msg.data.strip()
        if not item_name:
            return
        self.get_logger().info(f"[ITEM TOPIC RX] {item_name}")
        self._select_item_and_start_object_align(item_name)
        self._print_dashboard()

    def _handle_external_command(self, raw: str):
        cmd = raw.strip().lower()
        if cmd in ["status", "dashboard"]:
            self._set_state(event="status requested by topic")
        elif cmd in ["start2", "init2_start", "run2"]:
            self._start_stage2_from_init()
        elif cmd == "launch":
            self._launch_stage2_nodes()
            self.stage2_started = True
            self._set_state(event="topic gripper launch requested")
        elif cmd.startswith("item ") or cmd.startswith("buy "):
            item_name = raw.split(" ", 1)[1].strip()
            self._select_item_and_start_object_align(item_name)
        elif cmd in ["reset", "init2"]:
            self._reset_stage2()
        elif cmd == "kill_all":
            self._shutdown_all_processes()
        elif cmd.startswith("force_status "):
            # Debug-only: directly force the status string.
            # Example: force_status INIT2
            status = raw.split(" ", 1)[1].strip().upper()
            valid = [x.value for x in Stage2Status]
            if status in valid:
                self._set_state(stage="2" if status != Stage2Status.INIT3.value else "3", status=status, event=f"force_status by topic: {status}")
            else:
                self._set_state(event=f"invalid force_status by topic: {status}")
        else:
            with self.lock:
                current_status = self.status
            if current_status == Stage2Status.INIT2.value:
                self._select_item_and_start_object_align(raw)
            else:
                self._set_state(event=f"unknown topic command: {raw}")

    # ============================================================
    # Input
    # ============================================================
    def _input_loop(self):
        while rclpy.ok():
            try:
                raw = input("master2> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not raw:
                continue
            cmd = raw.lower()
            show = True

            if cmd in ["exit", "quit", "q"]:
                self._safe_clear_all_start_topics()
                self._shutdown_all_processes()
                rclpy.shutdown()
                break
            elif cmd in ["help", "h", "?"]:
                self._print_help()
                show = False
            elif cmd in ["status", "dashboard"]:
                self._set_state(event="status requested")
            elif cmd == "clear":
                os.system("clear")
                show = False
            elif cmd in ["start2", "init2_start", "run2"]:
                self._start_stage2_from_init()
            elif cmd == "launch":
                self._launch_stage2_nodes()
                self.stage2_started = True
                self._set_state(event="manual gripper launch requested")
            elif cmd.startswith("item ") or cmd.startswith("buy "):
                item_name = raw.split(" ", 1)[1].strip()
                self._select_item_and_start_object_align(item_name)
            elif cmd in ["reset", "init2"]:
                self._reset_stage2()
            elif cmd == "list":
                self._print_topics_and_nodes()
                show = False
            elif cmd == "items":
                self._print_items()
                show = False
            elif cmd == "kill_all":
                self._shutdown_all_processes()
            else:
                with self.lock:
                    current_status = self.status
                if current_status == Stage2Status.INIT2.value:
                    self._select_item_and_start_object_align(raw)
                else:
                    print(f"[WARN] Unknown command: {raw}. Type 'help'.")
                    self._set_state(event=f"unknown command: {raw}")
            if show:
                self._print_dashboard()

    # ============================================================
    # Stage flow
    # ============================================================
    def _start_stage2_from_init(self):
        with self.lock:
            stg, sts = self.stage, self.status
        if stg != "2" or sts != Stage2Status.INIT2.value:
            print(f"[WARN] start2 allowed only STAGE=2 STATUS=INIT2. current={stg}/{sts}")
            self._set_state(event=f"start2 rejected: {stg}/{sts}")
            return
        self._launch_stage2_nodes()
        self.stage2_started = True
        self._set_state(stage="2", status=Stage2Status.INIT2.value, event="start2 complete. gripper launch requested. waiting item command.")
        print("\n[STAGE2 READY] Gripper launch requested. Type 'item <상품명>'.")

    def _launch_stage2_nodes(self):
        if "gripper" not in self.node_commands:
            print("[WARN] gripper_command is empty. skip launch.")
            return
        self._launch_node("gripper")

    def _select_item_and_start_object_align(self, item_name: str):
        with self.lock:
            stg, sts = self.stage, self.status
        if stg != "2" or sts != Stage2Status.INIT2.value:
            print(f"[WARN] item command allowed only STAGE=2 STATUS=INIT2. current={stg}/{sts}")
            self._set_state(event=f"item rejected: {stg}/{sts}")
            return
        item = self._find_item_by_name(item_name)
        if item is None:
            print(f"[ERROR] Unknown item: {item_name}")
            print("[INFO] Type 'items' to see available items.")
            self._set_state(event=f"unknown item: {item_name}")
            return
        for key in ["aruco_id", "text_prompt"]:
            if key not in item:
                print(f"[ERROR] item '{item_name}' missing field: {key}")
                self._set_state(status=Stage2Status.FAILED.value, event=f"item DB missing {key}")
                return
        self.current_item = item
        self.current_item_input = item_name
        self._enter_object_align()

    def _enter_object_align(self):
        self._clear_finish_received()
        self._set_state(stage="2", status=Stage2Status.OBJECT_ALIGN.value, event="INIT2 -> OBJECT_ALIGN")
        print("\n[STATUS MOVE] INIT2 -> OBJECT_ALIGN")
        self._publish_target_info()
        self._publish_start("aruco_zed_start", True)
        self._publish_start("arm_picking_start", True)

    def _finish_object_align(self, finish_key: str):
        with self.lock:
            self.finish_received.add(finish_key)
        self._publish_start("aruco_zed_start", False)
        self._publish_start("arm_picking_start", False)
        print(f"\n[WAIT] clear wait {self.phase_clear_wait_sec:.2f}s before DETECTION_GRASPING")
        time.sleep(self.phase_clear_wait_sec)
        print("\n[STATUS MOVE] OBJECT_ALIGN -> DETECTION_GRASPING")
        self._enter_detection_grasping()

    def _enter_detection_grasping(self):
        self._clear_finish_received()
        self._set_state(stage="2", status=Stage2Status.DETECTION_GRASPING.value, event="OBJECT_ALIGN -> DETECTION_GRASPING")
        self._publish_target_info()
        self._publish_sam3_prompt()
        print(f"\n[WAIT] prompt wait {self.prompt_start_wait_sec:.2f}s before start topics")
        time.sleep(self.prompt_start_wait_sec)
        self._publish_start("sam3_start", True)
        self._publish_start("anygrasp_start", True)
        self._publish_start("cali_d405_start", True)
        self._publish_start("arm_picking_start", True)

    def _finish_detection_grasping(self, finish_key: str):
        with self.lock:
            self.finish_received.add(finish_key)
        self._publish_start("sam3_start", False)
        self._publish_start("anygrasp_start", False)
        self._publish_start("cali_d405_start", False)
        self._publish_start("arm_picking_start", False)
        print(f"\n[WAIT] clear wait {self.phase_clear_wait_sec:.2f}s before GRIPPER_ACTIVATE")
        time.sleep(self.phase_clear_wait_sec)
        print("\n[STATUS MOVE] DETECTION_GRASPING -> GRIPPER_ACTIVATE")
        self._enter_gripper_activate()

    def _enter_gripper_activate(self):
        self._clear_finish_received()
        self._set_state(stage="2", status=Stage2Status.GRIPPER_ACTIVATE.value, event="DETECTION_GRASPING -> GRIPPER_ACTIVATE")
        self._publish_start("gripper_start", True)

    def _finish_gripper_activate(self, finish_key: str):
        with self.lock:
            self.finish_received.add(finish_key)
        self._publish_start("gripper_start", False)
        time.sleep(self.phase_clear_wait_sec)
        self._set_state(stage="3", status=Stage2Status.INIT3.value, event=f"{finish_key} received. Stage 2 complete.")
        print("\n[STAGE MOVE] STAGE 2 -> STAGE 3")
        print("[STATUS MOVE] GRIPPER_ACTIVATE -> INIT3")

    def _reset_stage2(self):
        self._safe_clear_all_start_topics()
        self._clear_finish_received()
        self.current_item = None
        self.current_item_input = ""
        self._set_state(stage="2", status=Stage2Status.INIT2.value, event="stage2 reset to INIT2")
        print("[RESET] Stage 2 reset to STAGE=2 STATUS=INIT2")

    # ============================================================
    # Publish
    # ============================================================
    def _publish_start(self, key: str, value: bool):
        pub = self.start_publishers.get(key)
        topic = self.start_topics.get(key)
        if pub is None or topic is None:
            print(f"[ERROR] Unknown start key: {key}")
            self._set_state(status=Stage2Status.FAILED.value, event=f"unknown start key: {key}")
            return
        msg = Bool()
        msg.data = bool(value)
        pub.publish(msg)
        print(f"[PUB] {topic} std_msgs/Bool data: {str(value).lower()}")

    def _safe_clear_all_start_topics(self):
        for key in self.start_topics.keys():
            self._publish_start(key, False)

    def _publish_target_info(self):
        if self.current_item is None:
            return
        item_id = str(self.current_item.get("item_id", self.current_item.get("item_key", "")))
        product_name = str(self.current_item.get("product_name", item_id))
        text_prompt = str(self.current_item.get("text_prompt", ""))
        aruco_id = int(self.current_item.get("aruco_id", -1))
        shelf_layer = int(self.current_item.get("shelf_layer", 0))

        m = String(); m.data = product_name if product_name else item_id
        self.pub_target_item_name.publish(m)
        print(f"[PUB] /target_item_name std_msgs/String data: '{m.data}'")

        a = Int32(); a.data = aruco_id
        self.pub_target_aruco_id.publish(a)
        print(f"[PUB] /target_aruco_id std_msgs/Int32 data: {a.data}")

        p = String(); p.data = text_prompt
        self.pub_target_text_prompt.publish(p)
        print(f"[PUB] /target_text_prompt std_msgs/String data: '{p.data}'")

        l = Int32(); l.data = shelf_layer
        self.pub_target_shelf_layer.publish(l)
        print(f"[PUB] /target_shelf_layer std_msgs/Int32 data: {l.data}")

    def _publish_sam3_prompt(self):
        if self.current_item is None:
            return
        prompt = str(self.current_item.get("text_prompt", ""))
        m = String(); m.data = prompt
        self.pub_sam3_text_prompt.publish(m)
        print(f"[PUB] /sam3_text_prompt std_msgs/String data: '{m.data}'")

    # ============================================================
    # Finish callback
    # ============================================================
    def _finish_callback(self, finish_key: str, msg: Bool):
        if not msg.data:
            return
        with self.lock:
            stg, sts = self.stage, self.status
        print(f"\n[FINISH RX] {self.finish_topics.get(finish_key, finish_key)} std_msgs/Bool data: true")
        if finish_key != "arm_picking_finish":
            self._set_state(event=f"finish ignored unsupported: {finish_key}")
        elif stg == "2" and sts == Stage2Status.OBJECT_ALIGN.value:
            self._finish_object_align(finish_key)
        elif stg == "2" and sts == Stage2Status.DETECTION_GRASPING.value:
            self._finish_detection_grasping(finish_key)
        elif stg == "2" and sts == Stage2Status.GRIPPER_ACTIVATE.value:
            self._finish_gripper_activate(finish_key)
        else:
            self._set_state(event=f"arm_picking_finish ignored in {stg}/{sts}")
        self._print_dashboard()
        self._print_prompt()

    # ============================================================
    # Launch
    # ============================================================
    def _launch_node(self, node_key: str):
        if node_key not in self.node_commands:
            print(f"[ERROR] Unknown node key: {node_key}")
            self._set_state(status=Stage2Status.FAILED.value, event=f"unknown node key: {node_key}")
            return
        if node_key in self.launch_requested_nodes:
            print(f"[INFO] {node_key} launch already requested.")
            return
        cmd = self.node_commands[node_key]
        use_term = bool(self.get_parameter("use_new_terminal").value)
        term_cmd = self._make_terminal_command(cmd) if use_term else None
        if term_cmd is None:
            print(f"[WARN] No terminal found or disabled. Running without new terminal: {' '.join(cmd)}")
            proc = subprocess.Popen(cmd)
        else:
            print(f"[LAUNCH] {node_key}: {' '.join(cmd)}")
            proc = subprocess.Popen(term_cmd)
        self.processes[node_key] = proc
        self.launch_requested_nodes.add(node_key)

    def _make_terminal_command(self, ros_command: list):
        setup = os.path.expanduser("~/ros2_ws/install/setup.bash")
        if not os.path.exists(setup):
            setup = os.path.expanduser("~/colcon_ws/install/setup.bash")
        cmd = " ".join(shlex.quote(x) for x in ros_command)
        inside = f"source {setup}; {cmd}; echo ''; echo '[{cmd}] exited.'; exec bash"
        if shutil.which("gnome-terminal"):
            return ["gnome-terminal", "--", "bash", "-lc", inside]
        if shutil.which("xterm"):
            return ["xterm", "-hold", "-e", f"bash -lc {shlex.quote(inside)}"]
        if shutil.which("konsole"):
            return ["konsole", "-e", "bash", "-lc", inside]
        return None

    def _shutdown_all_processes(self):
        for key, proc in list(self.processes.items()):
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
            cmd = self.node_commands.get(key)
            if cmd:
                patterns = [" ".join(cmd)]
                if key == "gripper":
                    patterns += ["gripper_control.*gripper_node", "gripper_node"]
                for pat in patterns:
                    subprocess.run(["pkill", "-f", pat], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.processes.clear()
        self.launch_requested_nodes.clear()
        self.stage2_started = False
        self._set_state(event="all launched processes terminated")
        print("[INFO] all launched processes terminated")

    # ============================================================
    # Print helpers
    # ============================================================
    def _print_items(self):
        print("\n[Available items]")
        if not self.item_db:
            print("  - item_database.yaml empty or not loaded")
            return
        for k, v in self.item_db.items():
            print(f"  {k:<15} product_name={v.get('product_name','-')}, aruco_id={v.get('aruco_id','-')}, text_prompt={v.get('text_prompt','-')}, shelf_layer={v.get('shelf_layer','-')}, aliases={v.get('aliases',[])}")

    def _print_topics_and_nodes(self):
        print("\n[Node commands]")
        for k, cmd in self.node_commands.items():
            print(f"  {k:<20} -> {' '.join(cmd)}")
        print("\n[Start topics]")
        for k, t in self.start_topics.items():
            print(f"  {k:<25} -> {t}")
        print("\n[Finish topics]")
        for k, t in self.finish_topics.items():
            print(f"  {k:<25} -> {t}")
        print("\n[Target topics]")
        print("  target_item_name        -> /target_item_name       std_msgs/String")
        print("  target_aruco_id         -> /target_aruco_id        std_msgs/Int32")
        print("  target_text_prompt      -> /target_text_prompt     std_msgs/String")
        print("  sam3_text_prompt        -> /sam3_text_prompt       std_msgs/String")
        print("  target_shelf_layer      -> /target_shelf_layer     std_msgs/Int32")

    def _print_help(self):
        print("""
============================================================
Stage 2 Master Commands
============================================================
  start2       : gripper node launch only. STATUS remains INIT2.
  item <name>  : INIT2 -> OBJECT_ALIGN
  <name>       : same as item <name> in INIT2
  items        : print item DB
  status       : dashboard
  reset/init2  : publish all start topics false and return to INIT2
  list         : print topics
  kill_all     : terminate launched gripper process
  exit/quit/q  : publish all start topics false and shutdown

State transitions by /arm_picking_finish true:
  OBJECT_ALIGN        -> DETECTION_GRASPING
  DETECTION_GRASPING -> GRIPPER_ACTIVATE
  GRIPPER_ACTIVATE   -> INIT3

Important:
  Because start topics use TRANSIENT_LOCAL, every phase exit publishes false.
============================================================
""")


def main(args=None):
    rclpy.init(args=args)
    node = Stage2Master()
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