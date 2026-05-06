#!/usr/bin/env python3

import time

import rclpy
from rclpy.action import ActionServer
from rclpy.node import Node

from master_capstone_interfaces.action import BringupNode


class DummyTaskNode(Node):
    """
    아무 기능 없는 더미 노드.

    목적:
      - master가 새 터미널을 띄워서 이 노드를 실행할 수 있는지 확인
      - 실행된 노드가 action goal을 받을 수 있는지 확인
      - goal을 받으면 준비 완료 result를 반환

    고정 action server:
      /dummy_task_node/bringup
    """

    def __init__(self):
        super().__init__("dummy_task_node")

        self.action_server = ActionServer(
            self,
            BringupNode,
            "/dummy_task_node/bringup",
            self.execute_callback,
        )

        self.get_logger().info("dummy_task_node is running.")
        self.get_logger().info("Action server: /dummy_task_node/bringup")

    def execute_callback(self, goal_handle):
        goal = goal_handle.request

        self.get_logger().info(
            f"Bringup goal received. node_key={goal.node_key}, command={goal.command}"
        )

        feedback = BringupNode.Feedback()
        feedback.phase = "checking"
        feedback.detail = "dummy_task_node checking ready state"
        goal_handle.publish_feedback(feedback)

        time.sleep(0.3)

        feedback.phase = "ready"
        feedback.detail = "dummy_task_node is ready"
        goal_handle.publish_feedback(feedback)

        goal_handle.succeed()

        result = BringupNode.Result()
        result.success = True
        result.node_key = goal.node_key
        result.state = "ready"
        result.message = "dummy_task_node is ready"

        return result


def main(args=None):
    rclpy.init(args=args)

    node = DummyTaskNode()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().info("dummy_task_node stopped.")
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()