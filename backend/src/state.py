import asyncio
from typing import Dict, Any, Optional

class FleetState:
    def __init__(self):
        self._lock = asyncio.Lock()        # Stores the latest verified state per robot
        self._robots: Dict[str, Dict[str, Any]] = {}

    async def update_robot(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Updates a robot's state if the incoming event timestamp t is >= current t.
        Discards older out-of-order telemetry.
        """
        robot_id = event.get("robot_id")
        if not robot_id:
            return None

        async with self._lock:
            current = self._robots.get(robot_id) # Drop older events that arrived out of order due to network lag
            if current and event.get("t", 0) < current.get("t", 0):
                return None

            payload = {
                "robot_id": robot_id,
                "t": event.get("t"),
                "x": event.get("x"),
                "y": event.get("y"),
                "status": event.get("status"),
                "battery": event.get("battery"),
            }
            if "task_event" in event:
                payload["task_event"] = event["task_event"]

            self._robots[robot_id] = payload
            return payload

    async def get_all(self) -> Dict[str, Dict[str, Any]]:
        """Returns a snapshot of all robots for REST polling."""
        async with self._lock:
            return {k: v.copy() for k, v in self._robots.items()}

    async def get_robot(self, robot_id: str) -> Optional[Dict[str, Any]]:
        """Returns snapshot for a single robot."""
        async with self._lock:
            robot = self._robots.get(robot_id)
            return robot.copy() if robot else None