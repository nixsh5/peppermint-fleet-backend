import asyncio
from typing import List, Dict, Any
from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        self._active_connections: List[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self._active_connections.append(websocket)

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self._active_connections:
                self._active_connections.remove(websocket)

    async def broadcast(self, message: Dict[str, Any]):
        """
        Pushes updates to all connected WebSocket clients.
        Dead or failing connections are collected and cleaned up.
        """
        async with self._lock:
            dead_connections = []
            for connection in self._active_connections:
                try:
                    await connection.send_json(message)
                except Exception:
                    dead_connections.append(connection)

            for dead in dead_connections:
                if dead in self._active_connections:
                    self._active_connections.remove(dead)