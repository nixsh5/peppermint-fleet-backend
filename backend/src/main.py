import os
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from state import FleetState
from connection_mgr import ConnectionManager
from mqtt_client import MQTTSubscriber

logger = logging.getLogger("main")
logging.basicConfig(level=logging.INFO)

BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
MQTT_TOPIC = "robots/+/telemetry"

state = FleetState()
manager = ConnectionManager()
mqtt_sub: MQTTSubscriber = None

async def handle_telemetry_event(event: dict):
    """Callback triggered whenever an MQTT message arrives."""
    updated = await state.update_robot(event)
    if updated:         # Check valid, in-order telemetry to all connected WebSocket clients
        await manager.broadcast({
            "type": "robot_update",
            "data": updated
        })

@asynccontextmanager
async def lifespan(app: FastAPI):
    global mqtt_sub
    loop = asyncio.get_running_loop()
    mqtt_sub = MQTTSubscriber(
        broker_host=BROKER_HOST,
        broker_port=BROKER_PORT,
        topic=MQTT_TOPIC,
        on_event_callback=handle_telemetry_event,
        loop=loop
    )
    mqtt_sub.start()
    logger.info("Application startup complete. Telemetry ingestion running.")
    yield
    mqtt_sub.stop()
    logger.info("Application shutdown.")

app = FastAPI(title="Peppermint Robotics Fleet Backend", lifespan=lifespan)

app.add_middleware(         # Allow CORS for dashboard access, not block the request
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/robots")
async def get_all_robots():
    """Returns current state snapshot for polling clients."""
    return await state.get_all()

@app.get("/robots/{robot_id}")
async def get_robot(robot_id: str):
    robot = await state.get_robot(robot_id)
    if not robot:
        raise HTTPException(status_code=404, detail="Robot not found")
    return robot

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        current_state = await state.get_all()   # Send current state instantly upon connection
        await websocket.send_json({
            "type": "initial_state",
            "data": current_state
        })
        
        while True:                 # Keep socket open for any client pings and also listen on it
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as e:
        logger.warning(f"WebSocket client error: {e}")
        await manager.disconnect(websocket)