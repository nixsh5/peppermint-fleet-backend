import os
import json
import time
import logging
from multiprocessing import Process
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(processName)s] %(message)s")
logger = logging.getLogger("supervisor")

BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
DATA_DIR = os.getenv("DATA_DIR", "/app/data")
SPEED_FACTOR = float(os.getenv("REPLAY_SPEED_FACTOR", "5.0")) # Replay speed factor --> 5min:1min | 15min:3min

def run_robot_process(robot_id: str, events: list, broker_host: str, broker_port: int, speed: float):
    """Worker process representing a single, isolated robot publisher."""
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"robot-publisher-{robot_id}"
    )

    connected = False # Reconnection loop in case broker is initializing
    while not connected:
        try:
            client.connect(broker_host, broker_port, keepalive=60)
            connected = True
        except Exception:
            time.sleep(1)

    client.loop_start()
    topic = f"robots/{robot_id}/telemetry"
    logger.info(f"Started publisher for {robot_id} with {len(events)} events (speed={speed}x)")

    last_t = 0
    for event in events:
        current_t = event.get("t", 0)
        delay = (current_t - last_t) / speed
        if delay > 0:
            time.sleep(delay)
        last_t = current_t

        payload = json.dumps(event)
        client.publish(topic, payload, qos=1)

    logger.info(f"Finished replaying all events for {robot_id}")
    time.sleep(2)
    client.loop_stop()
    client.disconnect()

def main():
    robots_file = os.path.join(DATA_DIR, "robots.json")
    events_file = os.path.join(DATA_DIR, "events.jsonl")

    with open(robots_file, "r") as f:           # Load whole robot lineup
        robots_data = json.load(f)
    robot_ids = {r["robot_id"] for r in robots_data}

    robot_events = {r_id: [] for r_id in robot_ids}     # group events by robot_id
    with open(events_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            r_id = event.get("robot_id")
            if r_id in robot_events:
                robot_events[r_id].append(event)

    logger.info(f"Loaded roster of {len(robot_ids)} robots. Spawning independent processes...")

    processes = []
    for r_id, events in robot_events.items():
        events.sort(key=lambda e: e.get("t", 0))                # Sort by timestamp to preserve log order
        p = Process(
            target=run_robot_process,
            name=f"Process-{r_id}",
            args=(r_id, events, BROKER_HOST, BROKER_PORT, SPEED_FACTOR)
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    logger.info("All robot publisher processes have completed.")

if __name__ == "__main__":
    main()