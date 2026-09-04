# Fleet Management Dashboard: Backend Service

Backend service and multi-process fleet simulator for Peppermint Robotics, providing real-time telemetry ingestion, monotonic state management, and dual REST/WebSocket egress.

---

## Quickstart (Single Command)

Bring up the entire stack (Mosquitto MQTT broker, FastAPI backend, and the 8-process robot simulator) with Docker Compose:

```bash
docker compose up --build
```

* **Mosquitto Broker:** Listening on `localhost:1883`.
* **Backend API & WebSockets:** Running on `localhost:8000`.
* **Robot Fleet Simulator:** Spawns 8 isolated OS-level worker processes replaying `events.jsonl` over MQTT at 5x speed.

---

## Verifying the Endpoints

### 1. REST Snapshot (Polling)

Retrieve the latest state of all 8 robots:

```bash
curl -s http://localhost:8000/robots
```

or (jq is used for formatting the output in terminal and has to be installed using ```winget install jqlang.jq```)

```bash
curl -s http://localhost:8000/robots | jq
```

Or query an individual robot:

```bash
curl -s http://localhost:8000/robots/r1
```

or (jq is used for formatting the output in terminal and has to be installed using ```winget install jqlang.jq```)

```bash
curl -s http://localhost:8000/robots/r1 | jq
```

### 2. WebSocket Live Stream

Connect to `ws://localhost:8000/ws` to receive an instantaneous `initial_state` snapshot, followed by real-time `robot_update` event frames:

```bash
python3 -c "
import asyncio, websockets
async def listen():
    async with websockets.connect('ws://localhost:8000/ws') as ws:
        for _ in range(5):
            print(await ws.recv())
asyncio.run(listen())
"
```

---

## Architecture & Design Decisions

### 1. Multi-Process Simulator (`simulator/src/supervisor.py`)

* Per challenge guidelines, the simulator uses Python's `multiprocessing.Process` to spawn **8 isolated OS level worker processes** (one per robot in `robots.json`), avoiding single process routines.
* Each process acts as an independent MQTT client (`robot-publisher-rX`) publishing telemetry events to `robots/{robot_id}/telemetry` with QoS 1.
* Features a configurable `REPLAY_SPEED_FACTOR` (default `5.0x`) so the full 15-minute log replays in 3 minutes.

### 2. Ingestion & Monotonic State Engine (`backend/src/state.py`)

* Manages an in-memory dictionary protected by an `asyncio.Lock`, providing a single consistent source of truth for both REST and WebSocket clients.
* Enforces strict monotonic timestamp sequencing (`event.t >= current.t`): any delayed or out-of-order packets arriving from network retries are cleanly rejected before updating state or fanning out to clients.

### 3. WebSocket Fanout & Fault Isolation (`backend/src/connection_mgr.py`)

* Telemetry events that successfully update the fleet state are broadcast to active WebSocket clients.
* If a client connection drops abruptly or lags, it is safely collected and pruned from the active pool without throwing unhandled exceptions or stalling broadcasts to healthy clients.

---

## Tests for the Trickiest Part

The trickiest part of the backend is **maintaining state consistency and monotonic ordering during network churn, concurrent reads/writes, and abrupt socket disconnects**.

The test suite in `tests/test_state_engine.py` covers:

1. **Out-of-Order Packet Rejection:** Verifies that delayed telemetry packets from prior timestamps (e.g., retried packets arriving late over flaky Wi-Fi) are discarded and cannot overwrite newer state.
2. **Concurrent State & Polling Race Condition:** Simulates all 8 robots blasting updates concurrently while multiple REST clients request snapshots, ensuring thread safety and no partial writes.
3. **Dead WebSocket Client Pruning:** Verifies that an abruptly closed or failed socket does not crash or block the fanout loop for remaining healthy clients.

Run the tests using docker:

```bash
docker compose exec backend pytest
```

Run the tests locally (requires python installed):

```bash
pytest
```

---

## AI Delegation Notes

In accordance with the submission instructions:

* **AI Tooling Used:** Gemini for initial scaffolding.
* **Delegated Tasks:** Drafting initial boilerplate for FastAPI WebSocket lifecycles, boilerplate configuration for Mosquitto Dockerfiles, and generating template edge cases for `pytest`.
* **Authored & Refined Directly:** In-memory monotonic timestamp validation logic (`state.py`), the multi process supervisor split (`supervisor.py`), Docker Compose network and health check orchestration.

---

## What I Would Build Next

1. **Persistent History Store:** Add a time-series store (TimescaleDB or SQLite) to back the optional `GET /robots/history/{robot_id}` endpoint for historical playback.
2. **Deadman Watchdog / Heartbeat:** Run a background loop checking `last_seen` timestamps; if a robot fails to report within 15 seconds, automatically mark it as `offline` and broadcast an alert.
3. **Throttled WebSocket Batching:** Batch telemetry frames into 100ms ticks rather than emitting per MQTT event to preserve client performance under large fleet counts.

