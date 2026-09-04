# ANSWERS.md

### 1. What holds the fleet's current state, and why that shape?
The fleet's state is stored in an in memory dictionary inside the `FleetState` class in `backend/src/state.py`. It maps each `robot_id` to its latest telemetry dictionary: `robot_id`, `t`, `x`, `y`, `status`, `battery`, and optional `task_event`. Access and updates to this dictionary are guarded by an `asyncio.Lock`.

I chose this structure because both the polling REST endpoint (`GET /robots` in `backend/src/main.py`) and the WebSocket stream (`backend/src/connection_mgr.py`) need to read from the exact same source. In `state.py::update_robot()`, an incoming event is checked against the robot's current timestamp `t`. If a packet arrives with an older `t` (due to network delays or retries), it gets dropped immediately. When valid, the state is updated, and `handle_telemetry_event()` in `backend/src/main.py` pushes that same update straight to `manager.broadcast()`. This guarantees that whether a client polls via REST or streams via WebSockets, neither client sees conflicting, stale, or out-of-order robot movements.

---

### 2. Tradeoff: Robot transport mechanism and WebSocket reconciliation
I chose MQTT (via Mosquitto) with QoS 1 ("at least once" delivery) for the robots to communicate with the backend, instead of raw TCP sockets or direct HTTP callbacks. 

The tradeoff is reliability versus complexity. QoS 1 ensures telemetry packets are not silently dropped if there is a quick network glitch between the robot and the broker. However, its cost is that network retries can introduce duplicate messages or packets arriving out of order. I reconciled this part in `backend/src/state.py::update_robot()` by forcing timestamp monotonicity: if a retry brings an event older than what we already stored, it is discarded. For the WebSocket side, WebSockets operate on TCP ("maximum once" at the application layer). Rather than holding up all if the clients to guarantee delivery to a laggy connection, `connection_mgr.py::broadcast()` prunes any failed client connections immediately to protect the event loop. When a client reconnects, `main.py::websocket_endpoint()` immediately delivers a full `initial_state` snapshot before live streaming is resumed, keeping state consistent.

---

### 3. What I left out and what I would build next
Due to timeboxing, I left out persistent database storage for historical events (`GET /robots/history/{robot_id}`), as well as broker authentication/TLS. I focused the time on building a reliable 8 process simulation pipeline, handling flaky network edge cases, and writing targeted concurrency tests.

If I was given more time, I would:
1. **Add a TimescaleDB or SQLite store:** Persist incoming events to support historical replay and trajectory queries.
2. **Implement Heartbeat / Deadman Monitoring:** Add a background worker to `backend/src/main.py` that marks a robot as `offline` if it hasn't sent telemetry in over 15 seconds.
3. **Batch WebSocket Broadcasts:** Instead of sending an update per MQTT packet, batch updates on a 100ms intervals to handle higher fleet counts without choking any kind of connections with client.