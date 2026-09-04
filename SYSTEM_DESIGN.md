# SYSTEM_DESIGN.md

### 1. Adding a new feature later (e.g. Geofencing alerts)
The way the code is split right now makes it straightforward to drop in new features without rewriting what is already there. The MQTT receiver, the fleet state, and the WebSocket broadcaster are kept separate.

If we want to add geofence detection (for example, raising an alert if a robot wanders into an obstacle zone):
- I would create a helper file like `backend/src/geofence.py` with a simple check function: `check_bounds(robot_id, x, y)`.
- Inside `backend/src/main.py`, in the `handle_telemetry_event()` function, right after `state.update_robot(event)` runs successfully, I would pass the new coordinates into that check.
- If the robot crossed a boundary, I would call `manager.broadcast({"type": "alert", "robot_id": robot_id, ...})`.
The state store (`backend/src/state.py`) doesn't need to change at all, and the existing REST and WebSocket contracts continue working as they work normally.

---

### 2. Scaling from 8 to 500 robots: What breaks first?
The first bottleneck will be the lock in `backend/src/state.py` combined with how WebSockets push data in `backend/src/connection_mgr.py`.

Right now, every time an event arrives, `update_robot()` grabs a single `asyncio.Lock()` to update the main dictionary. With 8 robots, there is no noticeable bottleneck. But at 500 robots sending updates every second or two, that is hundreds of writes per second all waiting on one lock while REST queries are also trying to read it. 

Along with that, `connection_mgr.py` is looping through connected WebSocket clients one by one to send updates. If a couple of dashboard tabs have slow network connections, sending data to them will hold up the event loop and create backpressure. 

To fix this for 500 robots, I would:
1. Split the state lock so each robot has its own lock, or move shared state to Redis.
2. Batch WebSocket updates on a fixed intervals, instead of firing a separate message for every single event that arrives.

---

### 3. Handling low network bandwidth
If bandwidth is tight, sending full JSON payloads every 5 seconds per robot wastes too much data. I would change three things:

1. **Send updates only when things change:** In `simulator/src/supervisor.py`, instead of a fixed timer loop, the robot should only send telemetry if it has moved a meaningful distance, if its status changed (like going from active to error), or if the battery dropped. If it is just sitting idle, a tiny heartbeat ping every 30 seconds is enough to check if its alive or not.
2. **Switch to a compact binary format:** Right now each message is a text JSON string of about 100 bytes. Switching to Protocol Buffers or MessagePack would drop that payload down to around 15–20 bytes.
3. **Drop redundant fields:** The robot doesn't need to send useless static information like its own ID in the body if its already publishing to its own fixed topic, and timestamps can be packed and sent as delta integers.

---

### 4. Robot going down mid-task
If a robot battery dies or the software crashes, it won't cleanly close its network connection. The backend would just stop getting messages and assume the robot is still doing whatever it was last doing.

To catch this:
1. **MQTT Last Will:** When the robot connects to the broker in `supervisor.py`, we can configure an message on `robots/{id}/status` with `{"status": "offline"}`. If the broker stops receiving heartbeat pings from that robot, Mosquitto itself publishes that offline message automatically.
2. **Backend Timer:** In `backend/src/state.py`, we can save an updated `last_seen` timestamp whenever a robot reports. A quick background task running in `main.py` every few seconds can check if `current_time - last_seen > 15s`. If it is exceeding that, it automatically flips that robot's status to `offline` and calls the function `manager.broadcast()` so operators can see it immediately on screen.
3. The rest of the system can then flag the assigned task as failed and alert the operator or reassign the job.

---

### 5. Slow or unreliable connections & recovery
If the connection is bad, messages might take a long time to arrive, drop out, or would not be coming in sequence because of MQTT retries.

**What happens during the glitch:**
- The dashboard will continue showing the last position it knows about. If it hasn't heard from the robot in a while, the UI can mark the robot marker as "stale".
- If an older packet (say timestamp $t=20$) arrives *after* a newer packet ($t=30$), `update_robot()` in `backend/src/state.py` catches it: `if current and event.get("t") < current.get("t"): return None`. It simply drops the late packet so the robot never teleports backward on the screen or shows an older status.

**How it recovers:**
- The robot should keep an internal record of recent events which were not sent while its offline.
- When it connects back, the robot should immediately send its **current** state first so the live map is accurate right away.
- Then, it can stream the backlogged history in chunks or over a separate history topic so it doesn't flood and choke the ongoing telemetry feed.