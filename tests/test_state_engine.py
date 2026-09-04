import pytest
import asyncio
from state import FleetState
from connection_mgr import ConnectionManager
@pytest.mark.asyncio
async def test_out_of_order_telemetry_rejection():
    """
    Simulates network churn where an older event arrives AFTER a newer event.
    Verifies that state strictly preserves monotonic timestamps and discards stale updates.
    """
    state = FleetState()

    # 1. Ingest event at t=50
    newer_event = {
        "t": 50,
        "robot_id": "r1",
        "x": 580.9,
        "y": 29.4,
        "status": "active",
        "battery": 83.1
    }
    result = await state.update_robot(newer_event)
    assert result is not None
    assert (await state.get_robot("r1"))["t"] == 50

    # 2. Ingest delayed event at t=35 (e.g. late retry over flaky network)
    stale_event = {
        "t": 35,
        "robot_id": "r1",
        "x": 500.0,
        "y": 20.0,
        "status": "idle",
        "battery": 83.3
    }
    stale_result = await state.update_robot(stale_event)

    assert stale_result is None     # State update must be rejected/ignored
    current = await state.get_robot("r1")
    assert current["t"] == 50
    assert current["status"] == "active"
    assert current["x"] == 580.9

@pytest.mark.asyncio
async def test_concurrent_state_updates_and_rest_snapshot():
    """
    Simulates high concurrency: multiple robot updates hitting the state engine
    simultaneously while REST clients poll for snapshots. Ensures no race conditions
    or partial writes occur.
    """
    state = FleetState()

    async def simulate_robot_stream(robot_id: str, count: int):
        for seq in range(count):
            await state.update_robot({
                "t": seq,
                "robot_id": robot_id,
                "x": float(seq),
                "y": float(seq),
                "status": "active",
                "battery": 100.0 - (seq * 0.1)
            })
            await asyncio.sleep(0.001)

    tasks = [simulate_robot_stream(f"r{i}", 20) for i in range(1, 9)]   # concurrent streaming for 8 robots
    
    async def poll_snapshots():
        for _ in range(10):
            snapshot = await state.get_all()
            assert isinstance(snapshot, dict)
            await asyncio.sleep(0.002)

    await asyncio.gather(*tasks, poll_snapshots())

    final_state = await state.get_all()     # Verify that final state reflects terminal monotonic sequence
    assert len(final_state) == 8
    for i in range(1, 9):
        assert final_state[f"r{i}"]["t"] == 19

@pytest.mark.asyncio
async def test_websocket_dead_client_pruning():
    """
    Verifies that when a WebSocket connection drops abruptly, the broadcast
    loop does not block, crash, or fail to deliver to surviving clients.
    """
    manager = ConnectionManager()

    class MockWebSocket:
        def __init__(self, should_fail=False):
            self.should_fail = should_fail
            self.received = []

        async def accept(self):
            pass

        async def send_json(self, data):
            if self.should_fail:
                raise ConnectionResetError("Connection lost")
            self.received.append(data)

    healthy_client = MockWebSocket(should_fail=False)
    broken_client = MockWebSocket(should_fail=True)

    await manager.connect(healthy_client)
    await manager.connect(broken_client)
    assert len(manager._active_connections) == 2

    await manager.broadcast({"type": "ping", "data": 123})    # Broadcast message

    assert len(manager._active_connections) == 1    # Dead client pruned cleanly without crashing healthy broadcast
    assert healthy_client in manager._active_connections
    assert len(healthy_client.received) == 1