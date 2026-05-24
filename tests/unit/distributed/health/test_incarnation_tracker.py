import pytest

from hyperscale.distributed.swim.detection.incarnation_tracker import IncarnationTracker


@pytest.mark.asyncio
async def test_clear_suspicion_after_confirmation_accepts_same_incarnation_probe_ack() -> None:
    """A local post-suspicion ACK can clear SUSPECT without gossip priority reversal."""
    tracker = IncarnationTracker()
    node = ("127.0.0.1", 9001)

    await tracker.update_node(node, b"OK", 3, 1.0)
    await tracker.update_node(node, b"SUSPECT", 3, 2.0)
    tracker.record_node_death(node, 3, 3.0)

    cleared = await tracker.clear_suspicion_after_confirmation(node, 3, 4.0)

    state = tracker.get_node_state(node)
    assert cleared is True
    assert state is not None
    assert state.status == b"OK"
    assert state.incarnation == 3
    assert state.last_update_time == 4.0
    assert tracker.get_required_rejoin_incarnation(node) == 0


@pytest.mark.asyncio
async def test_clear_suspicion_after_confirmation_rejects_stale_confirmation() -> None:
    """A stale direct confirmation must not rewind a newer suspicion."""
    tracker = IncarnationTracker()
    node = ("127.0.0.1", 9002)

    await tracker.update_node(node, b"OK", 4, 1.0)
    await tracker.update_node(node, b"SUSPECT", 4, 2.0)

    cleared = await tracker.clear_suspicion_after_confirmation(node, 3, 3.0)

    state = tracker.get_node_state(node)
    assert cleared is False
    assert state is not None
    assert state.status == b"SUSPECT"
    assert state.incarnation == 4
