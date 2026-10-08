"""
A client's update position never skips an update it did not get.

The gate records every update it owes a job's client, numbered, and
replays from the client's position when the client's callback registers
again. The position was overwritten with each delivery: an update that
failed to reach the client followed by one that did moved it past the
failure, and the replay never resent it. The position is now the last
sequence below which every update reached the client; a later update
landing first is held aside until the gap fills -- or until the gap falls
out of the retained history, which can no longer resend it.
"""

import pytest

from hyperscale.distributed.nodes.gate.state import GateRuntimeState

JOB_ID = "job-1"
CALLBACK = ("127.0.0.1", 19500)


async def record(state: GateRuntimeState, count: int) -> list[int]:
    return [
        await state.record_client_update(JOB_ID, "job_status_push", b"update", 0.0)
        for _ in range(count)
    ]


@pytest.mark.asyncio
async def test_a_failed_update_holds_the_position_until_it_is_delivered() -> None:
    state = GateRuntimeState(forward_throughput_interval_start=0.0)
    first, failed, later = await record(state, 3)

    await state.set_client_update_position(JOB_ID, CALLBACK, first)
    await state.set_client_update_position(JOB_ID, CALLBACK, later)
    held_at_gap = await state.get_client_update_position(JOB_ID, CALLBACK)
    updates_to_replay, _oldest, _latest = await state.get_client_updates_since(JOB_ID, held_at_gap)

    await state.set_client_update_position(JOB_ID, CALLBACK, failed)
    after_replay = await state.get_client_update_position(JOB_ID, CALLBACK)

    assert held_at_gap == first
    assert [sequence for sequence, *_ in updates_to_replay] == [failed, later]
    assert after_replay == later


@pytest.mark.asyncio
async def test_a_gap_older_than_the_history_closes() -> None:
    state = GateRuntimeState(forward_throughput_interval_start=0.0)
    history_limit = 4
    state.set_client_update_history_limit(history_limit)
    never_delivered, *delivered = await record(state, history_limit + 2)

    for sequence in delivered:
        await state.set_client_update_position(JOB_ID, CALLBACK, sequence)

    assert never_delivered == 1
    assert await state.get_client_update_position(JOB_ID, CALLBACK) == delivered[-1]
    assert state._job_client_updates_delivered_ahead[JOB_ID][CALLBACK] == set()


@pytest.mark.asyncio
async def test_a_jobs_update_state_goes_with_it() -> None:
    state = GateRuntimeState(forward_throughput_interval_start=0.0)
    _first, _failed, later = await record(state, 3)
    await state.set_client_update_position(JOB_ID, CALLBACK, later)

    await state.cleanup_job_update_state(JOB_ID)

    assert JOB_ID not in state._job_client_updates_delivered_ahead
    assert await state.get_client_update_position(JOB_ID, CALLBACK) == 0
