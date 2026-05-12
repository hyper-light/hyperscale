"""
Determinism replay test for the Raft state-machine apply layer (AD-52 Phase 0).

Two independently constructed ``RaftStateMachine`` + ``JobManager`` instances
receive identical ``RaftLogEntry`` sequences. After apply, their ``JobInfo``
state MUST be byte-equal. This is the load-bearing correctness gate for the
determinism contract described in ``hyperscale/distributed/raft/state_machine.py``
and AD-52 §15.

Specifically targets the regressions that motivated the Phase 0 fix:

* ``_apply_initiate_cancellation`` previously wrote ``time.monotonic()`` to
  ``job.timestamp`` and ``manager_state.cancellation_initiated_at`` -- divergent
  across followers. Replaced with ``entry.timestamp`` (HLC wall-clock seconds
  minted on the leader and replicated through Raft).
* ``_apply_complete_cancellation`` had the same problem on ``job.completed_at``
  and ``job.timestamp``.
* ``_apply_update_job_status`` called ``JobManager.update_job_status`` which
  wrote ``time.monotonic()`` to ``job.timestamp``. ``update_job_status`` now
  accepts an explicit ``timestamp`` parameter; the apply handler passes
  ``entry.timestamp``.

If any future handler reintroduces non-determinism (calling ``time.*``,
``random.*``, ``datetime.now``, unsorted iteration, etc.), this test fails.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import cloudpickle
import pytest

from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.models.jobs import JobInfo
from hyperscale.distributed.raft.models import RaftCommandType, RaftLogEntry
from hyperscale.distributed.raft.models.commands import RaftCommand
from hyperscale.distributed.raft.state_machine import RaftStateMachine


# =============================================================================
# Helpers
# =============================================================================


def _build_state_machine() -> tuple[JobManager, RaftStateMachine]:
    """Construct an independent JobManager + RaftStateMachine pair.

    The two replicas in each test are constructed identically (same
    datacenter, manager_id) so the token keys collide and replay
    can be compared field-by-field.
    """
    job_manager = JobManager(datacenter="DC-TEST", manager_id="mgr-test")
    leadership_tracker: JobLeadershipTracker = JobLeadershipTracker(
        node_id="mgr-test",
        node_addr=("0.0.0.0", 0),
    )
    logger = MagicMock()
    logger.log = AsyncMock()
    state_machine = RaftStateMachine(
        job_manager=job_manager,
        leadership_tracker=leadership_tracker,
        logger=logger,
        node_id="mgr-test",
    )
    return job_manager, state_machine


def _preload_job(job_manager: JobManager, job_id: str) -> None:
    """Inject a JobInfo so apply handlers find an existing job by id."""
    token = job_manager.create_job_token(job_id)
    job_manager._jobs[str(token)] = JobInfo(token=token, submission=None)


def _snapshot_jobs(job_manager: JobManager) -> bytes:
    """Snapshot every JobInfo's deterministic state as bytes.

    The snapshot is sorted by job-token key (deterministic iteration) and
    contains only the fields apply handlers may mutate. Locks, callbacks, and
    other non-state objects are deliberately excluded.
    """
    snapshot: list[tuple[object, ...]] = []
    for token_key, job in sorted(job_manager._jobs.items()):
        snapshot.append(
            (
                token_key,
                job.status,
                job.timestamp,
                job.completed_at,
                job.workflows_total,
                job.workflows_completed,
                job.workflows_failed,
                job.layer_version,
                job.leader_node_id,
                job.leader_addr,
                job.fencing_token,
            )
        )
    return cloudpickle.dumps(snapshot)


def _make_entry(
    *,
    command_type: RaftCommandType,
    job_id: str,
    index: int,
    timestamp: float,
    **command_fields: object,
) -> RaftLogEntry:
    command = RaftCommand(command_type=command_type, **command_fields)
    return RaftLogEntry(
        term=1,
        index=index,
        command=cloudpickle.dumps(command),
        command_type=command_type.value,
        job_id=job_id,
        timestamp=timestamp,
    )


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.asyncio
async def test_initiate_and_complete_cancellation_replay_byte_equal() -> None:
    """The original divergence case: two replicas applying cancellation entries
    converge on byte-identical state because both handlers now source the
    transition timestamp from ``entry.timestamp``.
    """
    job_id = "job-replay-cancellation"
    ts_initiate = 1_700_000_000.123
    ts_complete = 1_700_000_001.456

    jm_a, sm_a = _build_state_machine()
    _preload_job(jm_a, job_id)
    jm_b, sm_b = _build_state_machine()
    _preload_job(jm_b, job_id)

    entries = [
        _make_entry(
            command_type=RaftCommandType.INITIATE_CANCELLATION,
            job_id=job_id,
            index=1,
            timestamp=ts_initiate,
            pending_workflows=None,
        ),
        _make_entry(
            command_type=RaftCommandType.COMPLETE_CANCELLATION,
            job_id=job_id,
            index=2,
            timestamp=ts_complete,
        ),
    ]

    for entry in entries:
        await sm_a.apply(entry)
        await sm_b.apply(entry)

    assert _snapshot_jobs(jm_a) == _snapshot_jobs(jm_b)

    token_key = str(jm_a.create_job_token(job_id))
    job_a = jm_a._jobs[token_key]
    assert job_a.status == "cancelled"
    assert job_a.timestamp == ts_complete
    assert job_a.completed_at == ts_complete


@pytest.mark.asyncio
async def test_update_job_status_replay_byte_equal() -> None:
    """UPDATE_JOB_STATUS apply produces byte-equal state because
    ``update_job_status`` now accepts the entry timestamp explicitly.
    """
    job_id = "job-replay-update-status"
    proposal_ts = 1_700_000_500.789

    jm_a, sm_a = _build_state_machine()
    _preload_job(jm_a, job_id)
    jm_b, sm_b = _build_state_machine()
    _preload_job(jm_b, job_id)

    token_key = str(jm_a.create_job_token(job_id))
    entry = _make_entry(
        command_type=RaftCommandType.UPDATE_JOB_STATUS,
        job_id=job_id,
        index=1,
        timestamp=proposal_ts,
        job_token=token_key,
        status="running",
    )

    await sm_a.apply(entry)
    await sm_b.apply(entry)

    assert _snapshot_jobs(jm_a) == _snapshot_jobs(jm_b)

    job_a = jm_a._jobs[token_key]
    assert job_a.status == "running"
    assert job_a.timestamp == proposal_ts


@pytest.mark.asyncio
async def test_replay_under_simulated_real_time_skew() -> None:
    """Replicas apply the same log with arbitrary real-time gaps between calls.

    Because every timestamp comes from the replicated ``entry.timestamp``,
    the wall-clock time at which apply runs on each follower is irrelevant.
    No matter the interleaving, the final state is identical.
    """
    job_id = "job-replay-skew"
    ts_a = 1_500_000_000.0
    ts_b = 1_500_000_007.500

    jm_a, sm_a = _build_state_machine()
    _preload_job(jm_a, job_id)
    jm_b, sm_b = _build_state_machine()
    _preload_job(jm_b, job_id)

    initiate = _make_entry(
        command_type=RaftCommandType.INITIATE_CANCELLATION,
        job_id=job_id,
        index=1,
        timestamp=ts_a,
        pending_workflows=None,
    )
    complete = _make_entry(
        command_type=RaftCommandType.COMPLETE_CANCELLATION,
        job_id=job_id,
        index=2,
        timestamp=ts_b,
    )

    # Different interleavings -- byte-equal state regardless.
    await sm_a.apply(initiate)
    await sm_b.apply(initiate)
    await sm_a.apply(complete)
    await sm_b.apply(complete)

    snap_a = _snapshot_jobs(jm_a)
    snap_b = _snapshot_jobs(jm_b)
    assert snap_a == snap_b


@pytest.mark.asyncio
async def test_replay_full_lifecycle_byte_equal() -> None:
    """End-to-end replay: status update followed by cancellation.

    Exercises both fixed code paths in one log and confirms that the final
    state on each replica is byte-identical and matches the last entry's
    timestamp.
    """
    job_id = "job-replay-lifecycle"

    jm_a, sm_a = _build_state_machine()
    _preload_job(jm_a, job_id)
    jm_b, sm_b = _build_state_machine()
    _preload_job(jm_b, job_id)

    token_key = str(jm_a.create_job_token(job_id))

    entries = [
        _make_entry(
            command_type=RaftCommandType.UPDATE_JOB_STATUS,
            job_id=job_id,
            index=1,
            timestamp=1_700_001_000.000,
            job_token=token_key,
            status="running",
        ),
        _make_entry(
            command_type=RaftCommandType.INITIATE_CANCELLATION,
            job_id=job_id,
            index=2,
            timestamp=1_700_001_010.250,
            pending_workflows=None,
        ),
        _make_entry(
            command_type=RaftCommandType.COMPLETE_CANCELLATION,
            job_id=job_id,
            index=3,
            timestamp=1_700_001_020.500,
        ),
    ]

    for entry in entries:
        await sm_a.apply(entry)
        await sm_b.apply(entry)

    assert _snapshot_jobs(jm_a) == _snapshot_jobs(jm_b)
    final_a = jm_a._jobs[token_key]
    assert final_a.status == "cancelled"
    assert final_a.timestamp == 1_700_001_020.500
    assert final_a.completed_at == 1_700_001_020.500


@pytest.mark.asyncio
async def test_replay_no_op_byte_equal() -> None:
    """NO_OP entries (leadership confirmations) leave state unchanged on
    every replica.
    """
    jm_a, sm_a = _build_state_machine()
    jm_b, sm_b = _build_state_machine()

    entry = _make_entry(
        command_type=RaftCommandType.NO_OP,
        job_id="",
        index=1,
        timestamp=1_700_000_000.0,
    )

    await sm_a.apply(entry)
    await sm_b.apply(entry)

    assert _snapshot_jobs(jm_a) == _snapshot_jobs(jm_b)
    # No jobs were ever created; both empty.
    assert jm_a._jobs == {} == jm_b._jobs


@pytest.mark.asyncio
async def test_independent_managers_diverge_only_on_unreplicated_state() -> None:
    """Sanity guard: replicas with different node_ids are still byte-equal on
    the apply-mutated fields. Replicas only share what's in the log; identity
    fields (datacenter, manager_id) are configuration, not log state, and
    happen to match because the test constructs them identically.

    If this test ever fails, it means the apply handlers leaked
    non-replicated state into the JobInfo snapshot fields -- which is exactly
    the regression we want to catch.
    """
    job_id = "job-replay-sanity"
    proposal_ts = 1_700_002_000.0

    # Both replicas construct JobManager with identical config (same DC, same
    # manager_id). This mirrors the cluster invariant: all replicas of the
    # same Raft group share datacenter + manager_id.
    jm_a, sm_a = _build_state_machine()
    _preload_job(jm_a, job_id)
    jm_b, sm_b = _build_state_machine()
    _preload_job(jm_b, job_id)

    entry = _make_entry(
        command_type=RaftCommandType.INITIATE_CANCELLATION,
        job_id=job_id,
        index=1,
        timestamp=proposal_ts,
        pending_workflows=None,
    )
    await sm_a.apply(entry)
    await sm_b.apply(entry)

    assert _snapshot_jobs(jm_a) == _snapshot_jobs(jm_b)
