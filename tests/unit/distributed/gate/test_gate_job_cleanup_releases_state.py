"""
Cleaning up a finished job releases the gate's per-job state.

GateServer kept its own copies of the per-job maps (submission,
workflow ids, client callback, DC managers) beside GateRuntimeState's.
Dispatch and the replica commit write GateRuntimeState; the job cleanup
popped only the server copies -- which nothing wrote -- so every job's
JobSubmission (with its pickled workflows), workflow ids, callback and
DC managers stayed in memory forever. Status queries, the takeover
notice to managers and timeout cancellation read the empty server
copies.

The job's lock in GateJobManager was never released either: every job
that ever took it left one behind for the gate's lifetime.

Driven through the real ``_cleanup_single_job`` over a real
GateRuntimeState populated the way dispatch populates it, and a real
GateJobManager.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.jobs.gates import GateJobManager
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.reliability.best_effort_metrics import BestEffortMetrics

JOB = "job-1"
OTHER_JOB = "job-2"
MANAGER_ADDR = ("10.0.0.7", 9000)


def populate(state: GateRuntimeState, job_id: str) -> None:
    state._job_submissions[job_id] = SimpleNamespace(job_id=job_id)
    state._job_workflow_ids[job_id] = {"workflow-1"}
    state._progress_callbacks[job_id] = ("10.0.0.9", 9500)
    state._job_dc_managers.setdefault(job_id, {})["dc-1"] = MANAGER_ADDR  # as dispatch writes it


def make_gate(state: GateRuntimeState, job_manager: GateJobManager) -> GateServer:
    gate = object.__new__(GateServer)
    gate._modular_state = state
    gate._raft = SimpleNamespace(consensus=SimpleNamespace(destroy_job_raft=AsyncMock()))
    gate._job_manager = job_manager
    gate._workflow_dc_results_lock = asyncio.Lock()
    gate._workflow_dc_results = {}
    gate._workflow_result_expected_dc_counts = {}
    gate._workflow_result_timeout_tokens = {}
    gate._finalized_workflow_results = set()
    gate._job_final_statuses = {}
    gate._job_global_result_sent = set()
    gate._job_completion_claimed = set()
    gate._job_leadership_tracker = SimpleNamespace(release_leadership=lambda job_id: None)
    gate._best_effort_manager = SimpleNamespace(cleanup=AsyncMock())
    gate._best_effort_metrics = BestEffortMetrics()
    gate._job_stats_crdt = {}
    gate._task_runner = SimpleNamespace(run=lambda *args, **kwargs: None)
    gate._windowed_stats = SimpleNamespace(cleanup_job_windows=None)
    gate._job_router = SimpleNamespace(cleanup_job_state=lambda job_id: None)
    gate._replication_coordinator = SimpleNamespace(clear_for_job=AsyncMock())
    gate._job_failover_coordinator = SimpleNamespace(forgotten_job_ids=[])
    gate._job_failover_coordinator.forget_job = gate._job_failover_coordinator.forgotten_job_ids.append
    return gate


@pytest.mark.asyncio
async def test_cleanup_releases_every_per_job_entry_and_only_that_job() -> None:
    state = GateRuntimeState(forward_throughput_interval_start=0.0)
    job_manager = GateJobManager()
    populate(state, JOB)
    populate(state, OTHER_JOB)
    for job_id in (JOB, OTHER_JOB):
        async with job_manager.lock_job(job_id):
            pass

    gate = make_gate(state, job_manager)
    for job_id in (JOB, OTHER_JOB):
        gate._best_effort_metrics.record_completion(job_id, "best_effort: min_dcs_reached (1/1)", 0.5)
    await GateServer._cleanup_single_job(gate, JOB)

    assert JOB not in state._job_submissions
    assert JOB not in state._job_workflow_ids
    assert JOB not in state._progress_callbacks
    assert not state.get_job_dc_managers(JOB)
    assert JOB not in job_manager._job_locks
    assert gate._job_failover_coordinator.forgotten_job_ids == [JOB]
    assert OTHER_JOB in state._job_submissions
    assert state.get_job_dc_managers(OTHER_JOB) == {"dc-1": MANAGER_ADDR}
    assert OTHER_JOB in job_manager._job_locks
    # The AD-44 per-job completion ratio leaves with the job.
    assert gate._best_effort_metrics.completion_ratio_by_job() == {OTHER_JOB: 0.5}
