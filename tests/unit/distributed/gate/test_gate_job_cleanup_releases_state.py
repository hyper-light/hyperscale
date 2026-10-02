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

Driven through the real ``_cleanup_single_job`` over a real
GateRuntimeState populated the way dispatch populates it.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState

JOB = "job-1"
OTHER_JOB = "job-2"
MANAGER_ADDR = ("10.0.0.7", 9000)


def populate(state: GateRuntimeState, job_id: str) -> None:
    state._job_submissions[job_id] = SimpleNamespace(job_id=job_id)
    state._job_workflow_ids[job_id] = {"workflow-1"}
    state._progress_callbacks[job_id] = ("10.0.0.9", 9500)
    state._job_dc_managers.setdefault(job_id, {})["dc-1"] = MANAGER_ADDR  # as dispatch writes it


def make_gate(state: GateRuntimeState) -> GateServer:
    gate = object.__new__(GateServer)
    gate._modular_state = state
    gate._raft = SimpleNamespace(consensus=SimpleNamespace(destroy_job_raft=AsyncMock()))
    gate._job_manager = SimpleNamespace(delete_job=lambda job_id: None)
    gate._workflow_dc_results_lock = asyncio.Lock()
    gate._workflow_dc_results = {}
    gate._workflow_result_expected_dc_counts = {}
    gate._workflow_result_timeout_tokens = {}
    gate._finalized_workflow_result_sequences = {}
    gate._job_final_statuses = {}
    gate._job_global_result_sent = set()
    gate._job_completion_claimed = set()
    gate._job_leadership_tracker = SimpleNamespace(release_leadership=lambda job_id: None)
    gate._best_effort_manager = SimpleNamespace(cleanup=AsyncMock())
    gate._job_reporter_tasks = {}
    gate._job_stats_crdt = {}
    gate._task_runner = SimpleNamespace(run=lambda *args, **kwargs: None)
    gate._windowed_stats = SimpleNamespace(cleanup_job_windows=None)
    gate._dispatch_time_tracker = SimpleNamespace(remove_job=AsyncMock())
    gate._job_router = SimpleNamespace(cleanup_job_state=lambda job_id: None)
    gate._replication_coordinator = SimpleNamespace(clear_for_job=lambda job_id: None)
    return gate


@pytest.mark.asyncio
async def test_cleanup_releases_every_per_job_entry_and_only_that_job() -> None:
    state = GateRuntimeState()
    populate(state, JOB)
    populate(state, OTHER_JOB)

    await GateServer._cleanup_single_job(make_gate(state), JOB)

    assert JOB not in state._job_submissions
    assert JOB not in state._job_workflow_ids
    assert JOB not in state._progress_callbacks
    assert not state.get_job_dc_managers(JOB)
    assert OTHER_JOB in state._job_submissions
    assert state.get_job_dc_managers(OTHER_JOB) == {"dc-1": MANAGER_ADDR}
