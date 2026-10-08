"""
A client's job tracking stays bounded.

The client never forgot a job: its status, results (with every
workflow's stats), callbacks, routing lock and leader records stayed for
the life of the process, so a long-lived client grew with every job it
submitted.

* releasing a job forgets everything tracked for it, and nothing of any
  other job;
* a finished job is forgotten once the retention has passed since a
  sweep first found it finished; an unfinished job never is;
* every submission sweeps, and a caller can release a job it is done
  with.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.models import GateLeaderInfo, ManagerLeaderInfo
from hyperscale.distributed.models.client import ClientJobResult
from hyperscale.distributed.nodes.client.client import HyperscaleClient
from hyperscale.distributed.nodes.client.state import ClientState

RELEASED_JOB = "job-released"
KEPT_JOB = "job-kept"
RETENTION_SECONDS = 300.0


def track_job(state: ClientState, job_id: str) -> None:
    state._jobs[job_id] = ClientJobResult(job_id=job_id, status="submitted")
    state._job_events[job_id] = asyncio.Event()
    state._job_expected_workflows[job_id] = frozenset({"wf-1"})
    state._job_results_events[job_id] = asyncio.Event()
    state._job_callbacks[job_id] = print
    state._job_targets[job_id] = ("10.0.0.1", 9000)
    state.initialize_cancellation_tracking(job_id)
    state._reporter_callbacks[job_id] = print
    state._workflow_callbacks[job_id] = print
    state._job_reporting_configs[job_id] = []
    state._progress_callbacks[job_id] = print
    state._gate_job_leaders[job_id] = GateLeaderInfo(
        gate_addr=("10.0.0.2", 9100),
        fence_token=1,
        last_updated=0.0,
    )
    state._manager_job_leaders[(job_id, "dc-1")] = ManagerLeaderInfo(
        manager_addr=("10.0.0.3", 9000),
        fence_token=1,
        datacenter_id="dc-1",
        last_updated=0.0,
    )
    state._request_routing_locks[job_id] = asyncio.Lock()


def per_job_maps(state: ClientState) -> list[dict]:
    return [
        state._jobs,
        state._job_events,
        state._job_expected_workflows,
        state._job_results_events,
        state._job_callbacks,
        state._job_targets,
        state._job_finished_seen_at,
        state._cancellation_events,
        state._cancellation_errors,
        state._cancellation_success,
        state._reporter_callbacks,
        state._workflow_callbacks,
        state._job_reporting_configs,
        state._progress_callbacks,
        state._gate_job_leaders,
        state._request_routing_locks,
    ]


def test_releasing_a_job_forgets_everything_tracked_for_it_only() -> None:
    state = ClientState()
    track_job(state, RELEASED_JOB)
    track_job(state, KEPT_JOB)

    state.release_job(RELEASED_JOB)

    assert all(RELEASED_JOB not in tracked for tracked in per_job_maps(state))
    assert list(state._manager_job_leaders) == [(KEPT_JOB, "dc-1")]
    assert KEPT_JOB in state._jobs and KEPT_JOB in state._request_routing_locks


def test_a_finished_job_is_forgotten_once_the_retention_has_passed() -> None:
    state = ClientState()
    track_job(state, RELEASED_JOB)
    track_job(state, KEPT_JOB)
    state._job_events[RELEASED_JOB].set()

    assert state.release_finished_jobs(now=100.0, retention_seconds=RETENTION_SECONDS) == []
    assert state.release_finished_jobs(now=100.0 + RETENTION_SECONDS - 1.0, retention_seconds=RETENTION_SECONDS) == []

    released = state.release_finished_jobs(now=100.0 + RETENTION_SECONDS, retention_seconds=RETENTION_SECONDS)

    assert released == [RELEASED_JOB]
    assert RELEASED_JOB not in state._jobs
    assert KEPT_JOB in state._jobs
    assert state._job_finished_seen_at == {}


def make_client(state: ClientState, now: float) -> tuple[HyperscaleClient, AsyncMock]:
    submit = AsyncMock(return_value="job-new")
    client = object.__new__(HyperscaleClient)
    client._state = state
    client._clock = SimpleNamespace(monotonic=lambda: now)
    client._config = SimpleNamespace(job_retention_seconds=RETENTION_SECONDS)
    client._submitter = SimpleNamespace(submit_job=submit)
    return client, submit


@pytest.mark.asyncio
async def test_every_submission_sweeps_finished_jobs() -> None:
    state = ClientState()
    track_job(state, RELEASED_JOB)
    state._job_events[RELEASED_JOB].set()
    state._job_finished_seen_at[RELEASED_JOB] = 0.0
    client, submit = make_client(state, now=RETENTION_SECONDS)

    job_id = await client.submit_job(workflows=[])

    assert job_id == "job-new"
    submit.assert_awaited_once()
    assert RELEASED_JOB not in state._jobs


def test_a_caller_can_release_a_job_it_is_done_with() -> None:
    state = ClientState()
    track_job(state, RELEASED_JOB)
    client, _ = make_client(state, now=0.0)

    client.release_job(RELEASED_JOB)

    assert all(RELEASED_JOB not in tracked for tracked in per_job_maps(state))
