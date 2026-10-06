"""
A best-effort job's workflow results reach the client before it completes.

When a best-effort job completed without some datacenters, the gate first
cancelled the job in them -- waiting out RPC timeouts to the lost
datacenter -- and only then released the per-workflow results that were
waiting on it, seconds after the client had the terminal result. The
results now go out from the datacenters that reported ahead of the
terminal result; the cancellation follows in the background.

* with unreported datacenters, the waiting workflow results are released
  before the global result is pushed, and the cancellation is scheduled,
  not awaited;
* without unreported datacenters nothing is released early.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import GlobalJobResult
from hyperscale.distributed.nodes.gate.server import GateServer

JOB_ID = "job-1"


def make_gate(events: list[str]) -> GateServer:
    gate = object.__new__(GateServer)

    async def release_workflow_results(job_id: str) -> None:
        events.append("release-workflow-results")

    async def push_global_job_result(result: GlobalJobResult) -> None:
        events.append("push-global-result")

    async def finalize_terminal_job(job_id: str, **kwargs) -> None:
        events.append("finalize")

    async def best_effort_cleanup(job_id: str) -> None:
        events.append("best-effort-cleanup")

    @asynccontextmanager
    async def lock_job(job_id: str):
        yield

    def run(call, *args, **kwargs) -> None:
        events.append(f"scheduled:{call.__name__}")

    gate._release_workflow_results = release_workflow_results
    gate._push_global_job_result = push_global_job_result
    gate._finalize_terminal_job = finalize_terminal_job
    gate._handle_update_by_tier = lambda *args: events.append("tier-update")
    gate._job_manager = SimpleNamespace(lock_job=lock_job, get_job=lambda job_id: None)
    gate._best_effort_manager = SimpleNamespace(cleanup=best_effort_cleanup)
    gate._task_runner = SimpleNamespace(run=run)
    gate._dispatch_to_reporters = SimpleNamespace(__name__="_dispatch_to_reporters")
    gate._abandon_unreported_datacenters = SimpleNamespace(__name__="_abandon_unreported_datacenters")
    return gate


@pytest.mark.asyncio
async def test_waiting_workflow_results_go_out_before_the_terminal_result() -> None:
    events: list[str] = []
    gate = make_gate(events)
    global_result = GlobalJobResult(
        job_id=JOB_ID,
        status="completed",
        completion_reason="best_effort: min_dcs_reached (1/1)",
        unreported_datacenters=["dc-west"],
    )

    await GateServer._finish_job_with_global_result(gate, JOB_ID, "running", global_result)

    assert events.index("release-workflow-results") < events.index("push-global-result")
    assert "scheduled:_abandon_unreported_datacenters" in events


@pytest.mark.asyncio
async def test_a_job_every_datacenter_reported_releases_nothing_early() -> None:
    events: list[str] = []
    gate = make_gate(events)
    global_result = GlobalJobResult(job_id=JOB_ID, status="completed")

    await GateServer._finish_job_with_global_result(gate, JOB_ID, "running", global_result)

    assert "release-workflow-results" not in events
    assert "scheduled:_abandon_unreported_datacenters" not in events
