"""
The node dashboards against a live manager and worker, in-process: both
run on loopback ports (the worker with its real executor pool), the
dashboard renders "ci" frames into a pipe standing in for the terminal,
and each test reads the frames the operator would see.

- A worker registering raises the manager's worker count, cores and table.
- The worker's own dashboard shows the manager it reports to, and counts
  the workflows that end while it watches by the status they ended with.

Run apart from tests/unit: a worker's executor processes start with
"spawn", re-importing the test runner, which a unit run's sys.path (its
``tests/unit/logging`` package shadows ``logging``) breaks.
"""

import asyncio
import pathlib

from hyperscale.distributed.models import WorkflowProgress
from hyperscale.ui.node_dashboard import ManagerDashboardReader, WorkerDashboardReader
from tests.integration.cli.node_processes import reserve_port_blocks, worker_port_span
from tests.integration.ui.node_dashboard_harness import (
    SHUTDOWN_SECONDS,
    WORKER_CORES,
    WORKER_REGISTRATION_SECONDS,
    ci_dashboard,
    dashboard_env,
    dashboard_tasks,
    new_worker,
    roomy_terminal,
    start_manager,
    terminal_pipe,
    wait_for_frame,
)

__all__ = ["roomy_terminal"]


async def test_a_registering_worker_shows_on_the_manager_dashboard(tmp_path: pathlib.Path) -> None:
    manager_port, worker_port = reserve_port_blocks([2, 2 + worker_port_span(WORKER_CORES)])
    env = dashboard_env(tmp_path)
    manager = await start_manager(env, manager_port)
    worker = new_worker(env, worker_port, manager_port)
    try:
        async with terminal_pipe() as collected:
            dashboard = ci_dashboard(ManagerDashboardReader(manager), manager, env, tmp_path / "manager.log")
            await dashboard.start()
            try:
                frame = await wait_for_frame(collected, "WORKERS 0", "CLUSTER standalone")
                assert f"tcp 127.0.0.1:{manager_port}" in frame
                assert "dc DC-DASH" in frame
                # The status line names the log file (clipped to the screen's width).
                assert f"ctrl-c stops the node | logs {str(tmp_path)[:40]}" in frame

                await asyncio.wait_for(worker.start(), timeout=WORKER_REGISTRATION_SECONDS)
                frame = await wait_for_frame(collected, "WORKERS 1", f"cores {WORKER_CORES} free {WORKER_CORES}")
                assert f"127.0.0.1:{worker_port}" in frame, "the worker's row is missing from the table"
            finally:
                await dashboard.stop()

        assert dashboard_tasks() == []
    finally:
        await worker.abort_and_wait(timeout=SHUTDOWN_SECONDS)
        await manager.abort_and_wait(timeout=SHUTDOWN_SECONDS)


async def test_the_worker_dashboard_shows_its_manager_and_counts_ended_workflows(tmp_path: pathlib.Path) -> None:
    manager_port, worker_port = reserve_port_blocks([2, 2 + worker_port_span(WORKER_CORES)])
    env = dashboard_env(tmp_path)
    manager = await start_manager(env, manager_port)
    worker = new_worker(env, worker_port, manager_port)
    try:
        await asyncio.wait_for(worker.start(), timeout=WORKER_REGISTRATION_SECONDS)
        async with terminal_pipe() as collected:
            dashboard = ci_dashboard(WorkerDashboardReader(worker), worker, env, tmp_path / "worker.log")
            await dashboard.start()
            try:
                await wait_for_frame(
                    collected,
                    f"primary 127.0.0.1:{manager_port}",
                    "MANAGERS connected",
                    f"CORES {WORKER_CORES} free {WORKER_CORES}",
                )

                # A workflow runs, then ends failed: the worker's own
                # bookkeeping (add, then mark and remove) as its executor
                # does it.
                progress = WorkflowProgress(
                    job_id="job-dash",
                    workflow_id="workflow-dash",
                    workflow_name="DashWorkflow",
                    status="running",
                    completed_count=42,
                    failed_count=1,
                    rate_per_second=7.5,
                    elapsed_seconds=1.0,
                )
                worker._worker_state.add_active_workflow("workflow-dash", progress, ("127.0.0.1", manager_port))
                await wait_for_frame(collected, "WORKFLOWS running 1", "DashWorkflow", "actions 42")

                progress.status = "failed"
                worker._worker_state.remove_active_workflow("workflow-dash")
                frame = await wait_for_frame(collected, "WORKFLOWS running 0", "failed 1 cancelled 0")
                assert "DashWorkflow" not in frame, "the ended workflow is still in the table"
            finally:
                await dashboard.stop()
    finally:
        await worker.abort_and_wait(timeout=SHUTDOWN_SECONDS)
        await manager.abort_and_wait(timeout=SHUTDOWN_SECONDS)

