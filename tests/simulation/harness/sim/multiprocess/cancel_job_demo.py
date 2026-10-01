"""
Client-initiated job cancellation over the multi-process coordinator --
the picklable client entry for scenarios that judge what a client is
told when it cancels a running job.

The client submits ``SimSustainedPingWorkflow`` (mid-flight for its whole
two-second duration) straight to a manager, cancels it once it is seen
running, and records every ``job_cancellation_complete`` push it receives
-- the count is the point: a cancellation must complete exactly once.

Entries record ``(tag, value, virtual_time)`` milestones only, so
identical-seed runs compare equal.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio

from hyperscale.distributed.models import JobCancellationComplete
from hyperscale.distributed.nodes.client import HyperscaleClient

from .job_dispatch_demo import SimSustainedPingWorkflow, _env
from .simulation_coordinator import SimulationCoordinator
from .worker_manager_demo import manager_entry, worker_entry

STATUS_POLL_SECONDS = 0.1


def cancelling_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    cancel_after_running_seconds,
    observe_seconds,
) -> None:
    """Client child: submit, cancel ``cancel_after_running_seconds`` after
    the job is first seen running, then observe for ``observe_seconds``.

    Logs ``("cancel-response", accepted, t)``, each
    ``("cancellation-push", success, t)`` the client receives, and
    ``("job-finished", status, t)``.
    """
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    completion_handler = client._cancellation_complete_handler
    handle_push = completion_handler.handle

    async def counting_handle(addr, data, clock_time):
        completion = JobCancellationComplete.load(data)
        log.append(("cancellation-push", completion.success, round(context.loop.time(), 6)))
        return await handle_push(addr, data, clock_time)

    completion_handler.handle = counting_handle

    async def submit() -> str:
        while True:
            try:
                return await client.submit_job(
                    workflows=[([], SimSustainedPingWorkflow())],
                    vus=2,
                    timeout_seconds=30.0,
                )
            except Exception as submit_error:
                log.append(("submit-rejected", type(submit_error).__name__, round(context.loop.time(), 6)))
                await asyncio.sleep(1.0)

    async def await_running(job_id: str) -> None:
        while True:
            job_result = client.get_job_status(job_id)
            if job_result is not None and job_result.status == "running":
                log.append(("running-seen", round(context.loop.time(), 6)))
                return
            await asyncio.sleep(STATUS_POLL_SECONDS)

    async def run() -> None:
        await client.start()
        job_id = await submit()
        log.append(("job-submitted", round(context.loop.time(), 6)))
        await await_running(job_id)
        await asyncio.sleep(cancel_after_running_seconds)

        response = await client.cancel_job(job_id, reason="sim cancel")
        log.append(("cancel-response", response.success, round(context.loop.time(), 6)))

        result = await client.wait_for_job(job_id, timeout=observe_seconds)
        log.append(("job-finished", result.status, round(context.loop.time(), 6)))
        await asyncio.sleep(observe_seconds)
        log.append(("observed-until", round(context.loop.time(), 6)))

    context.loop.create_task(run())


def run_cancel_job(
    max_virtual_time: float,
    cancel_after_running_seconds: float,
    observe_seconds: float,
    seed: int,
) -> dict:
    """One manager, one worker, and a client that cancels its running
    job. Returns every child's log."""
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=max_virtual_time, seed=seed)
    coordinator.add_process("manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc")
    coordinator.add_process(
        "worker", worker_entry, "sim-wkr", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
    )
    coordinator.add_process(
        "client",
        cancelling_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        cancel_after_running_seconds,
        observe_seconds,
    )
    return coordinator.run()
