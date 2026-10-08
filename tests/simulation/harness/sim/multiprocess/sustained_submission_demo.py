"""
Sustained-rate submission (SCENARIOS §7 "Sustained. 10 jobs/s for 60 s")
-- the picklable client entry of the sustained-workload scenario. The
manager and workers are ``fanout_demo``'s, observed the same way.

``paced_client_entry`` first submits one job, retrying until the manager
accepts it -- the cluster's readiness, an event of the run, not an
instant chosen in advance. That acceptance anchors the schedule: job
``k`` is submitted at ``anchor + k / rate`` on the virtual clock, for the
whole window, each awaited independently, so a slow job never delays the
next arrival.

Rows carry ordinals, statuses and virtual times only -- inside the
replay contract.

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname.
"""

import asyncio

from hyperscale.distributed.nodes.client import HyperscaleClient

from .child_context import ChildContext
from .job_dispatch_demo import SimPingWorkflow
from .workflow_lifecycle_demo import _env

SUBMIT_RETRY_SECONDS = 0.5


async def _submit_until_accepted(
    context: ChildContext,
    client: HyperscaleClient,
    log: list[tuple[object, ...]],
    ordinal: int,
    job_timeout_seconds: float,
) -> str:
    """Submit one ping job until accepted; each refusal is logged by type."""
    while True:
        try:
            return await client.submit_job(
                workflows=[([], SimPingWorkflow())],
                vus=SimPingWorkflow.vus,
                timeout_seconds=job_timeout_seconds,
            )
        except Exception as submit_error:
            log.append(("submit-rejected", ordinal, type(submit_error).__name__, round(context.loop.time(), 6)))
            await asyncio.sleep(SUBMIT_RETRY_SECONDS)


async def _await_job(
    context: ChildContext,
    client: HyperscaleClient,
    log: list[tuple[object, ...]],
    ordinal: int,
    job_id: str,
) -> None:
    """Await an accepted job's terminal: ``("job-finished", ordinal,
    status, t)``."""
    result = await client.wait_for_job(job_id)
    log.append(("job-finished", ordinal, result.status, round(context.loop.time(), 6)))


async def _submit_job(
    context: ChildContext,
    client: HyperscaleClient,
    log: list[tuple[object, ...]],
    ordinal: int,
    job_timeout_seconds: float,
) -> str:
    """Submit job ``ordinal`` until accepted: ``("job-submitted", ordinal,
    t)`` then ``("job-accepted", ordinal, t)``."""
    log.append(("job-submitted", ordinal, round(context.loop.time(), 6)))
    job_id = await _submit_until_accepted(context, client, log, ordinal, job_timeout_seconds)
    log.append(("job-accepted", ordinal, round(context.loop.time(), 6)))
    return job_id


async def _run_one_job(
    context: ChildContext,
    client: HyperscaleClient,
    log: list[tuple[object, ...]],
    ordinal: int,
    job_timeout_seconds: float,
) -> None:
    """Submit job ``ordinal`` and await its terminal."""
    job_id = await _submit_job(context, client, log, ordinal, job_timeout_seconds)
    await _await_job(context, client, log, ordinal, job_id)


def run_paced_jobs(
    context: ChildContext,
    client: HyperscaleClient,
    log: list[tuple[object, ...]],
    jobs_per_second: float,
    total_jobs: int,
    job_timeout_seconds: float,
) -> None:
    """Once the started ``client``'s first job is accepted (``("anchor",
    t)``), submit one ``SimPingWorkflow`` job every ``1 / jobs_per_second``
    virtual seconds until ``total_jobs`` (the first included) have gone
    out, and await every one."""
    pending_jobs: list[asyncio.Task[None]] = []

    def submit_at_its_arrival(ordinal: int) -> None:
        pending_jobs.append(
            context.loop.create_task(_run_one_job(context, client, log, ordinal, job_timeout_seconds))
        )

    async def run() -> None:
        await client.start()
        first_job_id = await _submit_job(context, client, log, 0, job_timeout_seconds)
        anchor = context.loop.time()
        log.append(("anchor", round(anchor, 6)))
        for ordinal in range(1, total_jobs):
            context.loop.call_at(anchor + ordinal / jobs_per_second, submit_at_its_arrival, ordinal)
        await _await_job(context, client, log, 0, first_job_id)

    context.loop.create_task(run())


def paced_client_entry(
    context: ChildContext,
    host: str,
    port: int,
    manager_tcp_address: tuple[str, int],
    jobs_per_second: float,
    window_seconds: float,
    job_timeout_seconds: float,
) -> None:
    """Client child submitting straight to a manager (``run_paced_jobs``)
    for ``window_seconds``: ``jobs_per_second * window_seconds`` jobs."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list[tuple[object, ...]] = []
    context.set_result(log)
    run_paced_jobs(
        context, client, log, jobs_per_second, round(jobs_per_second * window_seconds), job_timeout_seconds
    )
