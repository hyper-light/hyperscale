"""
Cross-datacenter dependency chains (SCENARIOS §7 "Cross-DC dependencies
(L3). Workflow B in DC-east depends on A in DC-west") -- the picklable
client entry of the cross-DC chain scenario.

A job's workflows are placed together: the gate routes the whole job to
the datacenters it picks, and each runs the job's dependency graph. B
comes to run in another datacenter than the A it depends on when the
datacenter running the chain is lost between them: AD-36 Part 13 moves
the lost datacenter's unfinished workflows (B) to a replacement within
the job's placement, and re-runs B's ancestors (A) there only for the
context B reads (AD-49) -- A's counted result stays the one the lost
datacenter delivered.

``chain_client_entry`` submits the chain ``SimChainA -> SimChainB``
through a gate, placed in one of two datacenters, and records each
workflow result the gate pushes -- with the datacenters whose results it
aggregates and the datacenter each re-ran -- plus the job's terminal.

Rows carry workflow names, datacenter names, statuses and virtual times
only -- inside the replay contract.

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname. The workflow classes are built inside a
function, so cloudpickle ships them by value.
"""

import asyncio

from hyperscale.distributed.models import WorkflowResultPush
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, step

from .child_context import ChildContext
from .workflow_lifecycle_demo import _env

CHAIN_VUS = 1
SUBMIT_RETRY_SECONDS = 1.0
STATUS_POLL_SECONDS = 0.5


def build_chain_workflows(
    upstream_seconds: float,
    downstream_seconds: float,
) -> tuple[type[Workflow], type[Workflow]]:
    """``SimChainA`` (one ACTION step of ``upstream_seconds``) and
    ``SimChainB`` (one of ``downstream_seconds``), B to depend on A."""

    class SimChainA(Workflow):
        vus = CHAIN_VUS
        duration = f"{2 * upstream_seconds:g}s"

        @step()
        async def upstream_action(self) -> dict[str, str]:
            await asyncio.sleep(upstream_seconds)
            return {"leg": "upstream"}

    class SimChainB(Workflow):
        vus = CHAIN_VUS
        duration = f"{2 * downstream_seconds:g}s"

        @step()
        async def downstream_action(self) -> dict[str, str]:
            await asyncio.sleep(downstream_seconds)
            return {"leg": "downstream"}

    return SimChainA, SimChainB


def _record_workflow_result(context: ChildContext, log: list[tuple[object, ...]], push: WorkflowResultPush) -> None:
    """``("workflow-result", name, status, datacenters aggregated,
    datacenters that re-ran it, t)``."""
    log.append(
        (
            "workflow-result",
            push.workflow_name,
            push.status,
            tuple(sorted(dc_result.datacenter for dc_result in push.per_dc_results)),
            tuple(sorted(dc_result.datacenter for dc_result in push.per_dc_results if dc_result.rerun_of)),
            round(context.loop.time(), 6),
        )
    )


def chain_client_entry(
    context: ChildContext,
    host: str,
    port: int,
    gate_tcp_address: tuple[str, int],
    placement: list[str],
    upstream_seconds: float,
    downstream_seconds: float,
    job_timeout_seconds: float,
) -> None:
    """Client child: submit ``SimChainA -> SimChainB`` through the gate,
    to run in one datacenter of ``placement``, and await the job.
    Records ``("job-submitted", t)``, every ``("status-seen", status,
    t)``, each ``workflow-result`` row and ``("job-finished", status,
    t)``."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        gates=[gate_tcp_address],
        **context.sim_kwargs(),
    )
    log: list[tuple[object, ...]] = []
    context.set_result(log)
    upstream_class, downstream_class = build_chain_workflows(upstream_seconds, downstream_seconds)

    async def submit_until_accepted() -> str:
        while True:
            try:
                return await client.submit_job(
                    workflows=[([], upstream_class()), ([upstream_class.__name__], downstream_class())],
                    vus=CHAIN_VUS,
                    timeout_seconds=job_timeout_seconds,
                    datacenters=list(placement),
                    datacenter_count=1,
                    on_workflow_result=lambda push: _record_workflow_result(context, log, push),
                )
            except Exception as submit_error:
                log.append(("submit-rejected", type(submit_error).__name__, round(context.loop.time(), 6)))
                await asyncio.sleep(SUBMIT_RETRY_SECONDS)

    async def watch_status(job_id: str) -> None:
        last_status: str | None = None
        while True:
            job_result = client.get_job_status(job_id)
            status = job_result.status if job_result is not None else None
            if status != last_status:
                last_status = status
                log.append(("status-seen", status, round(context.loop.time(), 6)))
            await asyncio.sleep(STATUS_POLL_SECONDS)

    async def run() -> None:
        await client.start()
        job_id = await submit_until_accepted()
        log.append(("job-submitted", round(context.loop.time(), 6)))
        status_watcher = context.loop.create_task(watch_status(job_id))
        result = await client.wait_for_job(job_id)
        status_watcher.cancel()
        log.append(("job-finished", result.status, round(context.loop.time(), 6)))

    context.loop.create_task(run())
