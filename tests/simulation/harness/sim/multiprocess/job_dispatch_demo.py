"""
A real job dispatched through the node pair over the multi-process
coordinator — the picklable client entry (and workflow) a test drives.

The client child runs the production ``HyperscaleClient`` (itself a
``MercurySyncBaseServer``): it cloudpickles ``SimPingWorkflow``, submits
it to the manager over the stream boundary with production retry
semantics (the manager rejects until it is leader with worker capacity —
each rejection is part of the deterministic schedule), then awaits the
manager's completion pushes. The manager provisions and dispatches to
the worker; the worker fans the workflow out to its executor-pool
children where ``WorkflowRunner`` generates VUs against *virtual* time
(the workflow ``duration`` elapses on the simulation clock, so the VU
iteration count is deterministic and replays exactly).

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname; the workflow class is cloudpickled by value
across the submission path exactly as in production.
"""

import asyncio
import os
import sys

import cloudpickle

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, step

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


class SimPingWorkflow(Workflow):
    """Minimal deterministic test workflow: two VUs of a half-virtual-second
    step for two virtual seconds."""

    vus = 2
    duration = "2s"

    @step()
    async def ping_action(self) -> dict[str, str]:
        await asyncio.sleep(0.5)
        return {"status": "ok"}


# Ship the workflow class BY VALUE, exactly as a user's script-defined
# workflow travels: the restricted unpickler's module allowlist admits
# hyperscale.* and ``__main__`` (user code) by reference only — a
# by-reference pickle of this tests-tree module is (correctly) refused
# by the manager's security layer. By-value registration reproduces the
# production shape: the class reconstructs from code objects with no
# module import on the receiving side.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def _env() -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET)


def dispatch_client_entry(context, host, port, manager_tcp_address) -> None:
    """Client child: submit ``SimPingWorkflow`` directly to a manager
    (L1/L2 topology) and await job completion.

    Submission retries on virtual time until the manager accepts (leader
    elected + worker registered with capacity); every rejection is
    logged as a milestone so the retry count is pinned by replay.
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
    _run_client_submission(context, client, log)


def gate_dispatch_client_entry(context, host, port, gate_tcp_address) -> None:
    """Client child: submit ``SimPingWorkflow`` through a GATE (the L3
    topology) and await job completion — same production flow, routed
    client -> gate -> datacenter manager -> worker."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        gates=[gate_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    _run_client_submission(context, client, log)


def pinned_gate_dispatch_client_entry(
    context, host, port, gate_tcp_address, pinned_datacenters
) -> None:
    """Client child: submit through a gate with an explicit datacenter
    placement CONSTRAINT (``datacenters=[...]``) — the job must run only
    in the listed datacenters, regardless of what free selection would
    have picked."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        gates=[gate_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    _run_client_submission(
        context, client, log, datacenters=list(pinned_datacenters)
    )


def _run_client_submission(context, client, log: list, datacenters=None) -> None:
    """Shared submit -> watch -> await-completion flow for the client
    entries; identical await ordering regardless of target tier so the
    pinned schedules of existing scenarios stay byte-for-byte.
    ``datacenters=None`` matches ``submit_job``'s own default, so
    existing entries are unchanged; a list applies the placement
    constraint."""

    async def run() -> None:
        await client.start()

        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], SimPingWorkflow())],
                    vus=2,
                    timeout_seconds=30.0,
                    datacenters=datacenters,
                )
            except Exception as submit_error:
                # Production behavior: the manager rejects submissions
                # until it is leader with registered capacity — retry.
                # Log the exception TYPE only: rejection texts can embed
                # per-run values (node ids, addresses), and the log is
                # part of the replay contract.
                log.append(
                    (
                        "submit-rejected",
                        type(submit_error).__name__,
                        round(context.loop.time(), 6),
                    )
                )
                await asyncio.sleep(1.0)

        log.append(("job-submitted", round(context.loop.time(), 6)))

        async def watch_status() -> None:
            last_status: str | None = None
            while True:
                job_result = client.get_job_status(job_id)
                status = job_result.status if job_result is not None else None
                if status != last_status:
                    last_status = status
                    log.append(
                        ("status-seen", status, round(context.loop.time(), 6))
                    )
                await asyncio.sleep(0.5)

        status_watcher = context.loop.create_task(watch_status())
        result = await client.wait_for_job(job_id, timeout=45.0)
        status_watcher.cancel()
        log.append(("job-finished", result.status, round(context.loop.time(), 6)))

    context.loop.create_task(run())
