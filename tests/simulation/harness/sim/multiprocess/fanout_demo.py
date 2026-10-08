"""
Fanout: one gateless manager, several workers and many clients each
submitting many jobs at once -- the picklable child entries of the fanout
scenarios.

The manager's AD-54 workflow lifecycle is observed
(``observe_workflow_lifecycle``: every transition by job ordinal, plus
its job, lifecycle and dispatcher counts), along with how many entries
its per-node state tables hold in all and how many per-job Raft groups
it runs. Each worker records every dispatch it runs and how many entries
its per-workflow state tables hold. Each client records, per job in the
order it submitted them, when it was accepted and when (and how) it
ended.

Rows carry ordinals, counts, statuses and virtual times only -- inside
the replay contract.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
from pathlib import Path

from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.nodes.worker.server import WorkerServer

from .job_dispatch_demo import SimPingWorkflow
from .peered_manager_demo import _env
from .workflow_lifecycle_demo import observe_workflow_lifecycle

WATCH_INTERVAL_SECONDS = 0.5


def _table_entries(owner: object) -> int:
    """How many entries the dict and set attributes of ``owner`` hold in
    all -- its per-node bookkeeping, whatever each table is keyed by."""
    return sum(
        len(value)
        for name in dir(owner)
        if name.startswith("_") and not name.startswith("__")
        if isinstance(value := getattr(owner, name, None), (dict, set))
    )


def fanout_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    job_retention_seconds,
    job_cleanup_interval_seconds,
) -> None:
    """Gateless manager child, its workflow lifecycle observed (see
    ``observe_workflow_lifecycle``), completed jobs retained
    ``job_retention_seconds`` and swept every
    ``job_cleanup_interval_seconds``. Also logs ``("state-entries", n,
    t)`` -- the entries its ``ManagerState`` tables hold in all -- and
    ``("raft-groups", n, t)`` on change."""
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(
            COMPLETED_JOB_MAX_AGE=job_retention_seconds,
            FAILED_JOB_MAX_AGE=job_retention_seconds,
            JOB_CLEANUP_INTERVAL=job_cleanup_interval_seconds,
        ),
        dc_id=datacenter_id,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

    async def watch() -> None:
        last_values: dict[str, int] = {}
        while True:
            values = {
                "state-entries": _table_entries(manager._manager_state),
                "raft-groups": manager._raft.consensus.active_instance_count,
            }
            for tag, value in values.items():
                if last_values.get(tag) != value:
                    last_values[tag] = value
                    log.append((tag, value, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())
    context.loop.create_task(watch())
    observe_workflow_lifecycle(context, manager, log)


def fanout_worker_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    seed_manager_address,
    total_cores,
) -> None:
    """Worker child with ``WORKER_MAX_CORES`` = ``total_cores``. Logs
    ``("dispatch-run", distinct, t)`` for every dispatch it runs --
    ``distinct`` how many different workflows it has run, so a workflow
    run twice shows as a run that adds none -- and ``("state-entries", n,
    t)`` -- the entries its ``WorkerState`` tables and core allocator hold
    in all -- on change."""
    worker = WorkerServer(
        host,
        tcp_port,
        udp_port,
        _env(WORKER_MAX_CORES=total_cores),
        dc_id=datacenter_id,
        seed_managers=[seed_manager_address],
        **context.sim_kwargs(),
        process_spawner=context,
    )
    log: list = []
    context.set_result(log)
    workflows_run: set[str] = set()
    handle_dispatch_execution = worker._handle_dispatch_execution

    async def counting_dispatch_execution(dispatch, address, allocation_result) -> bytes:
        workflows_run.add(dispatch.workflow_id)
        log.append(("dispatch-run", len(workflows_run), round(context.loop.time(), 6)))
        return await handle_dispatch_execution(dispatch, address, allocation_result)

    worker._handle_dispatch_execution = counting_dispatch_execution

    async def run() -> None:
        await worker.start()
        log.append(("worker-started", round(context.loop.time(), 6)))

    async def watch() -> None:
        last_entries = -1
        while True:
            entries = _table_entries(worker._worker_state) + _table_entries(worker._core_allocator)
            if entries != last_entries:
                last_entries = entries
                log.append(("state-entries", entries, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())
    context.loop.create_task(watch())


def fanout_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    jobs,
    submit_at,
    job_timeout_seconds,
    wait_timeout_seconds,
) -> None:
    """Client child: at ``submit_at`` submit ``jobs`` ``SimPingWorkflow``
    jobs at once and await every one. Logs ``("submit-rejected", ordinal,
    error type, t)`` per refused call, ``("job-accepted", ordinal, t)``
    and ``("job-finished", ordinal, status, t)``; ``ordinal`` is the
    job's place in this client's submissions."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def submit_and_await(ordinal: int) -> None:
        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], SimPingWorkflow())],
                    vus=2,
                    timeout_seconds=job_timeout_seconds,
                )
            except Exception as submit_error:
                log.append(("submit-rejected", ordinal, type(submit_error).__name__, round(context.loop.time(), 6)))
        log.append(("job-accepted", ordinal, round(context.loop.time(), 6)))
        result = await client.wait_for_job(job_id, timeout=wait_timeout_seconds)
        log.append(("job-finished", ordinal, result.status, round(context.loop.time(), 6)))

    async def run() -> None:
        await client.start()
        await asyncio.sleep(max(0.0, submit_at - context.loop.time()))
        await asyncio.gather(*(submit_and_await(ordinal) for ordinal in range(jobs)))

    context.loop.create_task(run())
