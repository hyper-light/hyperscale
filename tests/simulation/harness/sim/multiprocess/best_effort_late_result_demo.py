"""
AD-44 late datacenter results over the multi-process coordinator -- the
picklable gate and client entries for ``test_multiprocess_best_effort_late_results``.

* ``late_result_gate_entry`` -- a real durable ``GateServer`` (ledger on a
  SimFilesystem) fronting two datacenters under a chosen
  ``BEST_EFFORT_LATE_RESULT_POLICY``. Records the AD-44 log entries it
  writes and its ledger record of each job it holds.
* ``late_result_client_entry`` -- submits one two-datacenter best-effort
  job and, after ``wait_for_job`` returns, keeps recording every change
  of the job's result the gate pushes.

Milestones are value-shaped -- ``(tag, values..., virtual_time)``, never
node or job ids -- so identical-seed runs compare equal.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
from pathlib import Path

from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.raft.store import RaftStore
from hyperscale.distributed.swim.core.node_id_model import NodeId
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.logging.hyperscale_logging_models import (
    BestEffortCompletion,
    LateDatacenterResult,
)

from .gate_ledger_demo import _env
from .gate_replica_durability_demo import _StoreLog
from .soak_job_demo import SimSoakWorkflow

WATCH_INTERVAL_SECONDS = 0.5


def late_result_gate_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_managers,
    datacenter_manager_udp,
    late_result_policy,
    mutation=None,
) -> None:
    """Gate child with a durable ledger and Raft store (on this process's
    SimFilesystem, so a restarted generation resumes them) and the given
    late-result policy.

    ``mutation`` (a ``MUTATIONS`` key, for the scenario's mutation checks)
    swaps one AD-44 decision for its broken form -- the checks prove the
    scenario's assertions catch it.

    Milestones:

    * ``("gate-started", t)``
    * ``("late-datacenter-result", datacenter_id, outcome, job_status, t)``
      for every ``LateDatacenterResult`` it logs
    * ``("best-effort-completion", reason, final, unreported, t)`` for
      every ``BestEffortCompletion`` it logs
    * ``("ledger-job", status, completed_count, t)`` on every change of
      its ledger record of a job its job manager holds
    """
    log: list = []
    context.set_result(log)
    env = _env(BEST_EFFORT_LATE_RESULT_POLICY=late_result_policy)
    store = RaftStore(
        directory=Path(f"/sim/{host}-{tcp_port}/raft"),
        filesystem=context.filesystem,
        random_source=context.random,
        clock=context.clock,
        logger=_StoreLog(context, log),
        task_runner=TaskRunner(),
        set_aside_retained=env.RAFT_SET_ASIDE_RETAINED,
        storage_health=StorageHealth(),
    )

    async def run() -> None:
        fresh_node_id_full = NodeId.generate("global", host=host, port=udp_port).full
        await store.open(
            fresh_node_id_full,
            is_this_node=lambda node_id_full: NodeId.placement_of(node_id_full)
            == NodeId.placement_of(fresh_node_id_full),
        )
        gate = GateServer(
            host,
            tcp_port,
            udp_port,
            env,
            datacenter_managers=datacenter_managers,
            datacenter_manager_udp=datacenter_manager_udp,
            wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
            raft_store=store,
            **context.sim_kwargs(),
        )
        record_ad44_log_entries(gate, log, context)
        if mutation is not None:
            MUTATIONS[mutation](gate)
        await gate.start()
        log.append(("gate-started", round(context.loop.time(), 6)))
        context.loop.create_task(watch_ledger_jobs(gate))

    async def watch_ledger_jobs(gate: GateServer) -> None:
        last_records: dict[str, tuple[str, int]] = {}
        while True:
            for job_id, _job in sorted(gate._job_manager.items()):
                if (ledger_job := gate._job_ledger.get_job(job_id)) is None:
                    continue
                record = (ledger_job.status, ledger_job.completed_count)
                if last_records.get(job_id) != record:
                    last_records[job_id] = record
                    log.append(("ledger-job", *record, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())


def _never_folds_stragglers(gate: GateServer) -> None:
    """Mutation: a released job's straggler is handled like any result for
    an ended job (the late-result ``update`` policy's fold removed)."""
    gate._fold_straggler_result = gate._complete_with_final_result


async def _skip_restore(job_id: str) -> None:
    return None


def _never_restores_provisional(gate: GateServer) -> None:
    """Mutation: a gate leading a job after a restart or a takeover does
    not resume its provisional window from the replica."""
    gate._restore_provisional_release = _skip_restore


def _never_late(gate: GateServer) -> None:
    """Mutation: no final result for an ended job is judged late (the
    ``LateDatacenterResult`` log removed)."""
    gate._is_late_datacenter_result = lambda job_id, job, result: False


MUTATIONS = {
    "never-folds-stragglers": _never_folds_stragglers,
    "never-late": _never_late,
    "never-restores-provisional": _never_restores_provisional,
}


def record_ad44_log_entries(gate: GateServer, log: list, context) -> None:
    """Record each AD-44 log entry the gate writes, then write it as usual."""
    logger = gate._udp_logger
    write_entry = logger.log

    async def recording_log(entry, *args, **kwargs):
        if isinstance(entry, LateDatacenterResult):
            log.append(
                (
                    "late-datacenter-result",
                    entry.datacenter_id,
                    entry.outcome,
                    entry.job_status,
                    round(context.loop.time(), 6),
                )
            )
        elif isinstance(entry, BestEffortCompletion):
            log.append(
                (
                    "best-effort-completion",
                    entry.reason,
                    entry.final,
                    tuple(entry.unreported_datacenters),
                    round(context.loop.time(), 6),
                )
            )
        return await write_entry(entry, *args, **kwargs)

    logger.log = recording_log


def late_result_client_entry(
    context,
    host,
    port,
    gate_tcp_address,
    duration_seconds,
    job_timeout_seconds,
    submit_at,
    best_effort_min_dcs,
    best_effort_deadline_seconds,
) -> None:
    """Client child: one two-datacenter best-effort ``SimSoakWorkflow`` job.

    Milestones: ``("job-submitted", t)``, ``("status-seen", status, t)``,
    ``("job-finished", status, t)`` once ``wait_for_job`` returns, then
    ``("result-seen", status, completion_reason, unreported, is_final,
    reported_datacenters, total_completed, t)`` for the result it then
    holds and every change of it the gate pushes.
    """
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        gates=[gate_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def submit() -> str:
        workflow = SimSoakWorkflow()
        workflow.duration = f"{duration_seconds:g}s"
        workflow.timeout = f"{duration_seconds + 30.0:g}s"
        while True:
            try:
                return await client.submit_job(
                    workflows=[([], workflow)],
                    vus=2,
                    timeout_seconds=job_timeout_seconds,
                    datacenter_count=2,
                    best_effort=True,
                    best_effort_min_dcs=best_effort_min_dcs,
                    best_effort_deadline_seconds=best_effort_deadline_seconds,
                )
            except Exception as submit_error:
                log.append(("submit-rejected", type(submit_error).__name__, round(context.loop.time(), 6)))
                await asyncio.sleep(1.0)

    async def watch_status(job_id: str) -> None:
        last_status: str | None = None
        while True:
            job_result = client.get_job_status(job_id)
            status = job_result.status if job_result is not None else None
            if status != last_status:
                last_status = status
                log.append(("status-seen", status, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    async def watch_result(job_id: str) -> None:
        last_seen: tuple | None = None
        while True:
            job_result = client.get_job_status(job_id)
            seen = (
                job_result.status,
                job_result.completion_reason,
                tuple(job_result.unreported_datacenters),
                job_result.is_final,
                tuple(sorted(dc_result.datacenter for dc_result in job_result.per_datacenter_results)),
                job_result.total_completed,
            )
            if seen != last_seen:
                last_seen = seen
                log.append(("result-seen", *seen, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    async def run() -> None:
        await client.start()
        job_id = await submit()
        log.append(("job-submitted", round(context.loop.time(), 6)))
        status_watcher = context.loop.create_task(watch_status(job_id))
        result = await client.wait_for_job(job_id)
        status_watcher.cancel()
        log.append(("job-finished", result.status, round(context.loop.time(), 6)))
        context.loop.create_task(watch_result(job_id))

    context.loop.call_at(submit_at, lambda: context.loop.create_task(run()))
