"""
A submission storm against a manager driven to its highest overload level
(AD-18 OVERLOADED: AD-22 sheds everything but CRITICAL, the AD-24 limiter
refuses every non-CONTROL request) -- the picklable child entries of the
rejection-storm scenarios.

The manager's host readings follow a CPU script
(``ScriptedResourceMonitor``: SIM has no CPU, so the storm's cost on the
host is scripted); the detector, its hysteresis, the transport's AD-24
admission, the load shedder and every client are production code.

The manager records its overload state, its worker registry, every
SWIM death it declares, how it admitted each ``job_submission`` (with the
retry hint of each refusal), how many other requests it refused, and the
sizes of its per-client and per-job tables. Rows carry counts, states,
configured host names and virtual times only -- inside the replay
contract.

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
from .scripted_resource_monitor import ScriptedResourceMonitor

WATCH_INTERVAL_SECONDS = 0.25


def overloaded_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    cpu_steps,
    memory_percent,
) -> None:
    """Gateless manager child whose host readings follow ``cpu_steps``
    (``(at, cpu_percent)``) at a constant ``memory_percent``. Logs
    ``("overload", state, t)``, ``("worker-count", n, t)``,
    ``("unhealthy-workers", n, t)``, ``("jobs", n, t)``,
    ``("jobs-completed", n, t)``, ``("rate-limit-clients", counters,
    stress_counters, activity, t)``, ``("idempotency-entries", n, t)`` and
    ``("refused", handler, count, t)`` on change; ``("node-dead", host,
    t)`` for every SWIM death it declares; and ``("submission-admission",
    allowed, retry_after, client_host, t)`` for every ``job_submission``
    the transport admitted or refused."""
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id=datacenter_id,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    manager._resource_monitor = ScriptedResourceMonitor(context.loop, cpu_steps, memory_percent)
    log: list = []
    context.set_result(log)
    refusals: dict[str, int] = {}

    rate_limiter = manager._rate_limiter
    check_handler = rate_limiter.check_handler

    async def observed_check_handler(address, handler_name, priority):
        # The level the admission is judged at.
        level = manager._overload_detector.current_state.value
        admission = await check_handler(address, handler_name, priority)
        if handler_name == "job_submission":
            log.append(
                (
                    "submission-admission",
                    level,
                    admission.allowed,
                    admission.retry_after_seconds,
                    address[0],
                    round(context.loop.time(), 6),
                )
            )
        elif not admission.allowed:
            refusals[handler_name] = refusals.get(handler_name, 0) + 1
        return admission

    rate_limiter.check_handler = observed_check_handler
    manager.register_on_node_dead(
        lambda node_address: log.append(("node-dead", node_address[0], round(context.loop.time(), 6)))
    )

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

    async def watch() -> None:
        last_values: dict[str, object] = {}
        adaptive_limiter = rate_limiter._adaptive
        while True:
            jobs = list(manager._job_manager.iter_jobs())
            idempotency_ledger = manager._idempotency_ledger
            values: dict[str, object] = {
                "overload": manager._overload_detector.current_state.value,
                "worker-count": manager._manager_state.get_worker_count(),
                "unhealthy-workers": len(manager._manager_state.iter_worker_unhealthy_since()),
                "jobs": len(jobs),
                "jobs-completed": sum(1 for job in jobs if job.status == "completed"),
                "rate-limit-clients": (
                    len(adaptive_limiter._operation_counters),
                    len(adaptive_limiter._client_stress_counters),
                    len(adaptive_limiter._client_last_activity),
                ),
                "idempotency-entries": 0 if idempotency_ledger is None else len(idempotency_ledger._index),
            }
            values.update({f"refused:{name}": count for name, count in refusals.items()})
            for tag, value in values.items():
                if last_values.get(tag) != value:
                    last_values[tag] = value
                    if tag.startswith("refused:"):
                        log.append(("refused", tag.removeprefix("refused:"), value, round(context.loop.time(), 6)))
                    elif isinstance(value, tuple):
                        log.append((tag, *value, round(context.loop.time(), 6)))
                    else:
                        log.append((tag, value, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())
    context.loop.create_task(watch())


def storm_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    submitters,
    storm_start,
    storm_end,
    job_timeout_seconds,
    wait_timeout_seconds,
) -> None:
    """Client child: from ``storm_start`` until ``storm_end``,
    ``submitters`` concurrent loops each call ``submit_job`` back to back
    (a ``SimPingWorkflow`` each), never pausing of their own accord -- the
    client's own retry policy, honoring the cluster's hints, is all that
    paces them. Logs ``("storm-call", outcome, t)`` per call (``accepted``
    or the refusal's exception type) and, once every loop stopped, awaits
    each accepted job: ``("storm-job-finished", status, t)``."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    accepted_job_ids: list[str] = []

    async def submit_until_the_storm_ends() -> None:
        while context.loop.time() < storm_end:
            try:
                job_id = await client.submit_job(
                    workflows=[([], SimPingWorkflow())],
                    vus=2,
                    timeout_seconds=job_timeout_seconds,
                )
            except Exception as submit_error:
                log.append(("storm-call", type(submit_error).__name__, round(context.loop.time(), 6)))
                continue
            accepted_job_ids.append(job_id)
            log.append(("storm-call", "accepted", round(context.loop.time(), 6)))

    async def run() -> None:
        await client.start()
        await asyncio.sleep(max(0.0, storm_start - context.loop.time()))
        await asyncio.gather(*(submit_until_the_storm_ends() for _ in range(submitters)))
        log.append(("storm-stopped", round(context.loop.time(), 6)))
        for job_id in accepted_job_ids:
            result = await client.wait_for_job(job_id, timeout=wait_timeout_seconds)
            log.append(("storm-job-finished", result.status, round(context.loop.time(), 6)))

    context.loop.create_task(run())


def late_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    submit_at,
    job_timeout_seconds,
    wait_timeout_seconds,
) -> None:
    """Client child: at ``submit_at`` submit one ``SimPingWorkflow`` and
    await it. Logs ``("submit-rejected", error type, t)`` per refused
    call, ``("job-submitted", t)`` and ``("job-finished", status, t)``."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await client.start()
        await asyncio.sleep(max(0.0, submit_at - context.loop.time()))
        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], SimPingWorkflow())],
                    vus=2,
                    timeout_seconds=job_timeout_seconds,
                )
            except Exception as submit_error:
                log.append(("submit-rejected", type(submit_error).__name__, round(context.loop.time(), 6)))
        log.append(("job-submitted", round(context.loop.time(), 6)))
        result = await client.wait_for_job(job_id, timeout=wait_timeout_seconds)
        log.append(("job-finished", result.status, round(context.loop.time(), 6)))

    context.loop.create_task(run())


def tuned_worker_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    seed_manager_address,
    total_cores,
    env_overrides,
) -> None:
    """Worker child with ``WORKER_MAX_CORES`` = ``total_cores`` and
    ``env_overrides`` (``Env`` fields) applied. Logs ``("worker-started",
    t)``, and ``("workflows-active", n, t)`` and ``("pending-results", n,
    t)`` -- final results it holds for a later resend -- on change."""
    worker = WorkerServer(
        host,
        tcp_port,
        udp_port,
        _env(WORKER_MAX_CORES=total_cores, **env_overrides),
        dc_id=datacenter_id,
        seed_managers=[seed_manager_address],
        **context.sim_kwargs(),
        process_spawner=context,
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await worker.start()
        log.append(("worker-started", round(context.loop.time(), 6)))

    async def watch() -> None:
        last_values: dict[str, int] = {}
        while True:
            values = {
                "workflows-active": len(worker._active_workflows),
                "pending-results": worker._progress_reporter.get_pending_result_count(),
            }
            for tag, value in values.items():
                if last_values.get(tag) != value:
                    last_values[tag] = value
                    log.append((tag, value, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())
    context.loop.create_task(watch())
