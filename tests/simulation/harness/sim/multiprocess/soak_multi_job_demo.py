"""
Gateless (L2) LONG-HORIZON soak entries — the picklable children the
``tests/simulation/soak`` suite drives.

The committed entries cannot occupy a 1000s+ horizon in the gateless
topology: ``soak_gate_dispatch_client_entry`` submits through a GATE
(and gate topologies fail second/late jobs by design today — the
pinned gaps of commit c8b6cf99), while the gateless
``dispatch_client_entry`` / ``two_job_client_entry`` drive at most two
fixed ~2s ping jobs and RAISE out of ``wait_for_job`` expiry. The
entries here drive N sequential ``SimSoakWorkflow`` jobs at staggered
virtual submit instants from ONE client that stays alive to the
ceiling (a vanished client would trip the manager's probed
client-orphan virtual-time spin at ~vanish+150s — c8b6cf99).

Milestone vocabulary (value-shaped for the replay contract — ``(tag,
virtual_time[, small-value])`` only, never node ids or snowflakes),
job-scoped with the ``job<k>-`` prefix convention of
``two_job_client_entry``:

* ``("job<k>-rejected", ExcName, t)`` — each refused submission
* ``("job<k>-submitted", t)`` — acceptance
* ``("job<k>-status-seen", status, t)`` — every observed transition
* ``("job<k>-wait-timed-out", t)`` — ``wait_for_job`` deadline expiry;
  the entry keeps waiting UNBOUNDED (loud, never silent) so a late
  terminal still lands in the log
* ``("job<k>-finished", status, t)`` — result delivery (exactly once)

The manager entry mirrors ``worker_manager_demo.manager_entry`` but
records EVERY worker-registry count transition (``("worker-count", n,
t)``, the ``evicting_manager_entry`` watcher pattern) instead of the
one-shot registered/lost pair: over a long horizon with a mid-horizon
host kill and a late-joining replacement, membership convergence is a
SEQUENCE (0 -> 1 -> 2 -> 1 or 0 -> 1 -> 0 -> 1), not two instants.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname; ``SimSoakWorkflow`` is reused from
``soak_job_demo`` (already registered for by-value cloudpickle there,
so the workflow crosses the submission path exactly as a user's
script-defined class).
"""

import asyncio
import os
from pathlib import Path

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.manager.server import ManagerServer

from .soak_job_demo import SimSoakWorkflow
from .worker_manager_demo import apply_storage_fault_schedule

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env() -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET)


def soak_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    storage_fault_schedule=(),
) -> None:
    """Manager child for long-horizon soaks: a real gateless
    ``ManagerServer`` (WAL enabled — the full durable tier) plus a
    continuous worker-registry count watcher.

    Milestones: ``("manager-started", t)`` once, then ``("worker-count",
    n, t)`` on every registry-count transition at 0.5s virtual cadence —
    registration, SWIM-death reaping, and late-join recovery all appear
    as count steps on the one timeline.
    """
    apply_storage_fault_schedule(context, storage_fault_schedule)
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id=datacenter_id,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

    async def watch_worker_count() -> None:
        last_count = -1
        while True:
            count = manager._manager_state.get_worker_count()
            if count != last_count:
                last_count = count
                log.append(
                    ("worker-count", count, round(context.loop.time(), 6))
                )
            await asyncio.sleep(0.5)

    context.loop.create_task(run())
    context.loop.create_task(watch_worker_count())


def soak_multi_job_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    submit_times,
    durations,
    job_timeout_seconds,
    wait_timeout_seconds,
) -> None:
    """Client child: submit ``len(submit_times)`` sequential
    ``SimSoakWorkflow`` jobs directly to a manager (gateless L2), one
    per staggered virtual submit instant, and await each to its
    terminal.

    * ``submit_times`` — ascending virtual instants; job ``k`` submits
      at ``max(submit_times[k-1], previous job's terminal)`` (strictly
      sequential: at most ONE job is in flight, so a mid-horizon fault
      intersects a known job and the occupancy schedule stays
      deterministic under any recovery latency).
    * ``durations`` — per-job workflow duration in virtual seconds
      (parallel to ``submit_times``); each job gets a FRESH workflow
      instance with a proportionate workflow timeout.
    * ``job_timeout_seconds`` — the job-level timeout carried on every
      submission; sized by the plan so the INTENDED fault outcome (not
      an accidental timeout) decides each job.
    * ``wait_timeout_seconds`` — per-job ``wait_for_job`` deadline;
      expiry is recorded loudly and the entry then waits UNBOUNDED so a
      stranded job stays visible instead of killing the run.

    The entry (and its client) stays alive to the simulation ceiling —
    every completion push has a live destination, and the manager's
    dead-client orphan path is deliberately NOT in this scenario space.
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

    async def submit_and_await(job_index: int, workflow_duration: float) -> None:
        workflow = SimSoakWorkflow()
        workflow.duration = f"{workflow_duration:g}s"
        workflow.timeout = f"{workflow_duration + 30.0:g}s"

        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], workflow)],
                    vus=2,
                    timeout_seconds=job_timeout_seconds,
                )
            except Exception as submit_error:
                # Production behavior: the manager rejects until it is
                # leader with registered capacity (including the
                # zero-capacity window after a host kill) — retry on
                # virtual time. Type name only: rejection texts can
                # embed per-run values.
                log.append(
                    (
                        f"job{job_index}-rejected",
                        type(submit_error).__name__,
                        round(context.loop.time(), 6),
                    )
                )
                await asyncio.sleep(1.0)

        log.append((f"job{job_index}-submitted", round(context.loop.time(), 6)))

        async def watch_status() -> None:
            last_status: str | None = None
            while True:
                job_result = client.get_job_status(job_id)
                status = job_result.status if job_result is not None else None
                if status != last_status:
                    last_status = status
                    log.append(
                        (
                            f"job{job_index}-status-seen",
                            status,
                            round(context.loop.time(), 6),
                        )
                    )
                await asyncio.sleep(0.5)

        status_watcher = context.loop.create_task(watch_status())
        try:
            result = await client.wait_for_job(
                job_id, timeout=wait_timeout_seconds
            )
        except asyncio.TimeoutError:
            # LOUD, then keep waiting: the ceiling bounds the run, and a
            # late terminal must still reach the log.
            log.append(
                (f"job{job_index}-wait-timed-out", round(context.loop.time(), 6))
            )
            result = await client.wait_for_job(job_id)
        status_watcher.cancel()
        log.append(
            (
                f"job{job_index}-finished",
                result.status,
                round(context.loop.time(), 6),
            )
        )

    async def run() -> None:
        await client.start()
        for job_index, submit_at in enumerate(submit_times, 1):
            now = context.loop.time()
            if now < submit_at:
                await asyncio.sleep(submit_at - now)
            await submit_and_await(job_index, durations[job_index - 1])

    context.loop.create_task(run())
