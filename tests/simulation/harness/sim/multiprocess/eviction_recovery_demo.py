"""
Worker eviction + recovery over the multi-process coordinator — the
picklable child entries for the two-sided-deregistration scenario.

The manager child runs a real ``ManagerServer`` and, at a chosen
virtual instant, legitimately evicts its (perfectly healthy) worker via
the production failure path — the situation a stuck-then-recovered
worker lands in. Before the eviction notice existed this was a silent
permanent divergence: the manager forgot the worker but kept acking its
SWIM probes, so the worker never learned and never re-registered. The
scenario proves the closed loop: evict -> notice -> worker re-registers
-> a SECOND job dispatches and completes on the recovered registration.

Entries record ``(tag, value, virtual_time)`` milestones only — no node
ids, no snowflakes — so identical-seed runs compare equal end to end.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
from pathlib import Path
import os

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.manager.server import ManagerServer

from .job_dispatch_demo import SimPingWorkflow

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def evicting_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    evict_at,
) -> None:
    """Manager child that evicts its registered worker at ``evict_at``
    virtual seconds via the production failure path
    (``_handle_worker_failure`` — the same chokepoint SWIM-death and
    deadline eviction funnel through), then relies on the eviction
    notice to bring the worker back.

    Milestones: ``("worker-count", n, t)`` on every registry-count
    transition (1 -> 0 -> 1 is the evict + re-register signature) and
    ``("evicting", t)`` when the eviction fires.
    """
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

    async def evict_registered_worker() -> None:
        worker_ids = list(manager._manager_state._workers.keys())
        if not worker_ids:
            log.append(("evict-skipped-no-worker", round(context.loop.time(), 6)))
            return
        log.append(("evicting", round(context.loop.time(), 6)))
        try:
            # Our own eviction: not charged to any workflow's retry budget.
            await manager._handle_worker_failure(worker_ids[0], False)
        except Exception as evict_error:
            log.append(
                (
                    "evict-error",
                    type(evict_error).__name__,
                    str(evict_error)[:120],
                    round(context.loop.time(), 6),
                )
            )
            return
        # Event-anchored evidence the eviction deregistered: the notice
        # -> re-register loop closes FASTER than the 0.5s count watcher
        # samples (the whole point of the push), so the transient 0 is
        # invisible to polling. Sample synchronously on return instead.
        log.append(
            (
                "post-evict-count",
                manager._manager_state.get_worker_count(),
                round(context.loop.time(), 6),
            )
        )

    context.loop.create_task(run())
    context.loop.create_task(watch_worker_count())
    context.loop.call_at(
        evict_at,
        lambda: context.loop.create_task(evict_registered_worker()),
    )


def two_job_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    second_submit_at,
) -> None:
    """Client child submitting TWO jobs: one before the eviction, one
    after it — the second job completing proves the evicted worker
    re-registered into a fully serviceable state.

    Submissions retry on virtual time until the manager accepts, so the
    second job also absorbs any window where re-registration is still
    in flight.
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

    async def submit_and_await(job_tag: str) -> None:
        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], SimPingWorkflow())],
                    vus=2,
                    timeout_seconds=30.0,
                )
            except Exception as submit_error:
                log.append(
                    (
                        f"{job_tag}-rejected",
                        type(submit_error).__name__,
                        round(context.loop.time(), 6),
                    )
                )
                await asyncio.sleep(1.0)

        log.append((f"{job_tag}-submitted", round(context.loop.time(), 6)))
        result = await client.wait_for_job(job_id, timeout=45.0)
        log.append(
            (
                f"{job_tag}-finished",
                result.status,
                round(context.loop.time(), 6),
            )
        )

    async def run() -> None:
        await client.start()
        await submit_and_await("job1")

        # Wait out the eviction, then prove the recovered registration
        # carries real work again.
        now = context.loop.time()
        if now < second_submit_at:
            await asyncio.sleep(second_submit_at - now)
        await submit_and_await("job2")

    context.loop.create_task(run())
