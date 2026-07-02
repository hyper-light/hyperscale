"""
A real ``ManagerServer`` + ``WorkerServer`` pair exercised over the
multi-process coordinator — the picklable child entries a test drives.

The full Phase 6d composition in one scenario, every piece production
code: the manager child runs ``ManagerServer.start()`` (SWIM UDP + TCP
servers, Raft self-election, every background loop); the worker child
runs ``WorkerServer.start()`` — its executor pool spawns as further
coordinator children (the ``process_spawner`` seam), the pool leader
connects over the datagram boundary, TCP registration dials the manager
over the stream boundary, SWIM probes flow both ways — all at coherent
virtual time.

Entries record ``(tag, virtual_time)`` milestones only — no node ids,
no snowflakes — so replay comparisons pin the schedule without tripping
on values that are legitimately fresh per run (uuid-based identities,
encryption nonces).

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
import os

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.nodes.worker.server import WorkerServer

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def manager_entry(context, host, tcp_port, udp_port, datacenter_id) -> None:
    """Manager child: start a real ``ManagerServer`` and record when the
    first worker registers."""
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id=datacenter_id,
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

        while manager._manager_state.get_worker_count() < 1:
            await asyncio.sleep(0.5)
        log.append(("worker-registered", round(context.loop.time(), 6)))

        # Phase two: if the worker later disappears (fault-injection
        # scenarios kill it), record when the manager's registry reflects
        # the loss — SWIM suspicion + dead-worker reap end to end.
        while manager._manager_state.get_worker_count() > 0:
            await asyncio.sleep(0.5)
        log.append(("worker-lost", round(context.loop.time(), 6)))

    context.loop.create_task(run())


def worker_entry(
    context, host, tcp_port, udp_port, datacenter_id, seed_manager_address, total_cores
) -> None:
    """Worker child: start a real ``WorkerServer`` against the seed
    manager and record start + healthy-manager milestones.

    ``WORKER_MAX_CORES`` pins the pool size so the executor children
    (spawned via the ``process_spawner`` seam at
    ``executor-<host>-<port>`` ids) are deterministic in number and
    address.
    """
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

    async def run() -> None:
        await worker.start()
        log.append(("worker-started", round(context.loop.time(), 6)))

        while not worker._registry._healthy_manager_ids:
            await asyncio.sleep(0.5)
        log.append(("manager-healthy", round(context.loop.time(), 6)))

    async def watch_workflows() -> None:
        # Milestone every time the active-workflow count changes: shows
        # dispatch arrival and drain (final result sent) on virtual time.
        last_active_count = -1
        while True:
            active_count = len(worker._active_workflows)
            if active_count != last_active_count:
                last_active_count = active_count
                log.append(
                    (
                        "workflows-active",
                        active_count,
                        round(context.loop.time(), 6),
                    )
                )
            await asyncio.sleep(0.25)

    context.loop.create_task(run())
    context.loop.create_task(watch_workflows())
