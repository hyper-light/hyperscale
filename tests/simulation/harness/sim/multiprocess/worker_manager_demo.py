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
from pathlib import Path
import os

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.nodes.worker.server import WorkerServer

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def apply_storage_fault_schedule(context, storage_fault_schedule) -> None:
    """Arm this child's ``SimFilesystem`` knobs at virtual instants.

    Schedule entries (all times virtual seconds):
    ``("slow_disk", at_time, delay_seconds, until_time)`` — every
    storage operation costs ``delay_seconds`` of virtual time inside
    the window; ``("disk_full", at_time, remaining_bytes)`` — writes
    beyond the byte budget raise ``OSError(ENOSPC)`` from then on;
    ``("disk_full_window", at_time, remaining_bytes, until_time)`` — the
    same budget, freed again at ``until_time`` (space reclaimed).
    """
    filesystem = context.filesystem
    for event in storage_fault_schedule:
        kind = event[0]
        if kind == "slow_disk":
            _kind, at_time, delay_seconds, until_time = event
            context.loop.call_at(
                at_time, filesystem.set_slow_disk, delay_seconds
            )
            context.loop.call_at(until_time, filesystem.clear_slow_disk)
        elif kind == "disk_full":
            _kind, at_time, remaining_bytes = event
            context.loop.call_at(
                at_time, filesystem.set_disk_full, remaining_bytes
            )
        elif kind == "disk_full_window":
            _kind, at_time, remaining_bytes, until_time = event
            context.loop.call_at(
                at_time, filesystem.set_disk_full, remaining_bytes
            )
            context.loop.call_at(until_time, filesystem.clear_disk_full)
        else:
            raise ValueError(f"unknown storage fault kind: {kind}")


def manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    gate_tcp_address=None,
    gate_udp_address=None,
    storage_fault_schedule=(),
) -> None:
    """Manager child: start a real ``ManagerServer`` and record when the
    first worker registers.

    ``gate_tcp_address`` / ``gate_udp_address`` (optional) attach the
    manager to a gate tier — the L3 topology; omitted, the manager runs
    gateless (L1/L2) exactly as before.

    The manager always runs with ``wal_data_dir`` enabled: under SIM the
    path is this child's in-memory ``SimFilesystem``, so every scenario
    exercises the production storage stack (NodeWAL group commits,
    idempotency ledger, checkpoints) — the surface the storage-fault
    schedule targets.
    """
    apply_storage_fault_schedule(context, storage_fault_schedule)
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id=datacenter_id,
        gate_addrs=[gate_tcp_address] if gate_tcp_address else None,
        gate_udp_addrs=[gate_udp_address] if gate_udp_address else None,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
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


def gate_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    manager_tcp_address,
    manager_udp_address,
) -> None:
    """Gate child: a real ``GateServer`` fronting one datacenter.

    Records the datacenter's health classification every time it
    changes — the gate's view of the manager tier coming alive (and,
    under fault scenarios, dying) on virtual time.
    """
    gate = GateServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        datacenter_managers={datacenter_id: [manager_tcp_address]},
        datacenter_manager_udp={datacenter_id: [manager_udp_address]},
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await gate.start()
        log.append(("gate-started", round(context.loop.time(), 6)))

    async def watch_datacenter_health() -> None:
        last_health: str | None = None
        while True:
            health = gate._classify_datacenter_health(datacenter_id).health
            if health != last_health:
                last_health = health
                log.append(
                    ("dc-health", health, round(context.loop.time(), 6))
                )
            await asyncio.sleep(0.5)

    context.loop.create_task(run())
    context.loop.create_task(watch_datacenter_health())


def worker_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    seed_manager_address,
    total_cores,
    start_at=0.0,
) -> None:
    """Worker child: start a real ``WorkerServer`` against the seed
    manager and record start + healthy-manager milestones.

    ``WORKER_MAX_CORES`` pins the pool size so the executor children
    (spawned via the ``process_spawner`` seam at
    ``executor-<host>-<port>`` ids) are deterministic in number and
    address. ``start_at`` delays ``WorkerServer.start()`` to that
    virtual instant — a worker joining an already-running cluster
    (capacity returning after loss, retry targets appearing mid-job).
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

    # ``start_at=0.0`` keeps the original immediate-start scheduling
    # (create_task during the setup drain) byte-identical; a positive
    # start delays the whole startup sequence to that virtual instant.
    if start_at > 0.0:
        context.loop.call_at(start_at, lambda: context.loop.create_task(run()))
    else:
        context.loop.create_task(run())
    context.loop.create_task(watch_workflows())
