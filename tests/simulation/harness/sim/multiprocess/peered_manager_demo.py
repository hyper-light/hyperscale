"""
A peered manager tier (several ``ManagerServer`` children in one
datacenter) over the multi-process coordinator — the picklable manager
entry for scenarios that judge per-member job and per-job Raft group
state across a job's whole lifecycle.

Every member records, on each change: how many jobs its ``JobManager``
holds, how many per-job Raft groups its consensus runs, and how many
replicated job-ledger events it mirrors. Counts only —
no job ids or node ids — so the log stays inside the replay contract.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
import os
from pathlib import Path

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.manager.server import ManagerServer

from .job_dispatch_demo import multi_manager_client_entry
from .simulation_coordinator import SimulationCoordinator
from .worker_manager_demo import worker_entry

_AUTH_SECRET = "sim-multiprocess-secret-00000000"
WATCH_INTERVAL_SECONDS = 0.5
PEERED_MANAGERS = [(f"sim-mgr-{name}", 9000, 9001) for name in "abc"]


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def peered_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    peer_tcp_addresses,
    peer_udp_addresses,
    job_retention_seconds,
    job_cleanup_interval_seconds,
) -> None:
    """Manager child peered with ``peer_*_addresses``; logs
    ``("jobs", count, t)``, ``("raft-groups", count, t)`` and
    ``("replica-events", count, t)`` (AD-38 ledger events this member
    mirrors through its job groups) and ``("ledger", (active, terminal,
    synced_lsn, regional_lsn), t)`` transitions.

    Retention and cleanup cadence are scenario parameters so a run can
    reach the retention sweep inside its virtual-time ceiling.
    """
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(JOB_CLEANUP_INTERVAL=job_cleanup_interval_seconds),
        dc_id=datacenter_id,
        seed_managers=list(peer_tcp_addresses),
        manager_udp_peers=list(peer_udp_addresses),
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    manager._config.job_retention_seconds = job_retention_seconds
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

    async def watch_counts() -> None:
        last_counts: dict[str, int | tuple[int, ...]] = {}
        while True:
            counts = {
                "jobs": len(manager._job_manager._jobs),
                "raft-groups": manager._raft.consensus.active_instance_count,
                "replica-events": sum(
                    len(manager._ledger_replica.history(job_id))
                    for job_id in manager._ledger_replica.states()
                ),
            }
            if (ledger := manager._job_ledger) is not None:
                # (active jobs, terminal jobs, last fsynced LSN, last
                # REGIONAL LSN): the LSNs are equal on a job leader once
                # its newest entry (the terminal) replicated.
                counts["ledger"] = (
                    ledger.active_job_count,
                    ledger.cached_completed_count,
                    ledger._wal.last_synced_lsn,
                    ledger._wal.last_regional_lsn,
                )
            for tag, count in counts.items():
                if last_counts.get(tag) != count:
                    last_counts[tag] = count
                    log.append((tag, count, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())
    context.loop.create_task(watch_counts())


def run_peered_manager_job(
    max_virtual_time: float,
    job_retention_seconds: float,
    job_cleanup_interval_seconds: float,
    sustained: bool = False,
    job_timeout_seconds: float = 30.0,
    wait_timeout_seconds: float = 45.0,
    kill: tuple[str, float] | None = None,
) -> dict:
    """Three peered managers, one worker (seeded at the first manager) and
    a client submitting one job to the manager tier; ``kill`` is an
    optional ``(process_id, virtual_time)`` SIGKILL. Returns every
    child's log."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=max_virtual_time, seed=23
    )
    for host, tcp_port, udp_port in PEERED_MANAGERS:
        coordinator.add_process(
            host,
            peered_manager_entry,
            host,
            tcp_port,
            udp_port,
            "sim-dc",
            [(peer, peer_tcp) for peer, peer_tcp, _ in PEERED_MANAGERS if peer != host],
            [(peer, peer_udp) for peer, _, peer_udp in PEERED_MANAGERS if peer != host],
            job_retention_seconds,
            job_cleanup_interval_seconds,
        )
    seed_host, seed_tcp, _ = PEERED_MANAGERS[0]
    coordinator.add_process(
        "worker", worker_entry, "sim-wkr", 9000, 9001, "sim-dc", (seed_host, seed_tcp), 2
    )
    coordinator.add_process(
        "client",
        multi_manager_client_entry,
        "sim-cli",
        9500,
        [(host, tcp_port) for host, tcp_port, _ in PEERED_MANAGERS],
        sustained,
        job_timeout_seconds,
        wait_timeout_seconds,
    )
    if kill is not None:
        coordinator.schedule_kill(*kill)
    return coordinator.run()
