"""
A peered manager tier (several ``ManagerServer`` children in one
datacenter) over the multi-process coordinator — the picklable manager
entry for scenarios that judge per-member job and per-job Raft group
state across a job's whole lifecycle.

Every member records, on each change: how many jobs its ``JobManager``
holds and how many per-job Raft groups its consensus runs. Counts only —
no job ids or node ids — so the log stays inside the replay contract.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
import os
from pathlib import Path

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.manager.server import ManagerServer

_AUTH_SECRET = "sim-multiprocess-secret-00000000"
WATCH_INTERVAL_SECONDS = 0.5


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
    ``("jobs", count, t)`` and ``("raft-groups", count, t)`` transitions.

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
        last_counts: dict[str, int] = {}
        while True:
            counts = {
                "jobs": len(manager._job_manager._jobs),
                "raft-groups": manager._raft.consensus.active_instance_count,
            }
            for tag, count in counts.items():
                if last_counts.get(tag) != count:
                    last_counts[tag] = count
                    log.append((tag, count, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())
    context.loop.create_task(watch_counts())
