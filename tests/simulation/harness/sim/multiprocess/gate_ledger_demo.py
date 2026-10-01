"""
A durable gate tier over the multi-process coordinator — the picklable
gate entry (and scenario builder) for judging the AD-38 gate job ledger:
REGIONAL commits through each job's gate group and GLOBAL placement
across regions.

Three peered gates, each in a configurable region (its ``dc_id``) with a
SimFilesystem-backed ledger, front one datacenter (one manager, one
worker); the client submits through a non-seed gate. Every gate records,
on each change: jobs its GateJobManager holds, per-job Raft groups it
runs, replicated ledger events it mirrors, and its ledger watermarks.
Counts and LSNs only — never job or node ids — so replay comparisons
hold.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
import os
from pathlib import Path

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.gate.server import GateServer

from .gate_cluster_demo import multi_gate_manager_entry
from .job_dispatch_demo import gate_dispatch_client_entry
from .simulation_coordinator import SimulationCoordinator
from .worker_manager_demo import worker_entry

_AUTH_SECRET = "sim-multiprocess-secret-00000000"
WATCH_INTERVAL_SECONDS = 0.5
GATE_HOSTS = ("sim-gate-a", "sim-gate-b", "sim-gate-c")


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def ledger_gate_entry(
    context,
    host,
    tcp_port,
    udp_port,
    region,
    datacenter_managers,
    datacenter_manager_udp,
    gate_tcp_peers,
    gate_udp_peers,
    job_max_age_seconds,
    job_cleanup_interval_seconds,
) -> None:
    """Gate child with a durable ledger in ``region``.

    Logs ``("jobs", n, t)``, ``("raft-groups", n, t)``,
    ``("replica-events", n, t)`` and ``("ledger", (active, terminal,
    synced_lsn, regional_lsn, global_lsn), t)`` transitions.
    """
    gate = GateServer(
        host,
        tcp_port,
        udp_port,
        _env(
            FAILED_JOB_MAX_AGE=job_max_age_seconds,
            GATE_JOB_CLEANUP_INTERVAL=job_cleanup_interval_seconds,
        ),
        dc_id=region,
        datacenter_managers=datacenter_managers,
        datacenter_manager_udp=datacenter_manager_udp,
        gate_peers=gate_tcp_peers,
        gate_udp_peers=gate_udp_peers,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await gate.start()
        log.append(("gate-started", round(context.loop.time(), 6)))
        # The Raft integration and ledger replica exist once start() ran.
        context.loop.create_task(watch_counts())

    async def watch_counts() -> None:
        last_values: dict[str, int | tuple[int, ...]] = {}
        while True:
            values: dict[str, int | tuple[int, ...]] = {
                "jobs": len(list(gate._job_manager.items())),
                "raft-groups": gate._raft.consensus.active_instance_count,
                "replica-events": sum(
                    len(gate._ledger_replica.history(job_id))
                    for job_id in gate._ledger_replica.states()
                ),
            }
            if (ledger := gate._job_ledger) is not None:
                values["ledger"] = (
                    ledger.active_job_count,
                    ledger.cached_completed_count,
                    ledger._wal.last_synced_lsn,
                    ledger._wal.last_regional_lsn,
                    ledger._wal.last_global_lsn,
                )
            for tag, value in values.items():
                if last_values.get(tag) != value:
                    last_values[tag] = value
                    log.append((tag, value, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(run())


def run_gate_ledger_job(
    gate_regions: tuple[str, str, str],
    max_virtual_time: float,
    job_max_age_seconds: float,
    job_cleanup_interval_seconds: float,
) -> dict:
    """Three peered gates in ``gate_regions``, one datacenter (manager +
    worker), and a client submitting one job through gate-b."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=max_virtual_time, seed=43
    )
    datacenter_managers = {"sim-dc": [("sim-mgr", 9000)]}
    datacenter_manager_udp = {"sim-dc": [("sim-mgr", 9001)]}
    for gate_host, region in zip(GATE_HOSTS, gate_regions):
        peer_hosts = [host for host in GATE_HOSTS if host != gate_host]
        coordinator.add_process(
            gate_host,
            ledger_gate_entry,
            gate_host,
            9000,
            9001,
            region,
            datacenter_managers,
            datacenter_manager_udp,
            [(peer_host, 9000) for peer_host in peer_hosts],
            [(peer_host, 9001) for peer_host in peer_hosts],
            job_max_age_seconds,
            job_cleanup_interval_seconds,
        )
    coordinator.add_process(
        "manager",
        multi_gate_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        [(gate_host, 9000) for gate_host in GATE_HOSTS],
        [(gate_host, 9001) for gate_host in GATE_HOSTS],
    )
    coordinator.add_process(
        "worker", worker_entry, "sim-wkr", 9000, 9001, "sim-dc", ("sim-mgr", 9000), 2
    )
    coordinator.add_process(
        "client", gate_dispatch_client_entry, "sim-cli", 9500, (GATE_HOSTS[1], 9000)
    )
    return coordinator.run()
