"""
A storage-faultable multi-gate manager — the picklable child entry the
multi-DC fault program's per-DC STORAGE scenarios drive.

``gate_cluster_demo.multi_gate_manager_entry`` attaches a real
``ManagerServer`` to a whole gate tier but exposes no storage-fault
knob; ``worker_manager_demo.manager_entry`` has the knob but speaks to
at most ONE gate. This entry is their composition — gate-tier
attachment AND a ``storage_fault_schedule`` arg — so one datacenter's
manager can run slow_disk windows / disk_full budgets while the peer
datacenter stays clean (the cross-DC placement-under-storage-pressure
scenario class).

Milestones are identical to ``multi_gate_manager_entry`` (worker
registration / loss on virtual time), so scenario logs stay comparable
across faulted and clean managers.

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname.
"""

import asyncio
from pathlib import Path
import os

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.manager.server import ManagerServer

from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    apply_storage_fault_schedule,
)

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def faulted_multi_gate_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    gate_tcp_addresses,
    gate_udp_addresses,
    storage_fault_schedule=(),
) -> None:
    """Manager child: a real ``ManagerServer`` attached upstream to the
    WHOLE gate tier, with this child's ``SimFilesystem`` fault knobs
    armed from ``storage_fault_schedule`` (see
    ``worker_manager_demo.apply_storage_fault_schedule`` for the entry
    shapes: ``("slow_disk", at, delay, until)`` /
    ``("disk_full", at, remaining_bytes)``).

    The WAL always runs (``wal_data_dir`` on the child's in-memory
    filesystem), so the storage faults target the production ledger
    stack — NodeWAL group commits, idempotency ledger, checkpoints.
    """
    apply_storage_fault_schedule(context, storage_fault_schedule)
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id=datacenter_id,
        gate_addrs=gate_tcp_addresses,
        gate_udp_addrs=gate_udp_addresses,
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

        while manager._manager_state.get_worker_count() > 0:
            await asyncio.sleep(0.5)
        log.append(("worker-lost", round(context.loop.time(), 6)))

    context.loop.create_task(run())
