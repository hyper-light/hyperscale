"""
The manager's record of a worker's cores, sampled every window under
multi-process SIM -- the picklable manager entry a test drives.

The manager child runs the production ``ManagerServer`` exactly as
``worker_manager_demo.manager_entry`` does, and additionally samples
every worker record in its ``WorkerPool`` each sampling window: the
total cores, the free count the dashboard reads (available minus
reserved), and the reserved count. A record whose total flips, or
whose free count exceeds its total, is the bug this scenario pins.

Lives in an importable module because ``spawn`` re-imports the child
entry by module + qualname.
"""

import asyncio
from pathlib import Path

from hyperscale.distributed.nodes.manager.server import ManagerServer

from .child_context import ChildContext
from .worker_manager_demo import _env


def sampling_manager_entry(
    context: ChildContext,
    host: str,
    tcp_port: int,
    udp_port: int,
    datacenter_id: str,
    sampling_window_seconds: float,
) -> None:
    """Manager child: start a real ``ManagerServer`` and record, every
    ``sampling_window_seconds`` of virtual time, each local worker
    record's ``(total, free, reserved)`` cores as the dashboard reads
    them."""
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

    async def sample_worker_records() -> None:
        while True:
            for worker in sorted(manager._worker_pool._workers.values(), key=lambda record: record.worker_id):
                log.append(
                    (
                        "worker-cores",
                        worker.total_cores,
                        worker.available_cores - worker.reserved_cores,
                        worker.reserved_cores,
                        round(context.loop.time(), 6),
                    )
                )
            await asyncio.sleep(sampling_window_seconds)

    context.loop.create_task(run())
    context.loop.create_task(sample_worker_records())
