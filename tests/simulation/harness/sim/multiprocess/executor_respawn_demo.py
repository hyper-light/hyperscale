"""
A real ``WorkerServer`` whose pool leader's executor slots are observed
over the multi-process coordinator — the picklable child entry the
executor-respawn scenario drives.

Identical to ``worker_entry`` in ``worker_manager_demo`` (production
``WorkerServer.start()``: executor pool spawned as coordinator children
through the ``process_spawner`` seam, pool leader, manager registration)
plus one sampler: every ``poll_interval_seconds`` of virtual time it
records the pool leader's ``Provisioner`` slots — the executors it
would hand out — as listen ports, whenever they change. That is the
state the defect lived in: a dead executor's node kept being handed out
because nothing removed it.

Rows carry ports and virtual times only, so replay comparisons pin the
schedule. Lives in an importable module because ``spawn`` re-imports
the child entry by module + qualname.
"""

import asyncio

from hyperscale.core.jobs.protocols.node_id_derivation import (
    derive_protocol_node_id,
)
from hyperscale.distributed.nodes.worker.server import WorkerServer
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import _env


def slot_watching_worker_entry(
    context,
    host: str,
    tcp_port: int,
    udp_port: int,
    datacenter_id: str,
    seed_manager_address: tuple[str, int],
    total_cores: int,
    poll_interval_seconds: float,
) -> None:
    """Worker child: start a real ``WorkerServer`` and record its pool
    leader's executor slots.

    Rows:

    * ``("pool-ready", t)`` once ``WorkerServer.start()`` returns — the
      pool is up and every executor acknowledged.
    * ``("executor-slots", registered_ports, available_ports, t)`` each
      time the provisioner's registered or available executors change.
      ``registered`` minus ``available`` is what a dispatch holds.
    * ``("workflows-active", count, t)`` each time the worker's active
      workflow count changes (dispatch arrival and drain).
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

    executor_ports_by_node_id = {
        derive_protocol_node_id(executor_host, executor_port): executor_port
        for executor_host, executor_port in worker._lifecycle_manager.get_worker_ips()
    }

    async def run() -> None:
        await worker.start()
        log.append(("pool-ready", round(context.loop.time(), 6)))

    async def watch_executor_slots() -> None:
        last_slots: tuple | None = None
        while True:
            remote_manager = worker._lifecycle_manager.remote_manager
            provisioner = remote_manager._provisioner if remote_manager is not None else None
            slots = (
                tuple(sorted(executor_ports_by_node_id[node_id] for node_id in provisioner._all_nodes)),
                tuple(sorted(executor_ports_by_node_id[node_id] for node_id in provisioner._available_nodes)),
            ) if provisioner is not None else ((), ())
            if slots != last_slots:
                last_slots = slots
                log.append(("executor-slots", *slots, round(context.loop.time(), 6)))
            await asyncio.sleep(poll_interval_seconds)

    async def watch_workflows() -> None:
        last_active_count = -1
        while True:
            active_count = len(worker._active_workflows)
            if active_count != last_active_count:
                last_active_count = active_count
                log.append(("workflows-active", active_count, round(context.loop.time(), 6)))
            await asyncio.sleep(poll_interval_seconds)

    context.loop.create_task(run())
    context.loop.create_task(watch_executor_slots())
    context.loop.create_task(watch_workflows())
