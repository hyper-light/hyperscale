"""
A gate tier whose nodes each keep a Raft store on their SimFilesystem --
the picklable gate entry (and scenario builder) for judging the durability
of a job's gate replica (A2-G-266) across power loss.

Three peered gates front one datacenter (one manager, one worker); the
client submits one job through gate-b. Every gate opens its Raft store as
the run command does (``opened_raft_store``: the identity and groups its
disk holds, resumed) and records:

* ``("raft-store-opened", resumed, t)`` -- once per generation;
* ``("replicas", summary, t)`` on every change of the committed replicas
  its replication coordinator holds -- per replica its fence token,
  sequence, leader host, and whether it binds an idempotency key -- and
  ``("key-adopted", count, t)`` on every change of how many of those keys
  its idempotency cache holds as decided for the replica's job;
* ``("key-adopted-at-start", count, t)`` then ``("gate-started", t)`` at
  the instant ``start()`` returned -- the count read there, not by the
  sampling watcher, whose first sight of an adoption trails it by up to
  one sample interval.

Hosts and counts only -- never job or node ids -- so replay comparisons
hold. Faults are scheduled by the caller through ``on_coordinator``,
before the run, from the rows the run produces.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
import os
from collections.abc import Callable
from pathlib import Path

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKey
from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.raft.store import RaftStore
from hyperscale.distributed.swim.core.node_id_model import NodeId
from hyperscale.distributed.taskex import TaskRunner

from .gate_cluster_demo import multi_gate_manager_entry
from .job_dispatch_demo import gate_dispatch_client_entry
from .simulation_coordinator import SimulationCoordinator
from .worker_manager_demo import worker_entry

_AUTH_SECRET = "sim-multiprocess-secret-00000000"
SAMPLE_INTERVAL_SECONDS = 0.05
GATE_HOSTS = ("sim-gate-a", "sim-gate-b", "sim-gate-c")
SUBMISSION_GATE = GATE_HOSTS[1]
COORDINATOR_LATENCY = 0.01


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


class _StoreLog:
    """The Raft store's logger: records whether the store resumed."""

    def __init__(self, context, log: list) -> None:
        self._context = context
        self._log = log

    async def log(self, entry) -> None:
        if (resumed := getattr(entry, "resumed", None)) is not None:
            self._log.append(("raft-store-opened", resumed, round(self._context.loop.time(), 6)))


def durable_replica_gate_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_managers,
    datacenter_manager_udp,
    gate_tcp_peers,
    gate_udp_peers,
) -> None:
    """Gate child with a Raft store on this process's disk."""
    log: list = []
    context.set_result(log)
    env = _env()
    task_runner = TaskRunner()
    store = RaftStore(
        directory=Path(f"/sim/{host}-{tcp_port}/raft"),
        filesystem=context.filesystem,
        random_source=context.random,
        clock=context.clock,
        logger=_StoreLog(context, log),
        task_runner=task_runner,
        set_aside_retained=env.RAFT_SET_ASIDE_RETAINED,
        storage_health=StorageHealth(),
    )

    async def run() -> None:
        fresh_node_id_full = NodeId.generate("global", host=host, port=udp_port).full
        await store.open(
            fresh_node_id_full,
            is_this_node=lambda node_id_full: NodeId.placement_of(node_id_full)
            == NodeId.placement_of(fresh_node_id_full),
        )
        gate = GateServer(
            host,
            tcp_port,
            udp_port,
            env,
            datacenter_managers=datacenter_managers,
            datacenter_manager_udp=datacenter_manager_udp,
            gate_peers=gate_tcp_peers,
            gate_udp_peers=gate_udp_peers,
            raft_store=store,
            **context.sim_kwargs(),
        )
        context.loop.create_task(watch_replicas(gate))
        await gate.start()
        adopted_at_start = 0
        for replica in gate._replication_coordinator._committed_replicas.values():
            if replica.idempotency_key:
                entry = await gate._idempotency_cache.get(IdempotencyKey.parse(replica.idempotency_key))
                adopted_at_start += entry is not None and entry.job_id == replica.job_id
        log.append(("key-adopted-at-start", adopted_at_start, round(context.loop.time(), 6)))
        log.append(("gate-started", round(context.loop.time(), 6)))

    async def watch_replicas(gate: GateServer) -> None:
        last_values: dict[str, object] = {}
        while True:
            committed = sorted(
                gate._replication_coordinator._committed_replicas.values(),
                key=lambda replica: replica.job_id,
            )
            adopted = 0
            for replica in committed:
                if replica.idempotency_key:
                    entry = await gate._idempotency_cache.get(IdempotencyKey.parse(replica.idempotency_key))
                    adopted += entry is not None and entry.job_id == replica.job_id
            values: dict[str, object] = {
                "replicas": tuple(
                    (replica.fence_token, replica.sequence, replica.leader_addr[0], bool(replica.idempotency_key))
                    for replica in committed
                ),
                "key-adopted": adopted,
            }
            for tag, value in values.items():
                if last_values.get(tag) != value:
                    last_values[tag] = value
                    log.append((tag, value, round(context.loop.time(), 6)))
            await asyncio.sleep(SAMPLE_INTERVAL_SECONDS)

    context.loop.create_task(run())


def run_gate_replica_durability(
    max_virtual_time: float,
    on_coordinator: Callable[[SimulationCoordinator], None],
    seed: int = 47,
) -> dict:
    """Three durable gates, one datacenter (manager + worker), and a
    client submitting one job through gate-b; ``on_coordinator`` arms the
    run's faults before it starts."""
    coordinator = SimulationCoordinator(
        latency=COORDINATOR_LATENCY, max_virtual_time=max_virtual_time, seed=seed
    )
    datacenter_managers = {"sim-dc": [("sim-mgr", 9000)]}
    datacenter_manager_udp = {"sim-dc": [("sim-mgr", 9001)]}
    for gate_host in GATE_HOSTS:
        peer_hosts = [host for host in GATE_HOSTS if host != gate_host]
        coordinator.add_process(
            gate_host,
            durable_replica_gate_entry,
            gate_host,
            9000,
            9001,
            datacenter_managers,
            datacenter_manager_udp,
            [(peer_host, 9000) for peer_host in peer_hosts],
            [(peer_host, 9001) for peer_host in peer_hosts],
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
        "client", gate_dispatch_client_entry, "sim-cli", 9500, (SUBMISSION_GATE, 9000)
    )
    on_coordinator(coordinator)
    return coordinator.run()
