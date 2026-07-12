"""
Gate-tier topologies over the multi-process coordinator — the picklable
child entries for the multi-datacenter and gate-CLUSTER scenarios.

Generalizes ``worker_manager_demo``'s single-DC ``gate_entry`` /
``manager_entry`` shapes (which stay untouched so their pinned schedules
never move):

* ``gate_tier_entry`` — a real ``GateServer`` fronting ANY number of
  datacenters, optionally peered with other gates
  (``gate_peers`` / ``gate_udp_peers``) so a 3-gate SWIM cluster forms:
  peer discovery, gate leader election (the leadership stack the
  dedup / lease fixes hardened), per-job gate leadership.
* ``multi_gate_manager_entry`` — a real ``ManagerServer`` registered
  upstream with EVERY gate in the tier, not just one.

Entries record ``(tag, value, virtual_time)`` milestones only — health
class per DC, discovered-peer counts, worker registration — never node
ids or snowflakes, so two identical-seed runs compare equal end to end.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
from pathlib import Path
import os

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.manager.server import ManagerServer

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def gate_tier_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_managers,
    datacenter_manager_udp,
    gate_tcp_peers=None,
    gate_udp_peers=None,
) -> None:
    """Gate child: a real ``GateServer`` fronting one or more
    datacenters, optionally clustered with peer gates.

    Milestones:

    * ``("gate-started", t)``
    * ``("dc-health", dc_id, health, t)`` on every classification
      change, per datacenter (dc ids walked in sorted order so the
      poll sequence is replay-stable).
    * ``("gate-peers", count, t)`` on every discovered-active-peer-count
      change — the gate-cluster-formation signal
      (``_modular_state.get_active_peer_count()``), recorded only when
      peers are configured.
    """
    gate = GateServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        datacenter_managers=datacenter_managers,
        datacenter_manager_udp=datacenter_manager_udp,
        gate_peers=gate_tcp_peers,
        gate_udp_peers=gate_udp_peers,
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    datacenter_ids = sorted(datacenter_managers)

    async def run() -> None:
        await gate.start()
        log.append(("gate-started", round(context.loop.time(), 6)))

    async def watch_datacenter_health() -> None:
        last_health: dict[str, str | None] = {
            datacenter_id: None for datacenter_id in datacenter_ids
        }
        while True:
            for datacenter_id in datacenter_ids:
                health = gate._classify_datacenter_health(datacenter_id).health
                if health != last_health[datacenter_id]:
                    last_health[datacenter_id] = health
                    log.append(
                        (
                            "dc-health",
                            datacenter_id,
                            health,
                            round(context.loop.time(), 6),
                        )
                    )
            await asyncio.sleep(0.5)

    async def watch_gate_peers() -> None:
        last_peer_count = -1
        while True:
            peer_count = gate._modular_state.get_active_peer_count()
            if peer_count != last_peer_count:
                last_peer_count = peer_count
                log.append(
                    ("gate-peers", peer_count, round(context.loop.time(), 6))
                )
            await asyncio.sleep(0.5)

    context.loop.create_task(run())
    context.loop.create_task(watch_datacenter_health())
    if gate_tcp_peers:
        context.loop.create_task(watch_gate_peers())


def multi_gate_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    gate_tcp_addresses,
    gate_udp_addresses,
) -> None:
    """Manager child: a real ``ManagerServer`` attached upstream to the
    WHOLE gate tier (every gate's TCP + UDP address), recording worker
    registration/loss exactly like ``manager_entry``."""
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
