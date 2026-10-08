"""
The LEADER-WATCH gate child: ``gate_cluster_demo.gate_tier_entry``'s
exact topology shape (a real ``GateServer`` fronting N datacenters,
optionally peered into a gate cluster) plus a gate-LEADERSHIP watcher —
the picklable gate entry the gate-cluster fault scenarios drive when
they must assert leader convergence and stability windows.

``gate_tier_entry`` stays untouched (its pinned schedules never move);
this entry exists because leadership churn is otherwise invisible in
milestones: a kill or an isolation partition must provably converge the
tier back to EXACTLY ONE leader, and split-brain (two concurrent
leaders) or flapping (repeated gain/lose cycles after quiesce) are the
silent-wrongness classes the fault program hunts.

Milestones (``(tag, value, virtual_time)`` only — health class, counts,
0/1 flags; never node ids or terms' holders, so identical-seed runs
compare equal):

* ``("gate-started", t)``
* ``("dc-health", dc_id, health, t)`` per classification change (dc ids
  walked in sorted order, replay-stable poll sequence).
* ``("gate-peers", count, t)`` per discovered-active-peer-count change
  (only when peers are configured — the cluster-formation signal).
* ``("gate-leader", flag, t)`` per is-leader transition of THIS gate
  (0/1) — the union across gates yields leader counts over virtual
  time, which is what convergence/stability assertions consume.
"""

import asyncio
import os

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.gate.server import GateServer

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def leader_watch_gate_tier_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_managers,
    datacenter_manager_udp,
    gate_tcp_peers=None,
    gate_udp_peers=None,
    wal_data_dir=None,
) -> None:
    """Gate child: a real ``GateServer`` fronting one or more
    datacenters, optionally clustered with peer gates, recording
    datacenter health, active-peer count, and its own gate-leadership
    flag on every change.

    ``wal_data_dir`` (a path STRING under the child's SIM filesystem,
    e.g. ``/sim/<host>-<port>/gate-ledger``) arms the Phase 8 gate
    durable tier: accepted jobs persist to a JobLedger and a restarted
    generation recovers them at start. The SIM filesystem survives a
    ``schedule_restart`` power cycle exactly as the manager-restart
    recipes rely on, so gen-2 replays gen-1's WAL.
    """
    from pathlib import Path

    gate = GateServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        datacenter_managers=datacenter_managers,
        datacenter_manager_udp=datacenter_manager_udp,
        gate_peers=gate_tcp_peers,
        gate_udp_peers=gate_udp_peers,
        wal_data_dir=Path(wal_data_dir) if wal_data_dir else None,
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

    async def watch_gate_leadership() -> None:
        last_leader_flag = -1
        while True:
            leader_flag = int(gate._leader_election.state.is_leader())
            if leader_flag != last_leader_flag:
                last_leader_flag = leader_flag
                log.append(
                    ("gate-leader", leader_flag, round(context.loop.time(), 6))
                )
            await asyncio.sleep(0.5)

    context.loop.create_task(run())
    context.loop.create_task(watch_datacenter_health())
    if gate_tcp_peers:
        context.loop.create_task(watch_gate_peers())
    context.loop.create_task(watch_gate_leadership())
