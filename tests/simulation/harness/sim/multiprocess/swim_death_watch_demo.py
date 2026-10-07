"""
SWIM death-watch children: a real ``GateServer`` / ``ManagerServer``
recording when its SWIM layer may first SUSPECT each watched peer and the
instant it commits a peer DEAD — the event rows the dead-gate
dissemination scenarios derive their faults from and judge detection
latency by.

Rows (``(tag, peer_host, virtual_time)``; hosts are topology names such
as ``sim-gate-c``, never node ids, so identical-seed runs compare equal):

* ``("node-started", t)``
* ``("swim-suspectable", peer_host, t)`` — the peer has completed its
  registration handshake and is CONFIRMED (``is_peer_registered`` and
  ``can_suspect_peer``: the two gates ``start_suspicion`` applies), so
  from here on its death is detectable. Sampled once per SWIM protocol
  period, the cadence at which a probe round could first act on it.
* ``("swim-suspicion", peer_host, confirmations, required, min_timeout,
  max_timeout, start, expiry, t)`` — this node's global (AD-30) suspicion
  of the peer, on each change of its counted Lifeguard confirmations or
  expiry instant (``start + calculate_timeout()``): recorded the instant
  a suspect or confirm call changes it, and sampled once per SWIM
  protocol period like ``swim-suspectable`` for any other change.
* ``("swim-dead", peer_host, t)`` — this node's tracker took the peer DEAD
  (own suspicion expiry, gossip, or burst confirmation alike), written
  from ``register_on_node_dead`` at the transition's exact instant.

Lives in an importable module because ``spawn`` re-imports child entries
by module + qualname.
"""

import asyncio
import os
from pathlib import Path

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.manager.server import ManagerServer

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


def _env() -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET)


def _watch_swim_transitions(context, server, environment: Env, watched_peers: list, log: list) -> None:
    """Record each watched peer becoming suspectable, and every DEAD commit."""
    server.register_on_node_dead(
        lambda peer: log.append(("swim-dead", peer[0], round(context.loop.time(), 6)))
    )

    async def watch_suspectable() -> None:
        pending_peers = [tuple(peer) for peer in watched_peers]
        while pending_peers:
            suspectable_peers = [
                peer
                for peer in pending_peers
                if server.is_peer_registered(peer) and server.can_suspect_peer(peer)
            ]
            for peer in suspectable_peers:
                log.append(("swim-suspectable", peer[0], round(context.loop.time(), 6)))
                pending_peers.remove(peer)
            await asyncio.sleep(environment.SWIM_UDP_POLL_INTERVAL)

    detector = server._hierarchical_detector
    global_wheel = detector._global_wheel
    watched_peer_set = {tuple(peer) for peer in watched_peers}
    last_snapshots: dict[tuple, tuple | None] = {}

    def record_suspicion(peer: tuple) -> None:
        state = global_wheel.get_state_sync(peer)
        snapshot = None if state is None else (
            state.confirmation_count,
            state.required_confirmations,
            round(state.min_timeout, 6),
            round(state.max_timeout, 6),
            round(state.start_time, 6),
            round(state.start_time + state.calculate_timeout(), 6),
        )
        if snapshot is not None and snapshot != last_snapshots.get(peer):
            log.append(("swim-suspicion", peer[0], *snapshot, round(context.loop.time(), 6)))
        last_snapshots[peer] = snapshot

    def recording(detector_call):
        """Record the peer's suspicion the instant a suspect or confirm
        call returns: a confirmation that shortens the timeout below one
        sampling period, so the peer dies before the next sample, is
        still observed."""

        async def call_then_record(node, incarnation, from_node):
            outcome = await detector_call(node, incarnation, from_node)
            if tuple(node) in watched_peer_set:
                record_suspicion(tuple(node))
            return outcome

        return call_then_record

    detector.suspect_global = recording(detector.suspect_global)
    detector.confirm_global = recording(detector.confirm_global)

    async def watch_suspicions() -> None:
        while True:
            for peer in sorted(watched_peer_set):
                record_suspicion(peer)
            await asyncio.sleep(environment.SWIM_UDP_POLL_INTERVAL)

    context.loop.create_task(watch_suspectable())
    context.loop.create_task(watch_suspicions())


def swim_death_watch_gate_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_managers,
    datacenter_manager_udp,
    gate_tcp_peers,
    gate_udp_peers,
) -> None:
    """Gate child: a real ``GateServer`` peered into a gate cluster,
    recording when each peer gate becomes suspectable and every DEAD commit."""
    environment = _env()
    gate = GateServer(
        host,
        tcp_port,
        udp_port,
        environment,
        datacenter_managers=datacenter_managers,
        datacenter_manager_udp=datacenter_manager_udp,
        gate_peers=gate_tcp_peers,
        gate_udp_peers=gate_udp_peers,
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    _watch_swim_transitions(context, gate, environment, gate_udp_peers, log)

    async def run() -> None:
        await gate.start()
        log.append(("node-started", round(context.loop.time(), 6)))

    context.loop.create_task(run())


def swim_death_watch_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    gate_tcp_addresses,
    gate_udp_addresses,
) -> None:
    """Manager child: a real ``ManagerServer`` registered with every gate,
    recording when each gate becomes suspectable and every DEAD commit."""
    environment = _env()
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        environment,
        dc_id=datacenter_id,
        gate_addrs=gate_tcp_addresses,
        gate_udp_addrs=gate_udp_addresses,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    _watch_swim_transitions(context, manager, environment, gate_udp_addresses, log)

    async def run() -> None:
        await manager.start()
        log.append(("node-started", round(context.loop.time(), 6)))

    context.loop.create_task(run())
