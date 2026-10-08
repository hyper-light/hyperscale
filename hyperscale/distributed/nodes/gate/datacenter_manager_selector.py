"""
AD-28 manager selection within a datacenter for gate dispatch.
"""

from __future__ import annotations

from heapq import nlargest
from operator import itemgetter
from typing import Callable, Mapping

from hyperscale.distributed.discovery import DiscoveryService, SelectionResult
from hyperscale.distributed.models import ManagerHeartbeat


ManagerAddress = tuple[str, int]


class DatacenterManagerSelector:
    """Orders a datacenter's managers for dispatch and learns from outcomes.

    Order: the manager whose latest heartbeat claims leadership with the
    highest term first (only the DC leader accepts jobs; anything else
    answers with a redirect the gate would otherwise retry with backoff),
    then AD-28's weighted-rendezvous + EWMA ranking of the rest, then any
    manager discovery cannot rank yet, so a candidate is never dropped.

    Owns one DiscoveryService per datacenter, created on demand (so
    datacenters that join at runtime are covered), with peers keyed by
    "host:port" — one peer per manager, removed when the manager is
    forgotten, so discovery state stays bounded by live managers.
    """

    def __init__(
        self,
        create_discovery: Callable[[], DiscoveryService],
        get_manager_heartbeats: Callable[[str], Mapping[ManagerAddress, ManagerHeartbeat]],
    ) -> None:
        self._create_discovery = create_discovery
        self._get_manager_heartbeats = get_manager_heartbeats
        self._discovery_by_datacenter: dict[str, DiscoveryService] = {}
        self._datacenter_by_manager: dict[ManagerAddress, str] = {}

    @property
    def discovery_by_datacenter(self) -> dict[str, DiscoveryService]:
        """Live per-datacenter discovery services (read-only use)."""
        return self._discovery_by_datacenter

    def track_manager(self, datacenter_id: str, manager_addr: ManagerAddress) -> None:
        """Make ``manager_addr`` a selection candidate for its datacenter."""
        discovery = self._discovery_by_datacenter.get(datacenter_id)
        if discovery is None:
            discovery = self._create_discovery()
            self._discovery_by_datacenter[datacenter_id] = discovery

        peer_id = _peer_id(manager_addr)
        if not discovery.contains(peer_id):
            discovery.add_peer(
                peer_id=peer_id,
                host=manager_addr[0],
                port=manager_addr[1],
                role="manager",
                datacenter_id=datacenter_id,
            )
        self._datacenter_by_manager[manager_addr] = datacenter_id

    def forget_manager(self, manager_addr: ManagerAddress) -> None:
        """Drop a manager that went stale; its EWMA history goes with it."""
        datacenter_id = self._datacenter_by_manager.pop(manager_addr, None)
        if datacenter_id is None:
            return
        if discovery := self._discovery_by_datacenter.get(datacenter_id):
            discovery.remove_peer(_peer_id(manager_addr))

    def ordered_managers(
        self,
        datacenter_id: str,
        selection_key: str,
        managers: list[ManagerAddress],
    ) -> list[ManagerAddress]:
        """Dispatch order for ``managers`` (every one appears exactly once)."""
        ordered = [
            *self._current_leader(datacenter_id, managers),
            *self._ranked(datacenter_id, selection_key, managers),
            *managers,
        ]
        return list(dict.fromkeys(ordered))

    def record_success(
        self,
        datacenter_id: str,
        manager_addr: ManagerAddress,
        latency_ms: float,
    ) -> None:
        if discovery := self._discovery_by_datacenter.get(datacenter_id):
            discovery.record_success(_peer_id(manager_addr), latency_ms)

    def record_failure(self, datacenter_id: str, manager_addr: ManagerAddress) -> None:
        if discovery := self._discovery_by_datacenter.get(datacenter_id):
            discovery.record_failure(_peer_id(manager_addr))

    def decay_failures(self) -> None:
        for discovery in self._discovery_by_datacenter.values():
            discovery.decay_failures()

    def _current_leader(
        self,
        datacenter_id: str,
        managers: list[ManagerAddress],
    ) -> list[ManagerAddress]:
        heartbeats = self._get_manager_heartbeats(datacenter_id)
        leaders = [
            (heartbeat.term, manager_addr)
            for manager_addr in managers
            if _claims_leadership(heartbeat := heartbeats.get(manager_addr))
        ]
        # nlargest(1) is max(): the highest-term claimant, or none at all.
        return list(map(itemgetter(1), nlargest(1, leaders)))

    def _ranked(
        self,
        datacenter_id: str,
        selection_key: str,
        managers: list[ManagerAddress],
    ) -> list[ManagerAddress]:
        discovery = self._discovery_by_datacenter.get(datacenter_id)
        if discovery is None:
            return []

        candidates = {_peer_id(manager_addr): manager_addr for manager_addr in managers}
        selections = discovery.select_peers(
            selection_key,
            count=len(managers),
        )
        return _selected_candidates(selections, candidates)


def _selected_candidates(
    selections: list[SelectionResult],
    candidates: dict[str, ManagerAddress],
) -> list[ManagerAddress]:
    """The candidate managers discovery selected, in its ranking order."""
    return [
        candidates[selection.peer_id]
        for selection in selections
        if selection.peer_id in candidates
    ]


def _claims_leadership(heartbeat: ManagerHeartbeat | None) -> bool:
    """Whether a manager's latest heartbeat (if any) claims datacenter leadership."""
    return heartbeat is not None and heartbeat.is_leader


def _peer_id(manager_addr: ManagerAddress) -> str:
    return f"{manager_addr[0]}:{manager_addr[1]}"
