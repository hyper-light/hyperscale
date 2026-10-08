"""
Member-count convergence (SCENARIOS.md §11): all observers agree within
bounded gossip rounds of any membership change.

Observers are grouped by tier: the managers of each datacenter (their
``ManagerState.get_active_peer_count()``) and the gates
(``GateRuntimeState.get_active_peer_count()``). A change first shows on
the observer that detected it; from then on the others owe agreement
within one dissemination: the member gossip buffer rebroadcasts an
update ``max(1, int(lambda * ln(n + 1)))`` times, one per protocol period
(``SWIM_UDP_POLL_INTERVAL``), and each delivery may take up to
``SWIM_MAX_PROBE_TIMEOUT`` -- so the bound is ``(rounds + 1)`` periods
plus one probe timeout, with ``lambda`` read from the node's own
``GossipBuffer`` and ``n`` the tier's observer count.

Agreement is owed only once the harness has stabilized the cluster and
while no fault that splits views (network rule, pause, suspended
subprocess) is in force; a kill is not such a fault, since the survivors
owe a converged view of it.
"""

import math
from collections.abc import Callable
from typing import TYPE_CHECKING

from tests.simulation.harness.invariant_checks.live_nodes import responsive_handles
from tests.simulation.harness.invariant_result import InvariantResult
from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness

ObserverGroup = tuple[str, list[ServerHandle]]


class MemberCountConvergence:
    """Stateful evaluator: since when each observer tier has disagreed."""

    def __init__(self, clock: Callable[[], float]) -> None:
        self._clock = clock
        self._diverged_since: dict[str, float] = {}

    def evaluate(self, harness: "ClusterHarness") -> InvariantResult:
        """Holds while every tier agrees, or has disagreed for less than one dissemination."""
        if not _agreement_is_owed(harness):
            self._diverged_since.clear()
            return InvariantResult(holds=True)
        now = self._clock()
        details = [self._judge(group_name, observers, now) for group_name, observers in _observer_groups(harness)]
        detail = next(filter(None, details), "")
        return InvariantResult(holds=not detail, detail=detail)

    def _judge(self, group_name: str, observers: list[ServerHandle], now: float) -> str:
        member_counts = {handle.node_id: _active_peer_count(handle) for handle in observers}
        if len(set(member_counts.values())) <= 1:
            self._diverged_since.pop(group_name, None)
            return ""
        return self._overdue(group_name, observers, member_counts, now)

    def _overdue(self, group_name: str, observers: list[ServerHandle], member_counts: dict[str, int], now: float) -> str:
        diverged_seconds = now - self._diverged_since.setdefault(group_name, now)
        dissemination_bound = _dissemination_bound(observers)
        if diverged_seconds <= dissemination_bound:
            return ""
        return (
            f"{group_name} observers disagree on member count for {diverged_seconds:.1f}s "
            f"(dissemination bound {dissemination_bound:.1f}s): {member_counts}"
        )


def _agreement_is_owed(harness: "ClusterHarness") -> bool:
    return harness.stabilized and not harness.faults.has_active_disruption()


def _observer_groups(harness: "ClusterHarness") -> list[ObserverGroup]:
    groups = [(f"{dc_id} managers", _dc_managers(harness, dc_id)) for dc_id in harness.spec.datacenters]
    groups.append(("gates", responsive_handles(harness, ServerKind.GATE)))
    return groups


def _dc_managers(harness: "ClusterHarness", dc_id: str) -> list[ServerHandle]:
    return [handle for handle in responsive_handles(harness, ServerKind.MANAGER) if handle.dc_id == dc_id]


def _active_peer_count(handle: ServerHandle) -> int:
    if handle.kind is ServerKind.GATE:
        return handle.instance._modular_state.get_active_peer_count()
    return handle.instance._manager_state.get_active_peer_count()


def _dissemination_bound(observers: list[ServerHandle]) -> float:
    """``(rounds + 1)`` protocol periods plus one probe timeout (see module docstring)."""
    observer = observers[0].instance
    env = observer.env
    rebroadcast_rounds = max(1, int(observer._gossip_buffer.broadcast_multiplier * math.log(len(observers) + 1)))
    return (rebroadcast_rounds + 1) * env.SWIM_UDP_POLL_INTERVAL + env.SWIM_MAX_PROBE_TIMEOUT
