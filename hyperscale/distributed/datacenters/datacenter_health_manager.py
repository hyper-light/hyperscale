"""
Datacenter Health Manager - DC health classification based on manager health.

This class encapsulates the logic for classifying datacenter health based on
aggregated health signals from managers within each datacenter.

Health States (evaluated in order):
1. UNHEALTHY: No managers registered OR no workers registered
2. DEGRADED: Majority of workers unhealthy OR majority of managers unhealthy
3. BUSY: NOT degraded AND available_cores == 0 (transient, will clear)
4. HEALTHY: NOT degraded AND available_cores > 0

Key insight: BUSY ≠ UNHEALTHY
- BUSY = transient, will clear → accept job (queued)
- DEGRADED = structural problem, reduced capacity → may need intervention
- UNHEALTHY = severe problem → try fallback datacenter

See AD-16 in docs/architecture.md for full details.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from operator import attrgetter
from typing import Callable
from hyperscale.distributed.models import ManagerHeartbeat, DatacenterHealth, DatacenterStatus
from hyperscale.distributed.datacenters.datacenter_overload_config import (
    DatacenterOverloadConfig,
    DatacenterOverloadState,
)
from hyperscale.distributed.datacenters.datacenter_overload_classifier import (
    DatacenterOverloadClassifier,
    DatacenterOverloadSignals,
)
from hyperscale.distributed.health.phi_accrual_config import PhiAccrualConfig
from hyperscale.distributed.health.phi_accrual_detector import PhiAccrualDetector
from hyperscale.distributed.runtime import Clock, RealClock

from .cached_manager_info import CachedManagerInfo

_DEFAULT_CLOCK: Clock = RealClock()


class DatacenterHealthManager:
    """
    Manages datacenter health classification based on manager health.

    Tracks manager heartbeats for each datacenter and classifies overall
    DC health using the three-signal health model.

    Example usage:
        manager = DatacenterHealthManager(PhiAccrualConfig.for_manager_heartbeats(env))

        # Update manager heartbeats as they arrive
        manager.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat)

        # Get DC health status
        status = manager.get_datacenter_health("dc-1")
        if status.health == DatacenterHealth.HEALTHY.value:
            # OK to dispatch jobs
            pass

        # Get all DC statuses
        all_status = manager.get_all_datacenter_health()
    """

    def __init__(
        self,
        phi_config: PhiAccrualConfig,
        get_configured_managers: Callable[[str], list[tuple[str, int]]] | None = None,
        overload_config: DatacenterOverloadConfig | None = None,
    ):
        """
        Initialize DatacenterHealthManager.

        Args:
            phi_config: How each manager's heartbeats are judged (AD-52
                section 8): a manager counts while its phi-accrual
                detector is below the threshold.
            get_configured_managers: Optional callback to get configured managers
                                      for a DC (to know total expected managers).
            overload_config: Configuration for overload-based health classification.
        """
        self._phi_config = phi_config
        # One detector per manager edge, alive as long as its manager is
        # tracked.
        self._manager_detectors: dict[tuple[str, tuple[str, int]], PhiAccrualDetector] = {}
        self._get_configured_managers = get_configured_managers
        self._overload_classifier = DatacenterOverloadClassifier(overload_config)

        self._dc_manager_info: dict[str, dict[tuple[str, int], CachedManagerInfo]] = {}
        self._known_datacenters: set[str] = set()
        self._previous_health_states: dict[str, str] = {}
        self._pending_transitions: list[tuple[str, str, str]] = []

    # =========================================================================
    # Manager Heartbeat Updates
    # =========================================================================

    def update_manager(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
        heartbeat: ManagerHeartbeat,
    ) -> None:
        """
        Update manager heartbeat information.

        Args:
            dc_id: Datacenter ID the manager belongs to.
            manager_addr: (host, port) tuple for the manager.
            heartbeat: The received heartbeat message.
        """
        self._known_datacenters.add(dc_id)

        if dc_id not in self._dc_manager_info:
            self._dc_manager_info[dc_id] = {}

        now = _DEFAULT_CLOCK.monotonic()
        self._dc_manager_info[dc_id][manager_addr] = CachedManagerInfo(
            heartbeat=heartbeat,
            last_seen=now,
            is_alive=True,
        )
        if (detector := self._manager_detectors.get((dc_id, manager_addr))) is None:
            detector = PhiAccrualDetector(self._phi_config)
            self._manager_detectors[(dc_id, manager_addr)] = detector
        detector.heartbeat(now)

    def is_manager_suspected(self, manager_addr: tuple[str, int]) -> bool:
        """Whether phi accrual on this gate's heartbeats from the manager at
        ``manager_addr`` is past its threshold (AD-52 section 8), in any
        datacenter it reports in. A manager never heard from is unknown,
        not suspected."""
        now = _DEFAULT_CLOCK.monotonic()
        return any(
            not detector.is_available(now)
            for (_datacenter_id, detector_addr), detector in self._manager_detectors.items()
            if detector_addr == manager_addr
        )

    def mark_manager_dead(self, dc_id: str, manager_addr: tuple[str, int]) -> None:
        """Mark a manager as dead (failed SWIM probes)."""
        dc_managers = self._dc_manager_info.get(dc_id, {})
        if manager_addr in dc_managers:
            dc_managers[manager_addr].is_alive = False

    def remove_manager(self, dc_id: str, manager_addr: tuple[str, int]) -> None:
        """Remove a manager from tracking."""
        dc_managers = self._dc_manager_info.get(dc_id, {})
        dc_managers.pop(manager_addr, None)
        self._manager_detectors.pop((dc_id, manager_addr), None)

    def add_datacenter(self, dc_id: str) -> None:
        """Add a datacenter to tracking (even if no managers yet)."""
        self._known_datacenters.add(dc_id)
        if dc_id not in self._dc_manager_info:
            self._dc_manager_info[dc_id] = {}

    def get_manager_info(
        self, dc_id: str, manager_addr: tuple[str, int]
    ) -> CachedManagerInfo | None:
        """Get cached manager info."""
        return self._dc_manager_info.get(dc_id, {}).get(manager_addr)

    # =========================================================================
    # Health Classification
    # =========================================================================

    def get_datacenter_health(self, dc_id: str) -> DatacenterStatus:
        """
        Classify datacenter health based on manager heartbeats.

        Uses the three-signal health model to determine DC health:
        1. UNHEALTHY: No managers or no workers
        2. DEGRADED: Majority unhealthy
        3. BUSY: No capacity but healthy
        4. HEALTHY: Has capacity and healthy

        Args:
            dc_id: The datacenter to classify.

        Returns:
            DatacenterStatus with health classification.
        """
        best_heartbeat, alive_count, total_count = self.get_best_manager_heartbeat(
            dc_id
        )

        total_count = self._expected_manager_count(dc_id, total_count)

        if total_count == 0:
            return self._build_unhealthy_status(dc_id, 0, 0)

        # Managers are configured but not one has EVER sent a heartbeat:
        # the datacenter is still coming up (AWAITING_INITIAL in the
        # registration state machine), not broken. Classifying this as
        # UNHEALTHY made warmup indistinguishable from outage — gates
        # accepted jobs and insta-failed them during the first seconds
        # of a cluster's life. Heartbeats that existed and went stale
        # still classify UNHEALTHY below (real loss).
        if not self._dc_manager_info.get(dc_id):
            return self._build_initializing_status(dc_id)

        return self._classify_reporting_datacenter(
            dc_id, best_heartbeat, alive_count, total_count
        )

    def _expected_manager_count(self, dc_id: str, tracked_count: int) -> int:
        """Managers expected in the DC: the tracked count, raised to the configured count when known."""
        if self._get_configured_managers:
            configured = self._get_configured_managers(dc_id)
            return max(tracked_count, len(configured))
        return tracked_count

    def _classify_reporting_datacenter(
        self,
        dc_id: str,
        best_heartbeat: ManagerHeartbeat | None,
        alive_count: int,
        total_count: int,
    ) -> DatacenterStatus:
        """Classify a DC some manager has reported from: stale, unwritable, or by its workers."""
        if not best_heartbeat:
            return self._build_unhealthy_status(dc_id, alive_count, 0)

        # The authoritative manager cannot write durably (full or failing
        # disk): any job placed here fails at its first ledger write.
        # Route around it until its storage probe succeeds again.
        if not best_heartbeat.storage_writable:
            self._record_health_transition(dc_id, DatacenterHealth.UNHEALTHY.value)
            return self._build_unhealthy_status(
                dc_id, alive_count, best_heartbeat.worker_count
            )

        return self._classify_by_workers(dc_id, best_heartbeat, alive_count, total_count)

    def _classify_by_workers(
        self,
        dc_id: str,
        best_heartbeat: ManagerHeartbeat,
        alive_count: int,
        total_count: int,
    ) -> DatacenterStatus:
        """Classify a writable DC: BUSY with no workers, else by its overload signals (AD-16)."""
        # Live managers, zero workers: no capacity right now, but the
        # tier that accepts and queues work is up. Per the
        # DatacenterHealth contract that is BUSY ("transient, will clear
        # -> accept job (queued)") — the manager parks the job until a
        # worker registers, and bucket priority still prefers HEALTHY
        # datacenters for routing. Classifying it UNHEALTHY made worker
        # warmup (and total worker loss with a live manager) terminal:
        # gates accepted jobs on one selector's forgiving view, then
        # insta-failed them on the dispatch path's strict one.
        if best_heartbeat.worker_count == 0:
            return DatacenterStatus(
                dc_id=dc_id,
                health=DatacenterHealth.BUSY.value,
                available_capacity=0,
                manager_count=alive_count,
                worker_count=0,
                last_update=_DEFAULT_CLOCK.monotonic(),
            )

        signals = self._extract_overload_signals(
            best_heartbeat, alive_count, total_count, dc_id
        )
        overload_result = self._overload_classifier.classify(signals)

        health = self._map_overload_state_to_health(overload_result.state)
        self._record_health_transition(dc_id, health.value)

        return DatacenterStatus(
            dc_id=dc_id,
            health=health.value,
            available_capacity=best_heartbeat.available_cores,
            manager_count=alive_count,
            worker_count=best_heartbeat.healthy_worker_count,
            last_update=_DEFAULT_CLOCK.monotonic(),
            overloaded_worker_count=best_heartbeat.overloaded_worker_count,
            stressed_worker_count=best_heartbeat.stressed_worker_count,
            busy_worker_count=best_heartbeat.busy_worker_count,
            worker_overload_ratio=overload_result.worker_overload_ratio,
            health_severity_weight=overload_result.health_severity_weight,
            overloaded_manager_count=signals.overloaded_managers,
            stressed_manager_count=signals.stressed_managers,
            busy_manager_count=signals.busy_managers,
            manager_overload_ratio=overload_result.manager_overload_ratio,
            leader_overloaded=overload_result.leader_overloaded,
        )

    def _build_unhealthy_status(
        self,
        dc_id: str,
        manager_count: int,
        worker_count: int,
    ) -> DatacenterStatus:
        return DatacenterStatus(
            dc_id=dc_id,
            health=DatacenterHealth.UNHEALTHY.value,
            available_capacity=0,
            manager_count=manager_count,
            worker_count=worker_count,
            last_update=_DEFAULT_CLOCK.monotonic(),
        )

    def _build_initializing_status(
        self,
        dc_id: str,
    ) -> DatacenterStatus:
        """Status for a configured datacenter no manager has ever
        reported from — the pre-first-heartbeat warmup window."""
        return DatacenterStatus(
            dc_id=dc_id,
            health=DatacenterHealth.INITIALIZING.value,
            available_capacity=0,
            manager_count=0,
            worker_count=0,
            last_update=_DEFAULT_CLOCK.monotonic(),
        )

    def _extract_overload_signals(
        self,
        heartbeat: ManagerHeartbeat,
        alive_managers: int,
        total_managers: int,
        dc_id: str,
    ) -> DatacenterOverloadSignals:
        manager_health_counts = self._aggregate_manager_health_states(dc_id)

        return DatacenterOverloadSignals(
            total_workers=heartbeat.worker_count,
            healthy_workers=heartbeat.healthy_worker_count,
            overloaded_workers=heartbeat.overloaded_worker_count,
            stressed_workers=heartbeat.stressed_worker_count,
            busy_workers=heartbeat.busy_worker_count,
            total_managers=total_managers,
            alive_managers=alive_managers,
            total_cores=heartbeat.total_cores,
            available_cores=heartbeat.available_cores,
            overloaded_managers=manager_health_counts.get("overloaded", 0),
            stressed_managers=manager_health_counts.get("stressed", 0),
            busy_managers=manager_health_counts.get("busy", 0),
            leader_health_state=heartbeat.health_overload_state,
        )

    def _aggregate_manager_health_states(self, dc_id: str) -> dict[str, int]:
        dc_managers = self._dc_manager_info.get(dc_id, {})
        now = _DEFAULT_CLOCK.monotonic()
        counts: dict[str, int] = {
            "healthy": 0,
            "busy": 0,
            "stressed": 0,
            "overloaded": 0,
        }

        for manager_addr, info in dc_managers.items():
            if self._manager_is_live(dc_id, manager_addr, info, now):
                self._count_manager_health_state(counts, info)

        return counts

    def _manager_is_live(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
        info: CachedManagerInfo,
        now: float,
    ) -> bool:
        """A manager counts while its phi-accrual detector is available (AD-52 section 8) and SWIM has it alive."""
        return self._manager_detectors[(dc_id, manager_addr)].is_available(now) and info.is_alive

    @staticmethod
    def _count_manager_health_state(counts: dict[str, int], info: CachedManagerInfo) -> None:
        """Tally the manager's reported overload state; an unrecognized state counts as healthy."""
        health_state = info.heartbeat.health_overload_state
        counts[health_state if health_state in counts else "healthy"] += 1

    def _map_overload_state_to_health(
        self,
        state: DatacenterOverloadState,
    ) -> DatacenterHealth:
        mapping = {
            DatacenterOverloadState.HEALTHY: DatacenterHealth.HEALTHY,
            DatacenterOverloadState.BUSY: DatacenterHealth.BUSY,
            DatacenterOverloadState.DEGRADED: DatacenterHealth.DEGRADED,
            DatacenterOverloadState.UNHEALTHY: DatacenterHealth.UNHEALTHY,
        }
        return mapping.get(state, DatacenterHealth.DEGRADED)

    def get_health_severity_weight(self, dc_id: str) -> float:
        return self.get_datacenter_health(dc_id).health_severity_weight

    def _record_health_transition(self, dc_id: str, new_health: str) -> None:
        previous_health = self._previous_health_states.get(dc_id)
        self._previous_health_states[dc_id] = new_health

        if previous_health and previous_health != new_health:
            self._pending_transitions.append((dc_id, previous_health, new_health))

    def get_and_clear_health_transitions(
        self,
    ) -> list[tuple[str, str, str]]:
        transitions = list(self._pending_transitions)
        self._pending_transitions.clear()
        return transitions

    def known_datacenters(self) -> frozenset[str]:
        """Every datacenter this manager has heard of."""
        return frozenset(self._known_datacenters)

    def get_all_datacenter_health(self) -> dict[str, DatacenterStatus]:
        """Get health classification for all known datacenters."""
        return {
            dc_id: self.get_datacenter_health(dc_id)
            for dc_id in self._known_datacenters
        }

    def is_datacenter_healthy(self, dc_id: str) -> bool:
        """Check if a datacenter is healthy or busy (can accept jobs)."""
        status = self.get_datacenter_health(dc_id)
        return status.health in (
            DatacenterHealth.HEALTHY.value,
            DatacenterHealth.BUSY.value,
        )

    def get_healthy_datacenters(self) -> list[str]:
        """Get list of healthy datacenter IDs."""
        return [
            dc_id
            for dc_id in self._known_datacenters
            if self.is_datacenter_healthy(dc_id)
        ]

    # =========================================================================
    # Manager Selection
    # =========================================================================

    def get_best_manager_heartbeat(
        self, dc_id: str
    ) -> tuple[ManagerHeartbeat | None, int, int]:
        """
        Get the most authoritative manager heartbeat for a datacenter.

        Strategy:
        1. Prefer the LEADER's heartbeat if fresh
        2. Fall back to any fresh manager heartbeat
        3. Return None if no fresh heartbeats

        Returns:
            (best_heartbeat, alive_manager_count, total_manager_count)
        """
        dc_managers = self._dc_manager_info.get(dc_id, {})
        now = _DEFAULT_CLOCK.monotonic()

        live_heartbeats = self._live_heartbeats(dc_id, dc_managers, now)

        return self._preferred_heartbeat(live_heartbeats), len(live_heartbeats), len(dc_managers)

    def _live_heartbeats(
        self,
        dc_id: str,
        dc_managers: dict[tuple[str, int], CachedManagerInfo],
        now: float,
    ) -> list[ManagerHeartbeat]:
        """The heartbeats of the DC's fresh, alive managers, in tracking order."""
        return [
            info.heartbeat
            for manager_addr, info in dc_managers.items()
            if self._manager_is_live(dc_id, manager_addr, info, now)
        ]

    @staticmethod
    def _preferred_heartbeat(live_heartbeats: list[ManagerHeartbeat]) -> ManagerHeartbeat | None:
        """Prefer the (last-seen) leader's heartbeat; else keep the first fresh one as fallback."""
        # Track leader separately
        leader_heartbeat = next(filter(attrgetter("is_leader"), reversed(live_heartbeats)), None)
        if leader_heartbeat is not None:
            return leader_heartbeat
        return live_heartbeats[0] if live_heartbeats else None

    def get_leader_address(self, dc_id: str) -> tuple[str, int] | None:
        """
        Get the address of the DC leader manager.

        Returns:
            (host, port) of the leader, or None if no leader found.
        """
        dc_managers = self._dc_manager_info.get(dc_id, {})
        now = _DEFAULT_CLOCK.monotonic()

        for manager_addr, info in dc_managers.items():
            if self._manager_is_live_leader(dc_id, manager_addr, info, now):
                return manager_addr

        return None

    def _manager_is_live_leader(
        self,
        dc_id: str,
        manager_addr: tuple[str, int],
        info: CachedManagerInfo,
        now: float,
    ) -> bool:
        """A fresh, alive manager whose heartbeat claims DC leadership."""
        return self._manager_is_live(dc_id, manager_addr, info, now) and info.heartbeat.is_leader

    def get_alive_managers(self, dc_id: str) -> list[tuple[str, int]]:
        """Get list of alive manager addresses in a datacenter."""
        dc_managers = self._dc_manager_info.get(dc_id, {})
        now = _DEFAULT_CLOCK.monotonic()

        return [
            manager_addr
            for manager_addr, info in dc_managers.items()
            if self._manager_is_live(dc_id, manager_addr, info, now)
        ]

    # =========================================================================
    # Statistics
    # =========================================================================

    def count_active_datacenters(self) -> int:
        """Count datacenters with at least one alive manager."""
        count = 0
        for dc_id in self._known_datacenters:
            if self.get_alive_managers(dc_id):
                count += 1
        return count

    def get_stats(self) -> dict:
        """Get statistics about datacenter health tracking."""
        return {
            "known_datacenters": len(self._known_datacenters),
            "active_datacenters": self.count_active_datacenters(),
            "datacenters": {
                dc_id: {
                    "manager_count": len(self._dc_manager_info.get(dc_id, {})),
                    "alive_managers": len(self.get_alive_managers(dc_id)),
                    "health": self.get_datacenter_health(dc_id).health,
                }
                for dc_id in self._known_datacenters
            },
        }

    # =========================================================================
    # Cleanup
    # =========================================================================

    def cleanup_stale_managers(self, max_age_seconds: float) -> int:
        """
        Remove managers not heard from for ``max_age_seconds``, and their
        detectors.

        Returns:
            Number of managers removed.
        """
        timeout = max_age_seconds
        now = _DEFAULT_CLOCK.monotonic()
        removed = 0

        for dc_id in list(self._dc_manager_info.keys()):
            dc_managers = self._dc_manager_info[dc_id]
            to_remove = self._stale_manager_addresses(dc_managers, now, timeout)

            for addr in to_remove:
                dc_managers.pop(addr, None)
                self._manager_detectors.pop((dc_id, addr), None)
                removed += 1

        return removed

    @staticmethod
    def _stale_manager_addresses(
        dc_managers: dict[tuple[str, int], CachedManagerInfo],
        now: float,
        timeout: float,
    ) -> list[tuple[str, int]]:
        """Addresses of the managers last seen more than ``timeout`` seconds before ``now``."""
        return [
            manager_addr
            for manager_addr, info in dc_managers.items()
            if (now - info.last_seen) > timeout
        ]

_REHOMED = (
    CachedManagerInfo,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
