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
"""

import time
from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed.models import (
    ManagerHeartbeat,
    DatacenterHealth,
    DatacenterStatus,
)
from hyperscale.distributed.datacenters.datacenter_overload_config import (
    DatacenterOverloadConfig,
    DatacenterOverloadState,
)
from hyperscale.distributed.datacenters.datacenter_overload_classifier import (
    DatacenterOverloadClassifier,
    DatacenterOverloadSignals,
)


@dataclass(slots=True)
class ManagerInfo:
    """Cached information about a manager."""

    heartbeat: ManagerHeartbeat
    last_seen: float
    is_alive: bool = True


class DatacenterHealthManager:
    """
    Manages datacenter health classification based on manager health.

    Tracks manager heartbeats for each datacenter and classifies overall
    DC health using the three-signal health model.

    Example usage:
        manager = DatacenterHealthManager(heartbeat_timeout=30.0)

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
        heartbeat_timeout: float = 30.0,
        get_configured_managers: Callable[[str], list[tuple[str, int]]] | None = None,
        overload_config: DatacenterOverloadConfig | None = None,
    ):
        """
        Initialize DatacenterHealthManager.

        Args:
            heartbeat_timeout: Seconds before a heartbeat is considered stale.
            get_configured_managers: Optional callback to get configured managers
                                      for a DC (to know total expected managers).
            overload_config: Configuration for overload-based health classification.
        """
        self._heartbeat_timeout = heartbeat_timeout
        self._get_configured_managers = get_configured_managers
        self._overload_classifier = DatacenterOverloadClassifier(overload_config)

        self._dc_manager_info: dict[str, dict[tuple[str, int], ManagerInfo]] = {}
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

        self._dc_manager_info[dc_id][manager_addr] = ManagerInfo(
            heartbeat=heartbeat,
            last_seen=time.monotonic(),
            is_alive=True,
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

    def add_datacenter(self, dc_id: str) -> None:
        """Add a datacenter to tracking (even if no managers yet)."""
        self._known_datacenters.add(dc_id)
        if dc_id not in self._dc_manager_info:
            self._dc_manager_info[dc_id] = {}

    def get_manager_info(
        self, dc_id: str, manager_addr: tuple[str, int]
    ) -> ManagerInfo | None:
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
        best_heartbeat, alive_count, total_count = self._get_best_manager_heartbeat(
            dc_id
        )

        if self._get_configured_managers:
            configured = self._get_configured_managers(dc_id)
            total_count = max(total_count, len(configured))

        if total_count == 0:
            return self._build_unhealthy_status(dc_id, 0, 0)

        if not best_heartbeat or best_heartbeat.worker_count == 0:
            return self._build_unhealthy_status(dc_id, alive_count, 0)

        signals = self._extract_overload_signals(
            best_heartbeat, alive_count, total_count, dc_id
        )
        overload_result = self._overload_classifier.classify(signals)

        health = self._map_overload_state_to_health(overload_result.state)
        healthy_workers = getattr(
            best_heartbeat, "healthy_worker_count", best_heartbeat.worker_count
        )

        self._record_health_transition(dc_id, health.value)

        return DatacenterStatus(
            dc_id=dc_id,
            health=health.value,
            available_capacity=best_heartbeat.available_cores,
            queue_depth=getattr(best_heartbeat, "queue_depth", 0),
            manager_count=alive_count,
            worker_count=healthy_workers,
            last_update=time.monotonic(),
            overloaded_worker_count=getattr(
                best_heartbeat, "overloaded_worker_count", 0
            ),
            stressed_worker_count=getattr(best_heartbeat, "stressed_worker_count", 0),
            busy_worker_count=getattr(best_heartbeat, "busy_worker_count", 0),
            worker_overload_ratio=overload_result.worker_overload_ratio,
            health_severity_weight=overload_result.health_severity_weight,
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
            queue_depth=0,
            manager_count=manager_count,
            worker_count=worker_count,
            last_update=time.monotonic(),
        )

    def _extract_overload_signals(
        self,
        heartbeat: ManagerHeartbeat,
        alive_managers: int,
        total_managers: int,
        dc_id: str,
    ) -> DatacenterOverloadSignals:
        manager_health_counts = self._aggregate_manager_health_states(dc_id)
        leader_health_state = getattr(heartbeat, "health_overload_state", "healthy")

        return DatacenterOverloadSignals(
            total_workers=heartbeat.worker_count,
            healthy_workers=getattr(
                heartbeat, "healthy_worker_count", heartbeat.worker_count
            ),
            overloaded_workers=getattr(heartbeat, "overloaded_worker_count", 0),
            stressed_workers=getattr(heartbeat, "stressed_worker_count", 0),
            busy_workers=getattr(heartbeat, "busy_worker_count", 0),
            total_managers=total_managers,
            alive_managers=alive_managers,
            total_cores=heartbeat.total_cores,
            available_cores=heartbeat.available_cores,
            overloaded_managers=manager_health_counts.get("overloaded", 0),
            stressed_managers=manager_health_counts.get("stressed", 0),
            busy_managers=manager_health_counts.get("busy", 0),
            leader_health_state=leader_health_state,
        )

    def _aggregate_manager_health_states(self, dc_id: str) -> dict[str, int]:
        dc_managers = self._dc_manager_info.get(dc_id, {})
        now = time.monotonic()
        counts: dict[str, int] = {
            "healthy": 0,
            "busy": 0,
            "stressed": 0,
            "overloaded": 0,
        }

        for manager_addr, info in dc_managers.items():
            is_fresh = (now - info.last_seen) < self._heartbeat_timeout
            if not is_fresh or not info.is_alive:
                continue

            health_state = getattr(info.heartbeat, "health_overload_state", "healthy")
            if health_state in counts:
                counts[health_state] += 1
            else:
                counts["healthy"] += 1

        return counts

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
        status = self.get_datacenter_health(dc_id)
        return getattr(status, "health_severity_weight", 1.0)

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

    def _get_best_manager_heartbeat(
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
        now = time.monotonic()

        best_heartbeat: ManagerHeartbeat | None = None
        leader_heartbeat: ManagerHeartbeat | None = None
        alive_count = 0

        for manager_addr, info in dc_managers.items():
            is_fresh = (now - info.last_seen) < self._heartbeat_timeout

            if is_fresh and info.is_alive:
                alive_count += 1

                # Track leader separately
                if info.heartbeat.is_leader:
                    leader_heartbeat = info.heartbeat

                # Keep any fresh heartbeat as fallback
                if best_heartbeat is None:
                    best_heartbeat = info.heartbeat

        # Prefer leader if available
        if leader_heartbeat is not None:
            best_heartbeat = leader_heartbeat

        return best_heartbeat, alive_count, len(dc_managers)

    def get_leader_address(self, dc_id: str) -> tuple[str, int] | None:
        """
        Get the address of the DC leader manager.

        Returns:
            (host, port) of the leader, or None if no leader found.
        """
        dc_managers = self._dc_manager_info.get(dc_id, {})
        now = time.monotonic()

        for manager_addr, info in dc_managers.items():
            is_fresh = (now - info.last_seen) < self._heartbeat_timeout
            if is_fresh and info.is_alive and info.heartbeat.is_leader:
                return manager_addr

        return None

    def get_alive_managers(self, dc_id: str) -> list[tuple[str, int]]:
        """Get list of alive manager addresses in a datacenter."""
        dc_managers = self._dc_manager_info.get(dc_id, {})
        now = time.monotonic()

        result: list[tuple[str, int]] = []
        for manager_addr, info in dc_managers.items():
            is_fresh = (now - info.last_seen) < self._heartbeat_timeout
            if is_fresh and info.is_alive:
                result.append(manager_addr)

        return result

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

    def cleanup_stale_managers(self, max_age_seconds: float | None = None) -> int:
        """
        Remove managers with stale heartbeats.

        Args:
            max_age_seconds: Override timeout (defaults to configured timeout).

        Returns:
            Number of managers removed.
        """
        timeout = max_age_seconds or self._heartbeat_timeout
        now = time.monotonic()
        removed = 0

        for dc_id in list(self._dc_manager_info.keys()):
            dc_managers = self._dc_manager_info[dc_id]
            to_remove: list[tuple[str, int]] = []

            for manager_addr, info in dc_managers.items():
                if (now - info.last_seen) > timeout:
                    to_remove.append(manager_addr)

            for addr in to_remove:
                dc_managers.pop(addr, None)
                removed += 1

        return removed
