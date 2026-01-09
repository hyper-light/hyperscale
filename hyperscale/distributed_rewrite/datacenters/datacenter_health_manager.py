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

from hyperscale.distributed_rewrite.models import (
    ManagerHeartbeat,
    DatacenterHealth,
    DatacenterStatus,
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
    ):
        """
        Initialize DatacenterHealthManager.

        Args:
            heartbeat_timeout: Seconds before a heartbeat is considered stale.
            get_configured_managers: Optional callback to get configured managers
                                      for a DC (to know total expected managers).
        """
        self._heartbeat_timeout = heartbeat_timeout
        self._get_configured_managers = get_configured_managers

        # Per-datacenter, per-manager heartbeat tracking
        # dc_id -> {manager_addr -> ManagerInfo}
        self._dc_manager_info: dict[str, dict[tuple[str, int], ManagerInfo]] = {}

        # Known datacenter IDs (from configuration or discovery)
        self._known_datacenters: set[str] = set()

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
        # Get best manager heartbeat for this DC
        best_heartbeat, alive_count, total_count = self._get_best_manager_heartbeat(dc_id)

        # Get configured manager count if available
        if self._get_configured_managers:
            configured = self._get_configured_managers(dc_id)
            total_count = max(total_count, len(configured))

        # === UNHEALTHY: No managers registered ===
        if total_count == 0:
            return DatacenterStatus(
                dc_id=dc_id,
                health=DatacenterHealth.UNHEALTHY.value,
                available_capacity=0,
                queue_depth=0,
                manager_count=0,
                worker_count=0,
                last_update=time.monotonic(),
            )

        # === UNHEALTHY: No fresh heartbeats or no workers ===
        if not best_heartbeat or best_heartbeat.worker_count == 0:
            return DatacenterStatus(
                dc_id=dc_id,
                health=DatacenterHealth.UNHEALTHY.value,
                available_capacity=0,
                queue_depth=0,
                manager_count=alive_count,
                worker_count=0,
                last_update=time.monotonic(),
            )

        # Extract health info from best heartbeat
        total_workers = best_heartbeat.worker_count
        healthy_workers = getattr(best_heartbeat, "healthy_worker_count", total_workers)
        available_cores = best_heartbeat.available_cores

        # === Check for DEGRADED state ===
        is_degraded = False

        # Majority of managers unhealthy?
        manager_quorum = total_count // 2 + 1
        if total_count > 0 and alive_count < manager_quorum:
            is_degraded = True

        # Majority of workers unhealthy?
        worker_quorum = total_workers // 2 + 1
        if total_workers > 0 and healthy_workers < worker_quorum:
            is_degraded = True

        # === Determine final health state ===
        if is_degraded:
            health = DatacenterHealth.DEGRADED
        elif available_cores == 0:
            # Not degraded, but no capacity = BUSY (transient)
            health = DatacenterHealth.BUSY
        else:
            # Not degraded, has capacity = HEALTHY
            health = DatacenterHealth.HEALTHY

        return DatacenterStatus(
            dc_id=dc_id,
            health=health.value,
            available_capacity=available_cores,
            queue_depth=getattr(best_heartbeat, "queue_depth", 0),
            manager_count=alive_count,
            worker_count=healthy_workers,
            last_update=time.monotonic(),
        )

    def get_all_datacenter_health(self) -> dict[str, DatacenterStatus]:
        """Get health classification for all known datacenters."""
        return {dc_id: self.get_datacenter_health(dc_id) for dc_id in self._known_datacenters}

    def is_datacenter_healthy(self, dc_id: str) -> bool:
        """Check if a datacenter is healthy or busy (can accept jobs)."""
        status = self.get_datacenter_health(dc_id)
        return status.health in (DatacenterHealth.HEALTHY.value, DatacenterHealth.BUSY.value)

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
