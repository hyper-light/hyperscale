"""
Manager Dispatcher - Manager selection and routing within a datacenter.

This class encapsulates the logic for selecting and dispatching to managers
within a datacenter, including fallback and retry strategies.

Key responsibilities:
- Select best manager for a datacenter (prefer leader)
- Dispatch jobs to managers with retry logic
- Handle fallback to other DCs when primary fails
- Track dispatch success/failure for circuit breaking
"""

import time
from dataclasses import dataclass, field
from typing import Protocol, Callable, Awaitable

from hyperscale.distributed_rewrite.models import (
    DatacenterHealth,
)


class SendTcpProtocol(Protocol):
    """Protocol for TCP send function."""

    async def __call__(
        self,
        addr: tuple[str, int],
        endpoint: str,
        data: bytes,
        timeout: float = 5.0,
    ) -> tuple[bytes, float]: ...


@dataclass(slots=True)
class DispatchResult:
    """Result of a dispatch attempt."""

    success: bool
    datacenter: str
    manager_addr: tuple[str, int] | None = None
    response: bytes | None = None
    error: str | None = None
    latency_ms: float = 0.0


@dataclass(slots=True)
class DispatchStats:
    """Statistics for dispatch operations."""

    total_dispatches: int = 0
    successful_dispatches: int = 0
    failed_dispatches: int = 0
    fallback_dispatches: int = 0
    avg_latency_ms: float = 0.0
    last_dispatch_time: float = 0.0


class ManagerDispatcher:
    """
    Dispatches jobs to managers within datacenters.

    Handles manager selection, dispatch with retry, and fallback strategies.

    Example usage:
        dispatcher = ManagerDispatcher()

        # Configure datacenters
        dispatcher.add_datacenter("dc-1", [("10.0.0.1", 8080), ("10.0.0.2", 8080)])
        dispatcher.add_datacenter("dc-2", [("10.0.1.1", 8080)])

        # Dispatch to a specific DC
        result = await dispatcher.dispatch_to_datacenter(
            dc_id="dc-1",
            endpoint="job_submission",
            data=submission.dump(),
            send_tcp=gate_server.send_tcp,
        )

        # Dispatch with fallback
        successful, failed = await dispatcher.dispatch_with_fallback(
            endpoint="job_submission",
            data=submission.dump(),
            send_tcp=gate_server.send_tcp,
            primary_dcs=["dc-1"],
            fallback_dcs=["dc-2"],
        )
    """

    def __init__(
        self,
        dispatch_timeout: float = 5.0,
        max_retries_per_dc: int = 2,
    ):
        """
        Initialize ManagerDispatcher.

        Args:
            dispatch_timeout: Timeout for dispatch TCP calls.
            max_retries_per_dc: Max managers to try in a DC before failing.
        """
        self._dispatch_timeout = dispatch_timeout
        self._max_retries_per_dc = max_retries_per_dc

        # DC -> list of manager addresses
        self._dc_managers: dict[str, list[tuple[str, int]]] = {}

        # DC -> leader address (if known)
        self._dc_leaders: dict[str, tuple[str, int]] = {}

        # Per-DC dispatch statistics
        self._dc_stats: dict[str, DispatchStats] = {}

        # Overall statistics
        self._total_stats = DispatchStats()

    # =========================================================================
    # Datacenter Configuration
    # =========================================================================

    def add_datacenter(
        self,
        dc_id: str,
        manager_addrs: list[tuple[str, int]],
    ) -> None:
        """
        Add or update a datacenter's manager addresses.

        Args:
            dc_id: Datacenter ID.
            manager_addrs: List of (host, port) tuples for managers.
        """
        self._dc_managers[dc_id] = list(manager_addrs)
        if dc_id not in self._dc_stats:
            self._dc_stats[dc_id] = DispatchStats()

    def remove_datacenter(self, dc_id: str) -> None:
        """Remove a datacenter from dispatch tracking."""
        self._dc_managers.pop(dc_id, None)
        self._dc_leaders.pop(dc_id, None)
        self._dc_stats.pop(dc_id, None)

    def set_leader(self, dc_id: str, leader_addr: tuple[str, int]) -> None:
        """Set the known leader address for a datacenter."""
        self._dc_leaders[dc_id] = leader_addr

    def clear_leader(self, dc_id: str) -> None:
        """Clear the known leader for a datacenter."""
        self._dc_leaders.pop(dc_id, None)

    def get_managers(self, dc_id: str) -> list[tuple[str, int]]:
        """Get manager addresses for a datacenter."""
        return list(self._dc_managers.get(dc_id, []))

    def get_leader(self, dc_id: str) -> tuple[str, int] | None:
        """Get the known leader address for a datacenter."""
        return self._dc_leaders.get(dc_id)

    def has_datacenter(self, dc_id: str) -> bool:
        """Check if a datacenter is configured."""
        return dc_id in self._dc_managers

    def get_all_datacenters(self) -> list[str]:
        """Get all configured datacenter IDs."""
        return list(self._dc_managers.keys())

    # =========================================================================
    # Dispatch Operations
    # =========================================================================

    async def dispatch_to_datacenter(
        self,
        dc_id: str,
        endpoint: str,
        data: bytes,
        send_tcp: SendTcpProtocol,
    ) -> DispatchResult:
        """
        Dispatch to a specific datacenter.

        Tries the known leader first, then falls back to other managers.

        Args:
            dc_id: Target datacenter.
            endpoint: TCP endpoint to call.
            data: Data to send.
            send_tcp: TCP send function.

        Returns:
            DispatchResult indicating success/failure.
        """
        managers = self._dc_managers.get(dc_id, [])
        if not managers:
            return DispatchResult(
                success=False,
                datacenter=dc_id,
                error="No managers configured for datacenter",
            )

        # Build ordered list: leader first (if known), then others
        leader = self._dc_leaders.get(dc_id)
        ordered_managers: list[tuple[str, int]] = []

        if leader and leader in managers:
            ordered_managers.append(leader)
            ordered_managers.extend(m for m in managers if m != leader)
        else:
            ordered_managers = list(managers)

        # Try managers in order
        last_error: str | None = None
        attempts = 0

        for manager_addr in ordered_managers:
            if attempts >= self._max_retries_per_dc:
                break

            attempts += 1
            start_time = time.monotonic()

            try:
                response, _ = await send_tcp(
                    manager_addr,
                    endpoint,
                    data,
                    self._dispatch_timeout,
                )

                latency_ms = (time.monotonic() - start_time) * 1000

                # Success
                self._record_success(dc_id, latency_ms)

                return DispatchResult(
                    success=True,
                    datacenter=dc_id,
                    manager_addr=manager_addr,
                    response=response if isinstance(response, bytes) else None,
                    latency_ms=latency_ms,
                )

            except Exception as exception:
                last_error = str(exception)
                continue

        # All attempts failed
        self._record_failure(dc_id)

        return DispatchResult(
            success=False,
            datacenter=dc_id,
            error=last_error or "All manager attempts failed",
        )

    async def dispatch_with_fallback(
        self,
        endpoint: str,
        data: bytes,
        send_tcp: SendTcpProtocol,
        primary_dcs: list[str],
        fallback_dcs: list[str] | None = None,
        get_dc_health: Callable[[str], str] | None = None,
    ) -> tuple[list[str], list[str]]:
        """
        Dispatch to datacenters with automatic fallback.

        Priority: HEALTHY > BUSY > DEGRADED
        Only fails if ALL DCs are UNHEALTHY.

        Args:
            endpoint: TCP endpoint to call.
            data: Data to send.
            send_tcp: TCP send function.
            primary_dcs: Primary target DCs.
            fallback_dcs: Fallback DCs to try if primary fails.
            get_dc_health: Optional function to get DC health status.

        Returns:
            (successful_dcs, failed_dcs)
        """
        successful: list[str] = []
        failed: list[str] = []
        fallback_queue = list(fallback_dcs or [])

        for dc in primary_dcs:
            result = await self.dispatch_to_datacenter(
                dc_id=dc,
                endpoint=endpoint,
                data=data,
                send_tcp=send_tcp,
            )

            if result.success:
                successful.append(dc)
            else:
                # Try fallback DCs
                fallback_success = False

                while fallback_queue:
                    fallback_dc = fallback_queue.pop(0)

                    # Skip unhealthy fallback DCs if health function provided
                    if get_dc_health:
                        health = get_dc_health(fallback_dc)
                        if health == DatacenterHealth.UNHEALTHY.value:
                            continue

                    fallback_result = await self.dispatch_to_datacenter(
                        dc_id=fallback_dc,
                        endpoint=endpoint,
                        data=data,
                        send_tcp=send_tcp,
                    )

                    if fallback_result.success:
                        successful.append(fallback_dc)
                        fallback_success = True
                        self._total_stats.fallback_dispatches += 1
                        break

                if not fallback_success:
                    failed.append(dc)

        return successful, failed

    async def broadcast_to_all(
        self,
        endpoint: str,
        data: bytes,
        send_tcp: SendTcpProtocol,
        datacenters: list[str] | None = None,
    ) -> dict[str, DispatchResult]:
        """
        Broadcast to all (or specified) datacenters.

        Dispatches in parallel and collects results.

        Args:
            endpoint: TCP endpoint to call.
            data: Data to send.
            send_tcp: TCP send function.
            datacenters: Specific DCs to broadcast to (defaults to all).

        Returns:
            Dict mapping dc_id -> DispatchResult.
        """
        import asyncio

        target_dcs = datacenters or list(self._dc_managers.keys())

        # Dispatch to all DCs concurrently
        tasks = [
            self.dispatch_to_datacenter(
                dc_id=dc_id,
                endpoint=endpoint,
                data=data,
                send_tcp=send_tcp,
            )
            for dc_id in target_dcs
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Build result dict
        result_dict: dict[str, DispatchResult] = {}
        for i, result in enumerate(results):
            dc_id = target_dcs[i]
            if isinstance(result, Exception):
                result_dict[dc_id] = DispatchResult(
                    success=False,
                    datacenter=dc_id,
                    error=str(result),
                )
            else:
                result_dict[dc_id] = result

        return result_dict

    # =========================================================================
    # Statistics
    # =========================================================================

    def _record_success(self, dc_id: str, latency_ms: float) -> None:
        """Record a successful dispatch."""
        # Update DC stats
        dc_stats = self._dc_stats.get(dc_id)
        if dc_stats:
            dc_stats.total_dispatches += 1
            dc_stats.successful_dispatches += 1
            dc_stats.last_dispatch_time = time.monotonic()
            # Update running average latency
            if dc_stats.avg_latency_ms == 0:
                dc_stats.avg_latency_ms = latency_ms
            else:
                dc_stats.avg_latency_ms = (dc_stats.avg_latency_ms * 0.9) + (latency_ms * 0.1)

        # Update total stats
        self._total_stats.total_dispatches += 1
        self._total_stats.successful_dispatches += 1
        self._total_stats.last_dispatch_time = time.monotonic()

    def _record_failure(self, dc_id: str) -> None:
        """Record a failed dispatch."""
        # Update DC stats
        dc_stats = self._dc_stats.get(dc_id)
        if dc_stats:
            dc_stats.total_dispatches += 1
            dc_stats.failed_dispatches += 1

        # Update total stats
        self._total_stats.total_dispatches += 1
        self._total_stats.failed_dispatches += 1

    def get_stats(self, dc_id: str | None = None) -> dict:
        """Get dispatch statistics."""
        if dc_id:
            dc_stats = self._dc_stats.get(dc_id)
            if dc_stats:
                return {
                    "datacenter": dc_id,
                    "total_dispatches": dc_stats.total_dispatches,
                    "successful_dispatches": dc_stats.successful_dispatches,
                    "failed_dispatches": dc_stats.failed_dispatches,
                    "success_rate": (
                        dc_stats.successful_dispatches / dc_stats.total_dispatches
                        if dc_stats.total_dispatches > 0
                        else 0.0
                    ),
                    "avg_latency_ms": dc_stats.avg_latency_ms,
                }
            return {}

        return {
            "total_dispatches": self._total_stats.total_dispatches,
            "successful_dispatches": self._total_stats.successful_dispatches,
            "failed_dispatches": self._total_stats.failed_dispatches,
            "fallback_dispatches": self._total_stats.fallback_dispatches,
            "success_rate": (
                self._total_stats.successful_dispatches / self._total_stats.total_dispatches
                if self._total_stats.total_dispatches > 0
                else 0.0
            ),
            "per_dc": {
                dc_id: {
                    "total": stats.total_dispatches,
                    "success": stats.successful_dispatches,
                    "failed": stats.failed_dispatches,
                    "avg_latency_ms": stats.avg_latency_ms,
                }
                for dc_id, stats in self._dc_stats.items()
            },
        }

    def reset_stats(self) -> None:
        """Reset all statistics."""
        self._total_stats = DispatchStats()
        for dc_id in self._dc_stats:
            self._dc_stats[dc_id] = DispatchStats()
