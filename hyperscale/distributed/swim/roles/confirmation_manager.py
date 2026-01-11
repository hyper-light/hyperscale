"""
Role-aware confirmation manager for unconfirmed peers (AD-35 Task 12.5.3-12.5.6).

Manages the confirmation lifecycle for peers discovered via gossip but not yet
confirmed via bidirectional communication (ping/ack).
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable

from hyperscale.distributed.models.distributed import NodeRole
from hyperscale.distributed.swim.roles.confirmation_strategy import (
    RoleBasedConfirmationStrategy,
    get_strategy_for_role,
)
from hyperscale.distributed.swim.coordinates.coordinate_tracker import (
    CoordinateTracker,
)


@dataclass(slots=True)
class UnconfirmedPeerState:
    """State tracking for an unconfirmed peer."""

    peer_id: str
    peer_address: tuple[str, int]
    role: NodeRole
    discovered_at: float
    confirmation_attempts_made: int = 0
    next_attempt_at: float | None = None
    last_attempt_at: float | None = None


@dataclass
class ConfirmationResult:
    """Result of a confirmation attempt or cleanup decision."""

    peer_id: str
    confirmed: bool
    removed: bool
    attempts_made: int
    reason: str


class RoleAwareConfirmationManager:
    """
    Manages role-aware confirmation for unconfirmed peers (AD-35 Task 12.5.3).

    Features:
    - Role-specific timeout and retry strategies
    - Proactive confirmation for Gates/Managers
    - Passive-only strategy for Workers (no probing)
    - Vivaldi-aware timeout adjustment for latency-aware confirmation
    - LHM load-aware timeout scaling

    Usage:
        manager = RoleAwareConfirmationManager(
            coordinator_tracker=coord_tracker,
            send_ping=my_ping_function,
            get_lhm_multiplier=my_lhm_function,
        )

        # When peer is discovered via gossip
        manager.track_unconfirmed_peer(peer_id, address, role)

        # When peer responds to ping/ack
        manager.confirm_peer(peer_id)

        # Periodic cleanup (run in background)
        await manager.check_and_cleanup_unconfirmed_peers()
    """

    def __init__(
        self,
        coordinator_tracker: CoordinateTracker | None = None,
        send_ping: Callable[[str, tuple[str, int]], Awaitable[bool]] | None = None,
        get_lhm_multiplier: Callable[[], float] | None = None,
        on_peer_confirmed: Callable[[str], Awaitable[None]] | None = None,
        on_peer_removed: Callable[[str, str], Awaitable[None]] | None = None,
    ) -> None:
        """
        Initialize the confirmation manager.

        Args:
            coordinator_tracker: Vivaldi coordinate tracker for RTT estimation
            send_ping: Async function to send confirmation ping (returns True if successful)
            get_lhm_multiplier: Function returning current LHM load multiplier
            on_peer_confirmed: Callback when peer is confirmed
            on_peer_removed: Callback when peer is removed (with reason)
        """
        self._unconfirmed_peers: dict[str, UnconfirmedPeerState] = {}
        self._coordinator_tracker = coordinator_tracker
        self._send_ping = send_ping
        self._get_lhm_multiplier = get_lhm_multiplier or (lambda: 1.0)
        self._on_peer_confirmed = on_peer_confirmed
        self._on_peer_removed = on_peer_removed
        self._lock = asyncio.Lock()

        # Metrics
        self._total_confirmed: int = 0
        self._total_removed_by_role: dict[NodeRole, int] = {
            NodeRole.GATE: 0,
            NodeRole.MANAGER: 0,
            NodeRole.WORKER: 0,
        }
        self._total_proactive_attempts: int = 0

    async def track_unconfirmed_peer(
        self,
        peer_id: str,
        peer_address: tuple[str, int],
        role: NodeRole,
    ) -> None:
        """
        Start tracking an unconfirmed peer (AD-35 Task 12.5.3).

        Called when a peer is discovered via gossip but not yet confirmed
        via bidirectional communication.

        Args:
            peer_id: Unique identifier for the peer
            peer_address: (host, port) tuple
            role: Peer's role (Gate/Manager/Worker)
        """
        async with self._lock:
            if peer_id in self._unconfirmed_peers:
                return  # Already tracking

            now = time.monotonic()
            strategy = get_strategy_for_role(role)

            state = UnconfirmedPeerState(
                peer_id=peer_id,
                peer_address=peer_address,
                role=role,
                discovered_at=now,
            )

            # Schedule first proactive attempt if enabled
            if strategy.enable_proactive_confirmation:
                # Start proactive confirmation after half the passive timeout
                state.next_attempt_at = now + (strategy.passive_timeout_seconds / 2)

            self._unconfirmed_peers[peer_id] = state

    async def confirm_peer(self, peer_id: str) -> bool:
        """
        Mark a peer as confirmed (AD-35 Task 12.5.3).

        Called when bidirectional communication is established (ping/ack success).

        Args:
            peer_id: The peer that was confirmed

        Returns:
            True if peer was being tracked and is now confirmed
        """
        async with self._lock:
            if peer_id not in self._unconfirmed_peers:
                return False

            state = self._unconfirmed_peers.pop(peer_id)
            self._total_confirmed += 1

        if self._on_peer_confirmed:
            await self._on_peer_confirmed(peer_id)

        return True

    async def check_and_cleanup_unconfirmed_peers(self) -> list[ConfirmationResult]:
        """
        Check all unconfirmed peers and perform cleanup/confirmation (AD-35 Task 12.5.3).

        This should be called periodically (e.g., every 5 seconds).

        Actions:
        - For peers past passive timeout with no proactive confirmation: remove
        - For peers due for proactive attempt: send ping
        - For peers that exhausted retries: remove

        Returns:
            List of confirmation/removal results
        """
        results: list[ConfirmationResult] = []
        now = time.monotonic()

        async with self._lock:
            peers_to_process = list(self._unconfirmed_peers.items())

        for peer_id, state in peers_to_process:
            result = await self._process_unconfirmed_peer(peer_id, state, now)
            if result:
                results.append(result)

        return results

    async def _process_unconfirmed_peer(
        self,
        peer_id: str,
        state: UnconfirmedPeerState,
        now: float,
    ) -> ConfirmationResult | None:
        """Process a single unconfirmed peer."""
        strategy = get_strategy_for_role(state.role)
        effective_timeout = self._calculate_effective_timeout(strategy, state)
        elapsed = now - state.discovered_at

        # Check if past passive timeout
        if elapsed >= effective_timeout:
            if strategy.enable_proactive_confirmation:
                # Check if we've exhausted proactive attempts
                if state.confirmation_attempts_made >= strategy.confirmation_attempts:
                    return await self._remove_peer(
                        peer_id,
                        state,
                        "exhausted_proactive_attempts",
                    )
            else:
                # Passive-only strategy (workers): remove immediately
                return await self._remove_peer(
                    peer_id,
                    state,
                    "passive_timeout_expired",
                )

        # Check if due for proactive attempt
        if (
            strategy.enable_proactive_confirmation
            and state.next_attempt_at is not None
            and now >= state.next_attempt_at
        ):
            return await self._attempt_proactive_confirmation(peer_id, state, strategy, now)

        return None

    async def _attempt_proactive_confirmation(
        self,
        peer_id: str,
        state: UnconfirmedPeerState,
        strategy: RoleBasedConfirmationStrategy,
        now: float,
    ) -> ConfirmationResult | None:
        """
        Attempt proactive confirmation via ping (AD-35 Task 12.5.4).

        Args:
            peer_id: Peer to confirm
            state: Current state
            strategy: Confirmation strategy
            now: Current time

        Returns:
            ConfirmationResult if confirmed or exhausted, None if pending
        """
        self._total_proactive_attempts += 1

        # Update state
        async with self._lock:
            if peer_id not in self._unconfirmed_peers:
                return None

            state.confirmation_attempts_made += 1
            state.last_attempt_at = now

            # Schedule next attempt if not exhausted
            if state.confirmation_attempts_made < strategy.confirmation_attempts:
                state.next_attempt_at = now + strategy.attempt_interval_seconds
            else:
                state.next_attempt_at = None  # No more attempts

        # Send ping if callback is configured
        if self._send_ping:
            try:
                success = await self._send_ping(peer_id, state.peer_address)
                if success:
                    # Ping was acknowledged - peer is confirmed
                    return await self._confirm_peer_internal(peer_id, state)
            except Exception:
                pass  # Failed to send ping, will retry

        # Check if exhausted attempts
        if state.confirmation_attempts_made >= strategy.confirmation_attempts:
            return await self._remove_peer(
                peer_id,
                state,
                "exhausted_proactive_attempts",
            )

        return None

    async def _confirm_peer_internal(
        self,
        peer_id: str,
        state: UnconfirmedPeerState,
    ) -> ConfirmationResult:
        """Internal confirmation after successful ping."""
        async with self._lock:
            self._unconfirmed_peers.pop(peer_id, None)
            self._total_confirmed += 1

        if self._on_peer_confirmed:
            await self._on_peer_confirmed(peer_id)

        return ConfirmationResult(
            peer_id=peer_id,
            confirmed=True,
            removed=False,
            attempts_made=state.confirmation_attempts_made,
            reason="proactive_confirmation_success",
        )

    async def _remove_peer(
        self,
        peer_id: str,
        state: UnconfirmedPeerState,
        reason: str,
    ) -> ConfirmationResult:
        """Remove an unconfirmed peer (AD-35 Task 12.5.5)."""
        async with self._lock:
            self._unconfirmed_peers.pop(peer_id, None)
            self._total_removed_by_role[state.role] += 1

        if self._on_peer_removed:
            await self._on_peer_removed(peer_id, reason)

        return ConfirmationResult(
            peer_id=peer_id,
            confirmed=False,
            removed=True,
            attempts_made=state.confirmation_attempts_made,
            reason=reason,
        )

    def _calculate_effective_timeout(
        self,
        strategy: RoleBasedConfirmationStrategy,
        state: UnconfirmedPeerState,
    ) -> float:
        """
        Calculate effective timeout with Vivaldi and LHM adjustments.

        Formula: timeout = passive_timeout * latency_mult * load_mult * confidence_adj
        """
        base_timeout = strategy.passive_timeout_seconds

        # Get load multiplier from LHM
        load_multiplier = min(
            self._get_lhm_multiplier(),
            strategy.load_multiplier_max,
        )

        # Get latency multiplier from Vivaldi if enabled
        latency_multiplier = 1.0
        confidence_adjustment = 1.0

        if strategy.latency_aware and self._coordinator_tracker is not None:
            peer_coord = self._coordinator_tracker.get_peer_coordinate(state.peer_id)
            if peer_coord is not None:
                # Use RTT UCB to get conservative estimate
                rtt_ucb_ms = self._coordinator_tracker.estimate_rtt_ucb_ms(peer_coord)
                reference_rtt_ms = 10.0  # Same-datacenter baseline

                latency_multiplier = min(
                    10.0,  # Cap at 10x
                    max(1.0, rtt_ucb_ms / reference_rtt_ms),
                )

                # Confidence adjustment based on coordinate quality
                quality = self._coordinator_tracker.coordinate_quality(peer_coord)
                # Lower quality → higher adjustment (more conservative)
                confidence_adjustment = 1.0 + (1.0 - quality) * 0.5

        return base_timeout * latency_multiplier * load_multiplier * confidence_adjustment

    def get_unconfirmed_peer_count(self) -> int:
        """Get number of currently unconfirmed peers."""
        return len(self._unconfirmed_peers)

    def get_unconfirmed_peers_by_role(self) -> dict[NodeRole, int]:
        """Get count of unconfirmed peers by role."""
        counts: dict[NodeRole, int] = {
            NodeRole.GATE: 0,
            NodeRole.MANAGER: 0,
            NodeRole.WORKER: 0,
        }
        for state in self._unconfirmed_peers.values():
            counts[state.role] += 1
        return counts

    def get_metrics(self) -> dict:
        """Get confirmation manager metrics."""
        return {
            "unconfirmed_count": len(self._unconfirmed_peers),
            "unconfirmed_by_role": self.get_unconfirmed_peers_by_role(),
            "total_confirmed": self._total_confirmed,
            "total_removed_by_role": dict(self._total_removed_by_role),
            "total_proactive_attempts": self._total_proactive_attempts,
        }

    async def clear(self) -> None:
        """Clear all tracked peers."""
        async with self._lock:
            self._unconfirmed_peers.clear()
