"""``RoleAwareConfirmationManager`` -- pickled under the namespace
``hyperscale.distributed.swim.roles.confirmation_manager`` (see that module)."""

import asyncio
from typing import Callable, Awaitable
from hyperscale.distributed.models.distributed import NodeRole
from hyperscale.distributed.swim.roles.confirmation_strategy import RoleBasedConfirmationStrategy, get_strategy_for_role
from hyperscale.distributed.runtime import Clock, RealClock

from .confirmation_result import ConfirmationResult
from .unconfirmed_peer_state import UnconfirmedPeerState

_DEFAULT_CLOCK: Clock = RealClock()


class RoleAwareConfirmationManager:
    """
    Manages role-aware confirmation for unconfirmed peers (AD-35 Task 12.5.3).

    Features:
    - Role-specific timeout and retry strategies
    - Proactive confirmation for Gates/Managers
    - Passive-only strategy for Workers (no probing)
    - LHM load-aware timeout scaling

    Usage:
        manager = RoleAwareConfirmationManager(
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
        send_ping: Callable[[str, tuple[str, int]], Awaitable[bool]] | None = None,
        get_lhm_multiplier: Callable[[], float] | None = None,
        on_peer_confirmed: Callable[[str], Awaitable[None]] | None = None,
        on_peer_removed: Callable[[str, str], Awaitable[None]] | None = None,
    ) -> None:
        """
        Initialize the confirmation manager.

        Args:
            send_ping: Async function to send confirmation ping (returns True if successful)
            get_lhm_multiplier: Function returning current LHM load multiplier
            on_peer_confirmed: Callback when peer is confirmed
            on_peer_removed: Callback when peer is removed (with reason)
        """
        self._unconfirmed_peers: dict[str, UnconfirmedPeerState] = {}
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

            now = _DEFAULT_CLOCK.monotonic()
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
        now = _DEFAULT_CLOCK.monotonic()

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

        # Check if past passive timeout
        if removal_reason := self._expired_removal_reason(state, strategy, now):
            return await self._remove_peer(
                peer_id,
                state,
                removal_reason,
            )

        # Check if due for proactive attempt
        if self._proactive_attempt_due(state, strategy, now):
            return await self._attempt_proactive_confirmation(peer_id, state, strategy, now)

        return None

    def _expired_removal_reason(
        self,
        state: UnconfirmedPeerState,
        strategy: RoleBasedConfirmationStrategy,
        now: float,
    ) -> str | None:
        """The removal reason for a peer past its passive timeout (AD-35 Task 12.5.5), else None."""
        effective_timeout = self._calculate_effective_timeout(strategy)
        elapsed = now - state.discovered_at
        if elapsed >= effective_timeout:
            return self._passive_timeout_removal_reason(state, strategy)
        return None

    @staticmethod
    def _passive_timeout_removal_reason(
        state: UnconfirmedPeerState,
        strategy: RoleBasedConfirmationStrategy,
    ) -> str | None:
        """Why a timed-out peer goes: passive-only roles at once, proactive ones once attempts run out."""
        if not strategy.enable_proactive_confirmation:
            # Passive-only strategy (workers): remove immediately
            return "passive_timeout_expired"
        # Check if we've exhausted proactive attempts
        if state.confirmation_attempts_made >= strategy.confirmation_attempts:
            return "exhausted_proactive_attempts"
        return None

    @staticmethod
    def _proactive_attempt_due(
        state: UnconfirmedPeerState,
        strategy: RoleBasedConfirmationStrategy,
        now: float,
    ) -> bool:
        """True when a proactive strategy's next scheduled attempt time has arrived."""
        return (
            strategy.enable_proactive_confirmation
            and state.next_attempt_at is not None
            and now >= state.next_attempt_at
        )

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

            self._record_proactive_attempt(state, strategy, now)

        # Send ping if callback is configured
        if await self._ping_succeeded(peer_id, state):
            return await self._confirm_peer_internal(peer_id, state)

        return await self._remove_if_exhausted(peer_id, state, strategy)

    @staticmethod
    def _record_proactive_attempt(
        state: UnconfirmedPeerState,
        strategy: RoleBasedConfirmationStrategy,
        now: float,
    ) -> None:
        """Count an attempt and schedule the next one unless exhausted (caller holds the lock)."""
        state.confirmation_attempts_made += 1
        state.last_attempt_at = now

        # Schedule next attempt if not exhausted
        if state.confirmation_attempts_made < strategy.confirmation_attempts:
            state.next_attempt_at = now + strategy.attempt_interval_seconds
        else:
            state.next_attempt_at = None  # No more attempts

    async def _ping_succeeded(self, peer_id: str, state: UnconfirmedPeerState) -> bool:
        """Ping the peer when a ping callback is configured; True when it answered."""
        # ``send_ping`` answers False for a ping that went unanswered; one
        # that raises reaches the caller, which reports it.
        if self._send_ping and await self._send_ping(peer_id, state.peer_address):
            return True
        return False

    async def _remove_if_exhausted(
        self,
        peer_id: str,
        state: UnconfirmedPeerState,
        strategy: RoleBasedConfirmationStrategy,
    ) -> ConfirmationResult | None:
        """Remove the peer once its proactive attempts are exhausted; None while attempts remain."""
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
    ) -> float:
        """
        The role's passive timeout stretched by this node's own load (LHM),
        up to the role's cap.

        Network distance does not enter: a passive timeout is minutes and
        any round trip well under a second, so the RTT-ratio multiplier
        this applied (up to 10x for a 100ms peer, plus a coordinate
        quality factor) inflated a far peer's timeout by many minutes for
        no measured reason.
        """
        return strategy.passive_timeout_seconds * min(
            self._get_lhm_multiplier(),
            strategy.load_multiplier_max,
        )

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
