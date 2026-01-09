"""
Suspicion management for Lifeguard protocol.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Any

from .suspicion_state import SuspicionState


from ..core.protocols import LoggerProtocol


@dataclass
class SuspicionManager:
    """
    Manages suspicions for all nodes using the Lifeguard protocol.
    
    Key features:
    - Tracks active suspicions with confirmation counting
    - Calculates dynamic timeouts based on confirmations
    - Handles suspicion expiration and node death declaration
    - Supports refutation (clearing suspicion on higher incarnation)
    - Applies Local Health Multiplier to timeouts (Lifeguard)
    
    Resource limits:
    - max_suspicions: Maximum concurrent suspicions (default 1000)
    - orphaned_timeout: Cleanup suspicions with no timer after this time
    - Uses TaskRunner for timer management when available
    
    Thread safety:
    - Uses asyncio.Lock to protect dict modifications from async timer callbacks
    - All public methods that modify state are async to enable proper locking
    """
    suspicions: dict[tuple[str, int], SuspicionState] = field(default_factory=dict)
    min_timeout: float = 1.0
    max_timeout: float = 10.0
    
    # Resource limits
    max_suspicions: int = 1000
    """Maximum concurrent suspicions before refusing new ones."""
    
    orphaned_timeout: float = 300.0
    """Timeout for suspicions with failed/missing timers."""
    
    # Callbacks
    _on_suspicion_expired: Callable[[tuple[str, int], int], None] | None = None
    _n_members_getter: Callable[[], int] | None = None
    _lhm_multiplier_getter: Callable[[], float] | None = None
    
    # Task runner integration (optional, for proper task cleanup)
    _task_runner: Any | None = None
    _timer_tokens: dict[tuple[str, int], str] = field(default_factory=dict)
    
    # Track fallback tasks created when TaskRunner not available
    _pending_fallback_tasks: set[asyncio.Task] = field(default_factory=set)
    _unmanaged_tasks_created: int = 0
    
    # Logger for error reporting (optional)
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0
    _node_id: int = 0
    
    # Stats for monitoring
    _expired_count: int = 0
    _refuted_count: int = 0
    _orphaned_cleanup_count: int = 0
    _race_avoided_count: int = 0  # Double-check prevented race condition
    _stale_tokens_cleaned: int = 0  # Tokens cleaned without matching suspicion
    _lock_contention_count: int = 0  # Times lock was already held
    
    def __post_init__(self):
        """Initialize the lock after dataclass creation."""
        self._lock = asyncio.Lock()
    
    def set_logger(
        self,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
        node_id: int,
    ) -> None:
        """Set logger for error reporting."""
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id
    
    def _log_warning(self, message: str) -> None:
        """Log a warning message."""
        if self._logger:
            try:
                from hyperscale.logging.hyperscale_logging_models import ServerDebug
                self._logger.log(ServerDebug(
                    message=message,
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                ))
            except Exception:
                pass  # Don't let logging errors propagate
    
    def set_callbacks(
        self,
        on_expired: Callable[[tuple[str, int], int], None],
        get_n_members: Callable[[], int],
        get_lhm_multiplier: Callable[[], float] | None = None,
    ) -> None:
        """Set callback functions for suspicion events."""
        self._on_suspicion_expired = on_expired
        self._n_members_getter = get_n_members
        self._lhm_multiplier_getter = get_lhm_multiplier
    
    def set_task_runner(self, task_runner: Any) -> None:
        """
        Set the task runner for timer management.
        
        When set, timer tasks will be created through the TaskRunner
        which provides automatic cleanup via keep/max_age policies.
        """
        self._task_runner = task_runner
    
    def _get_lhm_multiplier(self) -> float:
        """Get the current LHM multiplier for timeout adjustment."""
        if self._lhm_multiplier_getter:
            return self._lhm_multiplier_getter()
        return 1.0
    
    def _get_n_members(self) -> int:
        """Get current member count."""
        if self._n_members_getter:
            return self._n_members_getter()
        return 1
    
    async def start_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> SuspicionState | None:
        """
        Start or update a suspicion for a node.
        
        If suspicion already exists with same incarnation, add confirmation.
        If new suspicion or higher incarnation, create new suspicion state.
        
        Timeouts are adjusted by the Local Health Multiplier per Lifeguard.
        
        Returns None if max_suspicions limit reached and this is a new suspicion.
        
        Note: This method is async to allow proper lock synchronization with
        async timer callbacks that also modify the suspicions dict.
        """
        async with self._lock:
            existing = self.suspicions.get(node)
            
            if existing:
                if incarnation < existing.incarnation:
                    # Stale suspicion message, ignore
                    return existing
                elif incarnation == existing.incarnation:
                    # Same suspicion, add confirmation
                    existing.add_confirmation(from_node)
                    # Recalculate timeout with new confirmation
                    await self._reschedule_timer(existing)
                    return existing
                else:
                    # Higher incarnation suspicion, replace
                    await self._cancel_timer(existing)
            else:
                # New suspicion - check limits
                if len(self.suspicions) >= self.max_suspicions:
                    # Try to cleanup orphaned suspicions first
                    self._cleanup_orphaned_unlocked()
                    
                    # Still at limit? Refuse new suspicion
                    if len(self.suspicions) >= self.max_suspicions:
                        return None
            
            # Apply LHM to timeouts - when we're unhealthy, extend timeouts
            # to reduce false positives caused by our own slow processing
            lhm_multiplier = self._get_lhm_multiplier()
            
            # Create new suspicion with LHM-adjusted timeouts
            state = SuspicionState(
                node=node,
                incarnation=incarnation,
                start_time=time.monotonic(),
                min_timeout=self.min_timeout * lhm_multiplier,
                max_timeout=self.max_timeout * lhm_multiplier,
                n_members=self._get_n_members(),
            )
            state.add_confirmation(from_node)
            self.suspicions[node] = state
            
            # Schedule expiration timer
            self._schedule_timer(state)
            
            return state
    
    def _schedule_timer(self, state: SuspicionState) -> None:
        """Schedule the expiration timer for a suspicion."""
        timeout = state.calculate_timeout()

        async def expire_suspicion():
            try:
                await asyncio.sleep(timeout)
                await self._handle_expiration(state)
            except asyncio.CancelledError:
                raise
        
        if self._task_runner:
            # Use TaskRunner for automatic cleanup
            run = self._task_runner.run(
                expire_suspicion,
                timeout=timeout + 5.0,  # Buffer for cleanup
                keep=100,
                max_age='5m',
                keep_policy='COUNT_AND_AGE',
            )
            if run:
                self._timer_tokens[state.node] = f"{run.task_name}:{run.run_id}"
        else:
            # Fallback to raw asyncio task
            state._timer_task = asyncio.create_task(expire_suspicion())
    
    async def _reschedule_timer(self, state: SuspicionState) -> None:
        """Reschedule timer with updated timeout (after new confirmation)."""
        await self._cancel_timer(state)
        remaining = state.time_remaining()
        if remaining > 0:
            async def expire_suspicion():
                try:
                    await asyncio.sleep(remaining)
                    await self._handle_expiration(state)
                except asyncio.CancelledError:
                    raise
            
            if self._task_runner:
                run = self._task_runner.run(
                    expire_suspicion,
                    timeout=remaining + 5.0,
                    keep=100,
                    max_age='5m',
                    keep_policy='COUNT_AND_AGE',
                )
                if run:
                    self._timer_tokens[state.node] = f"{run.task_name}:{run.run_id}"
            else:
                state._timer_task = asyncio.create_task(expire_suspicion())
        else:
            # Schedule immediate expiration via task to maintain async contract
            async def expire_now():
                try:
                    await self._handle_expiration(state)
                finally:
                    # Remove from tracked tasks when done
                    self._pending_fallback_tasks.discard(asyncio.current_task())
            
            if self._task_runner:
                self._task_runner.run(expire_now)
            else:
                # Track fallback task for cleanup
                task = asyncio.create_task(expire_now())
                self._pending_fallback_tasks.add(task)
                self._unmanaged_tasks_created += 1
    
    async def _cancel_timer(self, state: SuspicionState) -> None:
        """Cancel the timer for a suspicion."""
        # Cancel via TaskRunner if available
        if state.node in self._timer_tokens and self._task_runner:
            token = self._timer_tokens.pop(state.node, None)
            if token:
                try:
                    # Await the cancellation directly
                    await self._task_runner.cancel(token)
                except Exception as e:
                    self._log_warning(f"Failed to cancel timer via TaskRunner: {e}")

        # Also cancel the raw task if present
        state.cancel_timer()
    
    async def _handle_expiration(self, state: SuspicionState) -> None:
        """
        Handle suspicion expiration - declare node as DEAD.

        Uses lock + double-check pattern to prevent race conditions.
        This is async to properly coordinate with other async methods.
        """
        async with self._lock:
            # Double-check that suspicion still exists (may have been cancelled)
            if state.node not in self.suspicions:
                self._race_avoided_count += 1
                return

            # Verify this is the same suspicion (not a new one with same node)
            current = self.suspicions.get(state.node)
            if current is not state:
                self._race_avoided_count += 1
                return

            del self.suspicions[state.node]
            self._timer_tokens.pop(state.node, None)
            self._expired_count += 1

        # Call callback outside of lock to avoid deadlock
        if self._on_suspicion_expired:
            self._on_suspicion_expired(state.node, state.incarnation)
    
    async def confirm_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        """
        Add a confirmation to an existing suspicion.
        Returns True if the suspicion exists and confirmation was added.
        """
        async with self._lock:
            state = self.suspicions.get(node)
            if state and state.incarnation == incarnation:
                if state.add_confirmation(from_node):
                    self._reschedule_timer(state)
                    return True
            return False
    
    async def refute_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """
        Refute a suspicion (node proved it's alive with higher incarnation).
        Returns True if a suspicion was cleared.
        """
        async with self._lock:
            state = self.suspicions.get(node)
            if state and incarnation > state.incarnation:
                await self._cancel_timer(state)
                del self.suspicions[node]
                self._refuted_count += 1
                return True
            return False
    
    def get_suspicion(self, node: tuple[str, int]) -> SuspicionState | None:
        """Get the current suspicion state for a node, if any."""
        return self.suspicions.get(node)
    
    def is_suspected(self, node: tuple[str, int]) -> bool:
        """Check if a node is currently suspected."""
        return node in self.suspicions
    
    async def clear_all(self) -> None:
        """Clear all suspicions (e.g., on shutdown)."""
        async with self._lock:
            # Snapshot to avoid dict mutation during iteration
            for state in list(self.suspicions.values()):
                await self._cancel_timer(state)
                state.cleanup()  # Clean up confirmers set
            self.suspicions.clear()
            self._timer_tokens.clear()

            # Cancel any pending fallback tasks
            for task in list(self._pending_fallback_tasks):
                if not task.done():
                    task.cancel()
            self._pending_fallback_tasks.clear()
    
    def get_suspicions_to_regossip(self) -> list[SuspicionState]:
        """Get suspicions that should be re-gossiped."""
        # Read-only operation, no lock needed
        return [s for s in self.suspicions.values() if s.should_regossip()]
    
    def _cleanup_orphaned_unlocked(self) -> tuple[int, list[tuple[tuple[str, int], int]]]:
        """
        Internal: Cleanup orphaned suspicions without acquiring lock.
        
        Must be called while already holding the lock.
        
        Returns:
            Tuple of (count, list of (node, incarnation) for expired nodes).
        """
        now = time.monotonic()
        cutoff = now - self.orphaned_timeout

        to_remove = []
        # Snapshot to avoid dict mutation during iteration
        for node, state in list(self.suspicions.items()):
            # Check if timer is missing or dead
            has_timer_token = node in self._timer_tokens
            has_raw_timer = state._timer_task is not None and not state._timer_task.done()

            if not has_timer_token and not has_raw_timer:
                # No active timer - check age
                if state.start_time < cutoff:
                    to_remove.append(node)
        
        expired_nodes: list[tuple[tuple[str, int], int]] = []
        for node in to_remove:
            state = self.suspicions.pop(node)
            self._timer_tokens.pop(node, None)
            state.cleanup()  # Clean up confirmers set
            self._orphaned_cleanup_count += 1
            expired_nodes.append((state.node, state.incarnation))
        
        return len(to_remove), expired_nodes
    
    async def cleanup_orphaned(self) -> int:
        """
        Cleanup suspicions with no active timer (orphaned).
        
        This can happen if:
        - Timer task raised an exception
        - Timer was cancelled but suspicion wasn't removed
        
        Returns:
            Number of orphaned suspicions removed.
        """
        async with self._lock:
            count, expired_nodes = self._cleanup_orphaned_unlocked()
        
        # Call callbacks outside of lock to avoid deadlock
        for node, incarnation in expired_nodes:
            if self._on_suspicion_expired:
                self._on_suspicion_expired(node, incarnation)
        
        return count
    
    async def cleanup_stale_tokens(self) -> int:
        """
        Remove timer tokens that have no matching suspicion.
        
        This prevents memory leak if tokens accumulate due to:
        - Race conditions in cleanup
        - Suspicions removed without proper token cleanup
        
        Returns:
            Number of stale tokens removed.
        """
        async with self._lock:
            stale_tokens = []
            # Snapshot to avoid dict mutation during iteration
            for node in list(self._timer_tokens.keys()):
                if node not in self.suspicions:
                    stale_tokens.append(node)

            for node in stale_tokens:
                self._timer_tokens.pop(node, None)
                self._stale_tokens_cleaned += 1

            return len(stale_tokens)
    
    async def cleanup(self) -> dict[str, int]:
        """
        Run all cleanup operations.
        
        Returns:
            Dict with cleanup stats.
        """
        orphaned = await self.cleanup_orphaned()
        stale_tokens = await self.cleanup_stale_tokens()
        
        return {
            'orphaned_removed': orphaned,
            'stale_tokens_removed': stale_tokens,
            'active_suspicions': len(self.suspicions),
            'active_timer_tokens': len(self._timer_tokens),
            'total_expired': self._expired_count,
            'total_refuted': self._refuted_count,
        }
    
    def get_stats(self) -> dict[str, int]:
        """Get suspicion manager statistics for monitoring."""
        return {
            'active_suspicions': len(self.suspicions),
            'active_timers': len(self._timer_tokens),
            'total_expired': self._expired_count,
            'total_refuted': self._refuted_count,
            'orphaned_cleaned': self._orphaned_cleanup_count,
            'stale_tokens_cleaned': self._stale_tokens_cleaned,
            'race_conditions_avoided': self._race_avoided_count,
        }

