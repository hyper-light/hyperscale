"""
Suspicion management for Lifeguard protocol.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Any

from .suspicion_state import SuspicionState


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
    
    # Stats for monitoring
    _expired_count: int = 0
    _refuted_count: int = 0
    _orphaned_cleanup_count: int = 0
    _race_avoided_count: int = 0  # Double-check prevented race condition
    
    def __post_init__(self):
        """Initialize the lock after dataclass creation."""
        self._lock = asyncio.Lock()
    
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
    
    def start_suspicion(
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
        """
        existing = self.suspicions.get(node)
        
        if existing:
            if incarnation < existing.incarnation:
                # Stale suspicion message, ignore
                return existing
            elif incarnation == existing.incarnation:
                # Same suspicion, add confirmation
                existing.add_confirmation(from_node)
                # Recalculate timeout with new confirmation
                self._reschedule_timer(existing)
                return existing
            else:
                # Higher incarnation suspicion, replace
                self._cancel_timer(existing)
        else:
            # New suspicion - check limits
            if len(self.suspicions) >= self.max_suspicions:
                # Try to cleanup orphaned suspicions first
                self.cleanup_orphaned()
                
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
            await asyncio.sleep(timeout)
            self._handle_expiration(state)
        
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
                self._timer_tokens[state.node] = f"{run.name}:{run.run_id}"
        else:
            # Fallback to raw asyncio task
            state._timer_task = asyncio.create_task(expire_suspicion())
    
    def _reschedule_timer(self, state: SuspicionState) -> None:
        """Reschedule timer with updated timeout (after new confirmation)."""
        self._cancel_timer(state)
        remaining = state.time_remaining()
        if remaining > 0:
            async def expire_suspicion():
                await asyncio.sleep(remaining)
                self._handle_expiration(state)
            
            if self._task_runner:
                run = self._task_runner.run(
                    expire_suspicion,
                    timeout=remaining + 5.0,
                    keep=100,
                    max_age='5m',
                    keep_policy='COUNT_AND_AGE',
                )
                if run:
                    self._timer_tokens[state.node] = f"{run.name}:{run.run_id}"
            else:
                state._timer_task = asyncio.create_task(expire_suspicion())
        else:
            self._handle_expiration(state)
    
    def _cancel_timer(self, state: SuspicionState) -> None:
        """Cancel the timer for a suspicion."""
        # Cancel via TaskRunner if available
        if state.node in self._timer_tokens and self._task_runner:
            token = self._timer_tokens.pop(state.node, None)
            if token:
                try:
                    # Use task runner's run method instead of raw create_task
                    self._task_runner.run(self._task_runner.cancel, token)
                except Exception:
                    pass
        
        # Also cancel the raw task if present
        state.cancel_timer()
    
    def _handle_expiration(self, state: SuspicionState) -> None:
        """
        Handle suspicion expiration - declare node as DEAD.
        
        Uses double-check pattern to prevent race conditions with _cancel_timer.
        """
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
        if self._on_suspicion_expired:
            self._on_suspicion_expired(state.node, state.incarnation)
    
    def confirm_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
        from_node: tuple[str, int],
    ) -> bool:
        """
        Add a confirmation to an existing suspicion.
        Returns True if the suspicion exists and confirmation was added.
        """
        state = self.suspicions.get(node)
        if state and state.incarnation == incarnation:
            if state.add_confirmation(from_node):
                self._reschedule_timer(state)
                return True
        return False
    
    def refute_suspicion(
        self,
        node: tuple[str, int],
        incarnation: int,
    ) -> bool:
        """
        Refute a suspicion (node proved it's alive with higher incarnation).
        Returns True if a suspicion was cleared.
        """
        state = self.suspicions.get(node)
        if state and incarnation > state.incarnation:
            self._cancel_timer(state)
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
    
    def clear_all(self) -> None:
        """Clear all suspicions (e.g., on shutdown)."""
        for state in self.suspicions.values():
            self._cancel_timer(state)
        self.suspicions.clear()
        self._timer_tokens.clear()
    
    def get_suspicions_to_regossip(self) -> list[SuspicionState]:
        """Get suspicions that should be re-gossiped."""
        return [s for s in self.suspicions.values() if s.should_regossip()]
    
    def cleanup_orphaned(self) -> int:
        """
        Cleanup suspicions with no active timer (orphaned).
        
        This can happen if:
        - Timer task raised an exception
        - Timer was cancelled but suspicion wasn't removed
        
        Returns:
            Number of orphaned suspicions removed.
        """
        now = time.monotonic()
        cutoff = now - self.orphaned_timeout
        
        to_remove = []
        for node, state in self.suspicions.items():
            # Check if timer is missing or dead
            has_timer_token = node in self._timer_tokens
            has_raw_timer = state._timer_task is not None and not state._timer_task.done()
            
            if not has_timer_token and not has_raw_timer:
                # No active timer - check age
                if state.start_time < cutoff:
                    to_remove.append(node)
        
        for node in to_remove:
            state = self.suspicions.pop(node)
            self._timer_tokens.pop(node, None)
            self._orphaned_cleanup_count += 1
            # Treat as expired
            if self._on_suspicion_expired:
                self._on_suspicion_expired(state.node, state.incarnation)
        
        return len(to_remove)
    
    def cleanup(self) -> dict[str, int]:
        """
        Run all cleanup operations.
        
        Returns:
            Dict with cleanup stats.
        """
        orphaned = self.cleanup_orphaned()
        
        return {
            'orphaned_removed': orphaned,
            'active_suspicions': len(self.suspicions),
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
            'race_conditions_avoided': self._race_avoided_count,
        }

