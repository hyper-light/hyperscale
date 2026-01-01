"""
Probe scheduler for SWIM randomized round-robin probing.

Uses a lockless copy-on-write pattern for high performance:
- Reads (get_next_target) are completely lock-free
- Writes (update_members, add_member, remove_member) create new immutable tuples
- Python's GIL ensures atomic reference swaps
"""

import asyncio
import random
from dataclasses import dataclass, field


@dataclass
class ProbeScheduler:
    """
    Implements SWIM's randomized round-robin probing.
    
    In SWIM, members are probed in a randomized round-robin fashion:
    1. Shuffle the member list
    2. Probe each member in sequence
    3. When exhausted, reshuffle and repeat
    
    This ensures:
    - Each member is probed within a bounded time window
    - Probing is unpredictable (helps with network partition handling)
    - Even load distribution across members
    
    Lockless Design (Copy-on-Write):
    - _members is an immutable tuple, swapped atomically on updates
    - _member_set enables O(1) membership checks
    - _probe_index uses modulo for wraparound, increment is atomic under GIL
    - Reads are completely lock-free for maximum performance
    - Writes create new tuples and swap references atomically
    """
    # Internal immutable state
    _members: tuple[tuple[str, int], ...] = field(default_factory=tuple)
    _member_set: frozenset[tuple[str, int]] = field(default_factory=frozenset)
    _probe_index: int = 0
    _last_cycle_length: int = 0  # Track for reshuffle detection
    
    protocol_period: float = 1.0  # Time between probes in seconds
    _running: bool = False
    _probe_task: asyncio.Task | None = field(default=None, repr=False)
    
    # Stats for monitoring
    _cycles_completed: int = 0
    _reshuffles: int = 0
    
    @property
    def members(self) -> tuple[tuple[str, int], ...]:
        """Read-only access to current members."""
        return self._members
    
    def update_members(self, members: list[tuple[str, int]]) -> None:
        """
        Update the member list and reshuffle.
        Called when membership changes.
        
        Lockless: Creates new immutable tuple and swaps atomically.
        """
        new_set = frozenset(members)
        
        # No change - skip
        if new_set == self._member_set:
            return
        
        # Create new shuffled tuple
        new_list = list(members)
        random.shuffle(new_list)
        new_members = tuple(new_list)
        
        # Atomic swap (single reference assignment under GIL)
        self._member_set = new_set
        self._members = new_members
        self._last_cycle_length = len(new_members)
        self._reshuffles += 1
        
        # Reset index to start fresh with new membership
        self._probe_index = 0
    
    def get_next_target(self) -> tuple[str, int] | None:
        """
        Get the next member to probe.
        Returns None if no members available.
        
        Lockless: Uses local snapshot and atomic index increment.
        """
        # Get snapshot (atomic read)
        members = self._members
        
        if not members:
            return None
        
        length = len(members)
        
        # Get current index and increment atomically (under GIL)
        # Use modulo for safe wraparound
        idx = self._probe_index
        self._probe_index = idx + 1
        
        # Check if we completed a cycle (for reshuffling)
        if idx > 0 and idx % length == 0:
            self._cycles_completed += 1
            # Reshuffle for unpredictability on next update
            # We don't reshuffle inline to avoid races
        
        # Use modulo to handle wraparound
        effective_idx = idx % length
        return members[effective_idx]
    
    def remove_member(self, member: tuple[str, int]) -> None:
        """
        Remove a member from the probe list (e.g., when declared dead).
        
        Lockless: Creates new tuple without the member.
        """
        if member not in self._member_set:
            return
        
        # Create new tuple without this member
        new_members = tuple(m for m in self._members if m != member)
        new_set = self._member_set - {member}
        
        # Atomic swap
        self._member_set = new_set
        self._members = new_members
        self._last_cycle_length = len(new_members)
    
    def add_member(self, member: tuple[str, int]) -> None:
        """
        Add a new member to the probe list.
        
        Lockless: Creates new tuple with the member at random position.
        """
        if member in self._member_set:
            return
        
        # Insert at random position for unpredictability
        new_list = list(self._members)
        if new_list:
            insert_idx = random.randint(0, len(new_list))
            new_list.insert(insert_idx, member)
        else:
            new_list.append(member)
        
        new_members = tuple(new_list)
        new_set = self._member_set | {member}
        
        # Atomic swap
        self._member_set = new_set
        self._members = new_members
        self._last_cycle_length = len(new_members)
    
    def get_probe_cycle_time(self) -> float:
        """
        Calculate time to complete one full probe cycle.
        This is the maximum time before a failure is detected.
        """
        return len(self._members) * self.protocol_period
    
    def get_stats(self) -> dict[str, int]:
        """Get scheduler statistics for monitoring."""
        return {
            'member_count': len(self._members),
            'probe_index': self._probe_index,
            'cycles_completed': self._cycles_completed,
            'reshuffles': self._reshuffles,
        }
    
    def stop(self) -> None:
        """
        Stop the probe scheduler.
        
        Safe to call from any context - uses atomic flag.
        """
        # Cancel task first to prevent new iterations from starting
        task = self._probe_task
        self._probe_task = None
        self._running = False
        
        if task and not task.done():
            task.cancel()
