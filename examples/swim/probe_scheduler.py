"""
Probe scheduler for SWIM randomized round-robin probing.
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
    """
    members: list[tuple[str, int]] = field(default_factory=list)
    probe_index: int = 0
    protocol_period: float = 1.0  # Time between probes in seconds
    _running: bool = False
    _probe_task: asyncio.Task | None = field(default=None, repr=False)
    
    def update_members(self, members: list[tuple[str, int]]) -> None:
        """
        Update the member list and reshuffle.
        Called when membership changes.
        """
        self.members = list(members)
        random.shuffle(self.members)
        # Reset index if it's now out of bounds
        if self.probe_index >= len(self.members):
            self.probe_index = 0
    
    def get_next_target(self) -> tuple[str, int] | None:
        """
        Get the next member to probe.
        Returns None if no members available.
        """
        if not self.members:
            return None
        
        # If we've probed everyone, reshuffle
        if self.probe_index >= len(self.members):
            random.shuffle(self.members)
            self.probe_index = 0
        
        target = self.members[self.probe_index]
        self.probe_index += 1
        return target
    
    def remove_member(self, member: tuple[str, int]) -> None:
        """Remove a member from the probe list (e.g., when declared dead)."""
        if member in self.members:
            # Adjust index if needed
            idx = self.members.index(member)
            self.members.remove(member)
            if idx < self.probe_index:
                self.probe_index = max(0, self.probe_index - 1)
    
    def add_member(self, member: tuple[str, int]) -> None:
        """Add a new member to the probe list."""
        if member not in self.members:
            # Insert at random position for unpredictability
            if self.members:
                insert_idx = random.randint(0, len(self.members))
                self.members.insert(insert_idx, member)
                # Adjust probe index if we inserted before it
                if insert_idx <= self.probe_index:
                    self.probe_index += 1
            else:
                self.members.append(member)
    
    def get_probe_cycle_time(self) -> float:
        """
        Calculate time to complete one full probe cycle.
        This is the maximum time before a failure is detected.
        """
        return len(self.members) * self.protocol_period
    
    def stop(self) -> None:
        """Stop the probe scheduler."""
        self._running = False
        if self._probe_task and not self._probe_task.done():
            self._probe_task.cancel()
            self._probe_task = None

