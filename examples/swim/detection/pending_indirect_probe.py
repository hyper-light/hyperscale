"""
Pending indirect probe tracking for SWIM protocol.
"""

import time
from dataclasses import dataclass, field

# Maximum proxies per probe - typically k=3, but allow some margin
MAX_PROXIES = 10


@dataclass
class PendingIndirectProbe:
    """
    Tracks a pending indirect probe request.
    
    When a direct probe to a target fails, we ask k other nodes to 
    probe the target on our behalf. This tracks those pending requests.
    
    Memory safety:
    - Proxies set is bounded to max_proxies to prevent unbounded growth
    """
    target: tuple[str, int]
    requester: tuple[str, int]
    start_time: float
    timeout: float
    proxies: set[tuple[str, int]] = field(default_factory=set)
    received_acks: int = 0
    _completed: bool = False
    max_proxies: int = MAX_PROXIES
    _proxies_dropped: int = 0
    
    def add_proxy(self, proxy: tuple[str, int]) -> bool:
        """
        Add a proxy node that we asked to probe the target.
        
        Returns True if added, False if at max capacity.
        """
        if len(self.proxies) >= self.max_proxies:
            self._proxies_dropped += 1
            return False
        self.proxies.add(proxy)
        return True
    
    def record_ack(self) -> bool:
        """
        Record that we received an ack from one of the proxies.
        Returns True if this is the first ack (target is alive).
        """
        if self._completed:
            return False
        self.received_acks += 1
        if self.received_acks == 1:
            self._completed = True
            return True
        return False
    
    def is_expired(self) -> bool:
        """Check if the probe request has timed out."""
        return time.monotonic() - self.start_time > self.timeout
    
    def is_completed(self) -> bool:
        """Check if we've received an ack (probe succeeded)."""
        return self._completed

