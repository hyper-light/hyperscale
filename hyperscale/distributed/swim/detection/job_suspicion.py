"""``JobSuspicion`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.job_suspicion_manager`` (see that module)."""

import asyncio
import math
from dataclasses import dataclass, field
from hyperscale.distributed.protocol.time_quantum import TIME_REMAINDER_EPSILON_SECONDS

from .job_suspicion_manager_shared import _DEFAULT_CLOCK
from .job_suspicion_manager_shared import NodeAddress
from .job_suspicion_manager_shared import JobId


@dataclass(slots=True)
class JobSuspicion:
    """
    Suspicion state for a specific node within a specific job.

    Tracks the suspicion independently of global node status.
    """

    job_id: JobId
    node: NodeAddress
    incarnation: int
    start_time: float
    min_timeout: float
    max_timeout: float
    # Originator exclusion — same Lifeguard contract as
    # SuspicionState.originator: the suspecting node's own evidence is
    # the suspicion itself; only OTHER nodes' confirmations accelerate.
    originator: NodeAddress | None = None
    confirmers: set[NodeAddress] = field(default_factory=set)
    _logical_confirmation_count: int = 0

    # Timer management
    _poll_task: asyncio.Task | None = field(default=None, repr=False)
    _cancelled: bool = False

    def add_confirmation(self, from_node: NodeAddress) -> bool:
        """Add a confirmation from another node. Returns True if new."""
        if from_node == self.originator:
            return False

        if from_node in self.confirmers:
            return False

        self._logical_confirmation_count += 1
        if len(self.confirmers) < 1000:  # Bound memory
            self.confirmers.add(from_node)
        return True

    @property
    def confirmation_count(self) -> int:
        """Number of independent confirmations."""
        return max(len(self.confirmers), self._logical_confirmation_count)

    def calculate_timeout(self, n_members: int) -> float:
        """
        Calculate timeout using Lifeguard formula.

        timeout = max(min, max - (max - min) * log(C+1) / log(N+1))
        """
        c = self.confirmation_count
        n = max(1, n_members)

        if n <= 1:
            return self.max_timeout

        log_factor = math.log(c + 1) / math.log(n + 1)
        timeout = self.max_timeout - (self.max_timeout - self.min_timeout) * log_factor

        return max(self.min_timeout, timeout)

    def time_remaining(self, n_members: int) -> float:
        """Calculate time remaining before expiration.

        Sub-epsilon remainders report as 0.0 (the protocol epsilon-
        expiry contract, ``protocol.time_quantum``): the composed-float
        remainder can be a positive sub-quantum artifact (observed
        live: the gate's job-suspicion of a dead manager spun at one
        frozen virtual instant because ``remaining <= 0`` said "not
        yet" while ``sleep(min(interval, remaining))`` re-armed a timer
        the quantized clock could not honor — the frozen-instant
        livelock class)."""
        elapsed = _DEFAULT_CLOCK.monotonic() - self.start_time
        timeout = self.calculate_timeout(n_members)
        remaining = timeout - elapsed
        if remaining <= TIME_REMAINDER_EPSILON_SECONDS:
            return 0.0
        return remaining

    def cancel(self) -> None:
        """Cancel this suspicion's timer."""
        self._cancelled = True
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()

    def cleanup(self) -> None:
        """Clean up resources."""
        self.cancel()
        self.confirmers.clear()
