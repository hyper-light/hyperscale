"""``QueuePutResult`` -- pickled under the namespace
``hyperscale.distributed.reliability.robust_queue`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.reliability.backpressure import BackpressureSignal

from .queue_state import QueueState


@dataclass(slots=True)
class QueuePutResult:
    """Result of a put operation with backpressure information."""
    accepted: bool           # True if message was queued
    in_overflow: bool        # True if message went to overflow buffer
    dropped: bool            # True if message was dropped
    queue_state: QueueState  # Current queue state
    fill_ratio: float        # Primary queue fill ratio (0.0 - 1.0)
    backpressure: BackpressureSignal  # Backpressure signal for sender

    @property
    def suggested_delay_ms(self) -> int:
        """Convenience accessor for backpressure delay."""
        return self.backpressure.suggested_delay_ms
