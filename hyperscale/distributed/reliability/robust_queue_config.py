"""``RobustQueueConfig`` -- pickled under the namespace
``hyperscale.distributed.reliability.robust_queue`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from .robust_message_queue import RobustMessageQueue


@dataclass(slots=True)
class RobustQueueConfig:
    """Configuration for RobustMessageQueue."""

    # Primary queue settings
    maxsize: int = 1000              # Primary queue capacity

    # Overflow buffer settings
    overflow_size: int = 100         # Overflow ring buffer size
    preserve_newest: bool = True     # If True, drop oldest on overflow full

    # Backpressure thresholds (as fraction of primary capacity)
    throttle_threshold: float = 0.70   # Start suggesting delays
    batch_threshold: float = 0.85      # Suggest batching
    reject_threshold: float = 0.95     # Reject non-critical

    # Timing
    suggested_throttle_delay_ms: int = 50    # Delay at throttle level
    suggested_batch_delay_ms: int = 200      # Delay at batch level
    suggested_reject_delay_ms: int = 500     # Delay at reject level
    suggested_overflow_delay_ms: int = 100   # Delay when in overflow
