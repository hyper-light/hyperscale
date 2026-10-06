"""``QueueMetrics`` -- pickled under the namespace
``hyperscale.distributed.reliability.robust_queue`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class QueueMetrics:
    """Metrics for queue observability."""

    total_enqueued: int = 0           # Total messages accepted
    total_dequeued: int = 0           # Total messages consumed
    total_overflow: int = 0           # Messages that went to overflow
    total_dropped: int = 0            # Messages dropped (overflow full)
    total_oldest_dropped: int = 0     # Oldest messages evicted from overflow

    peak_primary_size: int = 0        # High water mark for primary
    peak_overflow_size: int = 0       # High water mark for overflow

    throttle_activations: int = 0     # Times we entered throttle state
    batch_activations: int = 0        # Times we entered batch state
    overflow_activations: int = 0     # Times we entered overflow state
    saturated_activations: int = 0    # Times both queues were full
