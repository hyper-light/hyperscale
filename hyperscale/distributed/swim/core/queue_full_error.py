"""``QueueFullError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .resource_error import ResourceError


class QueueFullError(ResourceError):
    """Message queue is full, cannot accept more work."""
    
    def __init__(self, queue_name: str, queue_size: int):
        super().__init__(
            message=f"Queue '{queue_name}' is full ({queue_size} items)",
            queue_name=queue_name,
            queue_size=queue_size,
        )
