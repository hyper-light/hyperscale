"""``QueueFullError`` -- pickled under the namespace
``hyperscale.distributed.reliability.robust_queue`` (see that module)."""



class QueueFullError(Exception):
    """Raised when both primary and overflow queues are exhausted."""
    pass
