"""``ConnectionRefusedError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .network_error import NetworkError


class ConnectionRefusedError(NetworkError):
    """Target node refused connection."""
    
    def __init__(self, target: tuple[str, int], cause: BaseException | None = None):
        super().__init__(
            message=f"Connection refused by {target[0]}:{target[1]}",
            target=target,
            cause=cause,
        )
