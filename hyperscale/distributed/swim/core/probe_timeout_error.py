"""``ProbeTimeoutError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .network_error import NetworkError


class ProbeTimeoutError(NetworkError):
    """Direct probe to a node timed out."""
    
    def __init__(
        self,
        target: tuple[str, int],
        timeout: float,
        cause: BaseException | None = None,
    ):
        super().__init__(
            message=f"Probe to {target[0]}:{target[1]} timed out after {timeout:.2f}s",
            target=target,
            timeout=timeout,
            cause=cause,
        )
