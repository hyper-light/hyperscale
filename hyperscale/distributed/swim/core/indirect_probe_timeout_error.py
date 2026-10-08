"""``IndirectProbeTimeoutError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_severity import ErrorSeverity
from .network_error import NetworkError


class IndirectProbeTimeoutError(NetworkError):
    """Indirect probe via proxy nodes all timed out."""
    
    def __init__(
        self,
        target: tuple[str, int],
        proxies: list[tuple[str, int]],
        timeout: float,
    ):
        super().__init__(
            message=f"Indirect probe to {target[0]}:{target[1]} via {len(proxies)} proxies timed out",
            severity=ErrorSeverity.DEGRADED,  # More serious than direct timeout
            target=target,
            proxies=proxies,
            timeout=timeout,
        )
