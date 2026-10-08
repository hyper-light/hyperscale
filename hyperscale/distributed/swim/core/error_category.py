"""``ErrorCategory`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from enum import Enum, auto


class ErrorCategory(Enum):
    """What kind of error is this?"""
    
    NETWORK = auto()
    """Timeouts, connection refused, DNS failures, unreachable hosts."""
    
    PROTOCOL = auto()
    """Malformed messages, unexpected state, version mismatch."""
    
    RESOURCE = auto()
    """Memory pressure, file descriptors, CPU saturation."""
    
    INTERNAL = auto()
    """Bugs, assertion failures, unexpected exceptions."""
    
    ELECTION = auto()
    """Leader election specific errors."""
