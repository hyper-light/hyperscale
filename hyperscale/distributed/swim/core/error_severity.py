"""``ErrorSeverity`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from enum import Enum, auto


class ErrorSeverity(Enum):
    """How serious is this error?"""
    
    TRANSIENT = auto()
    """Network blip, retry likely to succeed. No LHM impact."""
    
    DEGRADED = auto()
    """Partial failure, can continue with reduced functionality. Minor LHM impact."""
    
    FATAL = auto()
    """Cannot continue, must restart/escalate. Major LHM impact."""
