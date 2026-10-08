"""``StaleMessageError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_severity import ErrorSeverity
from .protocol_error import ProtocolError


class StaleMessageError(ProtocolError):
    """Message has old incarnation number."""
    
    def __init__(
        self,
        node: tuple[str, int],
        received_incarnation: int,
        current_incarnation: int,
    ):
        super().__init__(
            message=f"Stale message from {node[0]}:{node[1]}: incarnation {received_incarnation} < {current_incarnation}",
            severity=ErrorSeverity.TRANSIENT,  # Normal in async systems
            node=node,
            received_incarnation=received_incarnation,
            current_incarnation=current_incarnation,
        )
