"""``UnexpectedMessageError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .error_severity import ErrorSeverity
from .protocol_error import ProtocolError


class UnexpectedMessageError(ProtocolError):
    """Received message type not expected in current state."""
    
    def __init__(
        self,
        msg_type: bytes,
        expected: list[bytes] | None = None,
        source: tuple[str, int] | None = None,
    ):
        super().__init__(
            message=f"Unexpected message type: {msg_type!r}",
            severity=ErrorSeverity.TRANSIENT,  # Might just be timing
            msg_type=msg_type,
            expected=expected,
            source=source,
        )
