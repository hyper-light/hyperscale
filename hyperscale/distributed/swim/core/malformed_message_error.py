"""``MalformedMessageError`` -- pickled under the namespace
``hyperscale.distributed.swim.core.errors`` (see that module)."""

from .protocol_error import ProtocolError


class MalformedMessageError(ProtocolError):
    """Received message could not be parsed."""
    
    def __init__(
        self,
        raw_data: bytes,
        reason: str,
        source: tuple[str, int] | None = None,
        cause: BaseException | None = None,
    ):
        # Truncate raw data for logging
        preview = raw_data[:100].hex() if len(raw_data) > 100 else raw_data.hex()
        super().__init__(
            message=f"Malformed message: {reason}",
            raw_preview=preview,
            raw_length=len(raw_data),
            source=source,
            cause=cause,
        )
