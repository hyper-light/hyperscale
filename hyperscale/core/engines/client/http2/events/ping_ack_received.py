import binascii
from .base_event import BaseEvent

class PingAckReceived(BaseEvent):
    """
    The PingAckReceived event is fired whenever a PING acknowledgment is
    received. It contains the 'opaque data' of the PING+ACK frame, allowing the
    user to correlate PINGs and calculate RTT.

    .. versionadded:: 3.1.0

    .. versionchanged:: 4.0.0
       Removed deprecated but equivalent ``PingAcknowledged``.
    """

    def __init__(self) -> None:
        #: The data included on the ping.
        self.data: bytes | None = None

    def __repr__(self) -> str:
        decoded_data = binascii.hexlify(self.data).decode("ascii")
        return "<PingAckReceived ping_data:%s>" % decoded_data