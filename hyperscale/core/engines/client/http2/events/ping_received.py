import binascii
from .base_event import BaseEvent


class PingReceived(BaseEvent):
    """
    The PingReceived event is fired whenever a PING is received. It contains
    the 'opaque data' of the PING frame. A ping acknowledgment with the same
    'opaque data' is automatically emitted after receiving a ping.

    .. versionadded:: 3.1.0
    """

    def __init__(self) -> None:
        #: The data included on the ping.
        self.data: bytes | None = None
        self.flow_controlled_length = None


    def __repr__(self) -> str:
        decoded_data = binascii.hexlify(self.data).decode("ascii")
        return "<PingReceived ping_data:%s>" % decoded_data

