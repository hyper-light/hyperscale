from hyperscale.core.engines.client.http2.protocols import HTTP2Connection

from .grpc_transport_factory import GRPCTransportFactory


class GRPCConnection(HTTP2Connection):
    """
    A pooled HTTP/2 connection for gRPC: as HTTP2Connection, but its
    transports may also be cleartext (insecure channels, http:// targets).
    """

    __slots__ = ()

    def __init__(
        self,
        stream_id: int = 1,
        reset_connections: bool = False,
    ) -> None:
        super().__init__(
            stream_id=stream_id,
            reset_connections=reset_connections,
        )

        self._connection_factory = GRPCTransportFactory()
