from hyperscale.core.engines.client.shared.models import (
    RequestType,
    SocketProtocol,
    SocketType,
)


class ProtocolMap:
    def __init__(self) -> None:
        self.address_families = {
            RequestType.HTTP: SocketType.DEFAULT,
            RequestType.HTTP2: SocketType.HTTP2,
            # The family a prepared URL resolves in, as a plain-string HTTP/3
            # request does: the QUIC socket is dual-stack and reaches IPv4
            # addresses mapped, while resolving IPv6 alone fails every
            # IPv4-only host.
            RequestType.HTTP3: SocketType.DEFAULT,
            RequestType.WEBSOCKET: SocketType.DEFAULT,
            RequestType.GRAPHQL: SocketType.DEFAULT,
            RequestType.GRAPHQL_HTTP2: SocketType.HTTP2,
            RequestType.GRPC: SocketType.HTTP2,
            RequestType.SCP: SocketType.SSH,
            RequestType.UDP: SocketType.UDP,
            RequestType.PLAYWRIGHT: SocketType.NONE,
        }

        self.protocols = {
            RequestType.HTTP: SocketProtocol.DEFAULT,
            RequestType.HTTP2: SocketProtocol.HTTP2,
            RequestType.HTTP3: SocketProtocol.HTTP3,
            RequestType.WEBSOCKET: SocketProtocol.DEFAULT,
            RequestType.GRAPHQL: SocketProtocol.DEFAULT,
            RequestType.GRAPHQL_HTTP2: SocketProtocol.HTTP2,
            RequestType.GRPC: SocketProtocol.HTTP2,
            RequestType.SCP: SocketProtocol.SSH,
            RequestType.UDP: SocketProtocol.UDP,
            RequestType.PLAYWRIGHT: SocketProtocol.NONE,
        }

    def __getitem__(self, key: RequestType) -> SocketType:
        return self.address_families.get(key), self.protocols.get(key)
