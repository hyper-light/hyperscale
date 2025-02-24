import uuid
from typing import (
    Any,
    Generator,
    Generic,
    TypeVarTuple,
    Unpack,
)

from .graphql import MercurySyncGraphQLConnection
from .graphql_http2 import MercurySyncGraphQLHTTP2Connection
from .grpc import MercurySyncGRPCConnection
from .http import MercurySyncHTTPConnection
from .http2 import MercurySyncHTTP2Connection
from .http3 import MercurySyncHTTP3Connection
from .playwright import MercurySyncPlaywrightConnection
from .shared.models import RequestType
from .smtp import MercurySyncSMTPConnection
from .tcp import MercurySyncTCPConnection
from .udp import MercurySyncUDPConnection
from .websocket import MercurySyncWebsocketConnection

T = TypeVarTuple("T")

config_registry = []


class Client(Generic[Unpack[T]]):
    def __init__(self) -> None:
        self.client_id = str(uuid.uuid4())

        self.next_name = None

        self.graphql = MercurySyncGraphQLConnection()
        self.graphqlh2 = MercurySyncGraphQLHTTP2Connection()
        self.grpc = MercurySyncGRPCConnection()
        self.http = MercurySyncHTTPConnection()
        self.http2 = MercurySyncHTTP2Connection()
        self.http3 = MercurySyncHTTP3Connection()
        self.playwright = MercurySyncPlaywrightConnection()
        self.smtp = MercurySyncSMTPConnection()
        self.tcp = MercurySyncTCPConnection()
        self.udp = MercurySyncUDPConnection()
        self.websocket = MercurySyncWebsocketConnection()

    def __iter__(
        self,
    ) -> Generator[
        Any,
        None,
        MercurySyncGraphQLConnection
        | MercurySyncGraphQLHTTP2Connection
        | MercurySyncGRPCConnection
        | MercurySyncHTTPConnection
        | MercurySyncHTTP2Connection
        | MercurySyncHTTP3Connection
        | MercurySyncPlaywrightConnection
        | MercurySyncSMTPConnection
        | MercurySyncTCPConnection
        | MercurySyncUDPConnection
        | MercurySyncWebsocketConnection,
    ]:
        clients = [
            self.graphql,
            self.graphqlh2,
            self.grpc,
            self.http,
            self.http2,
            self.http3,
            self.playwright,
            self.smtp,
            self.tcp,
            self.udp,
            self.websocket,
        ]

        for client in clients:
            yield client

    def __getitem__(
        self,
        key: RequestType,
    ):
        match key:
            case RequestType.GRAPHQL:
                return self.graphql

            case RequestType.GRAPHQL_HTTP2:
                return self.graphqlh2

            case RequestType.GRPC:
                return self.grpc

            case RequestType.HTTP:
                return self.http

            case RequestType.HTTP2:
                return self.http2

            case RequestType.HTTP3:
                return self.http3

            case RequestType.PLAYWRIGHT:
                return self.playwright
            
            case RequestType.SMTP:
                return self.smtp
            
            case RequestType.TCP:
                return self.tcp

            case RequestType.UDP:
                return self.udp

            case RequestType.WEBSOCKET:
                return self.websocket

            case _:
                raise Exception("Err. - invalid client type.")

    def close(self):
        clients: list[
            MercurySyncGraphQLConnection
            | MercurySyncGraphQLHTTP2Connection
            | MercurySyncGRPCConnection
            | MercurySyncHTTPConnection
            | MercurySyncHTTP2Connection
            | MercurySyncHTTP3Connection
            | MercurySyncPlaywrightConnection
            | MercurySyncTCPConnection
            | MercurySyncUDPConnection
            | MercurySyncWebsocketConnection,
        ] = [
            self.graphql,
            self.graphqlh2,
            self.grpc,
            self.http,
            self.http2,
            self.http3,
            self.playwright,
            self.smtp,
            self.tcp,
            self.udp,
            self.websocket,
        ]

        for client in clients:
            client.close()
