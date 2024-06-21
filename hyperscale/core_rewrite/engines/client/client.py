import uuid
from typing import Generic

from typing_extensions import TypeVarTuple, Unpack

from .graphql import MercurySyncGraphQLConnection
from .graphql_http2 import MercurySyncGraphQLHTTP2Connection
from .grpc import MercurySyncGRPCConnection
from .http import MercurySyncHTTPConnection
from .http2 import MercurySyncHTTP2Connection
from .http3 import MercurySyncHTTP3Connection
from .playwright import MercurySyncPlaywrightConnection
from .udp import MercurySyncUDPConnection
from .websocket import MercurySyncWebsocketConnection

T = TypeVarTuple("T")

config_registry = []


class Client(Generic[Unpack[T]]):
    def __init__(
        self,
        graph_name: str,
        graph_id: str,
        stage_name: str,
        stage_id: str,
    ) -> None:
        self.client_id = str(uuid.uuid4())
        self.graph_name = graph_name
        self.graph_id = graph_id
        self.stage_name = stage_name
        self.stage_id = stage_id

        self.next_name = None
        self.suspend = False

        self.graphql = MercurySyncGraphQLConnection()
        self.graphqlh2 = MercurySyncGraphQLHTTP2Connection()
        self.grpc = MercurySyncGRPCConnection()
        self.http = MercurySyncHTTPConnection()
        self.http2 = MercurySyncHTTP2Connection()
        self.http3 = MercurySyncHTTP3Connection()
        self.playwright = MercurySyncPlaywrightConnection()
        self.udp = MercurySyncUDPConnection()
        self.websocket = MercurySyncWebsocketConnection()
