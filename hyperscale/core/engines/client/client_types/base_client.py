import asyncio
import uuid
from typing import Dict, Generic, TypeVar

from hyperscale.core.engines.client.store import ActionsStore
from hyperscale.core.engines.types import (
    MercuryGraphQLClient,
    MercuryGraphQLHTTP2Client,
    MercuryGRPCClient,
    MercuryHTTP2Client,
    MercuryHTTPClient,
    MercuryPlaywrightClient,
    MercuryUDPClient,
    MercuryWebsocketClient,
)
from hyperscale.core.engines.types.common.types import RequestTypes
from hyperscale.core.engines.types.graphql import GraphQLAction, GraphQLResult
from hyperscale.core.engines.types.graphql_http2 import (
    GraphQLHTTP2Action,
    GraphQLHTTP2Result,
)
from hyperscale.core.engines.types.grpc import GRPCAction, GRPCResult
from hyperscale.core.engines.types.http import HTTPAction, HTTPResult
from hyperscale.core.engines.types.http2 import HTTP2Action, HTTP2Result
from hyperscale.core.engines.types.playwright import PlaywrightCommand, PlaywrightResult
from hyperscale.core.engines.types.udp import UDPAction, UDPResult
from hyperscale.core.engines.types.websocket import WebsocketAction, WebsocketResult
from hyperscale.core.experiments.mutations.types.base.mutation import Mutation
from hyperscale.core.hooks.types.event.event import Event
from hyperscale.core.hooks.types.event.hook import EventHook
from hyperscale.logging.hyperscale_logger import HyperscaleLogger

S = TypeVar(
    "S",
    MercuryGraphQLClient,
    MercuryGraphQLHTTP2Client,
    MercuryGRPCClient,
    MercuryHTTPClient,
    MercuryHTTP2Client,
    MercuryPlaywrightClient,
    MercuryUDPClient,
    MercuryWebsocketClient,
)
A = TypeVar(
    "A",
    GraphQLAction,
    GraphQLHTTP2Action,
    GRPCAction,
    HTTPAction,
    HTTP2Action,
    PlaywrightCommand,
    UDPAction,
    WebsocketAction,
)
R = TypeVar(
    "R",
    GraphQLResult,
    GraphQLHTTP2Result,
    GRPCResult,
    HTTPResult,
    HTTP2Result,
    PlaywrightResult,
    UDPResult,
    WebsocketResult,
)


class BaseClient(Generic[S, A, R]):
    initialized = False
    setup = False

    def __init__(self) -> None:
        self.initialized = True
        self.metadata_string: str = None
        self.client_id = str(uuid.uuid4())
        self.session: S = None
        self.request_type: RequestTypes = None
        self.client_type: str = None

        self.actions: ActionsStore = None
        self.next_name = None
        self.intercept = False

        self.logger = HyperscaleLogger()
        self.logger.initialize()

        self.mutations: Dict[str, Mutation] = {}

    async def _execute_action(self, action: A) -> R:
        if self.mutations.get(action.name):
            mutation = self.mutations[action.name]

            if action.hooks.before is None:
                action.hooks.before = []

            mutation_event = Event(
                None,
                EventHook(mutation.name, mutation.name, mutation.mutate, action.name),
            )

            mutation_event.source.stage_instance = mutation.stage
            action.hooks.before.append([mutation_event])

        await self.logger.filesystem.aio["hyperscale.core"].debug(
            f"{self.metadata_string} - {self.client_type} Client {self.client_id} - Preparing Action - {action.name}:{action.action_id}"
        )
        await self.session.prepare(action)

        await self.logger.filesystem.aio["hyperscale.core"].debug(
            f"{self.metadata_string} - {self.client_type} Client {self.client_id} - Prepared Action - {action.name}:{action.action_id}"
        )

        if self.intercept:
            await self.logger.filesystem.aio["hyperscale.core"].debug(
                f"{self.metadata_string} - {self.client_type} Client {self.client_id} - Initiating suspense for Action - {action.name}:{action.action_id} - and storing"
            )
            self.actions.store(self.next_name, action, self.session)

            loop = asyncio.get_event_loop()
            self.waiter = loop.create_future()
            await self.waiter

        return await self.session.execute_prepared_request(action)
