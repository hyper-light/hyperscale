from typing import Dict, Iterator, List, Union

from hyperscale.core.engines.client.config import Config
from hyperscale.core.engines.client.store import ActionsStore
from hyperscale.core.engines.types.common import Timeouts
from hyperscale.core.engines.types.common.types import RequestTypes
from hyperscale.core.engines.types.http2 import (
    HTTP2Action,
    HTTP2Result,
    MercuryHTTP2Client,
)
from hyperscale.core.engines.types.tracing.trace_session import Trace, TraceSession
from hyperscale.logging.hyperscale_logger import HyperscaleLogger

from .base_client import BaseClient


class HTTP2Client(BaseClient[MercuryHTTP2Client, HTTP2Action, HTTP2Result]):
    def __init__(self, config: Config) -> None:
        super().__init__()

        if config is None:
            config = Config()

        tracing_session: Union[TraceSession, None] = None
        if config.tracing:
            trace_config_dict = config.tracing.to_dict()
            tracing_session = TraceSession(**trace_config_dict)

        self.session = MercuryHTTP2Client(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                connect_timeout=config.connect_timeout,
                total_timeout=config.request_timeout,
            ),
            reset_connections=config.reset_connections,
            tracing_session=tracing_session,
        )
        self.request_type = RequestTypes.HTTP2
        self.client_type = self.request_type.capitalize()

        self.actions: ActionsStore = None
        self.next_name = None
        self.intercept = False

        self.logger = HyperscaleLogger()
        self.logger.initialize()

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def get(
        self,
        url: str,
        headers: Dict[str, str] = {},
        user: str = None,
        tags: List[Dict[str, str]] = [],
        trace: Trace = None,
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(**trace.to_dict())

        request = HTTP2Action(
            self.next_name,
            url,
            method="GET",
            headers=headers,
            data=None,
            user=user,
            tags=tags,
        )

        return await self._execute_action(request)

    async def post(
        self,
        url: str,
        headers: Dict[str, str] = {},
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        trace: Trace = None,
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(**trace.to_dict())

        request = HTTP2Action(
            self.next_name,
            url,
            method="POST",
            headers=headers,
            data=data,
            user=user,
            tags=tags,
        )

        return await self._execute_action(request)

    async def put(
        self,
        url: str,
        headers: Dict[str, str] = {},
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        trace: Trace = None,
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(**trace.to_dict())

        request = HTTP2Action(
            self.next_name,
            url,
            method="PUT",
            headers=headers,
            data=data,
            user=user,
            tags=tags,
        )

        return await self._execute_action(request)

    async def patch(
        self,
        url: str,
        headers: Dict[str, str] = {},
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        trace: Trace = None,
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(**trace.to_dict())

        request = HTTP2Action(
            self.next_name,
            url,
            method="PATCH",
            headers=headers,
            data=data,
            user=user,
            tags=tags,
        )

        return await self._execute_action(request)

    async def delete(
        self,
        url: str,
        headers: Dict[str, str] = {},
        user: str = None,
        tags: List[Dict[str, str]] = [],
        trace: Trace = None,
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(**trace.to_dict())

        request = HTTP2Action(
            self.next_name,
            url,
            method="DELETE",
            headers=headers,
            data=None,
            user=user,
            tags=tags,
        )

        return await self._execute_action(request)
