from typing import (
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    TypeVar,
    Union,
)

from pydantic import BaseModel

from hyperscale.distributed.models.http import Request

from .base_wrapper import BaseWrapper
from .types import Handler, MiddlewareHandler, MiddlewareType

T = TypeVar("T")


class UnidirectionalWrapper(BaseWrapper):
    def __init__(
        self,
        name: str,
        handler: Handler,
        middleware_type: MiddlewareType = MiddlewareType.UNIDIRECTIONAL_BEFORE,
        methods: Optional[
            List[
                Literal[
                    "GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE", "TRACE"
                ]
            ]
        ] = None,
        responses: Optional[Dict[int, BaseModel]] = None,
        serializers: Optional[Dict[int, Callable[..., str]]] = None,
        response_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__()

        self.name = name
        self.path = handler.path
        self.methods: List[
            Literal["GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE", "TRACE"]
        ] = handler.methods

        if methods:
            self.methods.extend(methods)

        self.response_headers: Union[Dict[str, str], None] = handler.response_headers

        if self.response_headers and response_headers:
            self.response_headers.update(response_headers)

        elif response_headers:
            self.response_headers = response_headers

        self.responses = responses
        self.serializers = serializers
        self.limit = handler.limit

        self.handler = handler
        self.wraps = isinstance(handler, BaseWrapper)

        if self.handler.response_headers and self.response_headers:
            self.handler.response_headers = {}

        self.run: Optional[MiddlewareHandler] = None
        self.middleware_type = middleware_type

    async def __call__(self, request: Request):
        if self.wraps:
            result, status = await self.handler(request)

            (response, middleware_status), run_next = await self.run(
                request, result, status
            )

            result.headers.update(response.headers)

            if response.data:
                result.data = response.data

            if run_next is False:
                return response, middleware_status

            return result, status

        elif self.middleware_type == MiddlewareType.UNIDIRECTIONAL_BEFORE:
            (response, middleware_status), run_next = await self.run(
                request, None, None
            )

            if run_next is False:
                return response, middleware_status

            result, status = await self.handler(request)

            response.data = result

            return response, status

        else:
            result, status = await self.handler(request)

            (response, middleware_status), run_next = await self.run(
                request, result, status
            )

            if run_next is False:
                return response, middleware_status

            return response, status
