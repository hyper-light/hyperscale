from typing import Callable, Coroutine, Dict, Optional

from hyperscale.core.engines.types.common.url import URL

try:
    from opentelemetry.trace import Span

except ImportError:
    Span = object


class Request:
    url: URL
    method: str
    headers: Dict[str, str]


class Response:
    url: URL
    method: str
    headers: Dict[str, str]
    status: Optional[int]
    error: Exception


RequestHook = Optional[Callable[[Span, Request], None]]


ResponseHook = Optional[
    Callable[
        [
            Span,
            Response,
        ],
        None,
    ]
]


TraceSignal = Callable[[Span, Request, Response], Coroutine[None, None, None]]


UrlFilter = Callable[[str], str]
