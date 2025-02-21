from typing import Dict, Optional, Awaitable, Callable, Any

try:

    from opentelemetry.trace import Span

except Exception:
    class Span:
        pass


RequestHook = Optional[Callable[[Span, str, str, Dict[str, Any]], None]]
ResponseHook = Optional[Callable[[Span,  str, str, Dict[str, Any]], None,]]


TraceSignal = Callable[..., Awaitable[None]]
UrlFilter = Callable[[str],  str]
