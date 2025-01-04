from typing import Awaitable, Callable, List, Optional

from hyperscale.core.engines.client.shared.timeouts import Timeouts

from .context_hook import ContextHook
from .provide import Provide
from .use import Use


def state(
    *args: str,
    timeouts: Optional[Timeouts] = None,
    tags: Optional[List[str]] = None,
):
    def wrapper(func: Callable[..., Awaitable[Use | Provide]]):
        return ContextHook(
            list(args),
            func,
            timeouts=timeouts,
            tags=tags,
        )

    return wrapper
