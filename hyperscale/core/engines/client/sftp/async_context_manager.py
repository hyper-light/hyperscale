import functools
from typing import (
    AsyncContextManager,
    TypeVar,
    Awaitable,
    Callable,
    Generic,
    Generator,
    Type,
    Any,
)
from .types import TracebackType


ACM = TypeVar('ACM', bound=AsyncContextManager, covariant=True)

class ACMWrapper(Generic[ACM]):
    """Async context manager wrapper"""

    def __init__(self, coro: Awaitable[ACM]):
        self._coro = coro
        self._coro_result: ACM | None = None

    def __await__(self) -> Generator[Any, None, ACM]:
        return self._coro.__await__()

    async def __aenter__(self) -> ACM:
        self._coro_result = await self._coro

        return await self._coro_result.__aenter__()

    async def __aexit__(
            self,
            exc_type: Type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> bool | None:
        assert self._coro_result is not None

        exit_result = await self._coro_result.__aexit__(
            exc_type, exc_value, traceback)

        self._coro_result = None

        return exit_result


ACMCoro = Callable[..., Awaitable[ACM]]
ACMWrapperFunc = Callable[..., ACMWrapper[ACM]]




def async_context_manager(coro: ACMCoro[ACM]) -> ACMWrapperFunc[ACM]:
    """Decorator for functions returning asynchronous context managers

       This decorator can be used on functions which return objects
       intended to be async context managers. The object returned by
       the function should implement __aenter__ and __aexit__ methods
       to run when the async context is entered and exited.

       This wrapper also allows the use of "await" on the function being
       decorated, to return the context manager without entering it.

    """

    @functools.wraps(coro)
    def context_wrapper(*args, **kwargs) -> ACMWrapper[ACM]:
        """Return an async context manager wrapper for this coroutine"""

        return ACMWrapper(coro(*args, **kwargs))

    return context_wrapper