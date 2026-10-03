import asyncio
from typing import Awaitable, Optional, TypeVar

T = TypeVar("T")


def within_timeout(awaitable: Awaitable[T], timeout: Optional[float]) -> Awaitable[T]:
    """
    ``awaitable`` bounded by ``timeout`` as ``asyncio.wait_for`` bounds it --
    or, with no timeout, ``awaitable`` itself: ``wait_for(..., None)`` arms
    nothing and can never fire, yet still costs a timeout scope.
    """
    if timeout is None:
        return awaitable

    return asyncio.wait_for(awaitable, timeout=timeout)


def insert_timeout_error(error: BaseException) -> None:
    """
    What ``asyncio.timeout`` does when a phase that ran out raises something
    other than its cancellation: record the timeout in the error's context
    chain, where the cancellation sits.
    """
    while error.__context__ is not None:
        if isinstance(error.__context__, asyncio.CancelledError):
            timeout_error = TimeoutError()
            timeout_error.__context__ = timeout_error.__cause__ = error.__context__
            error.__context__ = timeout_error
            break

        error = error.__context__


class PhaseTimeout:
    """
    Bounds each phase of a request by its own timeout exactly as
    ``asyncio.wait_for(phase, timeout)`` does: a phase that runs out has its
    task cancelled and raises TimeoutError, a cancellation from elsewhere
    still propagates, and a phase that swallows its cancellation returns
    normally -- with the same ``cancelling()``/``uncancel()`` accounting, so
    it nests under other timeouts.

    Where ``wait_for`` makes, schedules and cancels a TimerHandle for every
    phase, a phase here only records its deadline. One timer, reused across
    phases and requests, stays armed at the current deadline or an earlier
    one: when it fires early -- a later phase moved the deadline -- it
    re-arms for the current deadline, and when no phase is running it lapses
    until the next phase arms it again.

    It bounds one phase at a time, so each in-flight request needs its own.
    """

    __slots__ = (
        "_timer",
        "_timer_deadline",
        "_deadline",
        "_task",
        "_expired",
    )

    def __init__(self) -> None:
        self._timer: Optional[asyncio.TimerHandle] = None
        self._timer_deadline = 0.0
        self._deadline: Optional[float] = None
        self._task: Optional[asyncio.Task] = None
        self._expired = False

    async def run(self, awaitable: Awaitable[T], timeout: Optional[float]) -> T:
        if timeout is None:
            return await awaitable

        if timeout <= 0:
            # wait_for's own path for a timeout already used up.
            return await asyncio.wait_for(awaitable, timeout)

        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("Timeout should be used inside a task")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        cancelling = task.cancelling()

        self._task = task
        self._expired = False
        self._deadline = deadline

        if self._timer is None or self._timer_deadline > deadline:
            self._arm(loop, deadline)

        try:
            result = await awaitable

        except asyncio.CancelledError as cancellation:
            if self._expired and task.uncancel() <= cancelling:
                raise TimeoutError from cancellation

            raise

        except BaseException as error:
            if self._expired and task.uncancel() <= cancelling:
                insert_timeout_error(error)

                if isinstance(error, ExceptionGroup):
                    for member in error.exceptions:
                        insert_timeout_error(member)

            raise

        else:
            if self._expired:
                task.uncancel()

            return result

        finally:
            self._deadline = None
            self._task = None

    def cancel(self) -> None:
        """Stop the timer once this PhaseTimeout will bound no more phases."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _arm(self, loop: asyncio.AbstractEventLoop, deadline: float) -> None:
        if self._timer is not None:
            self._timer.cancel()

        self._timer_deadline = deadline
        self._timer = loop.call_at(deadline, self._on_timer)

    def _on_timer(self) -> None:
        self._timer = None

        if (deadline := self._deadline) is None:
            # No phase running: lapse until the next phase arms the timer.
            return

        loop = asyncio.get_running_loop()
        if deadline > self._timer_deadline and deadline > loop.time():
            # A later phase moved the deadline: wait for it instead.
            self._arm(loop, deadline)
            return

        self._expired = True
        self._task.cancel()
