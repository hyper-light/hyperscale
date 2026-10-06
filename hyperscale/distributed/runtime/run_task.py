"""
``RunTask`` -- the shape of a bound ``TaskRunner.run``, for the
components that are handed it as a callback.
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Literal, Protocol

if TYPE_CHECKING:
    from hyperscale.distributed.taskex.run import Run


class RunTask(Protocol):
    """Submit a coroutine function to the task runner.

    ``TaskRunner.run`` bound on a runner matches it: ``call`` runs with
    ``args`` under the task named ``alias`` (else ``call``'s name), once or
    on ``schedule``. The submitted ``Run`` -- whose ``token`` cancels it --
    is returned; ``None`` when the runner skips the task or nothing ran. A task
    already registered under that name runs its own call, so the ``Run``'s
    result is typed ``object``.
    """

    def __call__(
        self,
        call: Callable[..., Awaitable[object]],
        *args: object,
        alias: str | None = None,
        run_id: int | None = None,
        timeout: str | int | float | None = None,
        schedule: str | None = None,
        trigger: Literal["MANUAL", "ON_START"] = "MANUAL",
        repeat: Literal["NEVER", "ALWAYS"] | int = "NEVER",
        keep: int | None = None,
        max_age: str | None = None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = "COUNT",
        **kwargs: object,
    ) -> "Run[object] | None": ...
