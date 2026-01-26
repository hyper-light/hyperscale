import msgspec
from typing import Callable, Awaitable, Literal, Any


class TaskCall:

    def __init__(
        self,
        func: (
            Callable[..., Awaitable[Any]] 
            | Callable[..., Any] 
        ),
        *args,
        alias: str | None = None,
        run_id: int | None = None,
        timeout: str | int | float | None = None,
        schedule: str | None = None,
        trigger: Literal["MANUAL", "ON_START"] = "MANUAL",
        repeat: Literal["NEVER", "ALWAYS"] | int = "NEVER",
        keep: int | None = None,
        max_age: str | None = None,
        keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = "COUNT",
        **kwargs,
    ):
        self.call: (
            Callable[..., Awaitable[Any]] 
            | Callable[..., Any] 
        ) = func
        self.name: str = func.__name__
        self.args: tuple[Any, ...] = args
        self.alias: str | None = alias
        self.run_id: int | None = run_id
        self.timeout: str | int | float | None = timeout
        self.schedule: str | None = schedule
        self.trigger: Literal["MANUAL", "ON_START"] = trigger
        self.repeat: Literal["NEVER", "ALWAYS"] | int = repeat
        self.keep: int | None = keep
        self.max_age: str | None = max_age
        self.keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = keep_policy
        self.kwargs: dict[str, Any] = kwargs

def task(
    *args,
    alias: str | None = None,
    run_id: int | None = None,
    timeout: str | int | float | None = None,
    schedule: str | None = None,
    trigger: Literal["MANUAL", "ON_START"] = "MANUAL",
    repeat: Literal["NEVER", "ALWAYS"] | int = "NEVER",
    keep: int | None = None,
    max_age: str | None = None,
    keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = "COUNT",
    **kwargs,
):
    def wraps(func):
        
        def wrapper():
            return TaskCall(
                func,
                *args,
                alias=alias,
                run_id=run_id,
                timeout=timeout,
                schedule=schedule,
                trigger=trigger,
                repeat=repeat,
                keep=keep,
                max_age=max_age,
                keep_policy=keep_policy,
                kwargs=kwargs,
            )
        
        wrapper.is_hook = True
        wrapper.type = 'task'
        
        return wrapper

    return wraps