import functools
from typing import Literal, Optional

from hyperscale.core.engines.client.time_parser import TimeParser

from .hook_type import HookType


def task(
    schedule: Optional[float | int | str] = None,
    trigger: Literal["MANUAL", "ON_START"] = "MANUAL",
    repeat: Literal["NEVER", "ALWAYS"] | int = "NEVER",
    timeout: Optional[int | float | str] = None,
    keep: Optional[int] = 10,
    max_age: Optional[str] = "10m",
    keep_policy: Literal["COUNT", "AGE", "COUNT_AND_AGE"] = "COUNT",
):
    if isinstance(schedule, str):
        parser = TimeParser(schedule)
        schedule = parser.time

    if isinstance(timeout, str):
        parser = TimeParser(timeout)
        timeout = parser.time

    if schedule is None and repeat == "ALWAYS":
        raise Exception(
            'Err. - A schedule must be specified if repeat is set as "ALWAYS" '
        )

    elif schedule is None and isinstance(repeat, int) and repeat > 0:
        raise Exception(
            'Err. - A non-zero number of repeats must be specified if repeat is set as "ALWAYS" '
        )

    if not isinstance(keep, int) and keep_policy == "COUNT":
        raise Exception(
            'Err. - cannot have null or non-zero keep count if keep_policy is "COUNT'
        )

    elif not isinstance(max_age, str) and keep_policy == "AGE":
        raise Exception('Err. - cannot have null max_age if keep_policy is "AGE')

    if max_age:
        max_age = TimeParser(max_age).time

    def wraps(func):
        func.name = func.__name__
        func.schedule = schedule
        func.trigger = trigger
        func.repeat = repeat
        func.as_task = True
        func.timeout = timeout
        func.keep = keep
        func.max_age = max_age
        func.keep_policy = keep_policy
        func.hook_type = HookType.TASK

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wraps
