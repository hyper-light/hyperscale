from __future__ import annotations

from typing import (
    Generic,
    TypeVar,
)

from .state_action import StateAction

T = TypeVar("T")


class Provide(Generic[T]):
    ACTION = StateAction.PROVIDE
