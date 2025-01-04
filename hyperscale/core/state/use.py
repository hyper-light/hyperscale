from __future__ import annotations

from typing import Generic, TypeVar

from .state_action import StateAction

T = TypeVar("T")


class Use(Generic[T]):
    ACTION = StateAction.USE
