from typing import Dict, Callable, Awaitable
from pydantic import BaseModel

ActionData = int | float | str | list[int] | list[float] | list[str]
Action = Callable[[ActionData, ActionData], Awaitable[ActionData]]
ActionSet = Dict[str, Action]


class Actions(BaseModel):
    add: Action


class Example:
    def __init__(self, actions: Dict[str, Action]):
        self.actions = actions


def update(prev: int, next: int):
    return prev + next


def update_two(prev: list[int], next: list[int]):
    prev.extend(next)

    return prev


example = Example(
    {
        "boop": {
            "add": update,
        },
    }
)

res = example.actions["add"](1, 2)
