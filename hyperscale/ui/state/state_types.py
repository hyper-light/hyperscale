from typing import (
    List,
    Tuple,
    Dict,
    Callable,
    Awaitable,
    TypeVar,
)


BaseDataType = int | float | str


ActionData = (
    BaseDataType
    | List[BaseDataType]
    | Tuple[BaseDataType, ...]
    | List[Tuple[BaseDataType, ...]]
    | Dict[str, BaseDataType]
)


K = TypeVar("K")
T = TypeVar("T", bound=ActionData)


Action = Callable[[K], Awaitable[Tuple[str | None, T]]]
