from typing import Generic, Optional, TypeVar

T = TypeVar("T")


class JobContext(Generic[T]):
    __slots__ = ("run_id", "data", "node_id")

    def __init__(
        self,
        data: T,
        run_id: Optional[int] = None,
        node_id: Optional[int] = None,
    ) -> None:
        self.run_id = run_id
        self.data = data
        self.node_id = node_id
