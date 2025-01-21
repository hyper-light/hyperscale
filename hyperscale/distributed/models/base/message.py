from typing import Generic, Optional, TypeVar

T = TypeVar("T")


class Message(Generic[T]):
    __slots__ = (
        "node_id",
        "name",
        "data",
        "error",
        "service_host",
        "service_port",
    )

    def __init__(
        self,
        node_id: int,
        name: str,
        data: Optional[T] = None,
        error: Optional[str] = None,
        service_host: Optional[int] = None,
        service_port: Optional[int] = None,
    ) -> None:
        self.node_id = node_id
        self.name = name
        self.data = data
        self.error = error
        self.service_host = service_host
        self.service_port = service_port
