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
        "request_id",
    )

    def __init__(
        self,
        node_id: int,
        name: str,
        data: Optional[T] = None,
        error: Optional[str] = None,
        service_host: Optional[int] = None,
        service_port: Optional[int] = None,
        request_id: Optional[int] = None,
    ) -> None:
        self.node_id = node_id
        self.name = name
        self.data = data
        self.error = error
        self.service_host = service_host
        self.service_port = service_port
        # The id of the request a reply answers: set by the requester and
        # echoed by the responder, so the reply reaches that request alone.
        self.request_id = request_id
