from typing import Awaitable, Callable, Literal

try:

    from playwright.async_api import (
        ConsoleMessage,
        Dialog,
        Download,
        Error,
        FileChooser,
        Frame,
        Page,
        Request,
        Response,
        WebSocket,
        Worker,
    )

except Exception:

    class ConsoleMessage:
        pass

    class Dialog:
        pass

    class Download:
        pass

    class Error:
        pass

    class FileChooser:
        pass

    class Frame:
        pass

    class Page:
        pass

    class Request:
        pass

    class Response:
        pass

    class WebSocket:
        pass

    class Worker:
        pass

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
)


class OnCommand(BaseModel):
    event: Literal[
        "close",
        "console",
        "crash",
        "dialog",
        "domcontentloaded",
        "download",
        "filechooser",
        "frameattached",
        "framedetached",
        "framenavigated",
        "load",
        "pageerror",
        "popup",
        "request",
        "requestfailed",
        "requestfinished",
        "response",
        "websocket",
        "worker",
    ]
    function: (
        Callable[[Page], Awaitable[None] | None]
        | Callable[[ConsoleMessage], Awaitable[None] | None]
        | Callable[[Page], Awaitable[None] | None]
        | Callable[[Dialog], Awaitable[None] | None]
        | Callable[[Page], Awaitable[None] | None]
        | Callable[[Download], Awaitable[None] | None]
        | Callable[[FileChooser], Awaitable[None] | None]
        | Callable[[Frame], Awaitable[None] | None]
        | Callable[[Frame], Awaitable[None] | None]
        | Callable[[Frame], Awaitable[None] | None]
        | Callable[[Page], Awaitable[None] | None]
        | Callable[[Error], Awaitable[None] | None]
        | Callable[[Page], Awaitable[None] | None]
        | Callable[[Request], Awaitable[None] | None]
        | Callable[[Request], Awaitable[None] | None]
        | Callable[[Request], Awaitable[None] | None]
        | Callable[[Response], Awaitable[None] | None]
        | Callable[[WebSocket], Awaitable[None] | None]
        | Callable[[Worker], Awaitable[None] | None]
    )
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed = True