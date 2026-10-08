from typing import Literal

import msgspec

from .ping_result_file import PingResultFile


class PingResult(msgspec.Struct, kw_only=True):
    """The result of one ``hyperscale ping`` request, as written to
    ``--filepath``: one schema for every protocol, null where a field does
    not apply to it.

    ``url`` and ``method`` are what the command requested. ``status`` and
    ``status_message`` are the server's answer (or the engine's, when it
    answers for a failed request); ``error`` is the error the engine or
    the command reported. ``elapsed`` is ``timings['request_end'] -
    timings['request_start']`` when both are recorded. ``content`` holds
    the body as text when it decodes as UTF-8, else as base64, named by
    ``content_encoding``.
    """

    protocol: str
    url: str
    method: str | None
    status: int | None = None
    status_message: str | None = None
    error: str | None = None
    headers: dict[str, str] | None = None
    trailers: dict[str, str] | None = None
    redirects: int | None = None
    timings: dict[str, float | None] | None = None
    elapsed: float | None = None
    content: str | None = None
    content_encoding: Literal["utf-8", "base64"] | None = None
    files: list[PingResultFile] | None = None
