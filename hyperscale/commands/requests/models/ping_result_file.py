from typing import Literal

import msgspec


class PingResultFile(msgspec.Struct, kw_only=True):
    """One file or directory entry a ping request transferred (SFTP).

    ``content`` holds the entry's bytes as text when they decode as UTF-8,
    else as base64; ``content_encoding`` says which, and both are null when
    the entry carried no data.
    """

    path: str
    file_type: str | None
    content: str | None
    content_encoding: Literal["utf-8", "base64"] | None
