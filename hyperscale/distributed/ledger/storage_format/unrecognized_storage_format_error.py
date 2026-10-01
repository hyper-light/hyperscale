from __future__ import annotations


class UnrecognizedStorageFormatError(Exception):
    """Persisted bytes this node cannot read: another format version,
    another program, or damage -- to the format header or to contents
    the format's own checks reject."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)
