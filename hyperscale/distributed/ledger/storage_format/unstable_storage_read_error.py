from __future__ import annotations

from pathlib import Path


class UnstableStorageReadError(Exception):
    """Two reads of the same file returned different bytes: the read path
    (not necessarily the disk) is faulty, so neither read can be trusted
    to describe what the file holds -- nothing is decided from them."""

    def __init__(self, path: Path) -> None:
        self.path = path
        super().__init__(f"{path} read differently on a second read; refusing to act on either")
