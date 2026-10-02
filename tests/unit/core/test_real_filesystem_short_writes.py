"""
RealFilesystem.append_fsync writes every byte or raises.

A raw (unbuffered) write may be SHORT: near ENOSPC the kernel writes
what fits, returns the count, and only the NEXT write raises. The
append wrote once and ignored the count, so a short write reported a
durable append for a torn record -- the WAL acknowledged data it never
wrote. It now writes until every byte lands, and a write that makes no
progress raises.

Short writes are forced by capping each os.write at a few bytes.
"""

import os
from pathlib import Path

import pytest

from hyperscale.core.runtime import real_filesystem
from hyperscale.core.runtime.real_filesystem import RealFilesystem

PAYLOAD = bytes(range(256)) * 9
SHORT_WRITE_BYTES = 7


@pytest.mark.asyncio
async def test_short_writes_still_append_every_byte(tmp_path: Path, monkeypatch) -> None:
    real_write = os.write
    write_calls: list[int] = []

    def short_write(descriptor: int, data) -> int:
        write_calls.append(len(data))
        return real_write(descriptor, bytes(data[:SHORT_WRITE_BYTES]))

    monkeypatch.setattr(real_filesystem.os, "write", short_write)
    target = tmp_path / "wal.log"
    filesystem = RealFilesystem()

    await filesystem.append_fsync(target, b"head:")
    await filesystem.append_fsync(target, PAYLOAD)

    assert target.read_bytes() == b"head:" + PAYLOAD
    assert len(write_calls) > 2, "the cap must actually have forced short writes"


@pytest.mark.asyncio
async def test_a_write_that_makes_no_progress_raises(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(real_filesystem.os, "write", lambda descriptor, data: 0)

    with pytest.raises(OSError):
        await RealFilesystem().append_fsync(tmp_path / "wal.log", PAYLOAD)
