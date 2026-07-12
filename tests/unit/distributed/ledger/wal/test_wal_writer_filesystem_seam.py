"""
WALWriter commits through the Phase 7 storage seam.

Pins the injection contract: every group commit is exactly one
``Filesystem.append_fsync`` call (one durable unit per batch), an
injected filesystem wins over the module default, and a filesystem
failure propagates to every submitter's future — the seam is where SIM
storage faults (slow_disk / disk_full / fsync_reorder) will land, so
its error path must already be loud.
"""

import asyncio
from pathlib import Path

import pytest

from hyperscale.distributed.ledger.wal.wal_writer import (
    WALWriter,
    WALWriterConfig,
    WriteRequest,
)


class RecordingFilesystem:
    """Captures append_fsync calls; optionally fails them."""

    def __init__(self, fail_with: Exception | None = None) -> None:
        self.append_calls: list[tuple[Path, bytes]] = []
        self._fail_with = fail_with

    async def mkdir(self, path, *, parents=False, exist_ok=False) -> None:
        return None

    async def append_fsync(self, path, data: bytes) -> None:
        if self._fail_with is not None:
            raise self._fail_with
        self.append_calls.append((Path(path), bytes(data)))


async def _submit_and_wait(writer: WALWriter, payload: bytes) -> None:
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    writer.submit(WriteRequest(data=payload, future=future))
    await asyncio.wait_for(future, timeout=5.0)


@pytest.mark.asyncio
async def test_commits_flow_through_injected_filesystem(tmp_path):
    recording = RecordingFilesystem()
    writer = WALWriter(
        path=tmp_path / "seam.wal",
        config=WALWriterConfig(batch_timeout_microseconds=100),
        filesystem=recording,
    )
    await writer.start()
    try:
        await _submit_and_wait(writer, b"entry-1|")
        await _submit_and_wait(writer, b"entry-2|")
    finally:
        await writer.stop()

    # Every commit went through the seam — and NOT to the real disk.
    written = b"".join(data for _path, data in recording.append_calls)
    assert written == b"entry-1|entry-2|"
    assert all(
        path == tmp_path / "seam.wal" for path, _data in recording.append_calls
    )
    assert not (tmp_path / "seam.wal").exists()


@pytest.mark.asyncio
async def test_group_commit_is_one_durable_unit(tmp_path):
    """Concurrently submitted entries batch into a single append_fsync
    call — the group-commit contract expressed through the seam."""
    recording = RecordingFilesystem()
    writer = WALWriter(
        path=tmp_path / "batch.wal",
        # A long batch window so both submissions land in one batch.
        config=WALWriterConfig(batch_timeout_microseconds=50_000),
        filesystem=recording,
    )
    await writer.start()
    try:
        loop = asyncio.get_running_loop()
        first: asyncio.Future[None] = loop.create_future()
        second: asyncio.Future[None] = loop.create_future()
        writer.submit(WriteRequest(data=b"alpha|", future=first))
        writer.submit(WriteRequest(data=b"beta|", future=second))
        await asyncio.wait_for(
            asyncio.gather(first, second), timeout=5.0
        )
    finally:
        await writer.stop()

    assert len(recording.append_calls) == 1, recording.append_calls
    assert recording.append_calls[0][1] == b"alpha|beta|"


@pytest.mark.asyncio
async def test_filesystem_failure_propagates_to_submitters(tmp_path):
    """A storage fault (the disk_full shape: OSError from the seam)
    must fail every submitter's future loudly — never a silent drop."""
    failing = RecordingFilesystem(fail_with=OSError(28, "No space left on device"))
    writer = WALWriter(
        path=tmp_path / "full.wal",
        config=WALWriterConfig(batch_timeout_microseconds=100),
        filesystem=failing,
    )
    await writer.start()
    try:
        future: asyncio.Future[None] = (
            asyncio.get_running_loop().create_future()
        )
        writer.submit(WriteRequest(data=b"doomed", future=future))
        with pytest.raises(OSError):
            await asyncio.wait_for(future, timeout=5.0)
        assert writer.has_error
    finally:
        await writer.stop()
