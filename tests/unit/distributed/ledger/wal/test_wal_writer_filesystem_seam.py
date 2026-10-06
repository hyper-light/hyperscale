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
    """Captures append_fsync calls over an in-memory file; can be told
    to fail appends (after writing a partial prefix, like a short write
    followed by ENOSPC) or to fail the rollback truncate."""

    def __init__(
        self,
        fail_with: OSError | None = None,
        partial_bytes_before_failure: int = 0,
        fail_truncate_with: OSError | None = None,
    ) -> None:
        self.append_calls: list[tuple[Path, bytes]] = []
        self.truncate_calls: list[int] = []
        self.content = b""
        self.created = False
        self.fail_with = fail_with
        self._partial_bytes_before_failure = partial_bytes_before_failure
        self._fail_truncate_with = fail_truncate_with

    async def mkdir(self, path, *, parents=False, exist_ok=False) -> None:
        return None

    async def exists(self, path) -> bool:
        return self.created

    async def file_size(self, path) -> int:
        return len(self.content)

    async def append_fsync(self, path, data: bytes) -> None:
        self.created = True
        if self.fail_with is not None:
            self.content += bytes(data[: self._partial_bytes_before_failure])
            raise self.fail_with
        self.append_calls.append((Path(path), bytes(data)))
        self.content += bytes(data)

    async def truncate(self, path, length: int) -> None:
        self.truncate_calls.append(length)
        if self._fail_truncate_with is not None:
            raise self._fail_truncate_with
        self.content = self.content[:length]


async def _submit_and_wait(writer: WALWriter, payload: bytes) -> None:
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    writer.submit(WriteRequest(data=payload, future=future))
    await asyncio.wait_for(future, timeout=5.0)


@pytest.mark.asyncio
async def test_commits_flow_through_injected_filesystem(tmp_path):
    recording = RecordingFilesystem()
    writer = WALWriter(
        path=tmp_path / "seam.wal",
        config=WALWriterConfig(),
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
        config=WALWriterConfig(),
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
async def test_a_storage_fault_fails_its_batch_rolls_back_and_the_writer_recovers(tmp_path):
    """A storage fault (the disk_full shape: OSError from the seam after
    a short write) fails that batch's submitters loudly, cuts the torn
    prefix back to the last committed record, and leaves the writer
    serving: once the fault clears, the next write commits."""
    filesystem = RecordingFilesystem()
    writer = WALWriter(
        path=tmp_path / "full.wal",
        config=WALWriterConfig(),
        filesystem=filesystem,
    )
    await writer.start()
    try:
        await _submit_and_wait(writer, b"committed|")

        filesystem.fail_with = OSError(28, "No space left on device")
        filesystem._partial_bytes_before_failure = 3
        with pytest.raises(OSError):
            await _submit_and_wait(writer, b"doomed|")
        assert filesystem.content == b"committed|", "the torn prefix was cut back"
        assert filesystem.truncate_calls == [len(b"committed|")]
        assert not writer.has_error
        assert writer.storage_failure is not None
        assert writer.storage_failure.errno == 28

        filesystem.fail_with = None
        await _submit_and_wait(writer, b"after|")
        assert filesystem.content == b"committed|after|"
        assert writer.storage_failure is None
    finally:
        await writer.stop()


@pytest.mark.asyncio
async def test_a_failed_rollback_latches_the_writer(tmp_path):
    """If the torn tail cannot be cut, the log is unusable: the writer
    latches and every later submit fails."""
    filesystem = RecordingFilesystem(
        fail_with=OSError(28, "No space left on device"),
        partial_bytes_before_failure=2,
        fail_truncate_with=OSError(5, "Input/output error"),
    )
    writer = WALWriter(
        path=tmp_path / "broken.wal",
        config=WALWriterConfig(),
        filesystem=filesystem,
    )
    await writer.start()
    try:
        with pytest.raises(OSError):
            await _submit_and_wait(writer, b"doomed|")
        await asyncio.sleep(0)
        assert writer.has_error
        with pytest.raises(OSError):
            await _submit_and_wait(writer, b"after|")
    finally:
        await writer.stop()
