"""
Every sync the real filesystem performs is durable on macOS.

On macOS fsync(2) only moves data to the drive, which may keep it in its
volatile cache; F_FULLFSYNC flushes it to permanent storage. The ledger,
idempotency WAL, checkpoints and incarnation store all synced with
os.fsync, so on macOS a power loss could drop records they had reported
durable (measured on an Apple SSD: fsync 0.1 ms -- a cache hand-off --
against F_FULLFSYNC 3.9 ms).

* where F_FULLFSYNC exists, a sync issues it and nothing else;
* a filesystem that cannot honor it is synced with fsync instead;
* any other failure raises -- a failed sync is never swallowed;
* elsewhere a sync is fsync;
* a real append + sync lands its bytes (F_FULLFSYNC on this machine if
  it is a Mac).
"""

import errno

import pytest

from hyperscale.core.runtime import real_filesystem as real_filesystem_module
from hyperscale.core.runtime.real_filesystem import RealFilesystem

FULL_SYNC_COMMAND = 51
DESCRIPTOR = 7


class RecordingFcntl:
    def __init__(self, failure_errno: int | None = None) -> None:
        self.calls: list[tuple[int, int]] = []
        self._failure_errno = failure_errno

    def fcntl(self, descriptor: int, command: int) -> int:
        self.calls.append((descriptor, command))
        if self._failure_errno is not None:
            raise OSError(self._failure_errno, "full sync refused")
        return 0


def install(
    monkeypatch: pytest.MonkeyPatch,
    full_sync_command: int | None,
    fcntl_module: RecordingFcntl,
) -> list[int]:
    fsynced: list[int] = []
    monkeypatch.setattr(real_filesystem_module, "_FULL_SYNC_COMMAND", full_sync_command)
    monkeypatch.setattr(real_filesystem_module, "fcntl", fcntl_module, raising=False)
    monkeypatch.setattr(real_filesystem_module.os, "fsync", fsynced.append)
    return fsynced


def test_a_sync_issues_a_full_sync_where_it_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    fcntl_module = RecordingFcntl()
    fsynced = install(monkeypatch, FULL_SYNC_COMMAND, fcntl_module)

    RealFilesystem._sync_durably(DESCRIPTOR)

    assert fcntl_module.calls == [(DESCRIPTOR, FULL_SYNC_COMMAND)]
    assert fsynced == []


@pytest.mark.parametrize("unsupported_errno", [errno.ENOTSUP, errno.EOPNOTSUPP, errno.ENOTTY, errno.EINVAL])
def test_a_filesystem_without_full_sync_is_fsynced(
    monkeypatch: pytest.MonkeyPatch,
    unsupported_errno: int,
) -> None:
    fsynced = install(monkeypatch, FULL_SYNC_COMMAND, RecordingFcntl(failure_errno=unsupported_errno))

    RealFilesystem._sync_durably(DESCRIPTOR)

    assert fsynced == [DESCRIPTOR]


def test_a_failed_full_sync_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    fsynced = install(monkeypatch, FULL_SYNC_COMMAND, RecordingFcntl(failure_errno=errno.EIO))

    with pytest.raises(OSError) as raised:
        RealFilesystem._sync_durably(DESCRIPTOR)

    assert raised.value.errno == errno.EIO
    assert fsynced == []


def test_elsewhere_a_sync_is_fsync(monkeypatch: pytest.MonkeyPatch) -> None:
    fcntl_module = RecordingFcntl()
    fsynced = install(monkeypatch, None, fcntl_module)

    RealFilesystem._sync_durably(DESCRIPTOR)

    assert fsynced == [DESCRIPTOR]
    assert fcntl_module.calls == []


@pytest.mark.asyncio
async def test_a_real_append_and_sync_lands_its_bytes(tmp_path) -> None:
    filesystem = RealFilesystem()
    path = tmp_path / "ledger.wal"
    try:
        await filesystem.append_fsync(path, b"first-record\n")
        await filesystem.append_fsync(path, b"second-record\n")
        await filesystem.atomic_write(tmp_path / "checkpoint.bin", b"checkpoint")
    finally:
        filesystem.shutdown()

    assert path.read_bytes() == b"first-record\nsecond-record\n"
    assert (tmp_path / "checkpoint.bin").read_bytes() == b"checkpoint"
