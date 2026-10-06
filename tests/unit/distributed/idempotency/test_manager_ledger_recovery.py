"""
ManagerIdempotencyLedger: WAL recovery + Phase 7 storage seam.

Pins the torn-tail fix and the seam wiring:

* A crash mid-append leaves a partial record at the WAL tail. Replay
  previously RAISED on it — one crash became a PERMANENT boot loop,
  since every subsequent start() re-hit the same debris. Replay now
  recovers every complete entry, drops the tail, and logs the data
  loss loudly (matching NodeWAL / RaftStore truncation tolerance).
* Persistence flows through the injected Filesystem (append_fsync, one
  durable unit per entry) — where SIM storage faults will land.
* Every entry is checksummed: a damaged entry is never replayed as a
  different key or job -- replay stops there, and what follows is
  preserved beside the WAL, never destroyed. A file without the WAL's
  format header is set aside, never read as one.
"""

import struct

import pytest

from hyperscale.core.runtime import RealFilesystem
from hyperscale.distributed.idempotency.idempotency_config import (
    IdempotencyConfig,
)
from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKey
from hyperscale.distributed.idempotency.idempotency_status import (
    IdempotencyStatus,
)
from hyperscale.distributed.idempotency.manager_ledger import (
    FRAME_HEADER,
    IDEMPOTENCY_WAL_FORMAT,
    ManagerIdempotencyLedger,
)


class _StubRunner:
    """No-op task runner: the cleanup loop never starts, keeping tests
    focused on persistence."""

    def run(self, *args, **kwargs):
        return None

    async def cancel(self, token: str) -> None:
        return None


class _RecordingLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def log(self, model) -> None:
        self.messages.append(model.message)


@pytest.fixture
def filesystem():
    real_filesystem = RealFilesystem(max_workers=2)
    yield real_filesystem
    real_filesystem.shutdown(wait=True)


def _ledger(wal_path, filesystem, logger=None):
    return ManagerIdempotencyLedger(
        IdempotencyConfig(),
        wal_path,
        _StubRunner(),
        logger if logger is not None else _RecordingLogger(),
        filesystem=filesystem,
    )


def _key(sequence: int) -> IdempotencyKey:
    return IdempotencyKey(
        client_id="client-a", sequence=sequence, nonce="feedface"
    )


@pytest.mark.asyncio
async def test_entries_survive_restart_through_the_seam(tmp_path, filesystem):
    wal_path = tmp_path / "idempotency.wal"

    ledger = _ledger(wal_path, filesystem)
    await ledger.start()
    await ledger.check_or_reserve(_key(1), "job-1")
    await ledger.commit(_key(1), b"result-1")
    await ledger.check_or_reserve(_key(2), "job-2")
    await ledger.close()

    restarted = _ledger(wal_path, filesystem)
    await restarted.start()

    committed = restarted.get_by_key(_key(1))
    assert committed is not None
    assert committed.status == IdempotencyStatus.COMMITTED
    assert committed.result_serialized == b"result-1"

    pending = restarted.get_by_key(_key(2))
    assert pending is not None
    assert pending.status == IdempotencyStatus.PENDING
    await restarted.close()


@pytest.mark.asyncio
async def test_torn_tail_is_recovered_not_a_boot_loop(tmp_path, filesystem):
    """Crash debris at the WAL tail must not prevent startup, and once
    recovered it is cut: entries appended afterwards survive every
    later restart."""
    wal_path = tmp_path / "idempotency.wal"

    ledger = _ledger(wal_path, filesystem)
    await ledger.start()
    await ledger.check_or_reserve(_key(1), "job-1")
    await ledger.commit(_key(1), b"result-1")
    await ledger.close()

    # Simulate a crash mid-append: a length prefix promising 500 bytes
    # followed by only 5 — the exact shape an interrupted append_fsync
    # leaves behind.
    with open(wal_path, "ab") as wal_file:
        wal_file.write(struct.pack(">I", 500) + b"short")

    # The first restart recovers past the debris (previously it raised
    # ValueError forever), reports it, and cuts it from the file.
    recording_logger = _RecordingLogger()
    restarted = _ledger(wal_path, filesystem, logger=recording_logger)
    await restarted.start()
    recovered = restarted.get_by_key(_key(1))
    assert recovered is not None
    assert recovered.status == IdempotencyStatus.COMMITTED
    assert any(
        "torn tail" in message for message in recording_logger.messages
    ), recording_logger.messages
    # An entry appended after recovering the debris must survive the
    # next restart -- left in place, the torn frame would hide it.
    await restarted.check_or_reserve(_key(2), "job-2")
    await restarted.commit(_key(2), b"result-2")
    await restarted.close()

    second_logger = _RecordingLogger()
    second_restart = _ledger(wal_path, filesystem, logger=second_logger)
    await second_restart.start()
    assert second_restart.get_by_key(_key(1)).status == IdempotencyStatus.COMMITTED
    after_debris = second_restart.get_by_key(_key(2))
    assert after_debris is not None, "the entry appended after the debris was lost"
    assert after_debris.status == IdempotencyStatus.COMMITTED
    assert not any("torn tail" in message for message in second_logger.messages)
    await second_restart.close()


@pytest.mark.asyncio
async def test_undecodable_frame_stops_replay_loudly(tmp_path, filesystem):
    """A complete-length frame with garbage payload (bit rot / interior
    torn write) stops replay at that frame instead of crashing."""
    wal_path = tmp_path / "idempotency.wal"

    ledger = _ledger(wal_path, filesystem)
    await ledger.start()
    await ledger.check_or_reserve(_key(1), "job-1")
    await ledger.close()

    with open(wal_path, "ab") as wal_file:
        wal_file.write(struct.pack(">I", 4) + b"\xff\xff\xff\xff")

    recording_logger = _RecordingLogger()
    restarted = _ledger(wal_path, filesystem, logger=recording_logger)
    await restarted.start()
    assert restarted.get_by_key(_key(1)) is not None
    assert any(
        "torn tail" in message for message in recording_logger.messages
    )
    await restarted.close()


@pytest.mark.asyncio
async def test_persistence_flows_through_injected_filesystem(tmp_path):
    """Every persist is one append_fsync durable unit on the seam —
    and never touches the real disk when a fake is injected."""

    class RecordingFilesystem:
        def __init__(self) -> None:
            self.appends: list[bytes] = []

        async def mkdir(self, path, *, parents=False, exist_ok=False):
            return None

        async def exists(self, path) -> bool:
            return False

        async def append_fsync(self, path, data: bytes) -> None:
            self.appends.append(bytes(data))

    recording = RecordingFilesystem()
    wal_path = tmp_path / "never-created.wal"
    ledger = _ledger(wal_path, recording)
    await ledger.start()
    await ledger.check_or_reserve(_key(7), "job-7")
    await ledger.commit(_key(7), b"result-7")
    await ledger.close()

    # The format header the new file starts with, then reserve and commit.
    assert len(recording.appends) == 3
    assert recording.appends[0] == IDEMPOTENCY_WAL_FORMAT.header
    assert not wal_path.exists()


@pytest.mark.asyncio
async def test_a_damaged_entry_is_never_replayed_and_what_follows_is_preserved(tmp_path, filesystem):
    """A bit flipped inside an entry's job id still decodes -- without the
    checksum it would replay as a different job under the key."""
    wal_path = tmp_path / "idempotency.wal"
    ledger = _ledger(wal_path, filesystem)
    await ledger.start()
    await ledger.check_or_reserve(_key(1), "job-1")
    await ledger.check_or_reserve(_key(2), "job-2")
    await ledger.check_or_reserve(_key(3), "job-3")
    await ledger.close()

    data = bytearray(wal_path.read_bytes())
    second_frame = IDEMPOTENCY_WAL_FORMAT.header_size + FRAME_HEADER.size + FRAME_HEADER.unpack_from(
        data, IDEMPOTENCY_WAL_FORMAT.header_size
    )[1]
    damaged_at = data.index(b"job-2", second_frame)
    data[damaged_at + 4] ^= 0x01  # "job-2" -> "job-3"
    wal_path.write_bytes(bytes(data))

    recording_logger = _RecordingLogger()
    restarted = _ledger(wal_path, filesystem, logger=recording_logger)
    await restarted.start()

    assert restarted.get_by_key(_key(1)).job_id == "job-1"
    assert restarted.get_by_key(_key(2)) is None and restarted.get_by_key(_key(3)) is None
    (preserved,) = list(tmp_path.glob("idempotency.wal.discarded-*"))
    assert preserved.read_bytes() == bytes(data[second_frame:])
    assert any("preserved at" in message for message in recording_logger.messages)
    await restarted.close()


@pytest.mark.asyncio
async def test_a_file_without_the_wal_header_is_set_aside(tmp_path, filesystem):
    wal_path = tmp_path / "idempotency.wal"
    foreign = struct.pack(">I", 4) + b"\x00\x01\x02\x03"
    wal_path.write_bytes(foreign)

    restarted = _ledger(wal_path, filesystem)
    await restarted.start()
    await restarted.check_or_reserve(_key(1), "job-1")
    await restarted.close()

    (set_aside,) = list(tmp_path.glob("idempotency.wal.unrecognized-*"))
    assert set_aside.read_bytes() == foreign
    replayed = _ledger(wal_path, filesystem)
    await replayed.start()
    assert replayed.get_by_key(_key(1)).job_id == "job-1"
    await replayed.close()
