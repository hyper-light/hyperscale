"""
ManagerIdempotencyLedger: WAL recovery + Phase 7 storage seam.

Pins the torn-tail fix and the seam wiring:

* A crash mid-append leaves a partial record at the WAL tail. Replay
  previously RAISED on it — one crash became a PERMANENT boot loop,
  since every subsequent start() re-hit the same debris. Replay now
  recovers every complete entry, drops the tail, and logs the data
  loss loudly (matching NodeWAL / RaftWAL truncation tolerance).
* Persistence flows through the injected Filesystem (append_fsync, one
  durable unit per entry) — where SIM storage faults will land.
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
    """The headline fix: crash debris at the WAL tail must not prevent
    startup — across ANY number of restarts."""
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

    for restart_round in range(2):
        recording_logger = _RecordingLogger()
        restarted = _ledger(wal_path, filesystem, logger=recording_logger)
        await restarted.start()  # previously raised ValueError forever

        recovered = restarted.get_by_key(_key(1))
        assert recovered is not None, f"restart {restart_round}"
        assert recovered.status == IdempotencyStatus.COMMITTED
        assert any(
            "torn tail" in message for message in recording_logger.messages
        ), recording_logger.messages
        await restarted.close()


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

    assert len(recording.appends) == 2  # reserve + commit
    assert not wal_path.exists()
