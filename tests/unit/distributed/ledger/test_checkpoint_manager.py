"""
CheckpointManager: durability + recovery through the Phase 7 seam.

Pins:

* save -> fresh-manager initialize round-trip (newest checkpoint wins);
* a corrupted checkpoint (CRC mismatch) is SKIPPED at load -- set aside
  with its bytes preserved, and reported -- and an older valid one is
  recovered instead -- the reason the CRC exists;
* cleanup retains the newest ``keep_count``;
* the atomic_write durability claim, testable only under SIM: a save
  followed by power loss (``SimFilesystem.crash()``) must still be
  fully readable — atomic_write is crash-durable BY CONSTRUCTION.
"""

import pathlib

import pytest

from hyperscale.core.runtime import RealFilesystem
from hyperscale.distributed.ledger.checkpoint.checkpoint import (
    Checkpoint,
    CheckpointManager,
)
from hyperscale.distributed.hlc import HLCTimestamp
from hyperscale.logging.hyperscale_logging_models import StorageFormatUnrecognized
from tests.simulation.harness.sim import SimFilesystem


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list = []

    async def log(self, entry) -> None:
        self.entries.append(entry)


@pytest.fixture
def filesystem():
    real_filesystem = RealFilesystem(max_workers=2)
    yield real_filesystem
    real_filesystem.shutdown(wait=True)


def _checkpoint(created_at_ms: int, local_lsn: int) -> Checkpoint:
    return Checkpoint(
        local_lsn=local_lsn,
        regional_lsn=local_lsn,
        global_lsn=local_lsn,
        hlc=HLCTimestamp(wall_ms=created_at_ms, logical=0, node_id=1),
        job_states={"job-1": {"status": "completed"}},
        created_at_ms=created_at_ms,
    )


@pytest.mark.asyncio
async def test_save_and_recover_latest(tmp_path, filesystem):
    manager = CheckpointManager(tmp_path / "checkpoints", filesystem=filesystem)
    await manager.initialize()
    await manager.save(_checkpoint(created_at_ms=1000, local_lsn=10))
    await manager.save(_checkpoint(created_at_ms=2000, local_lsn=20))

    recovered = CheckpointManager(
        tmp_path / "checkpoints", filesystem=filesystem
    )
    await recovered.initialize()
    assert recovered.has_checkpoint
    assert recovered.latest.created_at_ms == 2000
    assert recovered.latest.local_lsn == 20


@pytest.mark.asyncio
async def test_corrupted_checkpoint_is_skipped_for_older_valid_one(
    tmp_path, filesystem
):
    checkpoint_dir = tmp_path / "checkpoints"
    manager = CheckpointManager(checkpoint_dir, filesystem=filesystem)
    await manager.initialize()
    await manager.save(_checkpoint(created_at_ms=1000, local_lsn=10))
    newest_path = await manager.save(
        _checkpoint(created_at_ms=2000, local_lsn=20)
    )

    # Corrupt the newest checkpoint's payload (past the 16-byte header)
    # — the CRC check must reject it and recovery must fall back.
    corrupted = bytearray(newest_path.read_bytes())
    corrupted[-1] ^= 0xFF
    newest_path.write_bytes(bytes(corrupted))

    logger = RecordingLogger()
    recovered = CheckpointManager(checkpoint_dir, filesystem=filesystem, logger=logger)
    await recovered.initialize()
    assert recovered.has_checkpoint
    assert recovered.latest.created_at_ms == 1000
    (report,) = logger.entries
    assert (type(report), report.path) == (StorageFormatUnrecognized, str(newest_path))
    assert not newest_path.exists()
    assert pathlib.Path(report.set_aside_path).read_bytes() == bytes(corrupted)


@pytest.mark.asyncio
async def test_cleanup_keeps_newest(tmp_path, filesystem):
    manager = CheckpointManager(tmp_path / "checkpoints", filesystem=filesystem)
    await manager.initialize()
    for index in range(5):
        await manager.save(
            _checkpoint(created_at_ms=1000 + index, local_lsn=index)
        )

    removed = await manager.cleanup(keep_count=3)
    assert removed == 2

    recovered = CheckpointManager(
        tmp_path / "checkpoints", filesystem=filesystem
    )
    await recovered.initialize()
    assert recovered.latest.created_at_ms == 1004


@pytest.mark.asyncio
async def test_atomic_write_survives_power_loss_under_sim(tmp_path):
    """The durability contract itself: a saved checkpoint survives a
    crash that drops every un-fsynced write."""
    sim_filesystem = SimFilesystem()
    checkpoint_dir = tmp_path / "checkpoints"

    manager = CheckpointManager(checkpoint_dir, filesystem=sim_filesystem)
    await manager.initialize()
    await manager.save(_checkpoint(created_at_ms=3000, local_lsn=30))

    sim_filesystem.crash()

    recovered = CheckpointManager(checkpoint_dir, filesystem=sim_filesystem)
    await recovered.initialize()
    assert recovered.has_checkpoint
    assert recovered.latest.created_at_ms == 3000
    assert recovered.latest.local_lsn == 30
