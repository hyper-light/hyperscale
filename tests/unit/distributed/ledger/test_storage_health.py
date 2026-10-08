"""
A node's storage is unwritable from a refused write until a write at
least as large has succeeded.

A manager on a full disk cannot accept jobs, so it reports
``storage_writable`` and gates route around it. On a NEARLY full device
a small write still fits after a large one was refused: measured in the
disk-full SIM, the manager's small idempotency record succeeded right
after its submission payload hit ENOSPC, so "the last write succeeded"
reported the node writable again while it still could not take a job.
Health is therefore size-aware, and the manager's probe -- run from its
periodic loop, since a node routed around writes nothing on its own --
proves the refused size fits before placement returns.

Driven through the real JobLedger and the manager's real storage probe
over the SIM filesystem (a full device short-writes then raises ENOSPC):

* a large write refused on a full disk makes storage unwritable, and a
  smaller write that still fits does not clear it;
* the probe fails while the device is still too full, and once space
  frees it proves the refused size, restoring writability, and leaves
  no probe file behind;
* a node that never failed is writable.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from hyperscale.distributed.ledger.job_ledger import JobLedger
from hyperscale.distributed.ledger.storage_health import StorageHealth
from hyperscale.distributed.nodes.manager.server import ManagerServer
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

DATA_DIR = Path("/manager/data")
LARGE_PAYLOAD_BYTES = 4096
SMALL_RECORD = b"x" * 16


async def open_ledger(filesystem: SimFilesystem, storage_health: StorageHealth) -> JobLedger:
    return await JobLedger.open(
        wal_path=DATA_DIR / "wal",
        checkpoint_dir=DATA_DIR / "checkpoints",
        archive_dir=DATA_DIR / "archive",
        region_code="dc-east",
        gate_id="mgr-1",
        clock=new_hybrid_logical_clock(),
        filesystem=filesystem,
        storage_health=storage_health,
    )


def manager_with(filesystem: SimFilesystem, storage_health: StorageHealth) -> ManagerServer:
    manager = object.__new__(ManagerServer)
    manager._storage_health = storage_health
    manager._storage_filesystem = filesystem
    manager._config = SimpleNamespace(wal_data_dir=DATA_DIR)
    return manager


@pytest.mark.asyncio
async def test_a_small_write_does_not_prove_a_refused_large_one_fits() -> None:
    filesystem = SimFilesystem()
    storage_health = StorageHealth()
    ledger = await open_ledger(filesystem, storage_health)
    await ledger.create_job(spec_hash=b"spec", assigned_datacenters=("dc-east",), requestor_id="client:9500", job_id="job-1")
    assert ledger.storage_writable

    filesystem.set_disk_full(LARGE_PAYLOAD_BYTES // 2)
    with pytest.raises(OSError):
        await filesystem.atomic_write(DATA_DIR / "submission.bin", bytes(LARGE_PAYLOAD_BYTES))
    storage_health.record_failure(OSError(28, "No space left on device"), LARGE_PAYLOAD_BYTES)
    assert not ledger.storage_writable

    await filesystem.append_fsync(DATA_DIR / "small.log", SMALL_RECORD)
    storage_health.record_success(len(SMALL_RECORD))
    assert not ledger.storage_writable, "a small write that fits proves nothing about the large one"
    await ledger.close()


@pytest.mark.asyncio
async def test_the_probe_proves_the_refused_size_once_space_frees() -> None:
    filesystem = SimFilesystem()
    storage_health = StorageHealth()
    storage_health.record_failure(OSError(28, "No space left on device"), LARGE_PAYLOAD_BYTES)
    manager = manager_with(filesystem, storage_health)

    filesystem.set_disk_full(LARGE_PAYLOAD_BYTES // 2)
    await ManagerServer._probe_storage(manager)
    assert not storage_health.writable, "the device is still too full"

    filesystem.clear_disk_full()
    await ManagerServer._probe_storage(manager)
    assert storage_health.writable
    assert not await filesystem.exists(DATA_DIR / ".storage-probe")


@pytest.mark.asyncio
async def test_a_node_that_never_failed_is_writable() -> None:
    filesystem = SimFilesystem()
    storage_health = StorageHealth()
    ledger = await open_ledger(filesystem, storage_health)
    assert ledger.storage_writable
    await ManagerServer._probe_storage(manager_with(filesystem, storage_health))
    assert storage_health.writable
    await ledger.close()
