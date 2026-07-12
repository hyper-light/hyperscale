"""
JobArchiveStore: idempotent archival through the Phase 7 seam.

Pins write-if-absent idempotence, the read/delete round-trip, the
sharded cleanup sweep (now expressed through the seam's directory
operations instead of raw pathlib iteration — which would have crashed
under the SimulationLoop's executor ban), and crash durability of the
archived record under SIM power loss.
"""

import pytest

from hyperscale.core.runtime import RealFilesystem
from hyperscale.distributed.ledger.archive.job_archive_store import (
    JobArchiveStore,
)
from hyperscale.distributed.ledger.job_state import JobState
from hyperscale.logging.lsn import LSN
from tests.simulation.harness.sim import SimFilesystem


@pytest.fixture
def filesystem():
    real_filesystem = RealFilesystem(max_workers=2)
    yield real_filesystem
    real_filesystem.shutdown(wait=True)


def _job_state(job_id: str) -> JobState:
    hlc = LSN(logical_time=1, node_id=1, sequence=0, wall_clock=1000)
    return JobState.create(
        job_id=job_id,
        fence_token=1,
        assigned_datacenters=("dc-east",),
        created_hlc=hlc,
    ).with_completion("completed", total_completed=2, total_failed=0, hlc=hlc)


@pytest.mark.asyncio
async def test_write_if_absent_round_trip_and_idempotence(
    tmp_path, filesystem
):
    store = JobArchiveStore(tmp_path / "archive", filesystem=filesystem)
    await store.initialize()

    job_id = "east-1700000000123-abc"
    assert await store.write_if_absent(_job_state(job_id)) is True
    assert await store.exists(job_id)

    # Idempotent: a second archival of the same job is a no-op success.
    assert await store.write_if_absent(_job_state(job_id)) is True

    recovered = await store.read(job_id)
    assert recovered is not None
    assert recovered.job_id == job_id
    assert recovered.status == "completed"

    assert await store.delete(job_id) is True
    assert not await store.exists(job_id)
    assert await store.delete(job_id) is False


@pytest.mark.asyncio
async def test_cleanup_removes_expired_shards_only(tmp_path, filesystem):
    store = JobArchiveStore(tmp_path / "archive", filesystem=filesystem)
    await store.initialize()

    # Shard key = first 10 digits of the timestamp field (epoch seconds).
    old_job = "east-1600000000000-old"
    fresh_job = "east-1700000000000-new"
    await store.write_if_absent(_job_state(old_job))
    await store.write_if_absent(_job_state(fresh_job))

    current_time_ms = 1_700_000_500_000
    removed = await store.cleanup_older_than(
        max_age_ms=1_000_000_000, current_time_ms=current_time_ms
    )

    assert removed == 1
    assert not await store.exists(old_job)
    assert await store.exists(fresh_job)


@pytest.mark.asyncio
async def test_archive_survives_power_loss_under_sim(tmp_path):
    sim_filesystem = SimFilesystem()
    store = JobArchiveStore(tmp_path / "archive", filesystem=sim_filesystem)
    await store.initialize()

    job_id = "west-1700000000456-xyz"
    await store.write_if_absent(_job_state(job_id))

    sim_filesystem.crash()

    recovered_store = JobArchiveStore(
        tmp_path / "archive", filesystem=sim_filesystem
    )
    recovered = await recovered_store.read(job_id)
    assert recovered is not None
    assert recovered.status == "completed"
