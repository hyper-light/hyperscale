"""
AD-38's job event log is complete: all eight event types are written,
survive power loss, and replay to exactly the live state.

Four of the eight (JobProgressReported, JobCancellationAcked, JobFailed,
JobTimedOut) were declared and never written or replayed: failures and
timeouts were folded into JobCompleted, progress and per-datacenter
cancel confirmation were never durable. Recording them exposed three
recovery defects these tests also pin:

* checkpoints persisted active jobs without ``timeout_seconds``, so a
  checkpoint + restart resumed AD-34 tracking with a zero budget;
* recovery from a checkpoint restarted the fence counter at 1 —
  compaction dropped the JOB_CREATED entries replay advanced it from —
  so new jobs re-used fence tokens held by live jobs;
* JobState transitions rebuilt the record field-by-field and silently
  reset any field they did not name.

Every test runs on SimFilesystem across ledger generations, and crashes
(power loss: un-fsynced tails vanish) rather than closing cleanly
wherever the contract is about durability.
"""

from pathlib import Path

import pytest

from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.job_ledger import JobLedger
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

LOCAL = DurabilityLevel.LOCAL


async def _open_ledger(filesystem: SimFilesystem) -> JobLedger:
    return await JobLedger.open(
        wal_path=Path("/manager/ledger/wal"),
        checkpoint_dir=Path("/manager/ledger/checkpoints"),
        archive_dir=Path("/manager/ledger/archive"),
        region_code="dc-east",
        gate_id="mgr-1",
        clock=new_hybrid_logical_clock(),
        filesystem=filesystem,
    )


async def _create(ledger: JobLedger, job_id: str, timeout_seconds: float = 0.0) -> str:
    created_id, result = await ledger.create_job(
        spec_hash=b"spec",
        assigned_datacenters=("dc-east", "dc-west"),
        requestor_id="client-1",
        durability=LOCAL,
        job_id=job_id,
        timeout_seconds=timeout_seconds,
    )
    assert result.success and created_id == job_id
    return created_id


async def _logged_event_types(ledger: JobLedger) -> set[JobEventType]:
    return {entry.event_type async for entry in ledger._wal.iter_from(0)}


async def _record_every_event_type(ledger: JobLedger) -> dict[str, str]:
    """Drive one job through each terminal and the non-terminal events."""
    running = await _create(ledger, "job-running", timeout_seconds=90.0)
    await ledger.accept_job(running, "dc-east", 2, durability=LOCAL)
    await ledger.report_progress(running, "dc-east", 1, 0, durability=LOCAL)

    cancelling = await _create(ledger, "job-cancelling")
    await ledger.request_cancellation(cancelling, "user", "client-1", durability=LOCAL)
    await ledger.acknowledge_cancellation(cancelling, "dc-east", 2, durability=LOCAL)

    failed = await _create(ledger, "job-failed")
    await ledger.fail_job(failed, "boom", "dc-west", 0, 2, 1500, durability=LOCAL)

    timed_out = await _create(ledger, "job-timed-out")
    await ledger.report_progress(timed_out, "dc-east", 3, 1, durability=LOCAL)
    await ledger.time_out_job(timed_out, "job timeout", 3, 1, 90_000, durability=LOCAL)

    completed = await _create(ledger, "job-completed")
    await ledger.complete_job(completed, "completed", 2, 0, 500, durability=LOCAL)

    return {
        "running": running,
        "cancelling": cancelling,
        "failed": failed,
        "timed_out": timed_out,
        "completed": completed,
    }


@pytest.mark.asyncio
async def test_all_eight_event_types_are_written() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)

    await _record_every_event_type(ledger)

    assert await _logged_event_types(ledger) == set(JobEventType)
    await ledger.close()


@pytest.mark.asyncio
async def test_power_loss_replays_every_event_to_the_live_state() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)
    jobs = await _record_every_event_type(ledger)
    live = {name: ledger.get_job(job_id) for name, job_id in jobs.items()}
    await ledger.close()

    filesystem.crash()
    recovered = await _open_ledger(filesystem)

    recovered_jobs = {name: recovered.get_job(job_id) for name, job_id in jobs.items()}
    assert recovered_jobs == live
    assert recovered_jobs["failed"].status == "failed"
    assert recovered_jobs["timed_out"].status == "timeout"
    assert recovered_jobs["cancelling"].cancellation_acked_datacenters == frozenset({"dc-east"})
    assert recovered_jobs["running"].completed_count == 1
    await recovered.close()


@pytest.mark.asyncio
async def test_terminal_keeps_the_last_progress_hlc() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)
    job_id = await _create(ledger, "job-1")
    await ledger.report_progress(job_id, "dc-east", 3, 1, durability=LOCAL)
    progress_hlc = ledger.get_job(job_id).last_progress_hlc

    await ledger.time_out_job(job_id, "job timeout", 3, 1, 90_000, durability=LOCAL)

    timed_out = ledger.get_job(job_id)
    assert progress_hlc is not None
    assert timed_out.last_progress_hlc == progress_hlc
    assert timed_out.last_hlc > progress_hlc
    await ledger.close()


@pytest.mark.asyncio
async def test_checkpoint_and_power_loss_keep_every_active_field() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)
    jobs = await _record_every_event_type(ledger)
    before = {
        name: ledger.get_job(jobs[name]) for name in ("running", "cancelling")
    }
    await ledger.checkpoint()
    await ledger.close()

    filesystem.crash()
    recovered = await _open_ledger(filesystem)

    for name, job_state in before.items():
        assert recovered.get_job(jobs[name]) == job_state
    assert recovered.get_job(jobs["running"]).timeout_seconds == 90.0
    await recovered.close()


@pytest.mark.asyncio
async def test_fence_tokens_are_never_reissued_after_checkpoint_recovery() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)
    jobs = await _record_every_event_type(ledger)
    issued = {ledger.get_job(job_id).fence_token for job_id in jobs.values()}
    await ledger.checkpoint()
    await ledger.close()

    filesystem.crash()
    recovered = await _open_ledger(filesystem)
    new_job = await _create(recovered, "job-after-restart")

    assert recovered.get_job(new_job).fence_token > max(issued)
    await recovered.close()


@pytest.mark.asyncio
async def test_repeats_and_invalid_transitions_append_nothing() -> None:
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)
    job_id = await _create(ledger, "job-1")

    assert await ledger.acknowledge_cancellation(job_id, "dc-east", 1, durability=LOCAL) is None
    assert await ledger.report_progress(job_id, "dc-east", 0, 0, durability=LOCAL) is None
    assert await ledger.report_progress(job_id, "dc-east", 1, 0, durability=LOCAL) is not None
    assert await ledger.report_progress(job_id, "dc-east", 1, 0, durability=LOCAL) is None

    await ledger.request_cancellation(job_id, "user", "client-1", durability=LOCAL)
    assert await ledger.acknowledge_cancellation(job_id, "dc-east", 1, durability=LOCAL) is not None
    assert await ledger.acknowledge_cancellation(job_id, "dc-east", 1, durability=LOCAL) is None

    assert await ledger.fail_job(job_id, "boom", "dc-east", 1, 0, 10, durability=LOCAL) is not None
    assert await ledger.time_out_job(job_id, "late", 1, 0, 10, durability=LOCAL) is None
    assert await ledger.complete_job(job_id, "completed", 1, 0, 10, durability=LOCAL) is None
    assert await ledger.report_progress(job_id, "dc-east", 2, 0, durability=LOCAL) is None
    assert ledger.get_job(job_id).status == "failed"
    await ledger.close()
