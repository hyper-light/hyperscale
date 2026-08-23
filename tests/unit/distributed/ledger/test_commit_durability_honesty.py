"""
The durability-reporting contract: a ``CommitResult`` may never claim
a level the deployment cannot actually provide.

``CommitPipeline`` takes optional regional/global replicators. When one
is absent, the corresponding helper used to ``return True`` — so the
entry advanced through ``mark_regional`` / ``mark_global`` and the
result REPORTED cross-region durability while nothing had left the
node. Every consumer of that verdict — the caller's ack to a client,
the WAL's persisted durability state, an operator reading the level —
was told a job was globally durable when a single disk loss would take
it.

These tests pin the honest behavior in both directions: unconfigured
replication reports the level it actually reached and names why, and
configured replication still advances exactly as before.

Reporting the failure honestly is only half of it, because the ledger
appends to the WAL BEFORE it commits and only updates in-memory state
once the commit succeeds. An unsatisfiable request that got as far as
appending would leave a durable entry describing a job the live node
never tracked: reads say it does not exist, a restart replays the
entry, and recovered state gains a job live state never had. So the
ledger refuses an impossible request up front, before writing
anything, and its defaults ask for what a node can actually provide.

The same lie had one more layer: ``checkpoint()`` stamped its regional
and global watermarks from the local fsync watermark, so the persisted
checkpoint asserted cross-region durability for entries that never
left the node.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.job_ledger import JobLedger
from hyperscale.distributed.ledger.pipeline.commit_pipeline import CommitPipeline
from hyperscale.distributed.ledger.unsatisfiable_durability_error import (
    UnsatisfiableDurabilityError,
)
from hyperscale.distributed.ledger.wal.wal_entry import WALEntry
from tests.simulation.harness.sim import SimFilesystem

LEDGER_PATHS = {
    "wal_path": Path("/node/ledger/wal"),
    "checkpoint_dir": Path("/node/ledger/checkpoints"),
    "archive_dir": Path("/node/ledger/archive"),
    "region_code": "dc-east",
    "gate_id": "gate-1",
    "node_id": 1,
}


async def _open_ledger(filesystem: SimFilesystem, **replication) -> JobLedger:
    return await JobLedger.open(filesystem=filesystem, **LEDGER_PATHS, **replication)


async def _create(
    ledger: JobLedger, job_id: str, **durability
) -> tuple[str, object]:
    return await ledger.create_job(
        spec_hash=b"spec-hash",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        job_id=job_id,
        **durability,
    )


class _StubTransition:
    """WAL transition result: the pipeline only reads ``is_ok``."""

    def __init__(self, is_ok: bool = True) -> None:
        self.is_ok = is_ok
        self.value = "ok" if is_ok else "rejected"


class _StubWAL:
    """Records which durability transitions the pipeline attempted."""

    def __init__(self) -> None:
        self.marked_regional: list[int] = []
        self.marked_global: list[int] = []

    async def mark_regional(self, lsn: int) -> _StubTransition:
        self.marked_regional.append(lsn)
        return _StubTransition()

    async def mark_global(self, lsn: int) -> _StubTransition:
        self.marked_global.append(lsn)
        return _StubTransition()


class _StubEntry:
    __slots__ = ("lsn",)

    def __init__(self, lsn: int = 1) -> None:
        self.lsn = lsn


@pytest.mark.asyncio
async def test_local_commit_needs_no_replicator() -> None:
    """LOCAL is honest without replication: the fsync'd append IS the
    guarantee, and no transition is attempted."""
    wal = _StubWAL()
    pipeline = CommitPipeline(wal=wal)

    result = await pipeline.commit(_StubEntry(), DurabilityLevel.LOCAL)

    assert result.level_achieved == DurabilityLevel.LOCAL
    assert result.error is None
    assert result.success is True
    assert wal.marked_regional == []
    assert wal.marked_global == []


@pytest.mark.asyncio
async def test_regional_without_replicator_reports_local_and_fails() -> None:
    """The core anti-lie: REGIONAL requested, nothing configured to
    replicate with — the result reports LOCAL and carries an error
    naming the missing replicator. It must NOT mark the WAL regional."""
    wal = _StubWAL()
    pipeline = CommitPipeline(wal=wal)

    result = await pipeline.commit(_StubEntry(), DurabilityLevel.REGIONAL)

    assert result.level_achieved == DurabilityLevel.LOCAL
    assert result.success is False
    assert result.error is not None
    assert "no regional replicator" in str(result.error)
    assert wal.marked_regional == [], "durability state advanced without replication"


@pytest.mark.asyncio
async def test_global_without_replicator_reports_what_it_reached() -> None:
    """GLOBAL requested with only a regional replicator configured:
    the entry genuinely reaches REGIONAL, and the result says exactly
    that instead of claiming GLOBAL."""
    wal = _StubWAL()
    replicated: list[int] = []

    async def regional_replicator(entry) -> bool:
        replicated.append(entry.lsn)
        return True

    pipeline = CommitPipeline(wal=wal, regional_replicator=regional_replicator)

    result = await pipeline.commit(_StubEntry(7), DurabilityLevel.GLOBAL)

    assert replicated == [7]
    assert result.level_achieved == DurabilityLevel.REGIONAL
    assert result.success is False
    assert "no global replicator" in str(result.error)
    assert wal.marked_regional == [7]
    assert wal.marked_global == [], "claimed global durability with no replicator"


@pytest.mark.asyncio
async def test_configured_replication_still_advances_both_levels() -> None:
    """The other direction: with both replicators wired, a GLOBAL
    commit reaches GLOBAL and marks both transitions — the honesty fix
    must not have disabled working replication."""
    wal = _StubWAL()

    async def replicator(entry) -> bool:
        return True

    pipeline = CommitPipeline(
        wal=wal,
        regional_replicator=replicator,
        global_replicator=replicator,
    )

    result = await pipeline.commit(_StubEntry(3), DurabilityLevel.GLOBAL)

    assert result.level_achieved == DurabilityLevel.GLOBAL
    assert result.error is None
    assert result.success is True
    assert wal.marked_regional == [3]
    assert wal.marked_global == [3]


@pytest.mark.asyncio
async def test_replication_failure_still_reports_the_level_reached() -> None:
    """A configured replicator that REFUSES is distinct from one that
    is absent, and both stop the level from advancing."""
    wal = _StubWAL()

    async def refusing_replicator(entry) -> bool:
        return False

    pipeline = CommitPipeline(wal=wal, regional_replicator=refusing_replicator)

    result = await pipeline.commit(_StubEntry(), DurabilityLevel.REGIONAL)

    assert result.level_achieved == DurabilityLevel.LOCAL
    assert result.success is False
    assert "Regional replication failed" in str(result.error)
    assert wal.marked_regional == []


async def _replicator(entry: WALEntry) -> bool:
    return True


async def _refusing_replicator(entry: WALEntry) -> bool:
    """Configured, reachable, and refusing — the failure the pre-append
    guard cannot predict."""
    return False


def test_max_achievable_durability_reports_what_is_configured() -> None:
    """What the ledger checks a request against.

    Escalation is ordered, so a global replicator with no regional one
    behind it still tops out at LOCAL — an entry cannot become
    globally durable without becoming regionally durable first.
    """
    wal = _StubWAL()

    assert (
        CommitPipeline(wal=wal).max_achievable_durability == DurabilityLevel.LOCAL
    )
    assert (
        CommitPipeline(
            wal=wal, regional_replicator=_replicator
        ).max_achievable_durability
        == DurabilityLevel.REGIONAL
    )
    assert (
        CommitPipeline(
            wal=wal, global_replicator=_replicator
        ).max_achievable_durability
        == DurabilityLevel.LOCAL
    ), "a global replicator with no regional one behind it cannot escalate"
    assert (
        CommitPipeline(
            wal=wal,
            regional_replicator=_replicator,
            global_replicator=_replicator,
        ).max_achievable_durability
        == DurabilityLevel.GLOBAL
    )


@pytest.mark.asyncio
async def test_unsatisfiable_request_writes_nothing() -> None:
    """The refusal has to land BEFORE the append, or the WAL keeps an
    entry for a job the live node refuses to track."""
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)

    entries_before = ledger.pending_wal_entries

    with pytest.raises(UnsatisfiableDurabilityError) as raised:
        await _create(ledger, "ghost-job", durability=DurabilityLevel.GLOBAL)

    assert raised.value.requested == DurabilityLevel.GLOBAL
    assert raised.value.achievable == DurabilityLevel.LOCAL
    assert ledger.pending_wal_entries == entries_before, (
        "a refused request still appended to the WAL"
    )
    assert ledger.get_job("ghost-job") is None
    await ledger.close()


@pytest.mark.asyncio
async def test_default_durability_is_what_the_node_can_provide() -> None:
    """The defaults were GLOBAL and REGIONAL against a product with no
    replicator wired anywhere, so every caller who omitted the argument
    got a silent failure. They now ask for the level a node actually
    provides."""
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)

    job_id, create_result = await _create(ledger, "job-1")

    assert create_result.success is True
    assert create_result.level_achieved == DurabilityLevel.LOCAL
    assert ledger.get_job(job_id) is not None

    accept_result = await ledger.accept_job(
        "job-1", datacenter_id="dc-east", worker_count=2
    )
    assert accept_result is not None and accept_result.success is True

    complete_result = await ledger.complete_job(
        "job-1",
        final_status="completed",
        total_completed=1,
        total_failed=0,
        duration_ms=10,
    )
    assert complete_result is not None and complete_result.success is True
    await ledger.close()


@pytest.mark.asyncio
async def test_live_and_recovered_state_agree_after_a_refused_request() -> None:
    """The divergence the refusal exists to prevent, pinned end to end:
    a job the live node rejected must not exist after a restart."""
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)

    await _create(ledger, "real-job")
    with pytest.raises(UnsatisfiableDurabilityError):
        await _create(ledger, "ghost-job", durability=DurabilityLevel.GLOBAL)

    assert ledger.get_job("ghost-job") is None
    await ledger.close()

    recovered_ledger = await _open_ledger(filesystem)

    assert recovered_ledger.get_job("ghost-job") is None, (
        "a job the live node never tracked came back on recovery — live "
        "and recovered state diverged"
    )
    assert recovered_ledger.get_job("real-job") is not None
    await recovered_ledger.close()


@pytest.mark.asyncio
async def test_refused_replication_leaves_live_and_recovered_in_agreement() -> None:
    """The case the pre-append guard cannot cover: a replicator that IS
    configured and refuses.

    The entry is already fsync'd by then, and recovery replays it —
    ``_apply_entry`` dispatches on event type, and the WAL's applied
    and durability states live only in memory, so replay cannot tell a
    failed commit from a successful one. Skipping the in-memory apply
    would not undo the write; it would only make reads disagree with a
    restart. The apply is therefore unconditional and the RESULT
    carries the shortfall.
    """
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem, regional_replicator=_refusing_replicator)

    job_id, create_result = await _create(
        ledger, "half-durable-job", durability=DurabilityLevel.REGIONAL
    )

    assert create_result.success is False
    assert create_result.level_achieved == DurabilityLevel.LOCAL
    assert "Regional replication failed" in str(create_result.error)
    assert ledger.get_job(job_id) is not None, (
        "the entry is durable, so the live node has to track the job it "
        "will replay on the way back up"
    )
    await ledger.close()

    recovered_ledger = await _open_ledger(
        filesystem, regional_replicator=_refusing_replicator
    )

    assert recovered_ledger.get_job("half-durable-job") is not None, (
        "live and recovered state diverged on a failed replication"
    )
    assert recovered_ledger._wal.last_regional_lsn == 0, (
        "a refused replication advanced the regional watermark"
    )
    await recovered_ledger.close()


@pytest.mark.asyncio
async def test_checkpoint_watermarks_report_actual_replication() -> None:
    """A persisted checkpoint may not assert durability the entries
    never reached.

    With no replicator, the regional and global watermarks stay at
    zero however far the local fsync watermark advances.
    """
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)

    for index in range(3):
        await _create(ledger, f"job-{index}")

    await ledger.checkpoint()
    checkpoint = ledger._checkpoint_manager.latest

    assert checkpoint.local_lsn == ledger._wal.last_synced_lsn
    assert checkpoint.regional_lsn == 0, (
        "checkpoint claimed regional durability with nothing replicated"
    )
    assert checkpoint.global_lsn == 0, (
        "checkpoint claimed global durability with nothing replicated"
    )
    await ledger.close()


@pytest.mark.asyncio
async def test_checkpoint_watermark_advances_with_real_replication() -> None:
    """The other direction: a configured replicator makes the
    watermark real, and it survives a restart instead of resetting to
    zero and reporting less replication than the last checkpoint did.
    """
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem, regional_replicator=_replicator)

    await _create(ledger, "job-0", durability=DurabilityLevel.REGIONAL)

    await ledger.checkpoint()
    checkpoint = ledger._checkpoint_manager.latest

    assert checkpoint.regional_lsn == ledger._wal.last_synced_lsn
    assert checkpoint.global_lsn == 0, "no global replicator, no global claim"
    replicated_watermark = checkpoint.regional_lsn
    await ledger.close()

    recovered_ledger = await _open_ledger(
        filesystem, regional_replicator=_replicator
    )

    assert recovered_ledger._wal.last_regional_lsn == replicated_watermark, (
        "the replicated watermark reset on restart — the next checkpoint "
        "would report less durability than the previous one recorded"
    )
    await recovered_ledger.close()
