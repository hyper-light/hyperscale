from __future__ import annotations

import asyncio
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Callable, Awaitable, Mapping, Sequence

import msgspec

from hyperscale.logging.hyperscale_logging_models import (
    ArchiveError,
    ArchiveInfo,
    CheckpointError,
    CheckpointInfo,
)
from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp
from hyperscale.distributed.hlc.hybrid_logical_clock import HybridLogicalClock

from .archive.job_archive_store import JobArchiveStore

from hyperscale.distributed.runtime import Clock, Filesystem, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

if TYPE_CHECKING:
    from hyperscale.logging import Logger
from .cache.bounded_lru_cache import BoundedLRUCache
from .durability_level import DurabilityLevel
from .events.event_type import JobEventType
from .events.job_leadership_acquired import JobLeadershipAcquired
from .events.job_event import (
    JobCreated,
    JobDatacenterReassigned,
    JobAccepted,
    JobCancellationAcked,
    JobCancellationRequested,
    JobCompleted,
    JobFailed,
    JobProgressReported,
    JobRelinquished,
    JobTimedOut,
)
from .job_event_applier import (
    JOB_FAILED_STATUS,
    JOB_RELINQUISHED_STATUS,
    JOB_TIMED_OUT_STATUS,
    JobEventApplier,
)
from .job_commit_sequencer import JobCommitSequencer
from .job_id import JobIdGenerator
from .unsatisfiable_durability_error import UnsatisfiableDurabilityError
from .datacenter_reassignment import DatacenterReassignment
from .job_state import JobState
from .storage_health import StorageHealth
from .wal.node_wal import NodeWAL, WALAppendResult
from .wal.wal_entry import WALEntry
from .pipeline.commit_pipeline import (
    GLOBAL_TIMEOUT_SECONDS,
    REGIONAL_TIMEOUT_SECONDS,
    CommitPipeline,
    CommitResult,
)
from .checkpoint.checkpoint import Checkpoint, CheckpointManager

DEFAULT_COMPLETED_CACHE_SIZE = 10000

# Terminal jobs whose archive record is still owed (the WAL terminal is
# durable; only the cold-read copy is missing) are parked for healing.
# The park is bounded: past this, the OLDEST owed record is dropped with
# an error log — its durable truth stays in the WAL, exactly the
# pre-isolation status quo for every archive failure.
PENDING_ARCHIVE_LIMIT = 1024

# AD-38's compaction criterion, verbatim from architecture.md's control
# plane requirements: "Compaction: WAL size bounded to 2x active job
# state". Compaction only removes entries at or below a checkpoint's
# LSN, so with no checkpoint ever taken the bound cannot hold at all --
# pending entries accumulate for the process lifetime and recovery
# replays from LSN 0. Checkpointing at this ratio is what makes the
# stated bound true.
WAL_TO_ACTIVE_STATE_RATIO = 2

# A node with few or no active jobs still needs a floor: at 2x zero the
# ratio would checkpoint on every single append, trading the unbounded
# WAL for unbounded checkpoint writes.
MIN_CHECKPOINT_WAL_ENTRIES = 64

# The ratio alone leaves a LOW-rate node's small WAL un-checkpointed
# indefinitely, so its recovery cost keeps climbing with uptime while
# never crossing the floor. This is the age bound that holds
# architecture.md's other durability criterion -- "Recovery: <30
# seconds from crash to serving requests" -- for a node that only ever
# runs a handful of jobs.
CHECKPOINT_MAX_INTERVAL_SECONDS = 300.0

# Checkpoints on a cadence means checkpoint FILES on a cadence, so
# retention has to be live for the same reason the cadence does. Above
# one because recovery walks them newest-first and skips undecodable
# ones -- the older copies are the fallback for a checkpoint torn by a
# crash mid-write.
CHECKPOINT_RETENTION_COUNT = 3


class JobLedger:
    """The durable record of every job this node owns.

    Apply contract
    --------------

    Each write path appends to the WAL, applies the event to in-memory
    state, and then commits through the durability pipeline -- and the
    apply is UNCONDITIONAL, including when the commit reports a level
    below the one requested.

    The commit runs outside the ledger lock, in the job's turn
    (``JobCommitSequencer``): a replicated commit is a consensus round
    trip that can wait out an election, and holding the lock across it
    stalled every job's writes behind one job's group. Turns keep each
    job's commits in append order. An entry is marked APPLIED only after
    its commit finishes, so a checkpoint cannot compact an entry whose
    replication is still in flight.

    That is not a shortcut, it is the only way live and recovered
    state can agree. The append is fsync'd before the commit runs, and
    recovery replays every entry it finds: ``_apply_entry`` dispatches
    purely on event type, while the WAL's applied and durability
    states live only in memory and are never written back. So an entry
    whose replication failed is replayed exactly like one whose
    replication succeeded. Skipping the in-memory apply never undid
    the durable write -- it only made reads say a job did not exist
    while a restart said it did.

    Compensating for a failed commit (appending an abandonment record)
    would reintroduce the same class of divergence through a smaller
    window, since that record can itself fail to land. Applying
    unconditionally closes the window entirely: replay reproduces what
    apply did because both act on the same durable record.

    The commit result still carries the level actually achieved and
    the error that capped it, which is what a caller needing more than
    LOCAL durability acts on. A request the node cannot satisfy at all
    is refused before anything is written -- see
    ``_require_satisfiable_durability``.
    """

    __slots__ = (
        "_clock",
        "_wal",
        "_pipeline",
        "_checkpoint_manager",
        "_job_id_generator",
        "_archive_store",
        "_completed_cache",
        "_jobs_internal",
        "_jobs_snapshot",
        "_lock",
        "_next_fence_token",
        "_event_applier",
        "_commit_sequencer",
        "_logger",
        "_pending_archive_jobs",
        "_checkpoint_wal_ratio",
        "_min_checkpoint_wal_entries",
        "_checkpoint_max_interval_seconds",
        "_checkpoint_retention_count",
        "_last_checkpoint_at",
        "_storage_health",
    )

    def __init__(
        self,
        clock: HybridLogicalClock,
        wal: NodeWAL,
        pipeline: CommitPipeline,
        checkpoint_manager: CheckpointManager,
        job_id_generator: JobIdGenerator,
        archive_store: JobArchiveStore,
        storage_health: StorageHealth,
        completed_cache_size: int = DEFAULT_COMPLETED_CACHE_SIZE,
        logger: Logger | None = None,
        checkpoint_wal_ratio: int = WAL_TO_ACTIVE_STATE_RATIO,
        min_checkpoint_wal_entries: int = MIN_CHECKPOINT_WAL_ENTRIES,
        checkpoint_max_interval_seconds: float = CHECKPOINT_MAX_INTERVAL_SECONDS,
        checkpoint_retention_count: int = CHECKPOINT_RETENTION_COUNT,
    ) -> None:
        self._clock = clock
        # The same tracker ``wal`` records its commits into.
        self._storage_health = storage_health
        self._wal = wal
        self._pipeline = pipeline
        self._checkpoint_manager = checkpoint_manager
        self._job_id_generator = job_id_generator
        self._archive_store = archive_store
        self._logger = logger
        self._completed_cache: BoundedLRUCache[str, JobState] = BoundedLRUCache(
            max_size=completed_cache_size
        )
        self._jobs_internal: dict[str, JobState] = {}
        self._jobs_snapshot: Mapping[str, JobState] = MappingProxyType({})
        self._lock = asyncio.Lock()
        self._next_fence_token = 1
        self._event_applier = JobEventApplier()
        self._commit_sequencer = JobCommitSequencer()
        self._pending_archive_jobs: dict[str, JobState] = {}
        self._checkpoint_wal_ratio = checkpoint_wal_ratio
        self._min_checkpoint_wal_entries = min_checkpoint_wal_entries
        self._checkpoint_max_interval_seconds = checkpoint_max_interval_seconds
        self._checkpoint_retention_count = checkpoint_retention_count
        # A fresh ledger starts its interval now, so a node that boots
        # and immediately appends one entry does not age-trigger on a
        # clock read that predates its own existence.
        self._last_checkpoint_at = _DEFAULT_CLOCK.time()


    @classmethod
    async def open(
        cls,
        wal_path: Path,
        checkpoint_dir: Path,
        archive_dir: Path,
        region_code: str,
        gate_id: str,
        clock: HybridLogicalClock,
        regional_replicator: Callable[[WALEntry], Awaitable[bool]] | None = None,
        global_replicator: Callable[[WALEntry], Awaitable[bool]] | None = None,
        regional_timeout_seconds: float = REGIONAL_TIMEOUT_SECONDS,
        global_timeout_seconds: float = GLOBAL_TIMEOUT_SECONDS,
        completed_cache_size: int = DEFAULT_COMPLETED_CACHE_SIZE,
        logger: Logger | None = None,
        filesystem: Filesystem | None = None,
        checkpoint_wal_ratio: int = WAL_TO_ACTIVE_STATE_RATIO,
        min_checkpoint_wal_entries: int = MIN_CHECKPOINT_WAL_ENTRIES,
        checkpoint_max_interval_seconds: float = CHECKPOINT_MAX_INTERVAL_SECONDS,
        checkpoint_retention_count: int = CHECKPOINT_RETENTION_COUNT,
        storage_health: StorageHealth | None = None,
    ) -> JobLedger:
        """``clock`` is the owning node's HLC, so ledger events are
        causally ordered against its other WAL/Raft writes;
        ``filesystem`` is the Phase 7 storage seam (None binds each
        component's module default)."""
        # One storage-health tracker per node: the owner injects its own
        # to share with the node's other durable stores, or the ledger
        # owns one.
        node_storage_health = (
            storage_health if storage_health is not None else StorageHealth()
        )
        wal = await NodeWAL.open(
            path=wal_path,
            clock=clock,
            logger=logger,
            filesystem=filesystem,
            storage_health=node_storage_health,
        )

        pipeline = CommitPipeline(
            wal=wal,
            regional_replicator=regional_replicator,
            global_replicator=global_replicator,
            regional_timeout=regional_timeout_seconds,
            global_timeout=global_timeout_seconds,
            logger=logger,
        )

        checkpoint_manager = CheckpointManager(
            checkpoint_dir=checkpoint_dir, filesystem=filesystem, logger=logger
        )
        await checkpoint_manager.initialize()

        archive_store = JobArchiveStore(
            archive_dir=archive_dir, filesystem=filesystem, logger=logger
        )
        await archive_store.initialize()

        job_id_generator = JobIdGenerator(
            region_code=region_code,
            gate_id=gate_id,
        )

        ledger = cls(
            clock=clock,
            wal=wal,
            pipeline=pipeline,
            checkpoint_manager=checkpoint_manager,
            job_id_generator=job_id_generator,
            archive_store=archive_store,
            storage_health=node_storage_health,
            completed_cache_size=completed_cache_size,
            logger=logger,
            checkpoint_wal_ratio=checkpoint_wal_ratio,
            min_checkpoint_wal_entries=min_checkpoint_wal_entries,
            checkpoint_max_interval_seconds=checkpoint_max_interval_seconds,
            checkpoint_retention_count=checkpoint_retention_count,
        )

        await ledger._recover()
        return ledger

    async def _recover(self) -> None:
        checkpoint = self._checkpoint_manager.latest

        start_lsn = self._resume_from_checkpoint(checkpoint) if checkpoint is not None else 0

        async for entry in self._wal.iter_from(start_lsn):
            self._apply_entry(entry)
        # Everything recovered is now applied -- replayed, or held by the
        # checkpoint -- so the next checkpoint can compact it.
        await self._wal.mark_applied_through(self._wal.last_synced_lsn)

        await self._archive_terminal_jobs()
        self._publish_snapshot()

    def _resume_from_checkpoint(self, checkpoint: Checkpoint) -> int:
        """Restore the checkpoint's jobs, fence counter, clock and WAL
        watermarks: the LSN replay resumes from."""
        for job_id, job_dict in checkpoint.job_states.items():
            self._jobs_internal[job_id] = JobState.from_dict(job_id, job_dict)

        # Compaction dropped the JOB_CREATED entries replay would
        # advance the fence counter from; resume above everything the
        # checkpoint knows about (its own counter, or — for a
        # checkpoint predating that field — its jobs' tokens).
        self._next_fence_token = max(
            self._next_fence_token,
            checkpoint.next_fence_token,
            *(job.fence_token + 1 for job in self._jobs_internal.values()),
        )

        self._clock.witness(checkpoint.hlc)
        self._wal.restore_durability_watermarks(
            regional_lsn=checkpoint.regional_lsn,
            global_lsn=checkpoint.global_lsn,
        )
        # The log may hold nothing past the checkpoint (it dropped what
        # the checkpoint covers): numbering resumes after it.
        self._wal.restore_checkpointed_lsn(checkpoint.local_lsn)
        return checkpoint.local_lsn + 1

    async def _archive_terminal_jobs(self) -> None:
        """Recovery's terminal sweep. Archive writes are ISOLATED here
        for the same reason as ``complete_job``'s: this runs inside
        ``open()`` at node boot, and an unprotected ENOSPC would wedge
        the owning server's ``start()`` on a full disk — the cache is
        the truthful read surface either way (rebuilt from the WAL),
        the archive record stays owed until the disk heals."""
        terminal_job_ids: list[str] = []

        for job_id, job_state in self._jobs_internal.items():
            await self._archive_if_terminal(job_id, job_state, terminal_job_ids)

        for job_id in terminal_job_ids:
            del self._jobs_internal[job_id]

    async def _archive_if_terminal(
        self, job_id: str, job_state: JobState, terminal_job_ids: list[str]
    ) -> None:
        """Archive and cache a terminal job, noting it for removal from the
        active jobs."""
        if job_state.is_terminal:
            await self._archive_job_isolated(job_state)
            self._completed_cache.put(job_id, job_state)
            terminal_job_ids.append(job_id)

    @property
    def pending_archive_count(self) -> int:
        """Terminal jobs whose archive record is still owed — their WAL
        terminal is durable and reads serve from the completed cache;
        only the cold-read archive copy is missing (disk failure at
        write time, healed opportunistically)."""
        return len(self._pending_archive_jobs)

    async def _archive_job_isolated(self, terminal_job: JobState) -> bool:
        """Write one terminal job's archive record, CONTAINED.

        The archive is the ledger's cold-read copy of terminal state;
        the fsync'd WAL append that precedes every call is the durable
        truth.
        Pre-isolation, the first live archive failure (ENOSPC on the
        236-byte terminal copy after the 86-byte WAL append had fit)
        propagated out of the manager's completion handler: the
        completed-cache put, snapshot publish, ``mark_applied``, the
        tier-1 client push, and the gate notification all never ran —
        durably-COMPLETED work was reported to the client as a timeout.

        Failures are logged loudly and the job is PARKED; the park is
        healed by the next successful archive write (evidence the disk
        recovered) or a targeted ``get_archived_job``. The park is
        process-lifetime only — after a reboot, recovery's terminal
        sweep re-attempts every unarchived terminal it finds in the
        WAL, so nothing is owed silently across generations.

        Returns True when the record landed (or already existed).
        """
        try:
            await self._archive_store.write_if_absent(terminal_job)
        except Exception as archive_error:
            await self._park_failed_archive(terminal_job, archive_error)
            return False

        if self._pending_archive_jobs.pop(terminal_job.job_id, None) is not None:
            await self._log_archive_healed(terminal_job.job_id)
        return True

    async def _park_failed_archive(self, terminal_job: JobState, archive_error: Exception) -> None:
        """Park a terminal job whose archive write failed, evicting the
        oldest parked record past PENDING_ARCHIVE_LIMIT, and log it."""
        already_parked = terminal_job.job_id in self._pending_archive_jobs
        self._pending_archive_jobs[terminal_job.job_id] = terminal_job
        if not already_parked and (
            len(self._pending_archive_jobs) > PENDING_ARCHIVE_LIMIT
        ):
            evicted_job_id = next(iter(self._pending_archive_jobs))
            del self._pending_archive_jobs[evicted_job_id]
            await self._log_archive_error(
                evicted_job_id, "PendingArchiveOverflow"
            )
        await self._log_archive_error(
            terminal_job.job_id, type(archive_error).__name__
        )

    async def _heal_pending_archive_jobs(self) -> None:
        """Retry every parked archive record, oldest first — called
        only on fresh evidence the archive disk writes again (a
        just-landed record). Stops at the first failure: the disk is
        still bad and the rest would only churn."""
        for job_id in list(self._pending_archive_jobs):
            if not await self._heal_parked_archive(job_id):
                return

    async def _heal_parked_archive(self, job_id: str) -> bool:
        """Retry one parked archive record; False when the write failed
        again (a record no longer parked counts as healed)."""
        parked_job = self._pending_archive_jobs.get(job_id)
        if parked_job is None:
            return True
        return await self._archive_job_isolated(parked_job)

    async def _log_archive_error(self, job_id: str, error_type: str) -> None:
        if self._logger is not None:
            await self._logger.log(
                ArchiveError(
                    message=(
                        f"archive record for terminal job {job_id} not "
                        f"written ({error_type}); WAL terminal is durable, "
                        "record parked for healing"
                    ),
                    path=str(self._archive_store.archive_dir),
                    job_id=job_id,
                    error_type=error_type,
                )
            )

    async def _log_archive_healed(self, job_id: str) -> None:
        if self._logger is not None:
            await self._logger.log(
                ArchiveInfo(
                    message=(
                        f"parked archive record for terminal job {job_id} "
                        "healed"
                    ),
                    path=str(self._archive_store.archive_dir),
                    job_id=job_id,
                )
            )

    def _publish_snapshot(self) -> None:
        self._jobs_snapshot = MappingProxyType(dict(self._jobs_internal))

    def _apply_entry(self, entry: WALEntry) -> None:
        fence_token = self._event_applier.apply(entry, self._jobs_internal)
        if fence_token >= self._next_fence_token:
            self._next_fence_token = fence_token + 1

    def _require_satisfiable_durability(self, durability: DurabilityLevel) -> None:
        """Refuse an impossible durability request BEFORE anything is
        written.

        Every write path appends to the WAL first and only updates
        in-memory state once the commit succeeds. A request above what
        the configured replicators can reach can never succeed, so
        letting it through would fsync an entry describing a job the
        live node then refuses to track -- reads say it does not
        exist, and a restart replays the entry and says it does.
        Raising here is what keeps live and recovered state the same.
        """
        achievable = self._pipeline.max_achievable_durability
        if durability > achievable:
            raise UnsatisfiableDurabilityError(durability, achievable)

    async def create_job(
        self,
        spec_hash: bytes,
        assigned_datacenters: tuple[str, ...],
        requestor_id: str,
        durability: DurabilityLevel = DurabilityLevel.LOCAL,
        job_id: str | None = None,
        timeout_seconds: float = 0.0,
    ) -> tuple[str, CommitResult]:
        """``job_id`` records an externally-generated id (the client
        generates job ids at submission); None generates one here (the
        gate path)."""
        self._require_satisfiable_durability(durability)

        async with self._lock:
            if job_id is None:
                job_id = await self._job_id_generator.generate()
            fence_token = self._next_fence_token
            self._next_fence_token += 1

            hlc = self._clock.now()

            event = JobCreated(
                job_id=job_id,
                hlc=hlc,
                fence_token=fence_token,
                spec_hash=spec_hash,
                assigned_datacenters=assigned_datacenters,
                requestor_id=requestor_id,
                timeout_seconds=timeout_seconds,
            )

            append_result = await self._append(JobEventType.JOB_CREATED, event)

            # Applied unconditionally -- see the class docstring's apply
            # contract: the fsync'd append is replayed on recovery either
            # way, so gating this on replication only splits live state
            # from recovered state.
            self._jobs_internal[job_id] = JobState.create(
                job_id=job_id,
                fence_token=fence_token,
                assigned_datacenters=assigned_datacenters,
                created_hlc=hlc,
                requestor_id=requestor_id,
                timeout_seconds=timeout_seconds,
            )
            self._publish_snapshot()
            commit_turn = self._commit_sequencer.reserve(job_id)

        result = await self._commit_in_turn(job_id, append_result, durability, commit_turn)
        return job_id, result

    async def accept_job(
        self,
        job_id: str,
        datacenter_id: str,
        worker_count: int,
        durability: DurabilityLevel = DurabilityLevel.LOCAL,
    ) -> CommitResult | None:
        self._require_satisfiable_durability(durability)

        async with self._lock:
            job = self._jobs_internal.get(job_id)
            if job is None:
                return None

            hlc = self._clock.now()

            event = JobAccepted(
                job_id=job_id,
                hlc=hlc,
                fence_token=job.fence_token,
                datacenter_id=datacenter_id,
                worker_count=worker_count,
            )

            append_result = await self._append(JobEventType.JOB_ACCEPTED, event)

            # Applied unconditionally -- see the class docstring's apply
            # contract.
            commit_turn = self._apply_live(
                job_id,
                job.with_accepted(datacenter_id=datacenter_id, hlc=hlc),
            )

        return await self._commit_in_turn(job_id, append_result, durability, commit_turn)

    async def reassign_datacenter(
        self,
        job_id: str,
        reassignment: DatacenterReassignment,
        durability: DurabilityLevel = DurabilityLevel.LOCAL,
    ) -> CommitResult | None:
        """Record that the job moved off a datacenter it lost while it ran
        there (AD-36 ``JobDatacenterReassigned``): a node recovering the
        job from this ledger awaits its results where it runs now."""
        self._require_satisfiable_durability(durability)

        async with self._lock:
            job = self._jobs_internal.get(job_id)
            if job is None:
                return None

            hlc = self._clock.now()
            append_result = await self._append(
                JobEventType.JOB_DATACENTER_REASSIGNED,
                JobDatacenterReassigned(
                    job_id=job_id,
                    hlc=hlc,
                    fence_token=job.fence_token,
                    lost_datacenter=reassignment.lost_datacenter,
                    replacement_datacenter=reassignment.replacement_datacenter,
                    completed_workflow_ids=reassignment.completed_workflow_ids,
                    total_completed=reassignment.total_completed,
                    total_failed=reassignment.total_failed,
                ),
            )

            # Applied unconditionally -- see the class docstring's apply
            # contract.
            commit_turn = self._apply_live(
                job_id,
                job.with_datacenter_reassigned(reassignment=reassignment, hlc=hlc),
            )

        return await self._commit_in_turn(job_id, append_result, durability, commit_turn)

    async def request_cancellation(
        self,
        job_id: str,
        reason: str,
        requestor_id: str,
        durability: DurabilityLevel = DurabilityLevel.LOCAL,
    ) -> CommitResult | None:
        self._require_satisfiable_durability(durability)

        async with self._lock:
            job = self._jobs_internal.get(job_id)
            if job is None:
                return None

            if job.is_cancelled:
                return None

            hlc = self._clock.now()

            event = JobCancellationRequested(
                job_id=job_id,
                hlc=hlc,
                fence_token=job.fence_token,
                reason=reason,
                requestor_id=requestor_id,
            )

            append_result = await self._append(
                JobEventType.JOB_CANCELLATION_REQUESTED, event
            )

            # Applied unconditionally -- see the class docstring's apply
            # contract.
            commit_turn = self._apply_live(
                job_id, job.with_cancellation_requested(hlc=hlc)
            )

        return await self._commit_in_turn(job_id, append_result, durability, commit_turn)

    @staticmethod
    def _is_absent_or_terminal(job: JobState | None) -> bool:
        """Whether no live job is held: absent, or already ended."""
        return job is None or job.is_terminal

    def _cannot_acknowledge_cancellation(self, job: JobState | None) -> bool:
        """Whether ``job`` cannot take a cancellation ack: absent, ended,
        or never asked to cancel."""
        return self._is_absent_or_terminal(job) or not job.is_cancelled

    async def report_progress(
        self,
        job_id: str,
        datacenter_id: str,
        completed_count: int,
        failed_count: int,
        durability: DurabilityLevel = DurabilityLevel.LOCAL,
    ) -> CommitResult | None:
        """Record a job's aggregated workflow tallies (AD-38, LOCAL tier).

        Called when the tallies change (a workflow reaches a terminal
        state), so WAL growth is bounded by workflow count, not by the
        raw progress-update rate. Unchanged tallies append nothing.
        """
        self._require_satisfiable_durability(durability)

        async with self._lock:
            job = self._jobs_internal.get(job_id)
            if self._is_absent_or_terminal(job):
                return None

            if (job.completed_count, job.failed_count) == (completed_count, failed_count):
                return None

            hlc = self._clock.now()
            append_result = await self._append(
                JobEventType.JOB_PROGRESS_REPORTED,
                JobProgressReported(
                    job_id=job_id,
                    hlc=hlc,
                    fence_token=job.fence_token,
                    datacenter_id=datacenter_id,
                    completed_count=completed_count,
                    failed_count=failed_count,
                ),
            )
            commit_turn = self._apply_live(
                job_id, job.with_progress(completed_count, failed_count, hlc)
            )

        return await self._commit_in_turn(job_id, append_result, durability, commit_turn)

    async def acknowledge_cancellation(
        self,
        job_id: str,
        datacenter_id: str,
        workflows_cancelled: int,
        durability: DurabilityLevel = DurabilityLevel.LOCAL,
    ) -> CommitResult | None:
        """Record that ``datacenter_id`` confirmed the job's cancellation.

        Only a job with a recorded cancellation request can be acked, and
        each datacenter acks once — repeats append nothing.
        """
        self._require_satisfiable_durability(durability)

        async with self._lock:
            job = self._jobs_internal.get(job_id)
            if self._cannot_acknowledge_cancellation(job):
                return None

            if datacenter_id in job.cancellation_acked_datacenters:
                return None

            hlc = self._clock.now()
            append_result = await self._append(
                JobEventType.JOB_CANCELLATION_ACKED,
                JobCancellationAcked(
                    job_id=job_id,
                    hlc=hlc,
                    fence_token=job.fence_token,
                    datacenter_id=datacenter_id,
                    workflows_cancelled=workflows_cancelled,
                ),
            )
            commit_turn = self._apply_live(
                job_id, job.with_cancellation_acked(datacenter_id, hlc)
            )

        return await self._commit_in_turn(job_id, append_result, durability, commit_turn)

    async def complete_job(
        self,
        job_id: str,
        final_status: str,
        total_completed: int,
        total_failed: int,
        duration_ms: int,
        durability: DurabilityLevel = DurabilityLevel.LOCAL,
    ) -> CommitResult | None:
        return await self._record_terminal(
            job_id,
            JobEventType.JOB_COMPLETED,
            final_status,
            total_completed,
            total_failed,
            durability,
            lambda job, hlc: JobCompleted(
                job_id=job_id,
                hlc=hlc,
                fence_token=job.fence_token,
                final_status=final_status,
                total_completed=total_completed,
                total_failed=total_failed,
                duration_ms=duration_ms,
            ),
        )

    async def fail_job(
        self,
        job_id: str,
        error_message: str,
        failed_datacenter: str,
        total_completed: int,
        total_failed: int,
        duration_ms: int,
        durability: DurabilityLevel = DurabilityLevel.LOCAL,
    ) -> CommitResult | None:
        """Record a job's terminal failure (AD-38 ``JobFailed``)."""
        return await self._record_terminal(
            job_id,
            JobEventType.JOB_FAILED,
            JOB_FAILED_STATUS,
            total_completed,
            total_failed,
            durability,
            lambda job, hlc: JobFailed(
                job_id=job_id,
                hlc=hlc,
                fence_token=job.fence_token,
                error_message=error_message,
                failed_datacenter=failed_datacenter,
                total_completed=total_completed,
                total_failed=total_failed,
                duration_ms=duration_ms,
            ),
        )

    async def time_out_job(
        self,
        job_id: str,
        timeout_type: str,
        total_completed: int,
        total_failed: int,
        duration_ms: int,
        durability: DurabilityLevel = DurabilityLevel.LOCAL,
    ) -> CommitResult | None:
        """Record a job's terminal timeout (AD-38 ``JobTimedOut``).

        ``last_progress_hlc`` comes from the ledger's own progress record,
        so the event states how long the job had been silent.
        """
        return await self._record_terminal(
            job_id,
            JobEventType.JOB_TIMED_OUT,
            JOB_TIMED_OUT_STATUS,
            total_completed,
            total_failed,
            durability,
            lambda job, hlc: JobTimedOut(
                job_id=job_id,
                hlc=hlc,
                fence_token=job.fence_token,
                timeout_type=timeout_type,
                last_progress_hlc=job.last_progress_hlc,
                total_completed=total_completed,
                total_failed=total_failed,
                duration_ms=duration_ms,
            ),
        )

    async def relinquish_job(
        self,
        job_id: str,
        held_by: str,
    ) -> CommitResult | None:
        """Close this ledger's record of a job another manager of its
        datacenter leads, or led and ended (AD-38 ``JobRelinquished``).

        The record stops claiming the job: a restart neither resumes nor
        fails it, and reads of it here defer to its leader. LOCAL only --
        replicated, it would read as the job's end to every member.
        """
        return await self._record_terminal(
            job_id,
            JobEventType.JOB_RELINQUISHED,
            JOB_RELINQUISHED_STATUS,
            None,
            None,
            DurabilityLevel.LOCAL,
            lambda job, hlc: JobRelinquished(
                job_id=job_id,
                hlc=hlc,
                fence_token=job.fence_token,
                held_by=held_by,
            ),
        )

    async def _record_terminal(
        self,
        job_id: str,
        event_type: JobEventType,
        final_status: str,
        total_completed: int | None,
        total_failed: int | None,
        durability: DurabilityLevel,
        build_event: Callable[[JobState, HLCTimestamp], msgspec.Struct],
    ) -> CommitResult | None:
        """Shared terminal transition for complete / fail / time out /
        relinquish.

        A job reaches exactly one terminal: the first terminal event wins
        and later ones (from any of the paths) append nothing. Totals of
        None keep the record's own tallies as they stand under the lock --
        what replay reads at the same point of the log.
        """
        self._require_satisfiable_durability(durability)

        async with self._lock:
            job = self._jobs_internal.get(job_id)
            if self._is_absent_or_terminal(job):
                return None

            hlc = self._clock.now()
            append_result = await self._append(event_type, build_event(job, hlc))

            # Applied unconditionally -- see the class docstring's apply
            # contract: the fsync'd append is replayed on recovery either
            # way, so gating this on replication only splits live state
            # from recovered state.
            terminal_job = self._terminal_state(job, final_status, total_completed, total_failed, hlc)

            # Reads flip to the terminal the instant it is durable:
            # the cache/snapshot transition must neither wait on nor
            # abort with the archive leg — the archive is the
            # COLD-READ copy, the WAL commit above is the truth.
            self._completed_cache.put(job_id, terminal_job)
            del self._jobs_internal[job_id]
            self._publish_snapshot()

            if await self._archive_job_isolated(terminal_job):
                await self._heal_pending_archive_jobs()

            commit_turn = self._commit_sequencer.reserve(job_id)

        return await self._commit_in_turn(job_id, append_result, durability, commit_turn)

    @staticmethod
    def _terminal_state(
        job: JobState,
        final_status: str,
        total_completed: int | None,
        total_failed: int | None,
        hlc: HLCTimestamp,
    ) -> JobState:
        """``job`` ended at ``final_status``; totals of None keep the
        record's own tallies."""
        return job.with_completion(
            final_status=final_status,
            total_completed=job.completed_count if total_completed is None else total_completed,
            total_failed=job.failed_count if total_failed is None else total_failed,
            hlc=hlc,
        )

    async def _append(
        self,
        event_type: JobEventType,
        event: msgspec.Struct,
    ) -> WALAppendResult:
        return await self._wal.append(
            event_type=event_type,
            payload=event.to_bytes(),
        )

    def _apply_live(
        self,
        job_id: str,
        job: JobState,
    ) -> tuple[asyncio.Future[None] | None, asyncio.Future[None]]:
        """Apply under the ledger lock and take the job's commit turn
        (the lock fixes the turn order to the append order)."""
        # Applied unconditionally -- the class docstring's apply contract.
        self._jobs_internal[job_id] = job
        self._publish_snapshot()
        return self._commit_sequencer.reserve(job_id)

    async def _commit_in_turn(
        self,
        job_id: str,
        append_result: WALAppendResult,
        durability: DurabilityLevel,
        commit_turn: tuple[asyncio.Future[None] | None, asyncio.Future[None]],
    ) -> CommitResult:
        """Commit outside the ledger lock, in the job's append order.

        The entry turns APPLIED only once its commit has finished (or
        been cancelled), never before, so a checkpoint cannot compact it
        mid-replication and it never stays PENDING forever.
        """
        predecessor, turn = commit_turn
        try:
            return await self._commit_sequencer.run(
                job_id,
                predecessor,
                turn,
                lambda: self._pipeline.commit(
                    append_result.entry,
                    durability,
                    backpressure=append_result.backpressure,
                ),
            )
        finally:
            await self._wal.mark_applied(append_result.entry.lsn)

    async def record_leadership_acquired(
        self,
        job_id: str,
        leader_id: str,
        previous_leader_id: str | None,
        lease_fence_token: int,
    ) -> CommitResult | None:
        """Record that this node took ``job_id`` over (AD-38
        ``JobLeadershipAcquired``): who leads it from here, from whom, under
        which lease fence. LOCAL, like ``relinquish_job`` on the side that
        gave a job up. Records nothing for a job this ledger does not hold
        or that has ended."""
        async with self._lock:
            job = self._jobs_internal.get(job_id)
            if self._is_absent_or_terminal(job):
                return None

            hlc = self._clock.now()
            append_result = await self._append(
                JobEventType.JOB_LEADERSHIP_ACQUIRED,
                JobLeadershipAcquired(
                    job_id=job_id,
                    hlc=hlc,
                    fence_token=job.fence_token,
                    leader_id=leader_id,
                    previous_leader_id=previous_leader_id or "",
                    lease_fence_token=lease_fence_token,
                ),
            )
            commit_turn = self._apply_live(
                job_id,
                job.with_leadership_acquired(leader_id=leader_id, hlc=hlc),
            )

        return await self._commit_in_turn(job_id, append_result, DurabilityLevel.LOCAL, commit_turn)

    async def adopt_replicated_history(
        self,
        job_id: str,
        history: Sequence[tuple[JobEventType, bytes]],
    ) -> int:
        """Record a taken-over job's replicated events in this ledger.

        The events already reached the job's consensus group (REGIONAL)
        through the previous leader; a member taking the job over appends
        them to its own WAL and applies them through the same applier as
        recovery, so its later events for the job -- its terminal above
        all -- land on a ledger that knows the job, and a restart replays
        the adopted history like any other. Nothing is re-proposed.

        Adopts nothing when this ledger already holds the job (live or
        completed). Returns the number of events adopted.
        """
        async with self._lock:
            if self._adopts_nothing(job_id, history):
                return 0

            for event_type, payload in history:
                append_result = await self._wal.append(
                    event_type=event_type, payload=payload
                )
                self._apply_entry(append_result.entry)
                await self._wal.mark_applied(append_result.entry.lsn)

            # A history that ends terminal settles exactly like recovery.
            await self._archive_terminal_jobs()
            self._publish_snapshot()
            return len(history)

    def _adopts_nothing(self, job_id: str, history: Sequence[tuple[JobEventType, bytes]]) -> bool:
        """Whether the history is empty or the job is already held, live
        or completed (the caller holds the lock)."""
        return (
            not history
            or job_id in self._jobs_internal
            or self._completed_cache.get(job_id) is not None
        )

    def get_job(self, job_id: str) -> JobState | None:
        """``job_id``'s state as this node's own ledger holds it: active,
        or terminal while it stays cached. Reads are not leveled -- a job
        led elsewhere is not here, and a linearizable read would need a
        ReadIndex through the job's group (AD-52), which is not built."""
        active_job = self._jobs_snapshot.get(job_id)
        if active_job is not None:
            return active_job

        return self._completed_cache.get(job_id)

    async def get_archived_job(self, job_id: str) -> JobState | None:
        cached_job = self._completed_cache.get(job_id)
        if cached_job is not None:
            return await self._serve_cached_terminal(job_id, cached_job)

        archived_job = await self._archive_store.read(job_id)
        if archived_job is not None:
            await self._cache_archived_job(job_id, archived_job)

        return archived_job

    async def _serve_cached_terminal(self, job_id: str, cached_job: JobState) -> JobState:
        """A cached terminal, healing its archive record when still owed."""
        if job_id in self._pending_archive_jobs:
            # Targeted heal: the caller is reading exactly the
            # terminal whose archive record is still owed.
            await self._archive_job_isolated(cached_job)
        return cached_job

    async def _cache_archived_job(self, job_id: str, archived_job: JobState) -> None:
        """Cache a terminal read back from the archive unless one is cached."""
        async with self._lock:
            if self._completed_cache.get(job_id) is None:
                self._completed_cache.put(job_id, archived_job)

    def get_all_jobs(self) -> Mapping[str, JobState]:
        return self._jobs_snapshot

    async def checkpoint(self) -> Path:
        async with self._lock:
            hlc = self._clock.now()

            job_states = self._active_job_states()
            # A terminal job whose archive record is still owed rides the
            # checkpoint: its WAL entries fall at or below this checkpoint,
            # so replay no longer reaches them, and recovery's terminal
            # sweep re-attempts the archive from here instead.
            job_states.update(
                (job_id, job.to_dict()) for job_id, job in self._pending_archive_jobs.items()
            )

            # Each watermark reports the tier it actually reached.
            # Stamping all three from the local fsync watermark made
            # the persisted checkpoint assert cross-region durability
            # for entries that never left the node -- the same lie the
            # commit pipeline stopped telling, one layer down and
            # written to disk.
            checkpoint = Checkpoint(
                local_lsn=self._wal.last_synced_lsn,
                regional_lsn=self._wal.last_regional_lsn,
                global_lsn=self._wal.last_global_lsn,
                hlc=hlc,
                job_states=job_states,
                created_at_ms=int(_DEFAULT_CLOCK.time() * 1000),
                next_fence_token=self._next_fence_token,
            )

            path = await self._checkpoint_manager.save(checkpoint)
            await self._wal.compact(up_to_lsn=checkpoint.local_lsn)
            # Retention runs with every checkpoint, not on its own
            # cadence: a checkpoint is the only thing that ADDS a file,
            # so pruning here is what keeps the directory bounded no
            # matter how often checkpoints are taken.
            await self._checkpoint_manager.cleanup(
                keep_count=self._checkpoint_retention_count
            )
            # Every retained checkpoint replays from its own LSN on; the
            # log keeps exactly what the oldest of them still needs.
            if (covered_through_lsn := await self._checkpoint_manager.lowest_retained_local_lsn()) >= 0:
                await self._wal.discard_through(covered_through_lsn)
            self._last_checkpoint_at = _DEFAULT_CLOCK.time()

            return path

    def _active_job_states(self) -> dict[str, dict]:
        """Every non-terminal job's state as a checkpoint holds it (the
        caller holds the lock)."""
        return {
            job_id: job.to_dict()
            for job_id, job in self._jobs_internal.items()
            if not job.is_terminal
        }

    def _checkpoint_is_due(self) -> bool:
        """The AD-38 compaction trigger: pending WAL entries past 2x
        active job state, or an un-checkpointed WAL older than the
        interval.

        Read without the lock deliberately. Both inputs are plain
        counters and the decision is a heuristic -- ``checkpoint()``
        re-acquires the lock and re-reads state before writing
        anything, so a racing append only shifts WHEN the next
        checkpoint happens, never what it contains.
        """
        pending_entries = self._wal.pending_count
        if pending_entries == 0:
            # Nothing to compact. An idle node never writes a
            # checkpoint it would immediately re-derive on recovery.
            return False

        entry_threshold = max(
            self._min_checkpoint_wal_entries,
            self._checkpoint_wal_ratio * len(self._jobs_internal),
        )
        if pending_entries >= entry_threshold:
            return True

        elapsed = _DEFAULT_CLOCK.time() - self._last_checkpoint_at
        return elapsed >= self._checkpoint_max_interval_seconds

    async def maybe_checkpoint(self) -> Path | None:
        """Checkpoint if one is due, CONTAINED. Returns the checkpoint
        path when one was written, else ``None``.

        Designed to be called from an owning node's existing periodic
        loop: the not-due path reads two counters and returns, so the
        cadence costs a cheap call per tick.

        A checkpoint is pure optimization -- it bounds pending-entry
        memory and shortens replay, and every entry it would have
        compacted is still durable in the WAL. So a failing disk must
        not take down the caller's loop the way an unprotected raise
        would; the failure is logged loudly and the next tick retries
        against the (larger) WAL. Only ``OSError`` is contained, since
        that is the environmental failure this isolates -- anything
        else is a defect and still propagates.
        """
        if not self._checkpoint_is_due():
            return None
        return await self._checkpoint_contained()

    @property
    def storage_health(self) -> StorageHealth:
        """The node storage health this ledger records into -- shared
        with the node's other durable stores on the same device."""
        return self._storage_health

    @property
    def storage_writable(self) -> bool:
        """Whether the node's storage last proved writable."""
        return self._storage_health.writable

    async def _checkpoint_contained(self) -> Path | None:
        """Checkpoint now with storage failures CONTAINED (see
        ``maybe_checkpoint``)."""
        pending_before = self._wal.pending_count

        try:
            path = await self.checkpoint()
        except OSError as checkpoint_error:
            await self._log_checkpoint_error(
                type(checkpoint_error).__name__, pending_before
            )
            return None

        await self._log_checkpoint_taken(
            path, pending_before - self._wal.pending_count
        )
        return path

    async def _log_checkpoint_taken(self, path: Path, compacted: int) -> None:
        if self._logger is not None:
            await self._logger.log(
                CheckpointInfo(
                    message=(
                        f"ledger checkpoint written at LSN "
                        f"{self._wal.last_synced_lsn}; compacted "
                        f"{compacted} WAL entries"
                    ),
                    path=str(path),
                    checkpoint_lsn=self._wal.last_synced_lsn,
                    compacted_entries=compacted,
                    active_jobs=len(self._jobs_internal),
                )
            )

    async def _log_checkpoint_error(
        self, error_type: str, pending_entries: int
    ) -> None:
        if self._logger is not None:
            await self._logger.log(
                CheckpointError(
                    message=(
                        f"ledger checkpoint not written ({error_type}); "
                        f"{pending_entries} WAL entries stay pending and "
                        "recovery replays them -- durability is unaffected, "
                        "replay cost and memory are not"
                    ),
                    path=str(self._checkpoint_manager.checkpoint_dir),
                    error_type=error_type,
                    pending_entries=pending_entries,
                )
            )

    async def close(self) -> None:
        await self._wal.close()

    @property
    def job_count(self) -> int:
        return len(self._jobs_snapshot)

    @property
    def active_job_count(self) -> int:
        return len(self._jobs_internal)

    @property
    def cached_completed_count(self) -> int:
        return len(self._completed_cache)

    @property
    def pending_wal_entries(self) -> int:
        return self._wal.pending_count

    @property
    def archive_store(self) -> JobArchiveStore:
        return self._archive_store
