from __future__ import annotations

import asyncio
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Callable, Awaitable, Mapping

from hyperscale.logging.hyperscale_logging_models import ArchiveError, ArchiveInfo
from hyperscale.logging.lsn import HybridLamportClock

from .archive.job_archive_store import JobArchiveStore

from hyperscale.distributed.runtime import Clock, Filesystem, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

if TYPE_CHECKING:
    from hyperscale.logging import Logger
from .cache.bounded_lru_cache import BoundedLRUCache
from .consistency_level import ConsistencyLevel
from .durability_level import DurabilityLevel
from .events.event_type import JobEventType
from .events.job_event import (
    JobCreated,
    JobAccepted,
    JobCancellationRequested,
    JobCompleted,
)
from .job_id import JobIdGenerator
from .job_state import JobState
from .wal.node_wal import NodeWAL
from .wal.wal_entry import WALEntry
from .pipeline.commit_pipeline import CommitPipeline, CommitResult
from .checkpoint.checkpoint import Checkpoint, CheckpointManager

DEFAULT_COMPLETED_CACHE_SIZE = 10000

# Terminal jobs whose archive record is still owed (the WAL terminal is
# durable; only the cold-read copy is missing) are parked for healing.
# The park is bounded: past this, the OLDEST owed record is dropped with
# an error log — its durable truth stays in the WAL, exactly the
# pre-isolation status quo for every archive failure.
PENDING_ARCHIVE_LIMIT = 1024


class JobLedger:
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
        "_logger",
        "_pending_archive_jobs",
    )

    def __init__(
        self,
        clock: HybridLamportClock,
        wal: NodeWAL,
        pipeline: CommitPipeline,
        checkpoint_manager: CheckpointManager,
        job_id_generator: JobIdGenerator,
        archive_store: JobArchiveStore,
        completed_cache_size: int = DEFAULT_COMPLETED_CACHE_SIZE,
        logger: Logger | None = None,
    ) -> None:
        self._clock = clock
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
        self._pending_archive_jobs: dict[str, JobState] = {}

    @classmethod
    async def open(
        cls,
        wal_path: Path,
        checkpoint_dir: Path,
        archive_dir: Path,
        region_code: str,
        gate_id: str,
        node_id: int,
        regional_replicator: Callable[[WALEntry], Awaitable[bool]] | None = None,
        global_replicator: Callable[[WALEntry], Awaitable[bool]] | None = None,
        completed_cache_size: int = DEFAULT_COMPLETED_CACHE_SIZE,
        logger: Logger | None = None,
        clock: HybridLamportClock | None = None,
        filesystem: Filesystem | None = None,
    ) -> JobLedger:
        """``clock`` lets the owning node share its HLC so ledger
        events are causally ordered against its other WAL/Raft writes;
        ``filesystem`` is the Phase 7 storage seam (None binds each
        component's module default)."""
        if clock is None:
            clock = HybridLamportClock(node_id=node_id)
        wal = await NodeWAL.open(
            path=wal_path, clock=clock, logger=logger, filesystem=filesystem
        )

        pipeline = CommitPipeline(
            wal=wal,
            regional_replicator=regional_replicator,
            global_replicator=global_replicator,
            logger=logger,
        )

        checkpoint_manager = CheckpointManager(
            checkpoint_dir=checkpoint_dir, filesystem=filesystem
        )
        await checkpoint_manager.initialize()

        archive_store = JobArchiveStore(
            archive_dir=archive_dir, filesystem=filesystem
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
            completed_cache_size=completed_cache_size,
            logger=logger,
        )

        await ledger._recover()
        return ledger

    async def _recover(self) -> None:
        checkpoint = self._checkpoint_manager.latest

        if checkpoint is not None:
            for job_id, job_dict in checkpoint.job_states.items():
                self._jobs_internal[job_id] = JobState.from_dict(job_id, job_dict)

            await self._clock.witness(checkpoint.hlc)
            start_lsn = checkpoint.local_lsn + 1
        else:
            start_lsn = 0

        async for entry in self._wal.iter_from(start_lsn):
            self._apply_entry(entry)

        await self._archive_terminal_jobs()
        self._publish_snapshot()

    async def _archive_terminal_jobs(self) -> None:
        """Recovery's terminal sweep. Archive writes are ISOLATED here
        for the same reason as ``complete_job``'s: this runs inside
        ``open()`` at node boot, and an unprotected ENOSPC would wedge
        the owning server's ``start()`` on a full disk — the cache is
        the truthful read surface either way (rebuilt from the WAL),
        the archive record stays owed until the disk heals."""
        terminal_job_ids: list[str] = []

        for job_id, job_state in self._jobs_internal.items():
            if job_state.is_terminal:
                await self._archive_job_isolated(job_state)
                self._completed_cache.put(job_id, job_state)
                terminal_job_ids.append(job_id)

        for job_id in terminal_job_ids:
            del self._jobs_internal[job_id]

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
        the WAL commit that precedes every call is the durable truth.
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
            return False

        if self._pending_archive_jobs.pop(terminal_job.job_id, None) is not None:
            await self._log_archive_healed(terminal_job.job_id)
        return True

    async def _heal_pending_archive_jobs(self) -> None:
        """Retry every parked archive record, oldest first — called
        only on fresh evidence the archive disk writes again (a
        just-landed record). Stops at the first failure: the disk is
        still bad and the rest would only churn."""
        for job_id in list(self._pending_archive_jobs):
            parked_job = self._pending_archive_jobs.get(job_id)
            if parked_job is None:
                continue
            if not await self._archive_job_isolated(parked_job):
                return

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
        if entry.event_type == JobEventType.JOB_CREATED:
            event = JobCreated.from_bytes(entry.payload)
            self._jobs_internal[event.job_id] = JobState.create(
                job_id=event.job_id,
                fence_token=event.fence_token,
                assigned_datacenters=event.assigned_datacenters,
                created_hlc=event.hlc,
                requestor_id=event.requestor_id,
            )

            if event.fence_token >= self._next_fence_token:
                self._next_fence_token = event.fence_token + 1

        elif entry.event_type == JobEventType.JOB_ACCEPTED:
            event = JobAccepted.from_bytes(entry.payload)
            job = self._jobs_internal.get(event.job_id)
            if job:
                self._jobs_internal[event.job_id] = job.with_accepted(
                    datacenter_id=event.datacenter_id,
                    hlc=event.hlc,
                )

        elif entry.event_type == JobEventType.JOB_CANCELLATION_REQUESTED:
            event = JobCancellationRequested.from_bytes(entry.payload)
            job = self._jobs_internal.get(event.job_id)
            if job:
                self._jobs_internal[event.job_id] = job.with_cancellation_requested(
                    hlc=event.hlc,
                )

        elif entry.event_type == JobEventType.JOB_COMPLETED:
            event = JobCompleted.from_bytes(entry.payload)
            job = self._jobs_internal.get(event.job_id)
            if job:
                self._jobs_internal[event.job_id] = job.with_completion(
                    final_status=event.final_status,
                    total_completed=event.total_completed,
                    total_failed=event.total_failed,
                    hlc=event.hlc,
                )

    async def create_job(
        self,
        spec_hash: bytes,
        assigned_datacenters: tuple[str, ...],
        requestor_id: str,
        durability: DurabilityLevel = DurabilityLevel.GLOBAL,
        job_id: str | None = None,
    ) -> tuple[str, CommitResult]:
        """``job_id`` records an externally-generated id (the client
        generates job ids at submission); None generates one here (the
        gate path)."""
        async with self._lock:
            if job_id is None:
                job_id = await self._job_id_generator.generate()
            fence_token = self._next_fence_token
            self._next_fence_token += 1

            hlc = await self._clock.generate()

            event = JobCreated(
                job_id=job_id,
                hlc=hlc,
                fence_token=fence_token,
                spec_hash=spec_hash,
                assigned_datacenters=assigned_datacenters,
                requestor_id=requestor_id,
            )

            append_result = await self._wal.append(
                event_type=JobEventType.JOB_CREATED,
                payload=event.to_bytes(),
            )

            result = await self._pipeline.commit(
                append_result.entry,
                durability,
                backpressure=append_result.backpressure,
            )

            if result.success:
                self._jobs_internal[job_id] = JobState.create(
                    job_id=job_id,
                    fence_token=fence_token,
                    assigned_datacenters=assigned_datacenters,
                    created_hlc=hlc,
                    requestor_id=requestor_id,
                )
                self._publish_snapshot()
                await self._wal.mark_applied(append_result.entry.lsn)

            return job_id, result

    async def accept_job(
        self,
        job_id: str,
        datacenter_id: str,
        worker_count: int,
        durability: DurabilityLevel = DurabilityLevel.REGIONAL,
    ) -> CommitResult | None:
        async with self._lock:
            job = self._jobs_internal.get(job_id)
            if job is None:
                return None

            hlc = await self._clock.generate()

            event = JobAccepted(
                job_id=job_id,
                hlc=hlc,
                fence_token=job.fence_token,
                datacenter_id=datacenter_id,
                worker_count=worker_count,
            )

            append_result = await self._wal.append(
                event_type=JobEventType.JOB_ACCEPTED,
                payload=event.to_bytes(),
            )

            result = await self._pipeline.commit(
                append_result.entry,
                durability,
                backpressure=append_result.backpressure,
            )

            if result.success:
                self._jobs_internal[job_id] = job.with_accepted(
                    datacenter_id=datacenter_id,
                    hlc=hlc,
                )
                self._publish_snapshot()
                await self._wal.mark_applied(append_result.entry.lsn)

            return result

    async def request_cancellation(
        self,
        job_id: str,
        reason: str,
        requestor_id: str,
        durability: DurabilityLevel = DurabilityLevel.GLOBAL,
    ) -> CommitResult | None:
        async with self._lock:
            job = self._jobs_internal.get(job_id)
            if job is None:
                return None

            if job.is_cancelled:
                return None

            hlc = await self._clock.generate()

            event = JobCancellationRequested(
                job_id=job_id,
                hlc=hlc,
                fence_token=job.fence_token,
                reason=reason,
                requestor_id=requestor_id,
            )

            append_result = await self._wal.append(
                event_type=JobEventType.JOB_CANCELLATION_REQUESTED,
                payload=event.to_bytes(),
            )

            result = await self._pipeline.commit(
                append_result.entry,
                durability,
                backpressure=append_result.backpressure,
            )

            if result.success:
                self._jobs_internal[job_id] = job.with_cancellation_requested(hlc=hlc)
                self._publish_snapshot()
                await self._wal.mark_applied(append_result.entry.lsn)

            return result

    async def complete_job(
        self,
        job_id: str,
        final_status: str,
        total_completed: int,
        total_failed: int,
        duration_ms: int,
        durability: DurabilityLevel = DurabilityLevel.GLOBAL,
    ) -> CommitResult | None:
        async with self._lock:
            job = self._jobs_internal.get(job_id)
            if job is None:
                return None

            if job.is_terminal:
                return None

            hlc = await self._clock.generate()

            event = JobCompleted(
                job_id=job_id,
                hlc=hlc,
                fence_token=job.fence_token,
                final_status=final_status,
                total_completed=total_completed,
                total_failed=total_failed,
                duration_ms=duration_ms,
            )

            append_result = await self._wal.append(
                event_type=JobEventType.JOB_COMPLETED,
                payload=event.to_bytes(),
            )

            result = await self._pipeline.commit(
                append_result.entry,
                durability,
                backpressure=append_result.backpressure,
            )

            if result.success:
                completed_job = job.with_completion(
                    final_status=final_status,
                    total_completed=total_completed,
                    total_failed=total_failed,
                    hlc=hlc,
                )

                # Reads flip to the terminal the instant it is durable:
                # the cache/snapshot transition must neither wait on nor
                # abort with the archive leg — the archive is the
                # COLD-READ copy, the WAL commit above is the truth.
                self._completed_cache.put(job_id, completed_job)
                del self._jobs_internal[job_id]
                self._publish_snapshot()

                if await self._archive_job_isolated(completed_job):
                    await self._heal_pending_archive_jobs()

                await self._wal.mark_applied(append_result.entry.lsn)

            return result

    def get_job(
        self,
        job_id: str,
        consistency: ConsistencyLevel = ConsistencyLevel.SESSION,
    ) -> JobState | None:
        active_job = self._jobs_snapshot.get(job_id)
        if active_job is not None:
            return active_job

        return self._completed_cache.get(job_id)

    async def get_archived_job(self, job_id: str) -> JobState | None:
        cached_job = self._completed_cache.get(job_id)
        if cached_job is not None:
            if job_id in self._pending_archive_jobs:
                # Targeted heal: the caller is reading exactly the
                # terminal whose archive record is still owed.
                await self._archive_job_isolated(cached_job)
            return cached_job

        archived_job = await self._archive_store.read(job_id)
        if archived_job is not None:
            async with self._lock:
                if self._completed_cache.get(job_id) is None:
                    self._completed_cache.put(job_id, archived_job)

        return archived_job

    def get_all_jobs(self) -> Mapping[str, JobState]:
        return self._jobs_snapshot

    async def checkpoint(self) -> Path:
        async with self._lock:
            hlc = await self._clock.generate()

            job_states = {
                job_id: job.to_dict()
                for job_id, job in self._jobs_internal.items()
                if not job.is_terminal
            }

            checkpoint = Checkpoint(
                local_lsn=self._wal.last_synced_lsn,
                regional_lsn=self._wal.last_synced_lsn,
                global_lsn=self._wal.last_synced_lsn,
                hlc=hlc,
                job_states=job_states,
                created_at_ms=int(_DEFAULT_CLOCK.time() * 1000),
            )

            path = await self._checkpoint_manager.save(checkpoint)
            await self._wal.compact(up_to_lsn=checkpoint.local_lsn)

            return path

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
