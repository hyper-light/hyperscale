from __future__ import annotations

import asyncio
import time
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Awaitable, Mapping

from hyperscale.logging.lsn import HybridLamportClock

from .archive.job_archive_store import JobArchiveStore
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
    ) -> None:
        self._clock = clock
        self._wal = wal
        self._pipeline = pipeline
        self._checkpoint_manager = checkpoint_manager
        self._job_id_generator = job_id_generator
        self._archive_store = archive_store
        self._completed_cache: BoundedLRUCache[str, JobState] = BoundedLRUCache(
            max_size=completed_cache_size
        )
        self._jobs_internal: dict[str, JobState] = {}
        self._jobs_snapshot: Mapping[str, JobState] = MappingProxyType({})
        self._lock = asyncio.Lock()
        self._next_fence_token = 1

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
    ) -> JobLedger:
        clock = HybridLamportClock(node_id=node_id)
        wal = await NodeWAL.open(path=wal_path, clock=clock)

        pipeline = CommitPipeline(
            wal=wal,
            regional_replicator=regional_replicator,
            global_replicator=global_replicator,
        )

        checkpoint_manager = CheckpointManager(checkpoint_dir=checkpoint_dir)
        await checkpoint_manager.initialize()

        archive_store = JobArchiveStore(archive_dir=archive_dir)
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
        terminal_job_ids: list[str] = []

        for job_id, job_state in self._jobs_internal.items():
            if job_state.is_terminal:
                await self._archive_store.write_if_absent(job_state)
                self._completed_cache.put(job_id, job_state)
                terminal_job_ids.append(job_id)

        for job_id in terminal_job_ids:
            del self._jobs_internal[job_id]

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
    ) -> tuple[str, CommitResult]:
        async with self._lock:
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

            result = await self._pipeline.commit(append_result.entry, durability)

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

            result = await self._pipeline.commit(append_result.entry, durability)

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

            result = await self._pipeline.commit(append_result.entry, durability)

            if result.success:
                completed_job = job.with_completion(
                    final_status=final_status,
                    total_completed=total_completed,
                    total_failed=total_failed,
                    hlc=hlc,
                )

                await self._archive_store.write_if_absent(completed_job)
                self._completed_cache.put(job_id, completed_job)
                del self._jobs_internal[job_id]

                self._publish_snapshot()
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
            return cached_job

        archived_job = await self._archive_store.read(job_id)
        if archived_job is not None:
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
                created_at_ms=int(time.time() * 1000),
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
