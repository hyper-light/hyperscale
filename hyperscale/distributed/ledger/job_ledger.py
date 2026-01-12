from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Awaitable

from hyperscale.logging.lsn import LSN, HybridLamportClock

from .consistency_level import ConsistencyLevel
from .durability_level import DurabilityLevel
from .events.event_type import JobEventType
from .events.job_event import (
    JobCreated,
    JobAccepted,
    JobCancellationRequested,
    JobCancellationAcked,
    JobCompleted,
    JobFailed,
    JobTimedOut,
    JobEventUnion,
)
from .job_id import JobIdGenerator
from .wal.node_wal import NodeWAL
from .wal.wal_entry import WALEntry
from .pipeline.commit_pipeline import CommitPipeline, CommitResult
from .checkpoint.checkpoint import Checkpoint, CheckpointManager

if TYPE_CHECKING:
    pass


class JobState:
    __slots__ = (
        "_job_id",
        "_status",
        "_fence_token",
        "_assigned_datacenters",
        "_accepted_datacenters",
        "_cancelled",
        "_completed_count",
        "_failed_count",
        "_created_hlc",
        "_last_hlc",
    )

    def __init__(
        self,
        job_id: str,
        fence_token: int,
        assigned_datacenters: tuple[str, ...],
        created_hlc: LSN,
    ) -> None:
        self._job_id = job_id
        self._status = "pending"
        self._fence_token = fence_token
        self._assigned_datacenters = assigned_datacenters
        self._accepted_datacenters: set[str] = set()
        self._cancelled = False
        self._completed_count = 0
        self._failed_count = 0
        self._created_hlc = created_hlc
        self._last_hlc = created_hlc

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def status(self) -> str:
        return self._status

    @property
    def fence_token(self) -> int:
        return self._fence_token

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    @property
    def completed_count(self) -> int:
        return self._completed_count

    @property
    def failed_count(self) -> int:
        return self._failed_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self._job_id,
            "status": self._status,
            "fence_token": self._fence_token,
            "assigned_datacenters": list(self._assigned_datacenters),
            "accepted_datacenters": list(self._accepted_datacenters),
            "cancelled": self._cancelled,
            "completed_count": self._completed_count,
            "failed_count": self._failed_count,
        }


class JobLedger:
    """
    Global job ledger with event sourcing and tiered durability.

    Maintains authoritative job state with:
    - Per-node WAL for crash recovery
    - Tiered commit pipeline (LOCAL/REGIONAL/GLOBAL)
    - Event sourcing for audit trail
    - Checkpoint/compaction for efficiency
    """

    __slots__ = (
        "_clock",
        "_wal",
        "_pipeline",
        "_checkpoint_manager",
        "_job_id_generator",
        "_jobs",
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
    ) -> None:
        self._clock = clock
        self._wal = wal
        self._pipeline = pipeline
        self._checkpoint_manager = checkpoint_manager
        self._job_id_generator = job_id_generator
        self._jobs: dict[str, JobState] = {}
        self._lock = asyncio.Lock()
        self._next_fence_token = 1

    @classmethod
    async def open(
        cls,
        wal_path: Path,
        checkpoint_dir: Path,
        region_code: str,
        gate_id: str,
        node_id: int,
        regional_replicator: Callable[[WALEntry], Awaitable[bool]] | None = None,
        global_replicator: Callable[[WALEntry], Awaitable[bool]] | None = None,
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
        )

        await ledger._recover()
        return ledger

    async def _recover(self) -> None:
        checkpoint = self._checkpoint_manager.latest

        if checkpoint is not None:
            for job_id, job_dict in checkpoint.job_states.items():
                self._jobs[job_id] = self._job_state_from_dict(job_id, job_dict)

            await self._clock.witness(checkpoint.hlc)
            start_lsn = checkpoint.local_lsn + 1
        else:
            start_lsn = 0

        async for entry in self._wal.iter_from(start_lsn):
            await self._apply_entry(entry)

    def _job_state_from_dict(self, job_id: str, data: dict[str, Any]) -> JobState:
        state = JobState(
            job_id=job_id,
            fence_token=data.get("fence_token", 0),
            assigned_datacenters=tuple(data.get("assigned_datacenters", [])),
            created_hlc=LSN(0, 0, 0, 0),
        )
        state._status = data.get("status", "pending")
        state._cancelled = data.get("cancelled", False)
        state._completed_count = data.get("completed_count", 0)
        state._failed_count = data.get("failed_count", 0)
        state._accepted_datacenters = set(data.get("accepted_datacenters", []))
        return state

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

            entry = await self._wal.append(
                event_type=JobEventType.JOB_CREATED,
                payload=event.to_bytes(),
                fsync=True,
            )

            result = await self._pipeline.commit(entry, durability)

            if result.success:
                self._jobs[job_id] = JobState(
                    job_id=job_id,
                    fence_token=fence_token,
                    assigned_datacenters=assigned_datacenters,
                    created_hlc=hlc,
                )
                await self._wal.mark_applied(entry.lsn)

            return job_id, result

    async def accept_job(
        self,
        job_id: str,
        datacenter_id: str,
        worker_count: int,
        durability: DurabilityLevel = DurabilityLevel.REGIONAL,
    ) -> CommitResult | None:
        async with self._lock:
            job = self._jobs.get(job_id)
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

            entry = await self._wal.append(
                event_type=JobEventType.JOB_ACCEPTED,
                payload=event.to_bytes(),
                fsync=True,
            )

            result = await self._pipeline.commit(entry, durability)

            if result.success:
                job._accepted_datacenters.add(datacenter_id)
                job._status = "running"
                job._last_hlc = hlc
                await self._wal.mark_applied(entry.lsn)

            return result

    async def request_cancellation(
        self,
        job_id: str,
        reason: str,
        requestor_id: str,
        durability: DurabilityLevel = DurabilityLevel.GLOBAL,
    ) -> CommitResult | None:
        async with self._lock:
            job = self._jobs.get(job_id)
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

            entry = await self._wal.append(
                event_type=JobEventType.JOB_CANCELLATION_REQUESTED,
                payload=event.to_bytes(),
                fsync=True,
            )

            result = await self._pipeline.commit(entry, durability)

            if result.success:
                job._cancelled = True
                job._status = "cancelling"
                job._last_hlc = hlc
                await self._wal.mark_applied(entry.lsn)

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
            job = self._jobs.get(job_id)
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

            entry = await self._wal.append(
                event_type=JobEventType.JOB_COMPLETED,
                payload=event.to_bytes(),
                fsync=True,
            )

            result = await self._pipeline.commit(entry, durability)

            if result.success:
                job._status = final_status
                job._completed_count = total_completed
                job._failed_count = total_failed
                job._last_hlc = hlc
                await self._wal.mark_applied(entry.lsn)

            return result

    async def _apply_entry(self, entry: WALEntry) -> None:
        if entry.event_type == JobEventType.JOB_CREATED:
            event = JobCreated.from_bytes(entry.payload)
            self._jobs[event.job_id] = JobState(
                job_id=event.job_id,
                fence_token=event.fence_token,
                assigned_datacenters=event.assigned_datacenters,
                created_hlc=event.hlc,
            )

            if event.fence_token >= self._next_fence_token:
                self._next_fence_token = event.fence_token + 1

        elif entry.event_type == JobEventType.JOB_ACCEPTED:
            event = JobAccepted.from_bytes(entry.payload)
            job = self._jobs.get(event.job_id)
            if job:
                job._accepted_datacenters.add(event.datacenter_id)
                job._status = "running"

        elif entry.event_type == JobEventType.JOB_CANCELLATION_REQUESTED:
            event = JobCancellationRequested.from_bytes(entry.payload)
            job = self._jobs.get(event.job_id)
            if job:
                job._cancelled = True
                job._status = "cancelling"

        elif entry.event_type == JobEventType.JOB_COMPLETED:
            event = JobCompleted.from_bytes(entry.payload)
            job = self._jobs.get(event.job_id)
            if job:
                job._status = event.final_status
                job._completed_count = event.total_completed
                job._failed_count = event.total_failed

    def get_job(
        self,
        job_id: str,
        consistency: ConsistencyLevel = ConsistencyLevel.SESSION,
    ) -> JobState | None:
        return self._jobs.get(job_id)

    def get_all_jobs(self) -> dict[str, JobState]:
        return dict(self._jobs)

    async def checkpoint(self) -> Path:
        async with self._lock:
            hlc = await self._clock.generate()

            job_states = {job_id: job.to_dict() for job_id, job in self._jobs.items()}

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
        return len(self._jobs)

    @property
    def pending_wal_entries(self) -> int:
        return self._wal.pending_count
