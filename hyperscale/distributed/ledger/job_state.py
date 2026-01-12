from __future__ import annotations

from typing import Any

import msgspec

from hyperscale.logging.lsn import LSN


class JobState(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    status: str
    fence_token: int
    assigned_datacenters: tuple[str, ...]
    accepted_datacenters: frozenset[str]
    cancelled: bool
    completed_count: int
    failed_count: int
    created_hlc: LSN
    last_hlc: LSN

    @classmethod
    def create(
        cls,
        job_id: str,
        fence_token: int,
        assigned_datacenters: tuple[str, ...],
        created_hlc: LSN,
    ) -> JobState:
        return cls(
            job_id=job_id,
            status="pending",
            fence_token=fence_token,
            assigned_datacenters=assigned_datacenters,
            accepted_datacenters=frozenset(),
            cancelled=False,
            completed_count=0,
            failed_count=0,
            created_hlc=created_hlc,
            last_hlc=created_hlc,
        )

    def with_accepted(self, datacenter_id: str, hlc: LSN) -> JobState:
        return JobState(
            job_id=self.job_id,
            status="running",
            fence_token=self.fence_token,
            assigned_datacenters=self.assigned_datacenters,
            accepted_datacenters=self.accepted_datacenters | {datacenter_id},
            cancelled=self.cancelled,
            completed_count=self.completed_count,
            failed_count=self.failed_count,
            created_hlc=self.created_hlc,
            last_hlc=hlc,
        )

    def with_cancellation_requested(self, hlc: LSN) -> JobState:
        return JobState(
            job_id=self.job_id,
            status="cancelling",
            fence_token=self.fence_token,
            assigned_datacenters=self.assigned_datacenters,
            accepted_datacenters=self.accepted_datacenters,
            cancelled=True,
            completed_count=self.completed_count,
            failed_count=self.failed_count,
            created_hlc=self.created_hlc,
            last_hlc=hlc,
        )

    def with_completion(
        self,
        final_status: str,
        total_completed: int,
        total_failed: int,
        hlc: LSN,
    ) -> JobState:
        return JobState(
            job_id=self.job_id,
            status=final_status,
            fence_token=self.fence_token,
            assigned_datacenters=self.assigned_datacenters,
            accepted_datacenters=self.accepted_datacenters,
            cancelled=self.cancelled,
            completed_count=total_completed,
            failed_count=total_failed,
            created_hlc=self.created_hlc,
            last_hlc=hlc,
        )

    @property
    def is_cancelled(self) -> bool:
        return self.cancelled

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "fence_token": self.fence_token,
            "assigned_datacenters": list(self.assigned_datacenters),
            "accepted_datacenters": list(self.accepted_datacenters),
            "cancelled": self.cancelled,
            "completed_count": self.completed_count,
            "failed_count": self.failed_count,
        }

    @classmethod
    def from_dict(cls, job_id: str, data: dict[str, Any]) -> JobState:
        return cls(
            job_id=job_id,
            status=data.get("status", "pending"),
            fence_token=data.get("fence_token", 0),
            assigned_datacenters=tuple(data.get("assigned_datacenters", [])),
            accepted_datacenters=frozenset(data.get("accepted_datacenters", [])),
            cancelled=data.get("cancelled", False),
            completed_count=data.get("completed_count", 0),
            failed_count=data.get("failed_count", 0),
            created_hlc=LSN(0, 0, 0, 0),
            last_hlc=LSN(0, 0, 0, 0),
        )
