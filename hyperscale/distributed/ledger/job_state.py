from __future__ import annotations

import msgspec

from hyperscale.distributed.hlc.hlc_timestamp import HLCTimestamp

from .datacenter_reassignment import DatacenterReassignment
from .job_state_record import JobStateRecord

# Both timeout spellings are live vocabulary: managers write
# JobStatus.TIMEOUT.value ("timeout"), the gate timeout tracker records
# "timed_out" — the gate-side normalizers accept both and so must we.
# "relinquished" ends a record, not a job: its manager no longer leads it.
TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"completed", "failed", "cancelled", "timeout", "timed_out", "relinquished"}
)


class JobState(msgspec.Struct, frozen=True, array_like=True):
    job_id: str
    status: str
    fence_token: int
    assigned_datacenters: tuple[str, ...]
    accepted_datacenters: frozenset[str]
    cancelled: bool
    completed_count: int
    failed_count: int
    created_hlc: HLCTimestamp
    last_hlc: HLCTimestamp
    # "host:port" of the submitter's callback listener — recorded so a
    # RESTARTED manager can tell the client what happened to a
    # recovered job. Trailing + defaulted: old array_like records and
    # checkpoints decode cleanly.
    requestor_id: str = ""
    # Job-level timeout budget (seconds) — lets a restarted accepting
    # node resume AD-34 tracking with the REMAINING budget (elapsed
    # derived from created_hlc.wall_ms). Same trailing-defaulted
    # compatibility contract as requestor_id.
    timeout_seconds: float = 0.0
    # HLC of the most recent JobProgressReported — AD-38's JobTimedOut
    # carries it so a timeout records how long the job had been silent.
    last_progress_hlc: HLCTimestamp | None = None
    # Datacenters that confirmed cancellation (JobCancellationAcked).
    cancellation_acked_datacenters: frozenset[str] = frozenset()
    # Datacenters the job lost mid-run and moved off (AD-36
    # JobDatacenterReassigned), in the order they were lost.
    datacenter_reassignments: tuple[DatacenterReassignment, ...] = ()
    # The node that took the job over last (JobLeadershipAcquired); empty
    # while it is led by the node that created it.
    leader_id: str = ""

    @classmethod
    def create(
        cls,
        job_id: str,
        fence_token: int,
        assigned_datacenters: tuple[str, ...],
        created_hlc: HLCTimestamp,
        requestor_id: str = "",
        timeout_seconds: float = 0.0,
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
            requestor_id=requestor_id,
            timeout_seconds=timeout_seconds,
        )

    # Transitions copy the record with ``msgspec.structs.replace`` so every
    # field they do not name carries forward; hand-listing fields silently
    # reset any field added later.
    def with_accepted(self, datacenter_id: str, hlc: HLCTimestamp) -> JobState:
        return msgspec.structs.replace(
            self,
            status="running",
            accepted_datacenters=self.accepted_datacenters | {datacenter_id},
            last_hlc=hlc,
        )

    def with_leadership_acquired(self, leader_id: str, hlc: HLCTimestamp) -> JobState:
        return msgspec.structs.replace(self, leader_id=leader_id, last_hlc=hlc)

    def with_cancellation_requested(self, hlc: HLCTimestamp) -> JobState:
        return msgspec.structs.replace(
            self,
            status="cancelling",
            cancelled=True,
            last_hlc=hlc,
        )

    def with_completion(
        self,
        final_status: str,
        total_completed: int,
        total_failed: int,
        hlc: HLCTimestamp,
    ) -> JobState:
        return msgspec.structs.replace(
            self,
            status=final_status,
            completed_count=total_completed,
            failed_count=total_failed,
            last_hlc=hlc,
        )

    def with_progress(
        self,
        completed_count: int,
        failed_count: int,
        hlc: HLCTimestamp,
    ) -> JobState:
        return msgspec.structs.replace(
            self,
            completed_count=completed_count,
            failed_count=failed_count,
            last_hlc=hlc,
            last_progress_hlc=hlc,
        )

    def with_datacenter_reassigned(
        self,
        reassignment: DatacenterReassignment,
        hlc: HLCTimestamp,
    ) -> JobState:
        """The job runs in the replacement, not the lost datacenter."""
        return msgspec.structs.replace(
            self,
            assigned_datacenters=tuple(
                datacenter
                for datacenter in self.assigned_datacenters
                if datacenter != reassignment.lost_datacenter
            )
            + (
                (reassignment.replacement_datacenter,)
                if reassignment.replacement_datacenter
                else ()
            ),
            datacenter_reassignments=(*self.datacenter_reassignments, reassignment),
            last_hlc=hlc,
        )

    def with_cancellation_acked(self, datacenter_id: str, hlc: HLCTimestamp) -> JobState:
        return msgspec.structs.replace(
            self,
            cancellation_acked_datacenters=(
                self.cancellation_acked_datacenters | {datacenter_id}
            ),
            last_hlc=hlc,
        )

    @property
    def is_cancelled(self) -> bool:
        return self.cancelled

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def to_dict(self) -> JobStateRecord:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "fence_token": self.fence_token,
            "assigned_datacenters": list(self.assigned_datacenters),
            "accepted_datacenters": list(self.accepted_datacenters),
            "cancelled": self.cancelled,
            "completed_count": self.completed_count,
            "failed_count": self.failed_count,
            # HLCs serialize as their three components (wall_ms, logical,
            # node_id) -- the form msgspec gives the HLCTimestamp
            # NamedTuple inside Checkpoint; a packed integer would exceed
            # msgpack's 64 bits.
            "created_hlc": list(self.created_hlc),
            "last_hlc": list(self.last_hlc),
            "requestor_id": self.requestor_id,
            # Checkpoints persist ACTIVE jobs through this dict: a field
            # missing here is silently reset by a checkpoint + restart
            # (timeout_seconds used to be — the restarted node then
            # resumed AD-34 tracking with a zero budget).
            "timeout_seconds": self.timeout_seconds,
            "last_progress_hlc": (
                list(self.last_progress_hlc)
                if self.last_progress_hlc is not None
                else None
            ),
            "cancellation_acked_datacenters": list(
                self.cancellation_acked_datacenters
            ),
            "datacenter_reassignments": [
                msgspec.to_builtins(reassignment)
                for reassignment in self.datacenter_reassignments
            ],
            "leader_id": self.leader_id,
        }

    @staticmethod
    def _decode_hlc(raw: object) -> HLCTimestamp:
        """An HLC from its three-component form. Anything else is not a
        record this format wrote (older formats are versioned out before
        reaching here), so it is refused rather than read as time zero --
        a zero HLC would make every elapsed-time computation on the job
        wrong without a trace."""
        if isinstance(raw, (list, tuple)) and len(raw) == 3:
            return HLCTimestamp(*raw)
        raise ValueError(f"unrecognized HLC encoding: {raw!r}")

    @classmethod
    def from_dict(cls, job_id: str, data: JobStateRecord) -> JobState:
        created_hlc = cls._decode_hlc(data.get("created_hlc", 0))
        last_hlc = cls._decode_hlc(data.get("last_hlc", 0))

        return cls(
            job_id=job_id,
            status=data.get("status", "pending"),
            fence_token=data.get("fence_token", 0),
            assigned_datacenters=tuple(data.get("assigned_datacenters", [])),
            accepted_datacenters=frozenset(data.get("accepted_datacenters", [])),
            cancelled=data.get("cancelled", False),
            completed_count=data.get("completed_count", 0),
            failed_count=data.get("failed_count", 0),
            created_hlc=created_hlc,
            last_hlc=last_hlc,
            requestor_id=data.get("requestor_id", ""),
            timeout_seconds=data.get("timeout_seconds", 0.0),
            last_progress_hlc=(
                cls._decode_hlc(raw_progress_hlc)
                if (raw_progress_hlc := data.get("last_progress_hlc")) is not None
                else None
            ),
            cancellation_acked_datacenters=frozenset(
                data.get("cancellation_acked_datacenters", [])
            ),
            datacenter_reassignments=tuple(
                msgspec.convert(raw_reassignment, DatacenterReassignment)
                for raw_reassignment in data.get("datacenter_reassignments", [])
            ),
            leader_id=data.get("leader_id", ""),
        )
