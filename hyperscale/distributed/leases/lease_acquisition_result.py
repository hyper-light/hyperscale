"""``LeaseAcquisitionResult`` -- pickled under the namespace
``hyperscale.distributed.leases.job_lease`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass

from .job_lease_model import JobLease


@dataclass(slots=True)
class LeaseAcquisitionResult:
    success: bool
    lease: JobLease | None = None
    current_owner: str | None = None
    expires_in: float = 0.0
