"""
Lease management for distributed job ownership.

Provides time-bounded ownership semantics to prevent split-brain
scenarios during node failures and network partitions.
"""

from .job_lease import JobLease, JobLeaseManager, LeaseState

LeaseManager = JobLeaseManager

__all__ = ["JobLease", "JobLeaseManager", "LeaseManager", "LeaseState"]
