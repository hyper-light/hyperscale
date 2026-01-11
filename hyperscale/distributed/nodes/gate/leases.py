"""
Gate lease management module.

Provides at-most-once delivery semantics through lease and fence token
management.

Classes:
- JobLeaseManager: Per-job lease tracking with fence tokens
- DatacenterLeaseManager: Per-DC lease tracking for dispatch

These are re-exported from the leases and datacenters packages.
"""

from hyperscale.distributed.leases import LeaseManager as JobLeaseManager
from hyperscale.distributed.datacenters import LeaseManager as DatacenterLeaseManager

__all__ = [
    "JobLeaseManager",
    "DatacenterLeaseManager",
]
