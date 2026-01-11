"""
Gate job registry module.

Provides access to centralized job state management and consistent hashing
for job-to-gate ownership.

Classes:
- GateJobManager: Centralized job state with per-job locking
- ConsistentHashRing: Deterministic job-to-gate mapping

These are re-exported from the jobs.gates package.
"""

from hyperscale.distributed.jobs.gates import (
    GateJobManager,
    ConsistentHashRing,
)

__all__ = [
    "GateJobManager",
    "ConsistentHashRing",
]
