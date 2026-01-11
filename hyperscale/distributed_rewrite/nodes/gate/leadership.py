"""
Gate job leadership module.

Provides job leadership tracking with fencing tokens for the Context
Consistency Protocol.

Classes:
- JobLeadershipTracker: Per-job leadership tracking with fence tokens

This is re-exported from the jobs package.
"""

from hyperscale.distributed_rewrite.jobs import JobLeadershipTracker

__all__ = [
    "JobLeadershipTracker",
]
