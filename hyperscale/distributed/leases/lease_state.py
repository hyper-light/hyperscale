"""``LeaseState`` -- pickled under the namespace
``hyperscale.distributed.leases.job_lease`` (see that module)."""

from __future__ import annotations

from enum import Enum


class LeaseState(Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    RELEASED = "released"
