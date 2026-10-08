"""
The pluggable placement policy a gate's router consults (D-62).
"""

from typing import Protocol

from .datacenter_candidate import DatacenterCandidate
from .models import PlacementPlan
from .models import PlacementRequest


class PlacementPolicy(Protocol):
    """
    Decides where a job may run and in what order of preference.

    ``GateJobRouter`` owns what outlives one decision -- the per-job
    dispatch cooldowns and the AD-36 Part 11 counters -- and asks the
    policy everything else: which datacenters a job may be placed in, in
    what order (``place``), and which fallbacks AD-43 spillover may move a
    primary's share to (``spillover_candidates``). ``ConstrainedPlacementPolicy``
    is the gate's.
    """

    def place(self, request: PlacementRequest, candidates: list[DatacenterCandidate]) -> PlacementPlan:
        """Every datacenter ``request`` may be placed in, best first, from
        the router's view of every known datacenter."""
        ...

    def spillover_candidates(
        self,
        request: PlacementRequest,
        primary_datacenter: str,
        fallback_datacenters: list[str],
        candidates: list[DatacenterCandidate],
    ) -> list[str]:
        """The ``fallback_datacenters`` AD-43 spillover may move the job's
        share in ``primary_datacenter`` to, in their order."""
        ...
