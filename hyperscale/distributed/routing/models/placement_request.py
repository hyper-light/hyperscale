"""
What a job asks of its placement (D-62).
"""

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class PlacementRequest:
    """
    One routing of one job: the ``datacenter_count`` datacenters it runs
    in, within ``affinity`` when it names datacenters (its submission's
    ``datacenters``), outside ``occupied_datacenters`` (those it already
    runs in, or lost, when a lost datacenter's work is placed anew), with
    ``cooling_datacenters`` -- those that recently failed its dispatch --
    tried last. ``dispatch_latency_budget_ms`` is the most a datacenter's
    workflow dispatches may take to be answered, at their p95 (the D-5
    digest); 0 sets none.
    """

    job_id: str
    datacenter_count: int
    affinity: frozenset[str] | None
    occupied_datacenters: frozenset[str]
    cooling_datacenters: frozenset[str]
    dispatch_latency_budget_ms: float
