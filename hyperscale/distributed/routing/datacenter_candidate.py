"""
A datacenter as the gate's router sees it (AD-36 Part 1).
"""

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class DatacenterCandidate:
    """
    One datacenter's routing inputs.

    ``health_bucket`` is the gate's merged health classification (AD-16,
    AD-33) in upper case. Cores and queued workflows are the datacenter's
    AD-43 capacity. ``total_managers`` counts the managers the gate would
    dispatch to and ``healthy_managers`` those whose circuit is not open;
    ``circuit_breaker_pressure`` is the open share. ``health_severity_weight``
    (AD-17 overload) and ``slo_routing_factor`` (AD-42) multiply the score.
    """

    datacenter_id: str
    health_bucket: str
    available_cores: int
    total_cores: int
    queue_depth: int
    total_managers: int
    healthy_managers: int
    circuit_breaker_pressure: float
    health_severity_weight: float
    slo_routing_factor: float
