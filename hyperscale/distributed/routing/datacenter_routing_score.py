"""
A datacenter's routing score and its components (AD-36 Part 4).
"""

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class DatacenterRoutingScore:
    """
    ``final_score = latency_ms * load_factor * health_severity_weight *
    slo_routing_factor``; lower is better.
    """

    datacenter_id: str
    latency_ms: float
    load_factor: float
    health_severity_weight: float
    slo_routing_factor: float
    final_score: float
