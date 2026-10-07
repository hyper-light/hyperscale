"""
D-68: the sections of the shared metrics schema (``ClusterMetricsReply``)
that more than one role fills, built in one place so every role reports
them under the same keys.
"""

from hyperscale.distributed.models import ManagerHeartbeat
from hyperscale.distributed.slo import LatencyObservation


def latency_section(observation: LatencyObservation) -> dict[str, float]:
    """A latency observation's percentiles and sample count (a manager's
    ``dispatch_latency`` per worker)."""
    return {
        "p50_ms": observation.p50_ms,
        "p95_ms": observation.p95_ms,
        "p99_ms": observation.p99_ms,
        "sample_count": float(observation.sample_count),
    }


def slo_section(heartbeat: ManagerHeartbeat) -> dict[str, float]:
    """A datacenter's AD-42 latency SLO as a manager heartbeat carries it:
    what the manager built, and what a gate's health classification and
    routing read (``slo`` per datacenter)."""
    return {
        "p50_ms": heartbeat.slo_p50_ms,
        "p95_ms": heartbeat.slo_p95_ms,
        "p99_ms": heartbeat.slo_p99_ms,
        "sample_count": float(heartbeat.slo_sample_count),
        "compliance_score": heartbeat.slo_compliance_score,
        "routing_factor": heartbeat.slo_routing_factor,
    }
