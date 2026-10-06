"""
``hyperscale cluster --metrics`` prints a gate's AD-45 route learning per
datacenter, under the AD-45 metric names, labelled by datacenter; a node
without any (a manager) prints none of them.
"""

from hyperscale.commands.cluster import _prometheus_text
from hyperscale.distributed.cluster.models import ClusterMetricsReply


def test_a_gates_route_learning_is_printed_per_datacenter() -> None:
    reply = ClusterMetricsReply(
        member_id="gate-1",
        formation="formed",
        is_leader=True,
        route_learning={
            "dc-east": {
                "observed_latency_ms": 42.5,
                "blended_latency_ms": 40.0,
                "confidence": 0.75,
                "sample_count": 12.0,
            }
        },
    )

    text = _prometheus_text(reply)

    for line in (
        'route_learning_observed_latency_ms{member="gate-1",dc_id="dc-east"} 42.5',
        'route_learning_blended_latency_ms{member="gate-1",dc_id="dc-east"} 40.0',
        'route_learning_confidence{member="gate-1",dc_id="dc-east"} 0.75',
        'route_learning_sample_count{member="gate-1",dc_id="dc-east"} 12.0',
    ):
        assert line in text.splitlines(), text


def test_a_node_without_route_learning_prints_none() -> None:
    text = _prometheus_text(ClusterMetricsReply(member_id="manager-1", formation="formed", is_leader=False))

    assert "route_learning" not in text and "routing_" not in text


def test_a_gates_routing_counters_are_printed() -> None:
    reply = ClusterMetricsReply(
        member_id="gate-1",
        formation="formed",
        is_leader=True,
        routing={
            "decision:HEALTHY": 9,
            "decision:none": 1,
            "exclusion:unhealthy_status": 2,
            "fallback:dc-east>dc-west": 3,
            "cooldowns": 4,
        },
    )

    lines = _prometheus_text(reply).splitlines()

    for line in (
        'routing_decisions_total{member="gate-1",bucket="HEALTHY"} 9',
        'routing_decisions_total{member="gate-1",bucket="none"} 1',
        'routing_exclusions_total{member="gate-1",reason="unhealthy_status"} 2',
        'routing_fallback_used_total{member="gate-1",from_dc="dc-east",to_dc="dc-west"} 3',
        'routing_cooldowns_total{member="gate-1"} 4',
    ):
        assert line in lines, lines


def test_a_gates_datacenter_watches_are_printed_per_datacenter() -> None:
    reply = ClusterMetricsReply(
        member_id="gate-1",
        formation="formed",
        is_leader=True,
        datacenter_watches={
            "dc-east": {"staleness_seconds": 3.5, "disconnected": 0.0, "applied_index": 12.0},
            "dc-west": {"staleness_seconds": float("inf"), "disconnected": 1.0, "applied_index": 0.0},
        },
    )

    lines = _prometheus_text(reply).splitlines()

    for line in (
        'cluster_watch_staleness_seconds{member="gate-1",dc_id="dc-east"} 3.5',
        'cluster_watch_disconnected{member="gate-1",dc_id="dc-east"} 0.0',
        'cluster_watch_applied_index{member="gate-1",dc_id="dc-east"} 12.0',
        # Never observed: infinitely stale, in the exposition format's spelling.
        'cluster_watch_staleness_seconds{member="gate-1",dc_id="dc-west"} +Inf',
        'cluster_watch_disconnected{member="gate-1",dc_id="dc-west"} 1.0',
    ):
        assert line in lines, lines


def test_a_node_without_datacenter_watches_prints_none() -> None:
    text = _prometheus_text(ClusterMetricsReply(member_id="manager-1", formation="formed", is_leader=False))

    assert "cluster_watch_staleness_seconds" not in text and "cluster_watch_disconnected" not in text
