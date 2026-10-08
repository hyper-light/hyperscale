"""
D-68: one metrics schema for every role (``ClusterMetricsReply``), read by
``HyperscaleClient.cluster_metrics`` and printed by ``hyperscale cluster
--metrics``.

* every reply prints its role and state (``node_info``), and whichever of
  the shared sections it carries: capacity, workload, resources, dispatch
  throughput and outcomes, dispatch round trips per worker, latency SLO per
  datacenter;
* a worker holds no membership (empty ``formation``): none of the
  membership metrics print for it, while a gate's or manager's print as
  before;
* a field missing from a keyed section is not printed as a value;
* the schema reads across one version step (AD-25): a reply from a build
  older than these fields loads with each at its default, and the build
  that added them still prints it.
"""

import dataclasses

from hyperscale.commands.cluster import _prometheus_text
from hyperscale.distributed.cluster.models import ClusterMetricsReply
from hyperscale.distributed.cluster.models import cluster_metrics_reply as reply_module
from hyperscale.distributed.cluster.telemetry_sections import latency_section, slo_section
from hyperscale.distributed.models import ManagerHeartbeat
from hyperscale.distributed.models.message import Message
from hyperscale.distributed.slo import LatencyObservation

D68_FIELDS = (
    "role",
    "node_state",
    "capacity",
    "workload",
    "resources",
    "dispatch_throughput",
    "dispatch_outcomes",
    "dispatch_latency",
    "slo",
)
MEMBERSHIP_METRIC_PREFIXES = ("cluster_membership_", "cluster_size", "cluster_info", "cluster_raft_", "cluster_watch_")


def worker_reply() -> ClusterMetricsReply:
    return ClusterMetricsReply(
        member_id="worker-a@10.0.0.7:9000",
        formation="",
        is_leader=False,
        role="worker",
        node_state="healthy",
        capacity={"total_cores": 2, "available_cores": 1},
        workload={"active_workflows": 1, "pending_workflows": 0},
        resources={"cpu_percent": 12.5, "memory_percent": 40.0},
    )


def manager_reply() -> ClusterMetricsReply:
    return ClusterMetricsReply(
        member_id="manager-1",
        formation="formed",
        is_leader=True,
        role="manager",
        node_state="active",
        dispatch_outcomes={"accepted": 7, "unreachable": 1},
        dispatch_throughput={"observed": 0.5, "expected": 2.0},
        dispatch_latency={
            "worker-a": {"p50_ms": 21.0, "p95_ms": 24.0, "p99_ms": 25.0, "sample_count": 3.0},
            "worker-b": {"p50_ms": 270.0, "p95_ms": 280.0, "p99_ms": 281.0, "sample_count": 4.0},
        },
        slo={
            "dc-east": {
                "p50_ms": 40.0,
                "p95_ms": 270.0,
                "p99_ms": 280.0,
                "sample_count": 7.0,
                "compliance_score": 1.1,
                "routing_factor": 1.04,
            }
        },
    )


def membership_lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.removeprefix("# TYPE ").startswith(MEMBERSHIP_METRIC_PREFIXES)]


def test_a_workers_telemetry_prints_without_membership_metrics() -> None:
    lines = _prometheus_text(worker_reply()).splitlines()

    for line in (
        'node_info{member="worker-a@10.0.0.7:9000",role="worker",state="healthy"} 1',
        'node_capacity{member="worker-a@10.0.0.7:9000",kind="total_cores"} 2',
        'node_capacity{member="worker-a@10.0.0.7:9000",kind="available_cores"} 1',
        'node_workload{member="worker-a@10.0.0.7:9000",kind="active_workflows"} 1',
        'node_resource_percent{member="worker-a@10.0.0.7:9000",resource="cpu_percent"} 12.5',
    ):
        assert line in lines, lines
    assert membership_lines("\n".join(lines)) == []


def test_a_managers_dispatch_telemetry_prints_per_worker_and_datacenter() -> None:
    text = _prometheus_text(manager_reply())
    lines = text.splitlines()

    for line in (
        'node_info{member="manager-1",role="manager",state="active"} 1',
        'dispatch_outcomes_total{member="manager-1",outcome="accepted"} 7',
        'dispatch_throughput_per_second{member="manager-1",kind="expected"} 2.0',
        'dispatch_latency_p50_ms{member="manager-1",worker_id="worker-a"} 21.0',
        'dispatch_latency_p99_ms{member="manager-1",worker_id="worker-b"} 281.0',
        'dispatch_latency_sample_count{member="manager-1",worker_id="worker-b"} 4.0',
        'slo_latency_p95_ms{member="manager-1",dc_id="dc-east"} 270.0',
        'slo_routing_factor{member="manager-1",dc_id="dc-east"} 1.04',
    ):
        assert line in lines, text
    assert lines.count("# TYPE dispatch_latency_p50_ms gauge") == 1
    assert 'cluster_info{member="manager-1",cluster_uuid="",formation="formed",mode="",leader="true"} 1' in lines
    assert membership_lines(text)


def test_a_member_prints_the_same_membership_metrics_as_before_the_schema() -> None:
    bare = ClusterMetricsReply(member_id="gate-1", formation="formed", is_leader=False)
    with_telemetry = dataclasses.replace(bare, role="gate", node_state="active", workload={"active_jobs": 3})

    assert membership_lines(_prometheus_text(bare)) == membership_lines(_prometheus_text(with_telemetry))


def test_a_field_missing_from_a_keyed_section_is_not_printed() -> None:
    reply = dataclasses.replace(manager_reply(), dispatch_latency={"worker-a": {"p50_ms": 21.0}})

    lines = _prometheus_text(reply).splitlines()

    assert 'dispatch_latency_p50_ms{member="manager-1",worker_id="worker-a"} 21.0' in lines
    assert not [line for line in lines if line.startswith(("dispatch_latency_p95_ms", "# TYPE dispatch_latency_p95"))]


def test_the_shared_sections_carry_what_their_sources_hold() -> None:
    observation = LatencyObservation(
        target_id="worker-a", p50_ms=1.0, p95_ms=2.0, p99_ms=3.0, sample_count=4, window_start=0.0, window_end=60.0
    )
    heartbeat = ManagerHeartbeat(
        node_id="manager-1",
        datacenter="dc-east",
        is_leader=True,
        term=1,
        version=1,
        active_jobs=0,
        active_workflows=0,
        worker_count=1,
        healthy_worker_count=1,
        available_cores=2,
        total_cores=2,
        slo_p50_ms=5.0,
        slo_p95_ms=6.0,
        slo_p99_ms=7.0,
        slo_sample_count=8,
        slo_compliance_score=0.9,
        slo_routing_factor=0.96,
    )

    assert latency_section(observation) == {"p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0, "sample_count": 4.0}
    assert slo_section(heartbeat) == {
        "p50_ms": 5.0,
        "p95_ms": 6.0,
        "p99_ms": 7.0,
        "sample_count": 8.0,
        "compliance_score": 0.9,
        "routing_factor": 0.96,
    }


def older_build_reply_class() -> type[Message]:
    """``ClusterMetricsReply`` as a build before D-68 defined it."""
    older_fields = [
        (
            reply_field.name,
            reply_field.type,
            dataclasses.field(default=reply_field.default, default_factory=reply_field.default_factory),
        )
        if reply_field.default is not dataclasses.MISSING or reply_field.default_factory is not dataclasses.MISSING
        else (reply_field.name, reply_field.type)
        for reply_field in dataclasses.fields(ClusterMetricsReply)
        if reply_field.name not in D68_FIELDS
    ]
    older = dataclasses.make_dataclass("ClusterMetricsReply", older_fields, bases=(Message,), slots=True)
    older.__module__ = ClusterMetricsReply.__module__
    older.__qualname__ = ClusterMetricsReply.__qualname__
    return older


def test_a_reply_from_an_older_build_reads_each_new_field_as_its_default() -> None:
    older = older_build_reply_class()
    reply_module.ClusterMetricsReply = older
    try:
        wire_bytes = older(member_id="manager-1", formation="formed", is_leader=True, voters=3).dump()
    finally:
        reply_module.ClusterMetricsReply = ClusterMetricsReply

    received = ClusterMetricsReply.load(wire_bytes)

    assert type(received) is ClusterMetricsReply
    assert (received.member_id, received.voters) == ("manager-1", 3)
    for field_name in D68_FIELDS:
        default_reply = ClusterMetricsReply(member_id="", formation="", is_leader=False)
        assert getattr(received, field_name) == getattr(default_reply, field_name)
    assert 'node_info{member="manager-1",role="",state=""} 1' in _prometheus_text(received).splitlines()
