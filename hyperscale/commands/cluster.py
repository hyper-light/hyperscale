import asyncio
import sys

from hyperscale.commands.cli import AssertSet, command
from hyperscale.core.jobs.runner.shutdown_signals import ShutdownSignals
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.distributed.cluster import ClusterJoinError
from hyperscale.distributed.cluster.models import ClusterMetricsReply
from hyperscale.distributed.env import Env as HyperscaleEnv
from hyperscale.distributed.nodes import HyperscaleClient
from hyperscale.logging import LoggingConfig, LogLevelName

from .run.node_address import parse_node_address
from .run.shared import resolve_auth_secret


def _exit_with_error(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


@command(
    display_help_on_error=False,
    shortnames={"host": "H", "timeout": "T"},
)
async def cluster(
    node: str = None,
    watch: bool = False,
    metrics: bool = False,
    host: str = "127.0.0.1",
    port: int = 8500,
    timeout: str | None = None,
    acm_secret: str | None = None,
    log_level: AssertSet[LogLevelName] = "fatal",
):
    """
    Show a manager or gate cluster's membership as of now: its leader,
    voters, learners, cohort and mode. The cluster's leader answers after
    confirming with a quorum that it still leads, so nothing committed
    before the command ran is missing.

    @param node The host:port (TCP) of any manager or gate of the cluster (with --metrics, a worker too)
    @param watch Keep watching: print each membership change as it commits, until Ctrl-C
    @param metrics Print the node's own metrics (a gate's, manager's or worker's), in Prometheus text format
    @param host The local address this command listens on for the reply
    @param port The local TCP port this command listens on for the reply
    @param timeout How long to wait (defaults to the cluster's standard TCP timeout plus a formation interval)
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET, else the per-user cluster cookie)
    @param log_level The log level to use
    """
    try:
        node_addr = parse_node_address(node)

    except ValueError as address_error:
        _exit_with_error(str(address_error))

    LoggingConfig().update(log_level=log_level.data, log_output="stderr")
    env = HyperscaleEnv(
        MERCURY_SYNC_AUTH_SECRET=await resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
    )
    # A member passes the request to its leader (one standard request),
    # which confirms its leadership within its proposal timeout (one
    # formation interval).
    status_timeout = (
        TimeParser(timeout).time
        if timeout
        else max(env.MANAGER_TCP_TIMEOUT_STANDARD, env.GATE_TCP_TIMEOUT_STANDARD)
        + env.CLUSTER_FORMATION_INTERVAL_SECONDS
    )

    client = HyperscaleClient(host=host, port=port, env=env)
    await client.start()

    if watch:
        await _watch(client, node_addr, env)
        return

    if metrics:
        try:
            metrics_reply = await client.cluster_metrics(node_addr, status_timeout)

        except ClusterJoinError as metrics_error:
            _exit_with_error(f"metrics failed via {node_addr[0]}:{node_addr[1]}: {metrics_error}")

        finally:
            await client.stop()

        print(_prometheus_text(metrics_reply), end="")
        return

    try:
        reply = await client.cluster_status(node_addr, status_timeout)

    except ClusterJoinError as status_error:
        _exit_with_error(f"status failed via {node_addr[0]}:{node_addr[1]}: {status_error}")

    finally:
        await client.stop()

    if not reply.served:
        _exit_with_error(f"status refused via {node_addr[0]}:{node_addr[1]}: {reply.refusal}")

    print(f"cluster {reply.cluster_uuid} (mode {reply.mode}, read at index {reply.read_index})")
    print(f"leader   {reply.leader_member_id}")
    print(f"voters   {' '.join(reply.voters)}")
    print(f"learners {' '.join(reply.learners) or '-'}")
    print(f"cohort   {' '.join(reply.cohort)}")


async def _watch(client: HyperscaleClient, node_addr: tuple[str, int], env: HyperscaleEnv) -> None:
    """Follow the membership through the node's long polls (AD-52 section
    9): a snapshot, then each change in commit order. A node that cannot
    answer is asked again a formation interval later; Ctrl-C ends the
    watch (exit 130)."""
    wait_seconds = env.CLUSTER_WATCH_WAIT_SECONDS
    # The poll answers within its wait plus the request's own budget.
    poll_timeout = wait_seconds + max(env.MANAGER_TCP_TIMEOUT_STANDARD, env.GATE_TCP_TIMEOUT_STANDARD)
    cluster_uuid: str | None = None
    after_index = 0
    try:
        with ShutdownSignals(asyncio.current_task()):
            while True:
                try:
                    reply = await client.watch_cluster(
                        node_addr, cluster_uuid, after_index, wait_seconds, poll_timeout
                    )
                except ClusterJoinError as watch_error:
                    print(f"watch: {watch_error}", file=sys.stderr, flush=True)
                    await asyncio.sleep(env.CLUSTER_FORMATION_INTERVAL_SECONDS)
                    continue
                if not reply.served:
                    print(f"watch: {reply.refusal}", file=sys.stderr, flush=True)
                    await asyncio.sleep(env.CLUSTER_FORMATION_INTERVAL_SECONDS)
                    continue
                if reply.snapshot:
                    print(f"cluster {reply.cluster_uuid} at index {reply.applied_index} (mode {reply.mode})")
                    print(f"  voters   {' '.join(reply.voters)}")
                    print(f"  learners {' '.join(reply.learners) or '-'}")
                    print(f"  cohort   {' '.join(reply.cohort)}", flush=True)
                for index, kind, detail in reply.events:
                    print(f"{index:>8} {kind:<13} {detail}", flush=True)
                cluster_uuid = reply.cluster_uuid
                after_index = reply.applied_index

    except (KeyboardInterrupt, asyncio.CancelledError):
        raise SystemExit(130)

    finally:
        await client.stop()


def _prometheus_text(reply: ClusterMetricsReply) -> str:
    """The node's metrics in the Prometheus text exposition format (AD-52
    section 18 names), labelled with its member id: the telemetry every
    role shares (D-68), and -- from a member of a cluster (a gate or a
    manager; a worker holds no membership) -- its membership."""
    member = reply.member_id.replace('"', "")
    lines = [
        *(
            _membership_state_lines(member, reply)
            + _membership_change_lines(member, reply)
            + _membership_activity_lines(member, reply)
            if reply.formation
            else ()
        ),
        # AD-36 routing counters (gates only).
        *(
            line
            for metric_name, key_prefix, label_names in (
                ("routing_decisions_total", "decision:", ("bucket",)),
                ("routing_exclusions_total", "exclusion:", ("reason",)),
                ("routing_fallback_used_total", "fallback:", ("from_dc", "to_dc")),
            )
            if any(key.startswith(key_prefix) for key in reply.routing)
            for line in (
                f"# TYPE {metric_name} counter",
                *(
                    f"{metric_name}{{member=\"{member}\","
                    + ",".join(
                        f'{label_name}="{label_value}"'
                        for label_name, label_value in zip(label_names, key.removeprefix(key_prefix).split(">"))
                    )
                    + f"}} {count}"
                    for key, count in sorted(reply.routing.items())
                    if key.startswith(key_prefix)
                ),
            )
        ),
        *(
            (
                "# TYPE routing_cooldowns_total counter",
                f'routing_cooldowns_total{{member="{member}"}} {reply.routing["cooldowns"]}',
            )
            if "cooldowns" in reply.routing
            else ()
        ),
        # AD-45 route learning, per datacenter (gates only).
        *(
            line
            for metric_name, field_name in (
                ("route_learning_observed_latency_ms", "observed_latency_ms"),
                ("route_learning_blended_latency_ms", "blended_latency_ms"),
                ("route_learning_confidence", "confidence"),
                ("route_learning_sample_count", "sample_count"),
            )
            if reply.route_learning
            for line in (
                f"# TYPE {metric_name} gauge",
                *(
                    f'{metric_name}{{member="{member}",dc_id="{datacenter_id}"}} {learned[field_name]}'
                    for datacenter_id, learned in sorted(reply.route_learning.items())
                ),
            )
        ),
        # AD-52 section 10 watch of each datacenter's managers (gates only);
        # a watch with no observation yet is infinitely stale.
        *(
            line
            for metric_name, field_name in (
                ("cluster_watch_staleness_seconds", "staleness_seconds"),
                ("cluster_watch_disconnected", "disconnected"),
                ("cluster_watch_applied_index", "applied_index"),
            )
            if reply.datacenter_watches
            for line in (
                f"# TYPE {metric_name} gauge",
                *(
                    f'{metric_name}{{member="{member}",dc_id="{datacenter_id}"}} '
                    + ("+Inf" if watch[field_name] == float("inf") else f"{watch[field_name]}")
                    for datacenter_id, watch in sorted(reply.datacenter_watches.items())
                ),
            )
        ),
        *_ad44_metric_lines(member, reply),
        *_node_metric_lines(member, reply),
    ]
    return "\n".join(lines) + "\n"


def _membership_state_lines(member: str, reply: ClusterMetricsReply) -> list[str]:
    """The membership as the member applied it, and its Raft group's term,
    indexes and counters (AD-52 section 18)."""
    return [
        "# TYPE cluster_membership_size gauge",
        f'cluster_membership_size{{member="{member}",status="voter"}} {reply.voters}',
        f'cluster_membership_size{{member="{member}",status="learner"}} {reply.learners}',
        "# TYPE cluster_membership_holders gauge",
        f'cluster_membership_holders{{member="{member}"}} {reply.holders}',
        "# TYPE cluster_size gauge",
        f'cluster_size{{member="{member}"}} {reply.cohort_size}',
        "# TYPE cluster_info gauge",
        f'cluster_info{{member="{member}",cluster_uuid="{reply.cluster_uuid or ""}",'
        f'formation="{reply.formation}",mode="{reply.mode or ""}",leader="{str(reply.is_leader).lower()}"}} 1',
        "# TYPE cluster_raft_term gauge",
        f'cluster_raft_term{{member="{member}"}} {reply.raft.get("term", 0)}',
        "# TYPE cluster_raft_commit_index gauge",
        f'cluster_raft_commit_index{{member="{member}"}} {reply.raft.get("commit_index", 0)}',
        "# TYPE cluster_raft_applied_index gauge",
        f'cluster_raft_applied_index{{member="{member}"}} {reply.raft.get("applied_index", 0)}',
        "# TYPE cluster_raft_leader_election_total counter",
        f'cluster_raft_leader_election_total{{member="{member}",outcome="started"}} {reply.raft.get("elections_started", 0)}',
        f'cluster_raft_leader_election_total{{member="{member}",outcome="won"}} {reply.raft.get("elections_won", 0)}',
        "# TYPE cluster_raft_proposal_total counter",
        f'cluster_raft_proposal_total{{member="{member}",outcome="committed"}} {reply.raft.get("proposals_committed", 0)}',
        f'cluster_raft_proposal_total{{member="{member}",outcome="failed"}} {reply.raft.get("proposals_failed", 0)}',
        "# TYPE cluster_raft_snapshot_send_total counter",
        f'cluster_raft_snapshot_send_total{{member="{member}"}} {reply.raft.get("snapshots_sent", 0)}',
        "# TYPE cluster_raft_snapshot_receive_total counter",
        f'cluster_raft_snapshot_receive_total{{member="{member}"}} {reply.raft.get("snapshots_installed", 0)}',
        "# TYPE cluster_raft_lease_read_total counter",
        f'cluster_raft_lease_read_total{{member="{member}"}} {reply.raft.get("lease_reads", 0)}',
    ]


def _membership_change_lines(member: str, reply: ClusterMetricsReply) -> list[str]:
    """Each follower's replication lag while the member leads, and the
    membership changes it applied by kind (AD-52 section 18)."""
    return [
        "# TYPE cluster_raft_apply_lag_entries gauge",
        *(
            f'cluster_raft_apply_lag_entries{{member="{member}",follower_id="{follower}"}} {lag}'
            for follower, lag in sorted(reply.follower_lag.items())
        ),
        "# TYPE cluster_membership_change_total counter",
        *(
            f'cluster_membership_change_total{{member="{member}",type="{change_type}"}} {count}'
            for change_type, count in sorted(reply.changes_applied.items())
        ),
    ]


def _membership_activity_lines(member: str, reply: ClusterMetricsReply) -> list[str]:
    """The foundings the member proposed, groups it left, operator requests
    and watches it holds open (AD-52 section 18)."""
    return [
        "# TYPE cluster_bootstrap_founding_total counter",
        f'cluster_bootstrap_founding_total{{member="{member}"}} {reply.foundings_proposed}',
        "# TYPE cluster_groups_left_total counter",
        f'cluster_groups_left_total{{member="{member}"}} {reply.groups_left}',
        "# TYPE cluster_operator_request_total counter",
        *(
            f'cluster_operator_request_total{{member="{member}",request="{key.split(":")[0]}",'
            f'stage="{key.split(":")[1]}"}} {count}'
            for key, count in sorted(reply.operator_requests.items())
        ),
        "# TYPE cluster_watch_streams_open gauge",
        f'cluster_watch_streams_open{{member="{member}"}} {reply.open_watches}',
    ]


# D-68 sections keyed by worker or datacenter: (metric name, field) pairs.
_DISPATCH_LATENCY_METRICS = (
    ("dispatch_latency_p50_ms", "p50_ms"),
    ("dispatch_latency_p95_ms", "p95_ms"),
    ("dispatch_latency_p99_ms", "p99_ms"),
    ("dispatch_latency_sample_count", "sample_count"),
)
_SLO_METRICS = (
    ("slo_latency_p50_ms", "p50_ms"),
    ("slo_latency_p95_ms", "p95_ms"),
    ("slo_latency_p99_ms", "p99_ms"),
    ("slo_sample_count", "sample_count"),
    ("slo_compliance_score", "compliance_score"),
    ("slo_routing_factor", "routing_factor"),
)


def _node_metric_lines(member: str, reply: ClusterMetricsReply) -> list[str]:
    """The telemetry every role shares (D-68): its role and state, capacity,
    workload and resources, a manager's dispatch throughput, outcomes and
    per-worker round trips, and the AD-42 latency SLO per datacenter."""
    metric_specs = (
        ("node_capacity", "gauge", "kind", reply.capacity),
        ("node_workload", "gauge", "kind", reply.workload),
        ("node_resource_percent", "gauge", "resource", reply.resources),
        ("dispatch_throughput_per_second", "gauge", "kind", reply.dispatch_throughput),
        ("dispatch_outcomes_total", "counter", "outcome", reply.dispatch_outcomes),
    )
    return [
        "# TYPE node_info gauge",
        f'node_info{{member="{member}",role="{reply.role}",state="{reply.node_state}"}} 1',
        *(line for metric_spec in metric_specs for line in _labelled_metric_lines(member, *metric_spec)),
        *_keyed_field_lines(member, "worker_id", _DISPATCH_LATENCY_METRICS, reply.dispatch_latency),
        *_keyed_field_lines(member, "dc_id", _SLO_METRICS, reply.slo),
    ]


def _keyed_field_lines(
    member: str,
    label_name: str,
    metric_fields: tuple[tuple[str, str], ...],
    values: dict[str, dict[str, float]],
) -> list[str]:
    """A gauge per (metric, field) pair, a sample per key carrying the field."""
    return [
        line
        for metric_name, field_name in metric_fields
        for line in _labelled_metric_lines(member, metric_name, "gauge", label_name, _field_by_key(values, field_name))
    ]


def _field_by_key(values: dict[str, dict[str, float]], field_name: str) -> dict[str, float]:
    """``field_name``'s value under each key whose fields carry it."""
    return {key: fields[field_name] for key, fields in values.items() if field_name in fields}


def _ad44_metric_lines(member: str, reply: ClusterMetricsReply) -> list[str]:
    """The AD-44 metrics: retry budgets (managers, per job holding a budget)
    and best-effort completion (gates)."""
    metric_specs = (
        ("retry_budget_consumed_total", "counter", "job_id", reply.retry_budget_consumed),
        ("retry_budget_exhausted_total", "counter", "job_id", reply.retry_budget_exhausted),
        ("best_effort_completions_total", "counter", "reason", reply.best_effort_completions),
        ("best_effort_completion_ratio", "gauge", "job_id", reply.best_effort_completion_ratio),
        ("best_effort_late_results_total", "counter", "outcome", reply.best_effort_late_results),
    )
    return [line for metric_spec in metric_specs for line in _labelled_metric_lines(member, *metric_spec)]


def _labelled_metric_lines(
    member: str,
    metric_name: str,
    metric_type: str,
    label_name: str,
    values: dict[str, int] | dict[str, float],
) -> list[str]:
    """One metric's type line and a sample per label value; nothing when it has no samples."""
    if not values:
        return []
    return [f"# TYPE {metric_name} {metric_type}"] + [
        f'{metric_name}{{member="{member}",{label_name}="{label_value}"}} {value}'
        for label_value, value in sorted(values.items())
    ]
