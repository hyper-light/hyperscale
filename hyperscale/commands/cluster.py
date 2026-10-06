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

    @param node The host:port (TCP) of any manager or gate of the cluster
    @param watch Keep watching: print each membership change as it commits, until Ctrl-C
    @param metrics Print the node's own membership metrics, in Prometheus text format
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
    section 18 names), labelled with its member id."""
    member = reply.member_id.replace('"', "")
    lines = [
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
    ]
    return "\n".join(lines) + "\n"
