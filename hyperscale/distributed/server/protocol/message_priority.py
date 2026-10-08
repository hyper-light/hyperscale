"""``MessagePriority`` -- pickled under the namespace
``hyperscale.distributed.server.protocol.in_flight_tracker`` (see that module)."""

from enum import IntEnum

# AD-37 handler classification: every server-side handler (``@tcp.receive``)
# by name, in exactly one class. The one definition -- ``message_class``
# re-exports these, and ``tests/unit/distributed/reliability/
# test_handler_classification.py`` fails when a handler is added without a
# class, or a name here is not a handler. SWIM's UDP ``receive`` hook
# declares its own priority and admission group, so none of its message
# types appear here.
_CONTROL_HANDLERS: frozenset[str] = frozenset(
    {
        # Cancellation (AD-20): never lost to load, whatever its volume
        "cancel_job",
        "cancel_job_workflows",
        "cancel_workflow",
        "receive_cancel_single_workflow",
        "job_cancellation_complete",
        "workflow_cancellation_complete",
        "workflow_cancellation_query",
        # A gate's decision to time a job out (AD-34): a lifecycle command,
        # as cancellation is
        "job_global_timeout",
        # Leadership and its transfer: who owns a job or a datacenter --
        # losing one strands the work it names
        "job_leader_gate_transfer",
        "job_leader_manager_transfer",
        "job_leader_worker_transfer",
        "receive_job_leader_transfer",
        "receive_gate_job_leader_transfer",
        "receive_manager_job_leader_transfer",
        "job_leadership_announcement",
        "dc_leader_announcement",
        "workflow_reassignment",
        # Membership: operator joins, evictions, and the liveness check a
        # recovered peer must pass before it is re-admitted
        "node_join",
        "eviction_notice",
        "ping",
        # Liveness of running work: an extension (AD-26) withheld under
        # load is a workflow killed as stuck exactly when load slows it,
        # and a throttle (AD-41) shed under load cannot relieve it
        "extension_request",
        "extension_response",
        "throttle_workflow",
        # Per-job Raft consensus (manager and gate tiers): its volume is
        # set by the protocol (one coalesced request per group per peer
        # per heartbeat), and losing it costs elections, not just data
        "raft_request_vote",
        "raft_request_vote_response",
        "raft_append_entries",
        "raft_append_entries_response",
        "raft_ledger_proposal",
        "gate_raft_request_vote",
        "gate_raft_request_vote_response",
        "gate_raft_append_entries",
        "gate_raft_append_entries_response",
        "gate_raft_ledger_proposal",
        "gate_raft_ledger_placement",
        # Gate job replication's two-phase commit and its repair read:
        # consensus, as Raft is
        "gate_job_replica_prepare",
        "gate_job_replica_commit",
        "gate_job_replica_abort",
        "gate_job_replica_fetch",
        # AD-52 cluster membership group: formation, joins and the group's
        # Raft traffic -- losing it costs the cluster its membership
        "cluster_hello",
        "found_cluster",
        "cluster_join",
        "cluster_leave",
        "cluster_mode",
        "cluster_resize",
        "cluster_raft_request_vote",
        "cluster_raft_append_entries",
        "cluster_raft_install_snapshot",
        # AD-39 clock offset measurement: a throttled probe would delay
        # (or prevent) fencing a node whose clock ran away.
        "clock_offset_probe",
    }
)

_DISPATCH_HANDLERS: frozenset[str] = frozenset(
    {
        # Job submission and workflow dispatch
        "job_submission",
        "workflow_dispatch",
        # State sync
        "state_sync",
        "state_sync_request",
        "job_state_sync",
        # Registration and discovery of the nodes work is placed on
        "worker_register",
        "manager_register",
        "manager_peer_register",
        "gate_register",
        "register_callback",
        "worker_discovery",
        "manager_discovery",
        "worker_state_update",
        # Terminal results: a job's outcome, shed only past overload
        "workflow_final_result",
        "job_final_result",
        "job_final_result_forwarded",
        "receive_job_final_status",
        "receive_job_final_result",
        "receive_global_job_result",
        "workflow_result_push",
        "reporter_result_push",
    }
)

_DATA_HANDLERS: frozenset[str] = frozenset(
    {
        # Progress updates
        "workflow_progress",
        "receive_job_progress",
        # AD-34 timeout coordination reports
        "receive_job_progress_report",
        "receive_job_timeout_report",
        # Heartbeats and status (non-SWIM)
        "worker_heartbeat",
        "manager_status_update",
        # AD-41 peer-manager resource gossip
        "manager_resource_gossip",
        # Status and stats pushed to gates and clients
        "job_status_push",
        "job_status_push_forward",
        "job_batch_push",
        "windowed_stats_push",
    }
)

_TELEMETRY_HANDLERS: frozenset[str] = frozenset(
    {
        # Reads: status polls and queries a caller repeats, shed first
        "job_status",
        "workflow_query",
        "workflow_status_query",
        "list_workers",
        "datacenter_list",
        # AD-52 operator reads of cluster membership: a status read and a
        # watch's long poll (which holds its slot for the watch's wait)
        "cluster_status",
        "cluster_watch",
        "cluster_metrics",
    }
)


class MessagePriority(IntEnum):
    """
    Priority levels for incoming messages.

    Priority determines load shedding order - lower priorities are shed first.
    CRITICAL messages are NEVER shed regardless of system load.

    Maps to AD-37 MessageClass:
    - CRITICAL ← CONTROL (SWIM, cancellation, leadership)
    - HIGH ← DISPATCH (job submission, workflow dispatch)
    - NORMAL ← DATA (progress updates, stats)
    - LOW ← TELEMETRY (metrics, debug)
    """

    CRITICAL = 0  # Control-plane traffic; ungrouped CRITICAL is never shed.
    HIGH = 1  # Job dispatch, workflow commands, state sync
    NORMAL = 2  # Status updates, heartbeats (non-SWIM)
    LOW = 3  # Metrics, stats, telemetry, logs


def _classify_handler_to_priority(handler_name: str) -> MessagePriority:
    """
    Classify a handler name to MessagePriority using AD-37 classification.

    This is a module-internal function that duplicates the logic from
    message_class.py to avoid circular imports.

    Args:
        handler_name: Name of the handler (e.g., "receive_workflow_progress")

    Returns:
        MessagePriority for the handler
    """
    if handler_name in _CONTROL_HANDLERS:
        return MessagePriority.CRITICAL
    if handler_name in _DISPATCH_HANDLERS:
        return MessagePriority.HIGH
    if handler_name in _DATA_HANDLERS:
        return MessagePriority.NORMAL
    if handler_name in _TELEMETRY_HANDLERS:
        return MessagePriority.LOW
    # Default to NORMAL for unknown handlers (conservative)
    return MessagePriority.NORMAL
