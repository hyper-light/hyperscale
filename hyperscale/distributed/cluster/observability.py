"""
Observability surface (AD-52 §18).

Centralized metric emission + structured-event publishing for the
cluster module. Metric names follow AD-52 §18 verbatim so the
PrometheusRule shipped with the Helm chart matches.

Metrics are exposed via the hyperscale.distributed.monitoring layer —
this module owns the names + labels + recording-policy. The actual
Prometheus exposition format is the monitoring layer's job.

Structured events route through the AD-39 Logger. Field names follow
AD-52 §18's enumeration so dashboards and log searches are stable
across operators.
"""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from hyperscale.logging import Logger


# ---------------------------------------------------------------------------
# Metric name registry — AD-52 §18 verbatim.
# Comments capture the metric type (gauge / counter / histogram) and the
# label set so consumers don't have to cross-reference the spec.
# ---------------------------------------------------------------------------


CLUSTER_MEMBERSHIP_SIZE = "cluster_membership_size"  # gauge {cluster, role, status}
CLUSTER_MEMBERSHIP_CHANGE_TOTAL = "cluster_membership_change_total"  # counter {cluster, type, outcome}
CLUSTER_MEMBERSHIP_CHANGE_LATENCY_SECONDS = "cluster_membership_change_latency_seconds"  # histogram
CLUSTER_MEMBERSHIP_EPOCH = "cluster_membership_epoch"  # gauge {cluster}
CLUSTER_UUID_INFO = "cluster_uuid"  # info {cluster}
CLUSTER_SIZE = "cluster_size"  # gauge {cluster}

CLUSTER_BOOTSTRAP_STATE = "cluster_bootstrap_state"  # gauge {cluster}
CLUSTER_BOOTSTRAP_DURATION_SECONDS = "cluster_bootstrap_duration_seconds"  # histogram {cluster, outcome}
CLUSTER_JOIN_ATTEMPT_TOTAL = "cluster_join_attempt_total"  # counter {cluster, outcome}
CLUSTER_JOIN_DURATION_SECONDS = "cluster_join_duration_seconds"  # histogram
CLUSTER_LEARNER_COUNT = "cluster_learner_count"  # gauge {cluster}
CLUSTER_LEARNER_PROMOTE_TOTAL = "cluster_learner_promote_total"  # counter {cluster, outcome}
CLUSTER_LEARNER_EVICTION_TOTAL = "cluster_learner_eviction_total"  # counter {cluster, reason}

CLUSTER_RAFT_TERM = "cluster_raft_term"  # gauge {cluster}
CLUSTER_RAFT_COMMIT_INDEX = "cluster_raft_commit_index"  # gauge {cluster}
CLUSTER_RAFT_APPLY_LAG_ENTRIES = "cluster_raft_apply_lag_entries"  # gauge {cluster, follower_id}
CLUSTER_RAFT_APPENDENTRIES_INFLIGHT = "cluster_raft_appendentries_inflight"  # gauge {cluster, follower_id}
CLUSTER_RAFT_LEADER_ELECTION_TOTAL = "cluster_raft_leader_election_total"  # counter {cluster, outcome}
CLUSTER_RAFT_PROPOSAL_TOTAL = "cluster_raft_proposal_total"  # counter {cluster, entry_type}
CLUSTER_RAFT_SNAPSHOT_SEND_TOTAL = "cluster_raft_snapshot_send_total"  # counter {cluster, outcome}
CLUSTER_RAFT_SNAPSHOT_RECEIVE_TOTAL = "cluster_raft_snapshot_receive_total"  # counter {cluster, outcome}

CLUSTER_SWIM_STATE = "cluster_swim_state"  # gauge {cluster, peer_id, state}
CLUSTER_SWIM_PROBE_TOTAL = "cluster_swim_probe_total"  # counter {cluster, peer_id, outcome}
CLUSTER_PHI_ACCRUAL_VALUE = "cluster_phi_accrual_value"  # gauge {cluster, peer_id}
CLUSTER_PHI_THRESHOLD_BREACH_TOTAL = "cluster_phi_threshold_breach_total"  # counter {cluster, peer_id}
CLUSTER_EVICTION_PROPOSAL_TOTAL = "cluster_eviction_proposal_total"  # counter {cluster, reason}

CLUSTER_WATCH_STREAMS_OPEN = "cluster_watch_streams_open"  # gauge {cluster}
CLUSTER_WATCH_LAG_SECONDS = "cluster_watch_lag_seconds"  # histogram {cluster, watcher_id}
CLUSTER_WATCH_RECONNECT_TOTAL = "cluster_watch_reconnect_total"  # counter {cluster, outcome}
CLUSTER_WATCH_DELTA_BYTES = "cluster_watch_delta_bytes"  # histogram {cluster}

CLUSTER_DISCONNECTED_MODE_ACTIVE = "cluster_disconnected_mode_active"  # gauge {cluster, node_id}
CLUSTER_DISCONNECTED_MODE_SECONDS_TOTAL = "cluster_disconnected_mode_seconds_total"  # counter {cluster}
CLUSTER_SOFT_CACHE_STALENESS_SECONDS = "cluster_soft_cache_staleness_seconds"  # histogram
CLUSTER_SOFT_CACHE_READ_TOTAL = "cluster_soft_cache_read_total"  # counter {cluster, kind, freshness}


# ---------------------------------------------------------------------------
# Structured event names — AD-52 §18.
# ---------------------------------------------------------------------------


CLUSTER_BOOTSTRAP_STARTED = "ClusterBootstrapStarted"
CLUSTER_BOOTSTRAP_COMPLETED = "ClusterBootstrapCompleted"
CLUSTER_BOOTSTRAP_FAILED = "ClusterBootstrapFailed"
CLUSTER_MEMBER_ADDED = "ClusterMemberAdded"
CLUSTER_MEMBER_PROMOTED = "ClusterMemberPromoted"
CLUSTER_MEMBER_REMOVED = "ClusterMemberRemoved"
CLUSTER_LEADER_ELECTED = "ClusterLeaderElected"
CLUSTER_PARTITION_DETECTED = "ClusterPartitionDetected"
CLUSTER_PARTITION_HEALED = "ClusterPartitionHealed"
CLUSTER_FENCE_REJECTION = "ClusterFenceRejection"
CLUSTER_DISCONNECTED_MODE_ENTERED = "ClusterDisconnectedModeEntered"
CLUSTER_DISCONNECTED_MODE_EXITED = "ClusterDisconnectedModeExited"
CLUSTER_FORCE_REMOVE = "ClusterForceRemove"
CLUSTER_FREEZE = "ClusterFreeze"
CLUSTER_UNFREEZE = "ClusterUnfreeze"
CLUSTER_SNAPSHOT_IMPORTED = "ClusterSnapshotImported"
DATACENTER_REGISTERED = "DatacenterRegistered"
DATACENTER_REGENERATION_DETECTED = "DatacenterRegenerationDetected"


@runtime_checkable
class MetricsSink(Protocol):
    """Surface the cluster module needs from the monitoring layer.
    The hyperscale.distributed.monitoring module provides the
    implementation; we depend on the Protocol so the cluster module
    stays decoupled from any specific metrics backend."""

    def set_gauge(self, name: str, value: float, labels: dict[str, str]) -> None:
        ...

    def inc_counter(self, name: str, labels: dict[str, str], by: float = 1.0) -> None:
        ...

    def observe_histogram(self, name: str, value: float, labels: dict[str, str]) -> None:
        ...


class NullMetricsSink:
    """No-op sink for tests / environments without Prometheus. Honors
    the Protocol shape without any I/O."""

    def set_gauge(self, name: str, value: float, labels: dict[str, str]) -> None:
        return

    def inc_counter(self, name: str, labels: dict[str, str], by: float = 1.0) -> None:
        return

    def observe_histogram(self, name: str, value: float, labels: dict[str, str]) -> None:
        return


@dataclass(slots=True)
class ClusterObservability:
    """One instance per ClusterNode. Bundles a MetricsSink + Logger and
    exposes typed helpers for the common emit sites so callers don't
    repeat the metric-name + label-dict construction.
    """

    cluster_id: str
    metrics_sink: MetricsSink
    logger: "Logger | None" = None

    async def emit_event(self, event_name: str, **fields: object) -> None:
        if self.logger is None:
            return
        payload: dict[str, object] = {"event": event_name, "cluster_id": self.cluster_id}
        payload.update(fields)
        await self.logger.log(payload)

    def set_membership_epoch(self, epoch: int) -> None:
        self.metrics_sink.set_gauge(
            CLUSTER_MEMBERSHIP_EPOCH,
            float(epoch),
            {"cluster": self.cluster_id},
        )

    def set_membership_size(self, role: str, status: str, count: int) -> None:
        self.metrics_sink.set_gauge(
            CLUSTER_MEMBERSHIP_SIZE,
            float(count),
            {"cluster": self.cluster_id, "role": role, "status": status},
        )

    def inc_membership_change(self, change_type: str, outcome: str) -> None:
        self.metrics_sink.inc_counter(
            CLUSTER_MEMBERSHIP_CHANGE_TOTAL,
            {"cluster": self.cluster_id, "type": change_type, "outcome": outcome},
        )

    def observe_membership_change_latency(self, change_type: str, seconds: float) -> None:
        self.metrics_sink.observe_histogram(
            CLUSTER_MEMBERSHIP_CHANGE_LATENCY_SECONDS,
            seconds,
            {"cluster": self.cluster_id, "type": change_type},
        )

    def set_phi_value(self, peer_id: str, phi: float) -> None:
        self.metrics_sink.set_gauge(
            CLUSTER_PHI_ACCRUAL_VALUE,
            phi,
            {"cluster": self.cluster_id, "peer_id": peer_id},
        )

    def inc_phi_threshold_breach(self, peer_id: str) -> None:
        self.metrics_sink.inc_counter(
            CLUSTER_PHI_THRESHOLD_BREACH_TOTAL,
            {"cluster": self.cluster_id, "peer_id": peer_id},
        )

    def set_disconnected_mode_active(self, node_id: str, active: bool) -> None:
        self.metrics_sink.set_gauge(
            CLUSTER_DISCONNECTED_MODE_ACTIVE,
            1.0 if active else 0.0,
            {"cluster": self.cluster_id, "node_id": node_id},
        )

    def observe_soft_cache_staleness(self, kind: str, seconds: float) -> None:
        self.metrics_sink.observe_histogram(
            CLUSTER_SOFT_CACHE_STALENESS_SECONDS,
            seconds,
            {"cluster": self.cluster_id, "kind": kind},
        )
