"""
Cluster-ID isolation (SCENARIOS.md §11): no node ever holds membership
from another cluster.

Every node of the harness shares one ``CLUSTER_ID``, and every member in
any live node's SWIM membership (``IncarnationTracker``) is a node of
this harness. A member from outside -- a leftover node from an earlier
scenario on a reused port, or a foreign cluster's gossip -- is a breach.
"""

from typing import TYPE_CHECKING

from tests.simulation.harness.invariant_checks.live_nodes import all_live_handles

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


def cluster_isolation_violation(harness: "ClusterHarness") -> str:
    """Mixed cluster ids, or the first foreign member found, or ``""``."""
    return _mixed_cluster_ids(harness) or _foreign_member(harness)


def _mixed_cluster_ids(harness: "ClusterHarness") -> str:
    cluster_ids = {handle.instance.env.CLUSTER_ID for handle in all_live_handles(harness)}
    if len(cluster_ids) <= 1:
        return ""
    return f"live nodes run under more than one cluster id: {sorted(cluster_ids)}"


def _foreign_member(harness: "ClusterHarness") -> str:
    details = [
        _foreign_member_detail(harness, handle.node_id, member_address)
        for handle in all_live_handles(harness)
        for member_address, _member_state in handle.instance._incarnation_tracker.get_all_nodes()
    ]
    return next(filter(None, details), "")


def _foreign_member_detail(harness: "ClusterHarness", node_id: str, member_address: tuple[str, int]) -> str:
    if harness.address_to_node_id(member_address, kind="udp") is not None:
        return ""
    return f"{node_id} holds SWIM member {member_address} that is no node of this cluster"
