"""
Which nodes an invariant may read: started and not killed. A killed
node's instance is torn down, so its state is no longer the cluster's.
"""

from typing import TYPE_CHECKING

from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


def is_live(harness: "ClusterHarness", handle: ServerHandle) -> bool:
    """Started and not killed."""
    return handle.started and not harness.faults.is_killed(handle)


def all_live_handles(harness: "ClusterHarness") -> list[ServerHandle]:
    """Every live node, of any kind."""
    return [handle for handle in harness.all_handles() if is_live(harness, handle)]


def handles_of_kind(harness: "ClusterHarness", kind: ServerKind) -> list[ServerHandle]:
    """Every node of ``kind``, live or not."""
    return [handle for handle in harness.all_handles() if handle.kind is kind]


def live_handles(harness: "ClusterHarness", kind: ServerKind) -> list[ServerHandle]:
    """Every live node of ``kind``."""
    return [
        handle for handle in handles_of_kind(harness, kind) if is_live(harness, handle)
    ]


def responsive_handles(harness: "ClusterHarness", kind: ServerKind) -> list[ServerHandle]:
    """Every live node of ``kind`` that is not paused: the nodes that owe
    the protocol its timings."""
    return [
        handle
        for handle in live_handles(harness, kind)
        if not harness.faults.is_paused(handle)
    ]
