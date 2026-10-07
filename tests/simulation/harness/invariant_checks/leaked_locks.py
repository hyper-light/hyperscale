"""
LeakedLocksBounded (simulation_framework.md §12): a manager holds a
per-peer state lock only for a peer it still tracks, and a per-gate lock
only for a gate it still knows.

The design's bound, ``len(_peer_state_locks) <= active peers + 1``, is
wrong for a correct manager: a dead peer keeps its lock (and its epoch)
until the dead-peer reaper drops it -- two reap intervals after death,
longer while it still leads a job held here
(``ManagerServer._cleanup_stale_dead_manager_tracking``). So the bound is
set membership instead: every lock's peer is active, dead-but-not-yet-
reaped, known, or awaiting recovery verification. A lock outside that set
is one no cleanup path will ever drop.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.nodes.manager.state import ManagerState

from tests.simulation.harness.invariant_checks.live_nodes import live_handles
from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


def leaked_lock_violation(harness: "ClusterHarness") -> str:
    """The first manager holding a lock for a peer or gate it no longer tracks, or ``""``."""
    details = [_manager_lock_violation(handle) for handle in live_handles(harness, ServerKind.MANAGER)]
    return next(filter(None, details), "")


def _manager_lock_violation(handle: ServerHandle) -> str:
    state: ManagerState = handle.instance._manager_state
    leaked_peer_locks = set(state._peer_state_locks) - _tracked_peer_addresses(state)
    leaked_gate_locks = set(state._gate_state_locks) - set(state._known_gates)
    if not (leaked_peer_locks or leaked_gate_locks):
        return ""
    return (
        f"{handle.node_id} holds state locks for peers it no longer tracks: "
        f"manager peers {sorted(leaked_peer_locks)}, gates {sorted(leaked_gate_locks)}"
    )


def _tracked_peer_addresses(state: ManagerState) -> set[tuple[str, int]]:
    known_peer_addresses = {
        (peer.tcp_host, peer.tcp_port) for peer in state.get_known_manager_peer_values()
    }
    return (
        known_peer_addresses
        | state.get_active_manager_peers()
        | state.get_dead_managers()
        | set(state._recovery_verification_pending)
    )
