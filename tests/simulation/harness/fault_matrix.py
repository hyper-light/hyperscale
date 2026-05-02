"""
FaultMatrix — lifecycle-fault primitives for the simulation harness.

Phase 3 deliverable. Provides four operations against a registered
``ServerHandle``:

* ``kill(handle)``       — abrupt teardown, no graceful drain. Models a
                           SIGKILL on the server process. Existing
                           transports are aborted; outstanding RPCs to
                           the killed node will RST.
* ``restart(handle)``    — kill + reconstruct + start. The new instance
                           keeps the same host/ports/peers but gets a
                           fresh ``NodeId`` (different ``random``
                           component, hence a higher incarnation).
                           Models a process crash that the orchestrator
                           brings back up.
* ``pause(handle)``      — stop the server's outbound activity loops
                           (probe rounds, heartbeats, election ticks,
                           background dispatch) without freeing
                           transports. Models a SIGSTOP — the OS hasn't
                           closed sockets, but the process makes no
                           progress. Peers will eventually mark it
                           SUSPECT then DEAD.
* ``resume(handle)``     — restart the loops paused by ``pause``. The
                           refuting node bumps its incarnation on
                           receipt of stale suspicion so peers reconcile.

REAL-mode fidelity: servers run in-process, not as subprocesses, so
"SIGKILL" is approximated by ``instance.abort()`` (immediate transport
close, all server-owned tasks cancelled). True OS-level kill semantics
arrive in Phase 6 SIM mode where the deterministic scheduler can model
process boundaries explicitly.

Invariants the harness enforces around fault operations:

* Every ``kill`` / ``pause`` reserves the node's ports — ``restart`` /
  ``resume`` reuses them. The supervisor's port verifier won't see them
  as free until the harness finally tears down.
* The supervisor's leak detector ignores tasks owned by killed nodes
  during cleanup; ``abort()`` already cancels them.
"""

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tests.simulation.harness.errors import HarnessError
from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


class FaultError(HarnessError):
    """A fault primitive failed mid-flight."""


@dataclass(slots=True)
class FaultMatrix:
    """Stateful fault injector scoped to a single ``ClusterHarness``.

    The matrix tracks which handles are currently in a non-running state
    (killed, paused) so callers can query and restart selectively. It
    delegates the actual abort/start operations to the underlying server
    instance and only manages the bookkeeping.
    """

    harness: "ClusterHarness"
    _killed: set[str]
    _paused: dict[str, "_PausedState"]

    def __init__(self, harness: "ClusterHarness") -> None:
        self.harness = harness
        self._killed = set()
        self._paused = {}

    # =========================================================================
    # Kill / restart
    # =========================================================================

    async def kill(self, handle: ServerHandle) -> None:
        """Abrupt teardown of ``handle``. No graceful drain.

        After this returns the server's transports are closed, all
        background tasks the server owns are cancelled, and the handle's
        ``started`` flag is False. ``restart(handle)`` will rebuild and
        re-start it; until then the node is dark to the rest of the
        cluster.
        """
        self._require_known(handle)
        if handle.node_id in self._killed:
            return
        if not handle.started:
            raise FaultError(
                f"kill({handle.node_id}): node has not been started; "
                f"there is nothing to kill"
            )
        instance = handle.instance
        if hasattr(instance, "abort"):
            instance.abort()
        else:
            # Fall back to graceful stop for any non-base server kind
            # that hasn't surfaced abort() yet. Acceptable because the
            # only difference is a shorter drain window.
            await instance.stop(drain_timeout=0.0, broadcast_leave=False)
        handle.started = False
        self._killed.add(handle.node_id)

    async def restart(self, handle: ServerHandle) -> None:
        """Bring a killed node back. Construction is fresh.

        Builds a new server instance from the handle's captured
        ``builder`` closure (so the new instance gets a fresh
        ``NodeId`` and incarnation), starts it, and rebinds the
        handle. Other harness components hold the handle, not the
        instance, so they pick up the new instance transparently.
        """
        self._require_known(handle)
        if handle.node_id not in self._killed and handle.started:
            raise FaultError(
                f"restart({handle.node_id}): node is currently running. "
                f"Call kill() first."
            )
        if handle.builder is None:
            raise FaultError(
                f"restart({handle.node_id}): builder not captured. The "
                f"handle was constructed before Phase-3 builder support "
                f"landed; rebuild via the cluster harness."
            )
        new_instance = handle.builder()
        await new_instance.start()
        handle.instance = new_instance
        handle.started = True
        self._killed.discard(handle.node_id)

    # =========================================================================
    # Pause / resume
    # =========================================================================

    async def pause(self, handle: ServerHandle) -> None:
        """Stop outbound activity without freeing transports.

        Models a SIGSTOP: the process doesn't make progress but the OS
        still owns its sockets. Peers will probe, get no response,
        suspect, then declare the node DEAD after the suspicion timeout.
        ``resume()`` reverses this; the refuting node bumps its
        incarnation on first receipt of stale suspicion gossip.

        Implementation: cancels the SWIM probe cycle, the leader-election
        loop, the workflow-dispatcher loops (manager only), and the
        worker pool's heartbeat (worker only). The receive path stays
        live, so paused nodes still consume RAM (transports open) — the
        difference from ``kill()`` is observability: pauses are
        reversible.
        """
        self._require_known(handle)
        if handle.node_id in self._paused:
            return
        if not handle.started:
            raise FaultError(
                f"pause({handle.node_id}): node has not been started"
            )

        instance = handle.instance
        cancelled: list[asyncio.Task] = []

        # SWIM probe cycle (every node kind that runs SWIM)
        probe_task = getattr(instance, "_probe_task", None)
        if isinstance(probe_task, asyncio.Task) and not probe_task.done():
            probe_task.cancel()
            cancelled.append(probe_task)

        # Leader election (managers + gates that elect)
        election = getattr(instance, "_leader_election", None)
        if election is not None:
            await election.stop()

        # Cleanup loop (every node)
        cleanup = getattr(instance, "_cleanup_task", None)
        if isinstance(cleanup, asyncio.Task) and not cleanup.done():
            cleanup.cancel()
            cancelled.append(cleanup)

        # Manager-only background loops we'd want frozen
        for attr in (
            "_dead_node_reap_task",
            "_orphan_scan_task",
            "_discovery_maintenance_task",
            "_gate_heartbeat_task",
            "_unified_timeout_task",
        ):
            task = getattr(instance, attr, None)
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
                cancelled.append(task)

        # Drain in parallel; ignore CancelledError (expected)
        if cancelled:
            await asyncio.gather(*cancelled, return_exceptions=True)

        self._paused[handle.node_id] = _PausedState(handle=handle)

    async def resume(self, handle: ServerHandle) -> None:
        """Restart the loops cancelled by ``pause()``.

        Re-arms the SWIM probe cycle and leader-election loop directly.
        Per-kind background tasks (orphan scans, dead-node reaps, etc.)
        recover via their own restart hooks the next time the manager
        becomes leader or processes a heartbeat — no explicit re-arm
        needed at this layer. If a paused node missed enough probes to
        be declared DEAD, the cluster will treat its resumption as a
        new node joining (with a refuted incarnation), which is exactly
        the contract.
        """
        self._require_known(handle)
        state = self._paused.pop(handle.node_id, None)
        if state is None:
            raise FaultError(
                f"resume({handle.node_id}): node is not paused"
            )

        instance = handle.instance

        # Re-start probe cycle. Different kinds expose this through
        # different attributes — fall back to start_probe_cycle which
        # every SWIM-aware server has.
        if hasattr(instance, "start_probe_cycle") and hasattr(
            instance, "_task_runner"
        ):
            instance._task_runner.run(instance.start_probe_cycle)

        # Re-start leader election.
        if hasattr(instance, "start_leader_election"):
            await instance.start_leader_election()

        # Manager: re-start all the background loops that pause cancelled.
        if handle.kind is ServerKind.MANAGER and hasattr(
            instance, "_start_background_tasks"
        ):
            instance._start_background_tasks()

    # =========================================================================
    # Inspection
    # =========================================================================

    def is_killed(self, handle: ServerHandle) -> bool:
        return handle.node_id in self._killed

    def is_paused(self, handle: ServerHandle) -> bool:
        return handle.node_id in self._paused

    def killed_nodes(self) -> list[str]:
        return sorted(self._killed)

    def paused_nodes(self) -> list[str]:
        return sorted(self._paused)

    # =========================================================================
    # Helpers
    # =========================================================================

    def _require_known(self, handle: ServerHandle) -> None:
        if handle.node_id not in self.harness._handles_by_id:
            raise FaultError(
                f"unknown handle {handle.node_id!r}; not registered with "
                f"this harness"
            )


@dataclass(slots=True)
class _PausedState:
    """Internal bookkeeping for a paused handle."""

    handle: ServerHandle
