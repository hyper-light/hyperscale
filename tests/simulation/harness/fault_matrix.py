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
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import psutil

from tests.simulation.harness.errors import HarnessError
from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


class FaultError(HarnessError):
    """A fault primitive failed mid-flight."""


@dataclass(slots=True)
class _PartitionRule:
    """One bidirectional partition between two node-id groups.

    A send from any node in ``group_a`` to any node in ``group_b``,
    or vice versa, is dropped (returns synthetic timeout). The
    matrix can hold multiple ``_PartitionRule`` instances — they
    OR together, so overlapping partitions are additive.
    """

    group_a: frozenset[str]
    group_b: frozenset[str]


@dataclass(slots=True)
class _DelayRule:
    """One delay rule between optionally-wildcarded src/dst.

    ``src`` / ``dst`` of ``None`` matches any node. The harness
    applies the most-specific rule (both src and dst named) over
    less-specific (one or both wildcarded).
    """

    src: str | None
    dst: str | None
    delay_ms: float
    jitter_ms: float = 0.0
    started_at: float = 0.0
    target_delay_ms: float | None = None
    duration_seconds: float = 0.0
    expires_at: float | None = None


@dataclass(slots=True)
class _DropRule:
    """One drop-rate rule between optionally-wildcarded src/dst.

    Same matching semantics as ``_DelayRule``.
    """

    src: str | None
    dst: str | None
    probability: float
    expires_at: float | None = None


@dataclass(slots=True)
class _BandwidthRule:
    """One token-bucket bandwidth cap between optionally-wildcarded endpoints."""

    src: str | None
    dst: str | None
    bytes_per_second: float
    burst_bytes: float
    available_tokens: float
    last_refill_at: float


@dataclass(slots=True)
class _ReorderRule:
    """One synthetic reordering rule, defaulting to UDP-only realism."""

    src: str | None
    dst: str | None
    probability: float
    delay_ms: float
    jitter_ms: float
    protocol: Literal["udp", "tcp", "both"]


@dataclass(slots=True)
class _DuplicateRule:
    """One duplicate-delivery rule for datagram-style traffic."""

    src: str | None
    dst: str | None
    probability: float
    copies: int
    protocol: Literal["udp", "tcp", "both"]


@dataclass(slots=True)
class _TcpResetRule:
    """One TCP reset rule, optionally scoped by action and count."""

    src: str | None
    dst: str | None
    probability: float
    action: str | None
    remaining: int | None = None


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
    _partitions: list[_PartitionRule]
    _delays: list[_DelayRule]
    _drops: list[_DropRule]
    _bandwidth_caps: list[_BandwidthRule]
    _reorders: list[_ReorderRule]
    _duplicates: list[_DuplicateRule]
    _tcp_resets: list[_TcpResetRule]
    _resource_overrides: dict[str, tuple[Callable[[], float], Callable[[], float]]]
    _suspended_processes: dict[int, psutil.Process]

    def __init__(self, harness: "ClusterHarness") -> None:
        self.harness = harness
        self._killed = set()
        self._paused = {}
        self._partitions = []
        self._delays = []
        self._drops = []
        self._bandwidth_caps = []
        self._reorders = []
        self._duplicates = []
        self._tcp_resets = []
        self._resource_overrides = {}
        self._suspended_processes = {}

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
        if hasattr(instance, "abort_and_wait"):
            # Terminal-abort barrier: ``abort_and_wait`` flips
            # ``_running``, closes transports, *and* awaits cancellation
            # of every pending UDP/TCP response task before returning.
            # Without this, a SUSPECT handler that was spawned just
            # before the kill can still complete and emit an ALIVE /
            # refutation response via the captured transport reference,
            # which the cluster treats as authoritative liveness
            # evidence — the dead worker refutes its own death.
            await instance.abort_and_wait()
        elif hasattr(instance, "abort"):
            instance.abort()
            # Two loop yields to give the synchronous abort path a
            # chance to drain in-flight handlers via the post-abort
            # ``_running`` guards. Older builds without ``abort_and_wait``
            # still benefit from the same barrier semantics, just less
            # reliably.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
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
        # Phase 4: re-install the fault-injecting transport on the
        # rebuilt instance. The original methods on the new instance
        # are unwrapped; without this, partition / delay / drop rules
        # would silently stop applying to the restarted node.
        from tests.simulation.harness import fault_transport

        fault_transport.reinstall_for(handle, self.harness)

    async def kill_many(self, handles: list[ServerHandle]) -> None:
        """Abruptly kill every handle concurrently."""
        await asyncio.gather(*(self.kill(handle) for handle in handles))

    async def prepare_graceful_drain(
        self,
        handles: list[ServerHandle],
        reason: str = "simulation_graceful_stop",
    ) -> None:
        """Tell managers that workers are draining before local stop begins."""
        worker_ids_by_dc: dict[str, set[str]] = {}

        for handle in handles:
            self._require_known(handle)
            if handle.kind != ServerKind.WORKER:
                continue
            if handle.node_id in self._killed or not handle.started:
                continue

            node_id = getattr(handle.instance, "_node_id", None)
            worker_id = getattr(node_id, "full", handle.node_id)
            worker_ids_by_dc.setdefault(handle.dc_id, set()).add(worker_id)

        drain_tasks = []
        for dc_id, worker_ids in worker_ids_by_dc.items():
            for manager in self.harness.managers(dc_id):
                if not manager.started:
                    continue
                prepare = getattr(manager.instance, "prepare_workers_for_drain", None)
                if prepare is None:
                    continue
                drain_tasks.append(prepare(worker_ids, reason))

        if drain_tasks:
            await asyncio.gather(*drain_tasks)

    async def graceful_stop_many(
        self,
        handles: list[ServerHandle],
        drain_timeout: float = 5.0,
        window_seconds: float = 0.0,
    ) -> None:
        """Gracefully stop handles after batch-publishing drain intent."""
        await self.prepare_graceful_drain(handles)

        async def stop_after_delay(handle: ServerHandle, delay_seconds: float) -> None:
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            await self.graceful_stop(
                handle,
                drain_timeout=drain_timeout,
                prepare_drain=False,
            )

        handle_count = len(handles)
        async with asyncio.TaskGroup() as task_group:
            for handle_index, handle in enumerate(handles):
                delay_seconds = 0.0
                if window_seconds > 0.0 and handle_count > 0:
                    delay_seconds = handle_index * window_seconds / handle_count
                task_group.create_task(stop_after_delay(handle, delay_seconds))

    async def graceful_stop(
        self,
        handle: ServerHandle,
        drain_timeout: float = 5.0,
        prepare_drain: bool = True,
    ) -> None:
        """Gracefully stop ``handle`` while broadcasting a leave event."""
        self._require_known(handle)
        if handle.node_id in self._killed:
            return
        if not handle.started:
            raise FaultError(
                f"graceful_stop({handle.node_id}): node has not been started"
            )
        if prepare_drain:
            await self.prepare_graceful_drain([handle])
        await handle.instance.stop(
            drain_timeout=drain_timeout,
            broadcast_leave=True,
        )
        handle.started = False
        self._killed.add(handle.node_id)
        await asyncio.sleep(0.05)

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

        # Install a transport-level partition isolating the paused node
        # from every other harness-managed node. The receive path on
        # the paused server is still live (REAL mode can't freeze it
        # without a real SIGSTOP), so without this peers' probes would
        # still get acked and SWIM would never mark the node DEAD.
        # Wrapping send_tcp/send_udp at every other node as "drop to
        # paused_node_id" is the closest REAL-mode approximation of a
        # frozen receive path: probes go nowhere, peers time out, the
        # failure detector escalates SUSPECT → DEAD, and the reaper
        # removes the paused node from active_manager_peer_ids on each
        # surviving peer. ``resume()`` removes only this rule, leaving
        # any user-installed partitions intact.
        other_ids = frozenset(
            h.node_id
            for h in self.harness.all_handles()
            if h.node_id != handle.node_id
        )
        isolation_rule = _PartitionRule(
            group_a=frozenset({handle.node_id}),
            group_b=other_ids,
        )
        self._partitions.append(isolation_rule)

        self._paused[handle.node_id] = _PausedState(
            handle=handle, isolation_rule=isolation_rule
        )

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

        # Remove only the isolation rule we installed during pause(),
        # leaving any user-installed partitions intact.
        if state.isolation_rule is not None:
            try:
                self._partitions.remove(state.isolation_rule)
            except ValueError:
                # Rule already removed (e.g. by clear_network_faults
                # or heal_partition); resume should still proceed.
                pass

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

        # Re-run the production manager TCP registration handshake before
        # SWIM JOIN. Manager registration is the authoritative address-
        # reuse signal that clears stale DEAD/incarnation state on peers.
        if handle.kind is ServerKind.MANAGER and hasattr(
            instance, "_register_with_peer_managers"
        ):
            await instance._register_with_peer_managers()

        # Worker: re-run the production worker→manager TCP registration
        # handshake after the pause partition is lifted. SWIM ALIVE gossip
        # alone cannot restore a worker once managers have detached its
        # WorkerRegistration and address mapping; worker_register is the
        # authoritative recovery path for capacity, identity, and pool state.
        if handle.kind is ServerKind.WORKER:
            await instance.refresh_manager_registrations()

        # Re-announce membership to same-kind SWIM peers. While the
        # node was paused (and its peers' send_udp wrappers blocked by
        # the isolation rule), peers' SWIM probes timed out and the
        # node was marked DEAD. SWIM's ``probe_scheduler.remove_member``
        # ran on confirm, so peers stop probing the dead node — meaning
        # passive gossip alone won't surface the resumption. The
        # production join handler re-arms the recovery path
        # (incarnation refresh → ``update_node_state`` → DEAD→OK
        # callbacks → ``_handle_manager_peer_recovery`` →
        # ``_active_manager_peer_ids`` re-add).
        #
        # Restricting to same-kind peers avoids confusing role-aware
        # join validation (e.g. a manager joining a worker as a seed
        # exercises a path the production code doesn't normally take
        # and yields inconsistent recovery on the worker side).
        if hasattr(instance, "join_cluster"):
            for peer_handle in self.harness.all_handles():
                if peer_handle.node_id == handle.node_id:
                    continue
                if peer_handle.kind is not handle.kind:
                    continue
                if not peer_handle.started:
                    continue
                seed_addr = (peer_handle.host, peer_handle.udp_port)
                try:
                    await instance.join_cluster(seed_addr, timeout=2.0)
                except Exception:
                    # Soft pause is best-effort; if a single peer
                    # can't process the join it's fine — others will.
                    continue

    # =========================================================================
    # Network faults — partition / delay / drop / reorder / reset (Phase 4)
    # =========================================================================

    async def partition(
        self,
        group_a: list[ServerHandle] | list[str],
        group_b: list[ServerHandle] | list[str],
    ) -> None:
        """Install a bidirectional partition between two node groups.

        After this returns, sends from any node in ``group_a`` to any
        node in ``group_b`` (and vice versa) are dropped — the
        sender sees a synthetic ``asyncio.TimeoutError`` matching
        the production on-error contract. Existing transports stay
        open, but every message in flight or attempted across the
        partition fails fast.

        Multiple partitions compose: a node may participate in more
        than one ``_PartitionRule``. Drop happens if *any* rule
        matches — partitions OR together.

        Asymmetric partitions are not yet a separate primitive:
        call ``partition([a], [b])`` for symmetric, then to model
        asymmetric drop add a ``drop_rate(src=b, dst=a, probability=1.0)``
        without the matching ``a → b`` rule.
        """
        ids_a = frozenset(self._normalize(group_a))
        ids_b = frozenset(self._normalize(group_b))
        if ids_a & ids_b:
            raise FaultError(
                f"partition groups overlap: {sorted(ids_a & ids_b)}"
            )
        self._partitions.append(_PartitionRule(group_a=ids_a, group_b=ids_b))

    async def heal_partition(self) -> None:
        """Remove every partition rule installed via ``partition()``.

        Drop-rate and delay rules are unaffected — call
        ``clear_network_faults()`` if you want a complete reset.
        """
        self._partitions.clear()

    async def delay(
        self,
        ms: float,
        *,
        src: ServerHandle | str | None = None,
        dst: ServerHandle | str | None = None,
        jitter_ms: float = 0.0,
    ) -> None:
        """Add a one-way ``ms``-millisecond delay on (src, dst).

        ``src`` / ``dst`` of ``None`` is a wildcard. Multiple rules
        compose: the matcher picks the most-specific rule (both
        src and dst named beats one wildcard beats both wildcards),
        and within the same specificity the most-recently-installed
        rule wins.

        ``jitter_ms`` adds uniform random jitter on top of ``ms``;
        each send rolls independently.
        """
        if ms < 0.0:
            raise FaultError(f"delay ms must be non-negative; got {ms}")
        if jitter_ms < 0.0:
            raise FaultError(f"jitter_ms must be non-negative; got {jitter_ms}")
        self._delays.append(
            _DelayRule(
                src=self._normalize_one(src),
                dst=self._normalize_one(dst),
                delay_ms=ms,
                jitter_ms=jitter_ms,
                started_at=time.monotonic(),
            )
        )

    async def latency_drift(
        self,
        *,
        start_ms: float,
        end_ms: float,
        duration_seconds: float,
        src: ServerHandle | str | None = None,
        dst: ServerHandle | str | None = None,
        jitter_ms: float = 0.0,
    ) -> None:
        """Add a one-way delay that linearly drifts over ``duration_seconds``.

        This models slowly degrading RTT without scheduling a background
        task. The active delay is computed at send time from the rule's
        monotonic install timestamp, so clearing faults is immediate and
        there are no orphaned timers.
        """
        if start_ms < 0.0 or end_ms < 0.0:
            raise FaultError(
                f"latency drift endpoints must be non-negative; got "
                f"start_ms={start_ms}, end_ms={end_ms}"
            )
        if duration_seconds <= 0.0:
            raise FaultError(
                f"duration_seconds must be positive; got {duration_seconds}"
            )
        if jitter_ms < 0.0:
            raise FaultError(f"jitter_ms must be non-negative; got {jitter_ms}")
        self._delays.append(
            _DelayRule(
                src=self._normalize_one(src),
                dst=self._normalize_one(dst),
                delay_ms=start_ms,
                jitter_ms=jitter_ms,
                started_at=time.monotonic(),
                target_delay_ms=end_ms,
                duration_seconds=duration_seconds,
            )
        )

    async def drop_rate(
        self,
        probability: float,
        *,
        src: ServerHandle | str | None = None,
        dst: ServerHandle | str | None = None,
    ) -> None:
        """Drop sends from ``src`` to ``dst`` with the given probability.

        Wildcards work the same way as ``delay``. ``probability=1.0``
        is equivalent to a one-way partition (only this direction
        drops; the reverse direction continues to flow).

        Compositional with ``partition()``: a partition is checked
        first (binary deterministic block); if not partitioned, the
        drop_rate dice roll runs.
        """
        if not (0.0 <= probability <= 1.0):
            raise FaultError(
                f"drop probability must be in [0, 1]; got {probability}"
            )
        self._drops.append(
            _DropRule(
                src=self._normalize_one(src),
                dst=self._normalize_one(dst),
                probability=probability,
            )
        )

    async def drop_burst(
        self,
        *,
        duration_seconds: float,
        src: ServerHandle | str | None = None,
        dst: ServerHandle | str | None = None,
        probability: float = 1.0,
    ) -> None:
        """Drop matching sends for a bounded wall-clock window.

        The burst expires lazily during send-rule lookup and summary
        inspection, avoiding scheduled cleanup tasks while preserving a
        precise fault window.
        """
        if duration_seconds <= 0.0:
            raise FaultError(
                f"duration_seconds must be positive; got {duration_seconds}"
            )
        if not (0.0 <= probability <= 1.0):
            raise FaultError(
                f"drop probability must be in [0, 1]; got {probability}"
            )
        self._drops.append(
            _DropRule(
                src=self._normalize_one(src),
                dst=self._normalize_one(dst),
                probability=probability,
                expires_at=time.monotonic() + duration_seconds,
            )
        )

    async def bandwidth_cap(
        self,
        *,
        bytes_per_second: float,
        src: ServerHandle | str | None = None,
        dst: ServerHandle | str | None = None,
        burst_bytes: float | None = None,
    ) -> None:
        """Throttle matching sends with a token bucket.

        The cap is evaluated synchronously at send time and returns a
        synthetic delay for the caller to await. This models a saturated
        link without spinning a queue manager or background refill task.
        """
        if bytes_per_second <= 0.0:
            raise FaultError(
                f"bytes_per_second must be positive; got {bytes_per_second}"
            )
        normalized_burst_bytes = (
            bytes_per_second if burst_bytes is None else burst_bytes
        )
        if normalized_burst_bytes <= 0.0:
            raise FaultError(f"burst_bytes must be positive; got {burst_bytes}")
        now = time.monotonic()
        self._bandwidth_caps.append(
            _BandwidthRule(
                src=self._normalize_one(src),
                dst=self._normalize_one(dst),
                bytes_per_second=bytes_per_second,
                burst_bytes=normalized_burst_bytes,
                available_tokens=normalized_burst_bytes,
                last_refill_at=now,
            )
        )

    async def reorder(
        self,
        *,
        probability: float,
        delay_ms: float,
        src: ServerHandle | str | None = None,
        dst: ServerHandle | str | None = None,
        jitter_ms: float = 0.0,
        protocol: Literal["udp", "tcp", "both"] = "udp",
    ) -> None:
        """Delay a subset of matching sends so later packets can overtake them.

        Real TCP streams preserve byte order, so the default protocol is
        ``"udp"``. Tests may explicitly pass ``"tcp"`` or ``"both"`` when
        modeling application-level RPC reordering across separate calls.
        """
        if not (0.0 <= probability <= 1.0):
            raise FaultError(
                f"reorder probability must be in [0, 1]; got {probability}"
            )
        if delay_ms < 0.0:
            raise FaultError(f"delay_ms must be non-negative; got {delay_ms}")
        if jitter_ms < 0.0:
            raise FaultError(f"jitter_ms must be non-negative; got {jitter_ms}")
        self._validate_protocol(protocol)
        self._reorders.append(
            _ReorderRule(
                src=self._normalize_one(src),
                dst=self._normalize_one(dst),
                probability=probability,
                delay_ms=delay_ms,
                jitter_ms=jitter_ms,
                protocol=protocol,
            )
        )

    async def duplicate(
        self,
        *,
        probability: float,
        copies: int = 1,
        src: ServerHandle | str | None = None,
        dst: ServerHandle | str | None = None,
        protocol: Literal["udp", "tcp", "both"] = "udp",
    ) -> None:
        """Deliver extra copies of matching sends.

        Defaulting to UDP mirrors real networks: datagrams can duplicate,
        while TCP byte streams cannot. Duplicate TCP RPCs remain available
        for explicit application-level retry tests.
        """
        if not (0.0 <= probability <= 1.0):
            raise FaultError(
                f"duplicate probability must be in [0, 1]; got {probability}"
            )
        if copies < 1:
            raise FaultError(f"copies must be >= 1; got {copies}")
        self._validate_protocol(protocol)
        self._duplicates.append(
            _DuplicateRule(
                src=self._normalize_one(src),
                dst=self._normalize_one(dst),
                probability=probability,
                copies=copies,
                protocol=protocol,
            )
        )

    async def tcp_reset(
        self,
        *,
        probability: float = 1.0,
        src: ServerHandle | str | None = None,
        dst: ServerHandle | str | None = None,
        action: str | None = None,
        count: int | None = None,
    ) -> None:
        """Inject TCP connection resets for matching RPC attempts.

        ``count`` bounds how many resets the rule can produce. ``None``
        means the rule remains active until ``clear_network_faults``.
        """
        if not (0.0 <= probability <= 1.0):
            raise FaultError(
                f"tcp reset probability must be in [0, 1]; got {probability}"
            )
        if count is not None and count < 1:
            raise FaultError(f"count must be >= 1 when provided; got {count}")
        self._tcp_resets.append(
            _TcpResetRule(
                src=self._normalize_one(src),
                dst=self._normalize_one(dst),
                probability=probability,
                action=action,
                remaining=count,
            )
        )

    async def clear_network_faults(self) -> None:
        """Wipe every active network fault rule. ``kill`` /
        ``pause`` lifecycle state is unaffected."""
        self._partitions.clear()
        self._delays.clear()
        self._drops.clear()
        self._bandwidth_caps.clear()
        self._reorders.clear()
        self._duplicates.clear()
        self._tcp_resets.clear()

    # =========================================================================
    # Resource / subprocess faults — Phase 3 resource-pressure primitives
    # =========================================================================

    async def inject_worker_resources(
        self,
        handle: ServerHandle,
        *,
        cpu_percent: float | None = None,
        memory_percent: float | None = None,
    ) -> None:
        """Override a worker's CPU/memory samples until cleared."""
        self._require_worker(handle, "inject_worker_resources")
        instance = handle.instance
        if handle.node_id not in self._resource_overrides:
            self._resource_overrides[handle.node_id] = (
                instance._get_cpu_percent,
                instance._get_memory_percent,
            )
        original_cpu, original_memory = self._resource_overrides[handle.node_id]

        def get_cpu_percent() -> float:
            return cpu_percent if cpu_percent is not None else original_cpu()

        def get_memory_percent() -> float:
            return (
                memory_percent
                if memory_percent is not None
                else original_memory()
            )

        instance._get_cpu_percent = get_cpu_percent
        instance._get_memory_percent = get_memory_percent
        instance._backpressure_manager.set_resource_getters(
            get_cpu_percent,
            get_memory_percent,
        )
        await asyncio.sleep(0)

    async def clear_worker_resource_injection(self, handle: ServerHandle) -> None:
        """Restore worker resource getters after ``inject_worker_resources``."""
        self._require_worker(handle, "clear_worker_resource_injection")
        originals = self._resource_overrides.pop(handle.node_id, None)
        if originals is None:
            return
        original_cpu, original_memory = originals
        handle.instance._get_cpu_percent = original_cpu
        handle.instance._get_memory_percent = original_memory
        handle.instance._backpressure_manager.set_resource_getters(
            original_cpu,
            original_memory,
        )
        await asyncio.sleep(0)

    async def inject_event_loop_lag(
        self,
        handle: ServerHandle,
        *,
        critical: bool = False,
        repeats: int = 1,
    ) -> None:
        """Inject event-loop lag callbacks into a node's health pipeline."""
        self._require_known(handle)
        if repeats < 1:
            raise FaultError(f"repeats must be >= 1; got {repeats}")
        for _repeat_index in range(repeats):
            if critical:
                await handle.instance._on_event_loop_critical(1.0)
            else:
                await handle.instance._on_event_loop_lag(1.0)

    async def crash_worker_subprocess(
        self,
        handle: ServerHandle,
        *,
        index: int = 0,
    ) -> int:
        """Terminate one worker-pool child process and return its PID."""
        pid = self._select_worker_pid(handle, index, "crash_worker_subprocess")
        process = psutil.Process(pid)
        process.terminate()
        if await self._wait_for_process_exit(pid, timeout=2.0):
            return pid

        if psutil.pid_exists(pid):
            try:
                process.kill()
            except psutil.NoSuchProcess:
                return pid

        await self._wait_for_process_exit(pid, timeout=2.0)
        return pid

    async def hang_worker_subprocess(
        self,
        handle: ServerHandle,
        *,
        index: int = 0,
    ) -> int:
        """Suspend one worker-pool child process and return its PID."""
        pid = self._select_worker_pid(handle, index, "hang_worker_subprocess")
        process = psutil.Process(pid)
        process.suspend()
        self._suspended_processes[pid] = process
        await asyncio.sleep(0)
        return pid

    async def resume_worker_subprocess(self, pid: int) -> None:
        """Resume a worker-pool child suspended by ``hang_worker_subprocess``."""
        process = self._suspended_processes.pop(pid, None)
        if process is None:
            if not psutil.pid_exists(pid):
                return
            process = psutil.Process(pid)
        try:
            process.resume()
        except psutil.NoSuchProcess:
            return
        await asyncio.sleep(0)

    def is_partitioned(self, src_node_id: str, dst_node_id: str) -> bool:
        """True iff any installed partition rule blocks this pair.

        Symmetric: a rule with groups (a, b) blocks both a → b and
        b → a.
        """
        for rule in self._partitions:
            if (
                src_node_id in rule.group_a and dst_node_id in rule.group_b
            ) or (
                src_node_id in rule.group_b and dst_node_id in rule.group_a
            ):
                return True
        return False

    def drop_probability(
        self, src_node_id: str, dst_node_id: str
    ) -> float:
        """Return the most-specific drop probability for this pair.

        Returns 0.0 (never drop) when no rule matches.
        """
        self._prune_expired_network_rules()
        rule = self._most_specific(self._drops, src_node_id, dst_node_id)
        return 0.0 if rule is None else rule.probability

    def delay_seconds(
        self,
        src_node_id: str,
        dst_node_id: str,
        rng: random.Random,
    ) -> float:
        """Return the most-specific delay for this pair, in seconds.

        Applies jitter via the supplied ``rng`` so the per-source
        randomness stays deterministic per-test when the harness
        seeds it.
        """
        self._prune_expired_network_rules()
        rule = self._most_specific(self._delays, src_node_id, dst_node_id)
        if rule is None:
            return 0.0
        delay_ms = self._current_delay_ms(rule)
        if rule.jitter_ms > 0.0:
            delay_ms += rng.uniform(0.0, rule.jitter_ms)
        return delay_ms / 1000.0

    def bandwidth_delay_seconds(
        self,
        src_node_id: str,
        dst_node_id: str,
        payload_size_bytes: int,
    ) -> float:
        """Return the token-bucket delay for a payload on this link."""
        rule = self._most_specific(
            self._bandwidth_caps,
            src_node_id,
            dst_node_id,
        )
        if rule is None:
            return 0.0

        now = time.monotonic()
        elapsed = max(0.0, now - rule.last_refill_at)
        rule.available_tokens = min(
            rule.burst_bytes,
            rule.available_tokens + elapsed * rule.bytes_per_second,
        )
        rule.last_refill_at = now

        payload_size = max(0, payload_size_bytes)
        if rule.available_tokens >= payload_size:
            rule.available_tokens -= payload_size
            return 0.0

        missing_tokens = payload_size - rule.available_tokens
        rule.available_tokens = 0.0
        return missing_tokens / rule.bytes_per_second

    def reorder_delay_seconds(
        self,
        kind: str,
        src_node_id: str,
        dst_node_id: str,
        rng: random.Random,
    ) -> float:
        """Return extra hold time for a packet selected for reordering."""
        rule = self._most_specific_protocol(
            self._reorders,
            kind,
            src_node_id,
            dst_node_id,
        )
        if rule is None or rng.random() >= rule.probability:
            return 0.0
        delay_ms = rule.delay_ms
        if rule.jitter_ms > 0.0:
            delay_ms += rng.uniform(0.0, rule.jitter_ms)
        return delay_ms / 1000.0

    def duplicate_count(
        self,
        kind: str,
        src_node_id: str,
        dst_node_id: str,
        rng: random.Random,
    ) -> int:
        """Return how many additional copies should be delivered."""
        rule = self._most_specific_protocol(
            self._duplicates,
            kind,
            src_node_id,
            dst_node_id,
        )
        if rule is None or rng.random() >= rule.probability:
            return 0
        return rule.copies

    def should_reset_tcp(
        self,
        src_node_id: str,
        dst_node_id: str,
        action: str,
        rng: random.Random,
    ) -> bool:
        """Return whether a matching TCP reset should fire now."""
        for rule in reversed(self._tcp_resets):
            if rule.action is not None and rule.action != action:
                continue
            if rule.src is not None and rule.src != src_node_id:
                continue
            if rule.dst is not None and rule.dst != dst_node_id:
                continue
            if rule.remaining == 0:
                continue
            if rng.random() >= rule.probability:
                continue
            if rule.remaining is not None:
                rule.remaining -= 1
            return True
        return False

    @staticmethod
    def _most_specific(
        rules: list,
        src_node_id: str,
        dst_node_id: str,
    ):
        """Pick the most-specific matching rule.

        Specificity score: 2 = both fields named; 1 = one wildcard;
        0 = both wildcards. Ties broken by insertion order: the
        most-recently-added rule wins. This keeps the API
        compositional — a scenario can install a wildcard baseline
        then override for specific pairs.
        """
        best = None
        best_score = -1
        for rule in rules:
            score = 0
            if rule.src is not None:
                if rule.src != src_node_id:
                    continue
                score += 1
            if rule.dst is not None:
                if rule.dst != dst_node_id:
                    continue
                score += 1
            if score >= best_score:
                best = rule
                best_score = score
        return best

    @staticmethod
    def _most_specific_protocol(
        rules: list,
        kind: str,
        src_node_id: str,
        dst_node_id: str,
    ):
        protocol_rules = [
            rule
            for rule in rules
            if rule.protocol == "both" or rule.protocol == kind
        ]
        return FaultMatrix._most_specific(protocol_rules, src_node_id, dst_node_id)

    @staticmethod
    def _current_delay_ms(rule: _DelayRule) -> float:
        if rule.target_delay_ms is None or rule.duration_seconds <= 0.0:
            return rule.delay_ms
        elapsed = max(0.0, time.monotonic() - rule.started_at)
        progress = min(1.0, elapsed / rule.duration_seconds)
        return rule.delay_ms + (rule.target_delay_ms - rule.delay_ms) * progress

    @staticmethod
    def _validate_protocol(protocol: str) -> None:
        if protocol not in {"udp", "tcp", "both"}:
            raise FaultError(
                f"protocol must be 'udp', 'tcp', or 'both'; got {protocol!r}"
            )

    def _prune_expired_network_rules(self) -> None:
        now = time.monotonic()
        self._drops = [
            rule
            for rule in self._drops
            if rule.expires_at is None or rule.expires_at > now
        ]
        self._delays = [
            rule
            for rule in self._delays
            if rule.expires_at is None or rule.expires_at > now
        ]

    @staticmethod
    def _normalize_one(
        identifier: ServerHandle | str | None,
    ) -> str | None:
        if identifier is None:
            return None
        if isinstance(identifier, ServerHandle):
            return identifier.node_id
        return identifier

    @staticmethod
    def _normalize(
        items: list[ServerHandle] | list[str],
    ) -> list[str]:
        return [
            (item.node_id if isinstance(item, ServerHandle) else item)
            for item in items
        ]

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

    def partition_count(self) -> int:
        return len(self._partitions)

    def network_fault_summary(self) -> dict[str, int]:
        self._prune_expired_network_rules()
        return {
            "partitions": len(self._partitions),
            "delays": len(self._delays),
            "drops": len(self._drops),
            "bandwidth_caps": len(self._bandwidth_caps),
            "reorders": len(self._reorders),
            "duplicates": len(self._duplicates),
            "tcp_resets": len(
                [
                    rule
                    for rule in self._tcp_resets
                    if rule.remaining is None or rule.remaining > 0
                ]
            ),
        }

    # =========================================================================
    # Helpers
    # =========================================================================

    def _require_known(self, handle: ServerHandle) -> None:
        if handle.node_id not in self.harness._handles_by_id:
            raise FaultError(
                f"unknown handle {handle.node_id!r}; not registered with "
                f"this harness"
            )

    def _require_worker(self, handle: ServerHandle, operation: str) -> None:
        self._require_known(handle)
        if handle.kind is not ServerKind.WORKER:
            raise FaultError(
                f"{operation} expects a WORKER handle; got {handle.kind}"
            )

    def _select_worker_pid(
        self,
        handle: ServerHandle,
        index: int,
        operation: str,
    ) -> int:
        self._require_worker(handle, operation)
        pids = sorted(self.harness.supervisor.tracked_pids(handle.node_id))
        if not pids:
            raise FaultError(
                f"{operation}({handle.node_id}): no worker subprocess PIDs tracked"
            )
        if index < 0 or index >= len(pids):
            raise FaultError(
                f"{operation}({handle.node_id}): index {index} out of range "
                f"for {len(pids)} tracked subprocesses"
            )
        return pids[index]

    async def _wait_for_process_exit(self, pid: int, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._process_is_gone_or_zombie(pid):
                return True

            await asyncio.sleep(0.05)

        return self._process_is_gone_or_zombie(pid)

    @staticmethod
    def _process_is_gone_or_zombie(pid: int) -> bool:
        try:
            return psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return True


@dataclass(slots=True)
class _PausedState:
    """Internal bookkeeping for a paused handle.

    ``isolation_rule`` is the partition rule installed by ``pause()`` to
    drop traffic to/from the paused node — without it the node's UDP
    receive path is still live and peers' probes still get acked, so
    SWIM never marks the node DEAD. Stored so ``resume()`` can remove
    *just* this rule without disturbing user-installed partitions.
    """

    handle: ServerHandle
    isolation_rule: "_PartitionRule | None" = None
