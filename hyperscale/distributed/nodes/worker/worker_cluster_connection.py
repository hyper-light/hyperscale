"""``WorkerClusterConnection`` -- pickled under the namespace
``hyperscale.distributed.nodes.worker.cluster_connection`` (see that module)."""

from typing import TYPE_CHECKING
import asyncio
from typing import Awaitable, Callable
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning
from hyperscale.distributed.runtime import Clock, RealClock, Random, RealRandom

from .cluster_connection_state import ClusterConnectionState

if TYPE_CHECKING:
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.distributed.taskex.run import Run
    from hyperscale.logging import Logger

_DEFAULT_CLOCK: Clock = RealClock()

_DEFAULT_RANDOM: Random = RealRandom()


class WorkerClusterConnection:
    """Owns the worker's cluster-membership invariant and recovery.

    Constructed once on worker startup. Every code path that mutates
    ``_healthy_manager_ids`` (in either direction) signals this
    component by calling ``update()`` — synchronous, idempotent. The
    component then derives the current state and starts/stops the
    rejoin task as needed. In addition, the worker's heartbeat
    handler signals every received manager heartbeat via
    ``record_heartbeat`` so the staleness watchdog has data to act
    on.

    Recovery primitive is the existing ``_register_with_manager`` TCP
    handshake — no new protocol surface. Backoff uses the existing
    LHM multiplier so a stressed worker doesn't pile re-registration
    pressure onto an already-fragile cluster.
    """

    def __init__(
        self,
        seed_manager_tcp_addrs: list[tuple[str, int]],
        register_with_manager: Callable[[tuple[str, int]], Awaitable[bool]],
        get_lhm_multiplier: Callable[[], float],
        get_healthy_manager_ids: Callable[[], set[str]],
        mark_manager_unhealthy: Callable[[str], Awaitable[None]],
        invalidate_tcp_client: Callable[[tuple[str, int]], None],
        task_runner: "TaskRunner",
        logger: "Logger",
        node_host: str,
        node_port: int,
        node_id_short: str,
        liveness_check_interval_seconds: float,
        heartbeat_staleness_threshold_seconds: float,
        rejoin_base_backoff_seconds: float,
        rejoin_jitter_min_seconds: float = 0.0,
        rejoin_jitter_max_seconds: float = 0.0,
    ) -> None:
        self._seed_manager_tcp_addrs: list[tuple[str, int]] = list(
            seed_manager_tcp_addrs
        )
        self._register_with_manager: Callable[
            [tuple[str, int]], Awaitable[bool]
        ] = register_with_manager
        self._get_lhm_multiplier: Callable[[], float] = get_lhm_multiplier
        self._get_healthy_manager_ids: Callable[
            [], set[str]
        ] = get_healthy_manager_ids
        self._mark_manager_unhealthy: Callable[
            [str], Awaitable[None]
        ] = mark_manager_unhealthy
        self._invalidate_tcp_client: Callable[
            [tuple[str, int]], None
        ] = invalidate_tcp_client
        self._liveness_check_interval_seconds: float = liveness_check_interval_seconds
        self._heartbeat_staleness_threshold_seconds: float = (
            heartbeat_staleness_threshold_seconds
        )
        self._rejoin_base_backoff_seconds: float = rejoin_base_backoff_seconds
        self._rejoin_jitter_min_seconds: float = rejoin_jitter_min_seconds
        self._rejoin_jitter_max_seconds: float = max(
            rejoin_jitter_min_seconds,
            rejoin_jitter_max_seconds,
        )
        self._task_runner: "TaskRunner" = task_runner
        self._logger: "Logger" = logger
        self._node_host: str = node_host
        self._node_port: int = node_port
        self._node_id_short: str = node_id_short

        self._state: ClusterConnectionState = ClusterConnectionState.CONNECTING
        # ``TaskRunner.run`` returns a ``Run`` (taskex's wrapper), not
        # an ``asyncio.Task``. The Run exposes ``completed`` /
        # ``cancelled`` / ``failed`` properties for status and an
        # async ``cancel`` for teardown. ``_task_finished`` normalizes
        # the status check; ``_cancel_task`` awaits the cancel.
        self._rejoin_task = None
        self._liveness_task = None
        self._running: bool = True

        # Per-manager last heartbeat timestamp (monotonic). Populated
        # by ``record_heartbeat`` on every manager heartbeat we
        # successfully process, and on every successful
        # ``_register_with_manager`` response (registration is the
        # application-level handshake that proves the manager is both
        # reachable AND knows about us — strictly stronger evidence
        # than a SWIM probe response).
        self._manager_last_heartbeat: dict[str, float] = {}

    @property
    def state(self) -> ClusterConnectionState:
        """Current connection state (for introspection / diagnostics)."""
        return self._state

    def start(self) -> None:
        """Launch the periodic liveness watchdog.

        Must be called once after construction. Separated from
        ``__init__`` so the caller can finish wiring (the registry's
        change-signal hook, etc.) before the watchdog starts firing.
        """
        if self._task_is_running(self._liveness_task):
            return
        self._liveness_task = self._task_runner.run(
            self._liveness_watchdog,
            alias="worker_cluster_liveness_watchdog",
        )
        if self._isolated_with_seeds():
            self._transition_to(ClusterConnectionState.RECONNECTING)

    def _isolated_with_seeds(self) -> bool:
        """Whether no manager is healthy while seeds exist to rejoin through."""
        return bool(len(self._get_healthy_manager_ids()) == 0 and self._seed_manager_tcp_addrs)

    def record_heartbeat(self, manager_id: str) -> None:
        """Record that we received evidence of a live manager.

        Called by the heartbeat handler on each manager SWIM
        heartbeat, and by the registration handler on each successful
        ``_register_with_manager`` round-trip. Both signals
        independently prove the manager is reachable from our
        perspective. We don't require both — either is sufficient
        to reset the staleness clock.
        """
        self._manager_last_heartbeat[manager_id] = _DEFAULT_CLOCK.monotonic()

    def add_seed_manager(self, manager_tcp_addr: tuple[str, int]) -> None:
        """Adopt a manager joined at runtime as a rejoin seed.

        An operator join (``hyperscale join``) is as explicit as a boot
        seed, so the rejoin loop must fall back to it on isolation just
        like the managers configured at startup. Deduplicated, so the
        list grows only with distinct operator-chosen managers.
        """
        if manager_tcp_addr not in self._seed_manager_tcp_addrs:
            self._seed_manager_tcp_addrs.append(manager_tcp_addr)

    def adopt_cohort(self, cohort: frozenset[tuple[str, int]]) -> None:
        """The datacenter's managers, as its watched membership last showed
        them (AD-52 section 10), become the rejoin seeds: a manager a resize
        added is one to rejoin through, one it removed no longer is."""
        self._seed_manager_tcp_addrs = sorted(cohort)

    @property
    def seed_manager_tcp_addrs(self) -> list[tuple[str, int]]:
        return self._seed_manager_tcp_addrs

    def update(self) -> None:
        """Re-derive state from the live-manager set and transition.

        Called by every site that has just mutated the worker's
        healthy-manager set. Synchronous and idempotent — safe to
        invoke from any context, including registry-lock-holding
        callers (the rejoin task is *scheduled* via TaskRunner, not
        run inline). No-op once ``stop()`` has been called.
        """
        if not self._running:
            return

        # Side-effect on every update: seed a heartbeat timestamp for
        # any newly-healthy manager that doesn't yet have one. The
        # registry only adds a manager to ``_healthy_manager_ids`` via
        # explicit signals (registration response, AD-29 peer
        # confirmation, manager push-down) — every one of those is a
        # live application-level interaction. Recording a heartbeat
        # here means the staleness watchdog gives every newly-healthy
        # manager the full window from "now" before downgrading it.
        # Without this seed, a manager added via a path that doesn't
        # call ``record_heartbeat`` would be marked stale by the
        # watchdog as soon as the threshold elapses since process
        # start.
        healthy_ids = self._get_healthy_manager_ids()
        self._seed_missing_heartbeats(healthy_ids)
        self._transition_for_healthy_count(len(healthy_ids))

    def _seed_missing_heartbeats(self, healthy_ids: set[str]) -> None:
        """Give each newly-healthy manager a heartbeat stamp of now (see ``update``)."""
        now = _DEFAULT_CLOCK.monotonic()
        for manager_id in healthy_ids:
            if manager_id not in self._manager_last_heartbeat:
                self._manager_last_heartbeat[manager_id] = now

    def _transition_for_healthy_count(self, healthy_count: int) -> None:
        """Move to CONNECTED with live managers, or RECONNECTING after an isolation."""
        if healthy_count > 0:
            self._transition_to(ClusterConnectionState.CONNECTED)
        elif self._state == ClusterConnectionState.CONNECTING:
            # Still in startup; no healthy managers is the expected
            # initial state. Only transition to RECONNECTING once we
            # had at least one and then lost it (i.e. an actual
            # isolation event). The initial registration loop in
            # WorkerServer drives the first connection.
            return
        else:
            self._transition_to(ClusterConnectionState.RECONNECTING)

    async def stop(self) -> None:
        """Tear down the connection-tracker on worker shutdown.

        Cancels both background tasks and prevents further state
        transitions. ``update()`` becomes a no-op.
        """
        self._running = False
        await self._cancel_task_attribute("_rejoin_task")
        await self._cancel_task_attribute("_liveness_task")

    def _transition_to(self, new_state: ClusterConnectionState) -> None:
        if self._state == new_state:
            return

        old_state = self._state
        self._state = new_state
        self._enter_state(old_state, new_state)

    def _enter_state(
        self,
        old_state: ClusterConnectionState,
        new_state: ClusterConnectionState,
    ) -> None:
        """Run the side effects of entering ``new_state`` from ``old_state``."""
        if new_state == ClusterConnectionState.RECONNECTING:
            self._task_runner.run(self._log_isolation, old_state)
            self._start_rejoin_task()
        elif new_state == ClusterConnectionState.CONNECTED:
            self._enter_connected(old_state)

    def _enter_connected(self, old_state: ClusterConnectionState) -> None:
        """Log a recovery when CONNECTED is reached from RECONNECTING."""
        if old_state == ClusterConnectionState.RECONNECTING:
            # Recovery succeeded — the rejoin loop will observe
            # the state change on its next check and exit
            # voluntarily. We don't ``cancel()`` here because
            # cancelling mid-register-call leaves the response
            # handler racing the next dispatch. The task is
            # self-terminating and short-lived.
            self._task_runner.run(self._log_recovery)

    def _start_rejoin_task(self) -> None:
        if self._task_is_running(self._rejoin_task):
            return
        self._rejoin_task = self._task_runner.run(
            self._rejoin_loop,
            alias="worker_cluster_rejoin",
        )

    async def _cancel_task_attribute(self, attr_name: str) -> None:
        """Clear a task attribute and cancel the task it held if still running."""
        task = getattr(self, attr_name, None)
        setattr(self, attr_name, None)
        if not self._task_is_running(task):
            return
        await self._await_task_cancel(task)

    async def _await_task_cancel(self, task: "Run") -> None:
        """Cancel a running ``Run``, re-raising only a cancel aimed at this task."""
        cancels_requested_before_wait = asyncio.current_task().cancelling()
        try:
            await self._cancel_task(task)
        except asyncio.CancelledError:
            # The task we cancelled ended; a cancel aimed at this task
            # while it waited goes on.
            if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                raise

    @staticmethod
    async def _cancel_task(task: "Run") -> None:
        """Await a ``Run``'s cancel; a failure in the cancelled task is not ours."""
        try:
            await task.cancel()
        except Exception:
            pass

    def _task_is_running(self, task: "Run | None") -> bool:
        """Return True if ``task`` exists and is not yet finished."""
        return task is not None and not self._task_finished(task)

    @staticmethod
    def _task_finished(task) -> bool:
        """Return True if a ``TaskRunner`` ``Run`` is no longer active.

        ``Run.completed`` covers the success path and ``Run.cancelled``
        / ``Run.failed`` cover the unsuccessful terminations. The
        union of these is what we need: "do nothing further".
        """
        return bool(
            getattr(task, "completed", False)
            or getattr(task, "cancelled", False)
            or getattr(task, "failed", False)
        )

    async def _liveness_watchdog(self) -> None:
        """Scan registry-healthy managers for heartbeat staleness.

        Runs every ``liveness_check_interval_seconds``. For each
        registry-healthy manager, compares its
        ``_manager_last_heartbeat`` entry against
        ``heartbeat_staleness_threshold_seconds``. Stale managers are
        downgraded via ``mark_manager_unhealthy``, which fires the
        registry signal that drives ``update()``. The watchdog
        itself does not touch the state machine directly — it only
        feeds the existing one-way data flow (registry → update),
        keeping the transition logic concentrated in one place.

        A manager with no recorded heartbeat at all is treated as
        not-yet-stale until ``heartbeat_staleness_threshold_seconds``
        has elapsed since the watchdog itself started. This avoids
        marking freshly-registered managers stale before their first
        heartbeat round-trip.
        """
        watchdog_start = _DEFAULT_CLOCK.monotonic()
        try:
            while self._running:
                await _DEFAULT_CLOCK.sleep(self._liveness_check_interval_seconds)
                await self._liveness_pass(watchdog_start)
        except asyncio.CancelledError:
            return

    async def _liveness_pass(self, watchdog_start: float) -> None:
        """One watchdog scan: downgrade every stale registry-healthy manager."""
        if not self._running:
            return

        stale_managers = self._collect_stale_managers(_DEFAULT_CLOCK.monotonic(), watchdog_start)

        if not stale_managers:
            return

        await self._mark_stale_managers_unhealthy(stale_managers)

        await self._logger.log(
            ServerWarning(
                message=(
                    f"Liveness watchdog: marked {len(stale_managers)} "
                    f"manager(s) unhealthy for heartbeat staleness > "
                    f"{self._heartbeat_staleness_threshold_seconds}s"
                ),
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id_short,
            )
        )

    def _collect_stale_managers(self, now: float, watchdog_start: float) -> list[str]:
        """The registry-healthy managers whose heartbeat is past the staleness threshold."""
        return [
            manager_id
            for manager_id in list(self._get_healthy_manager_ids())
            if self._is_manager_stale(manager_id, now, watchdog_start)
        ]

    def _is_manager_stale(self, manager_id: str, now: float, watchdog_start: float) -> bool:
        """Whether a manager's last heartbeat (or the watchdog start) is past the threshold."""
        last_heartbeat = self._manager_last_heartbeat.get(manager_id)
        if last_heartbeat is None:
            # No heartbeat recorded yet. Treat the
            # watchdog start time as the reference so a
            # manager that never produces a heartbeat
            # *does* eventually get marked stale, but
            # only after the full threshold has passed
            # since *we* started watching.
            return not (now - watchdog_start < self._heartbeat_staleness_threshold_seconds)
        return now - last_heartbeat >= self._heartbeat_staleness_threshold_seconds

    async def _mark_stale_managers_unhealthy(self, stale_managers: list[str]) -> None:
        """Downgrade each stale manager, logging any that fail to downgrade."""
        # Mark stale managers unhealthy. Each call fires the
        # registry change-signal which calls back into
        # ``update()`` — this is the single transition path,
        # so the state machine reacts uniformly whether the
        # trigger came from SWIM, peer-confirmation, or here.
        for manager_id in stale_managers:
            try:
                await self._mark_manager_unhealthy(manager_id)
            except Exception as mark_error:
                await self._logger.log(
                    ServerWarning(
                        message=(
                            f"Liveness watchdog: failed to mark "
                            f"manager {manager_id[:8]}... unhealthy: "
                            f"{mark_error}"
                        ),
                        node_host=self._node_host,
                        node_port=self._node_port,
                        node_id=self._node_id_short,
                    )
                )

    async def _rejoin_loop(self) -> None:
        """Retry seed managers until the live-manager set recovers.

        On each iteration:
          1. Check we're still in RECONNECTING (state may have flipped
             via SWIM gossip / manager push-down — exit voluntarily).
          2. Try ``_register_with_manager`` against each seed in
             configured order. ``register_with_manager`` is
             responsible for the per-call timeout and retries against
             a single seed; we just orchestrate the per-pass order.
          3. After a full pass, sleep ``rejoin_base_backoff_seconds ×
             max(1, lhm_multiplier)`` so a stressed worker doesn't
             pile re-registration pressure on a fragile cluster.

        The loop has no fixed retry budget — it runs as long as the
        worker is isolated. Backoff is the throttle, ``stop()`` is
        the kill switch.
        """
        try:
            await self._run_rejoin_passes()
        except asyncio.CancelledError:
            return

    async def _run_rejoin_passes(self) -> None:
        """Run seed passes with backoff between them while still RECONNECTING."""
        while self._still_reconnecting():
            if await self._rejoin_pass():
                return
            await self._rejoin_backoff()

    def _still_reconnecting(self) -> bool:
        """Whether the tracker runs and is still in RECONNECTING."""
        return self._running and self._state == ClusterConnectionState.RECONNECTING

    async def _rejoin_pass(self) -> bool:
        """Try every seed once; True when the rejoin loop should exit."""
        for seed_addr in self._seed_manager_tcp_addrs:
            if await self._attempt_seed(seed_addr):
                return True

        # Full pass yielded no healthy manager — back off
        # before the next pass. LHM-scaled so the cadence
        # tracks the worker's self-health.
        return not self._still_reconnecting()

    async def _attempt_seed(self, seed_addr: tuple[str, int]) -> bool:
        """Re-register through one seed; True when the rejoin loop should exit."""
        if not self._still_reconnecting():
            return True
        # Drop any cached TCP client transport to this
        # seed before re-registering. We reach this code
        # only because the staleness watchdog has decided
        # the cluster (including this seed) is no longer
        # reachable at the application layer; a cached
        # transport that still reports ``is_closing() ==
        # False`` would otherwise route the new register
        # call back through whatever socket originally
        # accepted us — fatal in the kill→restart case
        # where a fresh process now listens on the same
        # address. Invalidating forces
        # ``_connect_tcp_client`` on the next send, which
        # establishes the connection against whoever is
        # listening *now*.
        self._invalidate_tcp_client(seed_addr)
        try:
            await self._register_with_manager(seed_addr)
        except Exception as register_error:
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Cluster-rejoin: register attempt against "
                        f"seed {seed_addr} raised: {register_error}"
                    ),
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id_short,
                )
            )
        # After each register attempt the response handler
        # may have already added a healthy manager —
        # re-check before moving to the next seed so we
        # exit as early as possible.
        return len(self._get_healthy_manager_ids()) > 0

    async def _rejoin_backoff(self) -> None:
        """Sleep the LHM-scaled, jittered backoff between rejoin passes."""
        backoff = self._rejoin_base_backoff_seconds * max(
            1.0, self._get_lhm_multiplier()
        )
        if self._rejoin_jitter_max_seconds > 0.0:
            backoff += _DEFAULT_RANDOM.uniform(
                self._rejoin_jitter_min_seconds,
                self._rejoin_jitter_max_seconds,
            )
        await _DEFAULT_CLOCK.sleep(backoff)

    async def _log_isolation(self, previous_state: ClusterConnectionState) -> None:
        await self._logger.log(
            ServerWarning(
                message=(
                    f"Worker cluster connection lost (previous={previous_state.value}); "
                    f"starting rejoin task against {len(self._seed_manager_tcp_addrs)} seed(s)"
                ),
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id_short,
            )
        )

    async def _log_recovery(self) -> None:
        await self._logger.log(
            ServerInfo(
                message="Worker cluster connection recovered; rejoin task exiting",
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id_short,
            )
        )
