"""
Federated Health Monitor for cross-cluster health probing.

Gates use this to monitor datacenter manager clusters that are globally
distributed. Uses a SWIM-style probe/ack mechanism but with:
- Higher latency tolerance (50-300ms RTT)
- Longer suspicion timeouts (30s)
- No gossip exchange (irrelevant across clusters)
- Aggregate health responses from DC leaders

This is NOT cluster membership - just health monitoring using probe/ack.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Awaitable
from hyperscale.distributed.models import Message
from hyperscale.distributed.swim.core.protocols import LoggerProtocol
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.logging.hyperscale_logging_models import ServerError

from .federated_health_monitor_shared import _DEFAULT_CLOCK
from .cross_cluster_ack import CrossClusterAck
from .cross_cluster_probe import CrossClusterProbe
from .dc_health_state import DCHealthState
from .dc_leader_announcement import DCLeaderAnnouncement
from .dc_reachability import DCReachability


@dataclass(slots=True)
class FederatedHealthMonitor:
    """
    Monitors external datacenter clusters using SWIM-style probes.

    NOT a SWIM cluster member - uses probe/ack for health detection
    with separate incarnation tracking and suspicion state.

    Designed for high-latency, globally distributed links:
    - Longer probe intervals (2s default)
    - Longer suspicion timeouts (30s default)
    - Higher failure tolerance before marking unreachable
    """

    # Probe configuration (tuned for global distribution)
    probe_interval: float = 2.0  # Seconds between probes to each DC
    probe_timeout: float = 5.0  # Timeout for single probe
    suspicion_timeout: float = 30.0  # Time before suspected -> unreachable
    max_consecutive_failures: int = 5  # Failures before suspected

    # Identity
    cluster_id: str = ""
    node_id: str = ""

    # Callbacks (set by owner)
    _send_udp: Callable[[tuple[str, int], bytes], Awaitable[bool]] | None = None
    _on_dc_health_change: Callable[[str, str], None] | None = None  # (dc, new_health)
    _on_dc_latency: Callable[[str, float], None] | None = (
        None  # (dc, latency_ms) - Phase 7
    )
    _on_dc_leader_change: (
        Callable[[str, str, tuple[str, int], tuple[str, int], int], None] | None
    ) = None  # (dc, leader_node_id, tcp_addr, udp_addr, term)
    on_probe_error: Callable[[str, list[str]], Awaitable[None]] | None = None

    # State
    _dc_health: dict[str, DCHealthState] = field(default_factory=dict)
    _running: bool = False
    _probe_task: asyncio.Task | None = None

    # Logging
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0

    def set_callbacks(
        self,
        send_udp: Callable[[tuple[str, int], bytes], Awaitable[bool]],
        cluster_id: str,
        node_id: str,
        on_dc_health_change: Callable[[str, str], None] | None = None,
        on_dc_latency: Callable[[str, float], None] | None = None,
        on_dc_leader_change: Callable[
            [str, str, tuple[str, int], tuple[str, int], int], None
        ]
        | None = None,
    ) -> None:
        """
        Set callback functions.

        Args:
            send_udp: Async function to send UDP packets.
            cluster_id: This gate's cluster ID.
            node_id: This gate's node ID.
            on_dc_health_change: Called when DC health changes (dc, new_health).
            on_dc_latency: Called with latency measurements (dc, latency_ms).
                Used for cross-DC correlation to distinguish network issues.
            on_dc_leader_change: Called when DC leader changes (dc, leader_node_id, tcp_addr, udp_addr, term).
                Used to propagate DC leadership changes to peer gates.
        """
        self._send_udp = send_udp
        self.cluster_id = cluster_id
        self.node_id = node_id
        self._on_dc_health_change = on_dc_health_change
        self._on_dc_latency = on_dc_latency
        self._on_dc_leader_change = on_dc_leader_change

    def set_logger(
        self,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
    ) -> None:
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port

    async def _log_error(self, message: str) -> None:
        if self._logger:

            await self._logger.log(
                ServerError(
                    message=message,
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self.node_id,
                )
            )

    def add_datacenter(
        self,
        datacenter: str,
        leader_udp_addr: tuple[str, int],
        leader_tcp_addr: tuple[str, int] | None = None,
        leader_node_id: str = "",
        leader_term: int = 0,
    ) -> None:
        """Add or update a datacenter to monitor."""
        if datacenter in self._dc_health:
            self._refresh_datacenter_leader(
                self._dc_health[datacenter],
                leader_udp_addr,
                leader_tcp_addr,
                leader_node_id,
                leader_term,
            )
        else:
            self._dc_health[datacenter] = DCHealthState(
                datacenter=datacenter,
                leader_udp_addr=leader_udp_addr,
                leader_tcp_addr=leader_tcp_addr,
                leader_node_id=leader_node_id,
                leader_term=leader_term,
            )

    def _refresh_datacenter_leader(
        self,
        state: DCHealthState,
        leader_udp_addr: tuple[str, int],
        leader_tcp_addr: tuple[str, int] | None,
        leader_node_id: str,
        leader_term: int,
    ) -> None:
        """Refresh a monitored DC's leader: its address, any given identity, and a higher term."""
        state.leader_udp_addr = leader_udp_addr
        self._apply_optional_leader_identity(state, leader_tcp_addr, leader_node_id)
        if leader_term > state.leader_term:
            state.leader_term = leader_term

    @staticmethod
    def _apply_optional_leader_identity(
        state: DCHealthState,
        leader_tcp_addr: tuple[str, int] | None,
        leader_node_id: str,
    ) -> None:
        """Record the leader's TCP address and node id, each only when given."""
        if leader_tcp_addr:
            state.leader_tcp_addr = leader_tcp_addr
        if leader_node_id:
            state.leader_node_id = leader_node_id

    def remove_datacenter(self, datacenter: str) -> None:
        """Stop monitoring a datacenter."""
        self._dc_health.pop(datacenter, None)

    def update_leader(
        self,
        datacenter: str,
        leader_udp_addr: tuple[str, int],
        leader_tcp_addr: tuple[str, int] | None = None,
        leader_node_id: str = "",
        leader_term: int = 0,
    ) -> bool:
        """
        Update DC leader address (from leader announcement).

        Returns True if leader actually changed (term is higher), False otherwise.
        """
        if datacenter not in self._dc_health:
            return self._add_announced_datacenter(
                datacenter,
                leader_udp_addr,
                leader_tcp_addr,
                leader_node_id,
                leader_term,
            )

        state = self._dc_health[datacenter]

        # Only update if term is higher (prevent stale updates)
        if leader_term < state.leader_term:
            return False

        return self._apply_leader_update(
            state,
            datacenter,
            leader_udp_addr,
            leader_tcp_addr,
            leader_node_id,
            leader_term,
        )

    def _add_announced_datacenter(
        self,
        datacenter: str,
        leader_udp_addr: tuple[str, int],
        leader_tcp_addr: tuple[str, int] | None,
        leader_node_id: str,
        leader_term: int,
    ) -> bool:
        """Start monitoring a DC first learned from a leader announcement; always a change."""
        self.add_datacenter(
            datacenter,
            leader_udp_addr,
            leader_tcp_addr,
            leader_node_id,
            leader_term,
        )
        # New DC is considered a change
        self._notify_leader_change(
            datacenter,
            leader_node_id,
            leader_tcp_addr,
            leader_udp_addr,
            leader_term,
        )
        return True

    def _notify_leader_change(
        self,
        datacenter: str,
        leader_node_id: str,
        leader_tcp_addr: tuple[str, int] | None,
        leader_udp_addr: tuple[str, int],
        leader_term: int,
    ) -> None:
        """Report a DC leader change, when a callback is set and the leader's TCP address is known."""
        if self._on_dc_leader_change and leader_tcp_addr:
            self._on_dc_leader_change(
                datacenter,
                leader_node_id,
                leader_tcp_addr,
                leader_udp_addr,
                leader_term,
            )

    def _apply_leader_update(
        self,
        state: DCHealthState,
        datacenter: str,
        leader_udp_addr: tuple[str, int],
        leader_tcp_addr: tuple[str, int] | None,
        leader_node_id: str,
        leader_term: int,
    ) -> bool:
        """Record a current-or-newer leader announcement; whether the leader changed."""
        previous_leader_node_id = state.leader_node_id
        previous_leader_udp_addr = state.leader_udp_addr

        # Check if this is an actual leader change (term increased or node changed)
        leader_changed = self._is_leader_change(state, leader_term, leader_node_id)

        self._store_leader_details(state, leader_udp_addr, leader_tcp_addr, leader_node_id, leader_term)

        self._reset_probe_state_if_leader_moved(
            state,
            leader_changed,
            previous_leader_node_id,
            previous_leader_udp_addr,
            leader_udp_addr,
        )

        # Fire callback if leader actually changed
        if leader_changed:
            self._notify_leader_change(
                datacenter,
                leader_node_id,
                leader_tcp_addr,
                leader_udp_addr,
                leader_term,
            )

        return leader_changed

    @staticmethod
    def _is_leader_change(state: DCHealthState, leader_term: int, leader_node_id: str) -> bool:
        """Whether an announcement raises the term or names another leader."""
        return (
            leader_term > state.leader_term or leader_node_id != state.leader_node_id
        )

    @staticmethod
    def _store_leader_details(
        state: DCHealthState,
        leader_udp_addr: tuple[str, int],
        leader_tcp_addr: tuple[str, int] | None,
        leader_node_id: str,
        leader_term: int,
    ) -> None:
        """Record an announced leader's addresses (TCP only when given), node id and term."""
        state.leader_udp_addr = leader_udp_addr
        if leader_tcp_addr:
            state.leader_tcp_addr = leader_tcp_addr
        state.leader_node_id = leader_node_id
        state.leader_term = leader_term

    def _reset_probe_state_if_leader_moved(
        self,
        state: DCHealthState,
        leader_changed: bool,
        previous_leader_node_id: str,
        previous_leader_udp_addr: tuple[str, int] | None,
        leader_udp_addr: tuple[str, int],
    ) -> None:
        """Reset probe state when the leader changed or moved address."""
        if leader_changed or previous_leader_udp_addr != leader_udp_addr:
            self._reset_probe_state_for_leader_change(
                state,
                previous_leader_node_id,
                previous_leader_udp_addr,
            )

    def _reset_probe_state_for_leader_change(
        self,
        state: DCHealthState,
        previous_leader_node_id: str,
        previous_leader_udp_addr: tuple[str, int] | None,
    ) -> None:
        """Clear negative probe state when probing moves to a different leader."""
        if (
            state.leader_node_id == previous_leader_node_id
            and state.leader_udp_addr == previous_leader_udp_addr
        ):
            return

        state.reachability = DCReachability.UNKNOWN
        state.last_probe_sent = 0.0
        state.last_ack_received = 0.0
        state.consecutive_failures = 0
        state.incarnation = 0
        state.last_ack = None
        state.suspected_at = 0.0

    def get_dc_health(self, datacenter: str) -> DCHealthState | None:
        """Get current health state for a datacenter."""
        return self._dc_health.get(datacenter)

    def get_all_dc_health(self) -> dict[str, DCHealthState]:
        """Get health state for all monitored datacenters."""
        return dict(self._dc_health)

    def get_healthy_datacenters(self) -> list[str]:
        """Get list of DCs that can accept jobs."""
        # Snapshot to avoid dict mutation during iteration
        return [
            dc
            for dc, state in list(self._dc_health.items())
            if state.is_healthy_for_jobs
        ]

    async def start(self) -> None:
        """Start the health monitoring probe loop."""
        self._running = True
        # Phase 6b: explicit ``loop.create_task`` so the task binds to
        # the loop ``start`` was called from rather than implicitly going
        # through ``get_running_loop`` at task-creation time.
        self._probe_task = asyncio.get_running_loop().create_task(
            self._probe_loop()
        )

    async def stop(self) -> None:
        """Stop the health monitoring probe loop."""
        self._running = False
        if self._probe_task:
            await self._await_cancelled_probe_task(self._probe_task)
            self._probe_task = None

    @staticmethod
    async def _await_cancelled_probe_task(probe_task: asyncio.Task) -> None:
        """Cancel the probe task and wait for it to end, re-raising only a cancel aimed at the caller."""
        probe_task.cancel()
        cancels_requested_before_wait = asyncio.current_task().cancelling()
        try:
            await probe_task
        except asyncio.CancelledError:
            # The task we cancelled ended; a cancel aimed at this task
            # while it waited goes on.
            if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                raise

    async def _probe_loop(self) -> None:
        """Main probe loop - probes all DCs in round-robin."""
        while self._running:
            await self._probe_loop_iteration()

    async def _probe_loop_iteration(self) -> None:
        """One probe round; an error is reported and waited out, a cancellation logged and re-raised."""
        try:
            await self._probe_round()

        except asyncio.CancelledError:
            await self._log_error("Probe loop cancelled")
            # Ended as cancelled, never as returned: a stop awaiting
            # this task must still see a cancel aimed at itself.
            raise
        except Exception as error:
            await self._report_probe_loop_error(error)
            await _DEFAULT_CLOCK.sleep(1.0)

    async def _probe_round(self) -> None:
        """Probe every monitored DC once, spreading the probe interval across them."""
        dcs = list(self._dc_health.keys())
        if not dcs:
            await _DEFAULT_CLOCK.sleep(self.probe_interval)
            return

        # Probe each DC with interval spread across all DCs
        interval_per_dc = self.probe_interval / len(dcs)

        await self._probe_datacenters(dcs, interval_per_dc)

    async def _probe_datacenters(self, dcs: list[str], interval_per_dc: float) -> None:
        """Probe each DC in turn, checking ack timeouts after each, until stopped."""
        for dc in dcs:
            if not self._running:
                break
            await self._probe_datacenter(dc)
            self._check_ack_timeouts()
            await _DEFAULT_CLOCK.sleep(interval_per_dc)

    async def _report_probe_loop_error(self, error: Exception) -> None:
        """Report a probe-loop error to the error callback, else log it."""
        if self.on_probe_error:
            await self._notify_probe_error(
                lambda: self.on_probe_error(
                    f"Federated health probe loop error: {error}",
                    list(self._dc_health.keys()),
                ),
                error,
            )
        else:
            await self._log_error(f"Probe loop error: {error}")

    async def _report_datacenter_probe_error(self, datacenter: str, error: Exception) -> None:
        """Report a failed probe to the error callback, else log it."""
        if self.on_probe_error:
            await self._notify_probe_error(
                lambda: self.on_probe_error(
                    f"Probe to {datacenter} failed: {error}",
                    [datacenter],
                ),
                error,
            )
        else:
            await self._log_error(f"Probe to {datacenter} failed: {error}")

    async def _notify_probe_error(
        self,
        notify: Callable[[], Awaitable[None]],
        error: Exception,
    ) -> None:
        """Run the error callback, logging the callback's own failure alongside the original error."""
        try:
            await notify()
        except Exception as callback_error:
            await self._log_error(
                f"on_probe_error callback failed: {callback_error}, original error: {error}"
            )

    async def _probe_datacenter(self, datacenter: str) -> None:
        """Send a probe to a datacenter's leader."""
        state = self._dc_health.get(datacenter)
        if not self._is_probeable(state):
            return

        await self._send_datacenter_probe(datacenter, state)

    def _is_probeable(self, state: DCHealthState | None) -> bool:
        """Whether a DC has a known leader address and a UDP sender is set."""
        return state and state.leader_udp_addr and self._send_udp

    async def _send_datacenter_probe(self, datacenter: str, state: DCHealthState) -> None:
        """Send one probe to the DC leader under the probe timeout, counting a failure."""
        # Build probe
        probe = CrossClusterProbe(
            source_cluster_id=self.cluster_id,
            source_node_id=self.node_id,
            source_addr=(self.node_id, 0),  # Will be filled by transport
        )

        state.last_probe_sent = _DEFAULT_CLOCK.monotonic()

        # Send probe (with timeout)
        try:
            probe_data = b"xprobe>" + probe.dump()
            success = await _DEFAULT_CLOCK.wait_for(
                self._send_udp(state.leader_udp_addr, probe_data),
                timeout=self.probe_timeout,
            )

            self._settle_probe_send(state, success)
        except asyncio.TimeoutError:
            self._handle_probe_failure(state)
        except Exception as error:
            self._handle_probe_failure(state)
            await self._report_datacenter_probe_error(datacenter, error)

    def _settle_probe_send(self, state: DCHealthState, success: bool) -> None:
        """Count a probe the transport reported unsent as a failure."""
        if not success:
            self._handle_probe_failure(state)

    def _check_ack_timeouts(self) -> None:
        """
        Check all DCs for ack timeout and transition to SUSPECTED/UNREACHABLE.

        This handles loss after a DC has already returned at least one ack. A
        never-acked DC remains UNKNOWN so "not yet established" is not treated
        as confirmed reachability failure.
        """
        now = _DEFAULT_CLOCK.monotonic()
        ack_grace_period = self.probe_timeout * self.max_consecutive_failures

        for state in self._dc_health.values():
            self._check_ack_timeout(state, now, ack_grace_period)

    def _check_ack_timeout(self, state: DCHealthState, now: float, ack_grace_period: float) -> None:
        """Escalate a once-acked, not yet unreachable DC whose last ack is older than the grace period."""
        if self._lacks_ack_baseline(state):
            return

        reference_time = state.last_ack_received

        time_since_reference = now - reference_time

        if time_since_reference > ack_grace_period:
            old_reachability = state.reachability

            self._advance_reachability(state, lambda: now)

            self._notify_reachability_change(state, old_reachability)

    @staticmethod
    def _lacks_ack_baseline(state: DCHealthState) -> bool:
        """Whether a DC is UNKNOWN or UNREACHABLE, or has never acked."""
        return state.reachability in (
            DCReachability.UNKNOWN,
            DCReachability.UNREACHABLE,
        ) or state.last_ack_received == 0.0

    def _advance_reachability(self, state: DCHealthState, read_now: Callable[[], float]) -> None:
        """REACHABLE becomes SUSPECTED; a SUSPECTED past the suspicion timeout becomes UNREACHABLE."""
        if state.reachability == DCReachability.REACHABLE:
            state.reachability = DCReachability.SUSPECTED
            state.suspected_at = read_now()
            return
        self._expire_suspicion(state, read_now)

    def _expire_suspicion(self, state: DCHealthState, read_now: Callable[[], float]) -> None:
        """Mark a DC suspected for longer than the suspicion timeout UNREACHABLE."""
        if state.reachability == DCReachability.SUSPECTED and read_now() - state.suspected_at > self.suspicion_timeout:
            state.reachability = DCReachability.UNREACHABLE

    def _notify_reachability_change(self, state: DCHealthState, old_reachability: DCReachability) -> None:
        """Report a DC's health when its reachability changed and a callback is set."""
        if state.reachability != old_reachability and self._on_dc_health_change:
            self._on_dc_health_change(state.datacenter, state.effective_health)

    def _handle_probe_failure(self, state: DCHealthState) -> None:
        state.consecutive_failures += 1

        old_reachability = state.reachability

        if self._is_unestablished(state):
            return

        if state.consecutive_failures >= self.max_consecutive_failures:
            self._advance_reachability(state, _DEFAULT_CLOCK.monotonic)

        self._notify_reachability_change(state, old_reachability)

    @staticmethod
    def _is_unestablished(state: DCHealthState) -> bool:
        """Whether a DC was never successfully probed and is still UNKNOWN."""
        return (
            state.reachability == DCReachability.UNKNOWN
            and not state.has_successful_probe
        )

    def handle_ack(self, ack: CrossClusterAck) -> None:
        """Handle an xack response from a DC leader."""
        state = self._dc_health.get(ack.datacenter)
        if self._ignores_ack(state, ack):
            return

        self._accept_ack(state, ack)

    def _ignores_ack(self, state: DCHealthState | None, ack: CrossClusterAck) -> bool:
        """Whether an ack is for an unmonitored DC, from a non-leader, or stale."""
        return not state or not ack.is_leader or self._is_stale_ack(state, ack)

    @staticmethod
    def _is_stale_ack(state: DCHealthState, ack: CrossClusterAck) -> bool:
        """Whether an ack carries an older leader term or incarnation."""
        # Check incarnation for staleness (a stale ack is ignored)
        return ack.leader_term < state.leader_term or ack.incarnation < state.incarnation

    def _accept_ack(self, state: DCHealthState, ack: CrossClusterAck) -> None:
        """Mark the DC reachable from a current leader ack, recording latency and leader."""
        old_reachability = state.reachability
        old_health = state.effective_health

        now = _DEFAULT_CLOCK.monotonic()

        # Calculate latency for cross-DC correlation (Phase 7)
        # Latency = time between sending probe and receiving ack
        self._report_ack_latency(state, ack, now)

        # Update state
        state.incarnation = ack.incarnation
        state.last_ack_received = now
        state.last_ack = ack
        state.consecutive_failures = 0
        state.reachability = DCReachability.REACHABLE

        # Update leader info from ack
        if ack.is_leader:
            state.leader_node_id = ack.node_id
            state.leader_term = ack.leader_term

        # Notify on change
        self._notify_ack_health_change(state, old_reachability, old_health)

    def _report_ack_latency(self, state: DCHealthState, ack: CrossClusterAck, now: float) -> None:
        """Report the probe round trip an ack closes (Phase 7), when a latency callback is set."""
        if state.last_probe_sent > 0 and self._on_dc_latency:
            latency_ms = (now - state.last_probe_sent) * 1000
            self._on_dc_latency(ack.datacenter, latency_ms)

    def _notify_ack_health_change(
        self,
        state: DCHealthState,
        old_reachability: DCReachability,
        old_health: str,
    ) -> None:
        """Report the DC's health when the ack changed its reachability or health."""
        new_health = state.effective_health
        if self._health_changed(state, old_reachability, old_health, new_health) and self._on_dc_health_change:
            self._on_dc_health_change(state.datacenter, new_health)

    @staticmethod
    def _health_changed(
        state: DCHealthState,
        old_reachability: DCReachability,
        old_health: str,
        new_health: str,
    ) -> bool:
        """Whether reachability or effective health differs from before."""
        return state.reachability != old_reachability or new_health != old_health

_REHOMED = (
    DCReachability,
    CrossClusterProbe,
    CrossClusterAck,
    DCLeaderAnnouncement,
    DCHealthState,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
