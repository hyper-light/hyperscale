"""
Failure detector — SWIM + phi-accrual hybrid (AD-52 §8).

Two complementary detectors running concurrently:

  - SWIM (existing implementation at hyperscale.distributed.swim)
    drives cluster-wide convergence: who is ALIVE, SUSPECT, DEAD,
    TOMBSTONE. SWIM events trigger Raft REMOVE proposals after a
    tombstone retention window (default 10 min).

  - Phi-accrual (cluster.phi_accrual.PhiAccrualDetector, per peer-edge)
    drives per-edge decisions: circuit-breaker trips, locality-aware
    routing (AD-36), and SWIM probe-budget allocation.

This module is the composition root that holds both, observes their
outputs, and yields Decision objects the caller acts on:

  - DeclareSuspect(node_id)            — SWIM transition
  - DeclareDead(node_id)               — SWIM transition
  - ProposeRemove(node_id, reason)     — tombstone window expired
  - CircuitBreakerTrip(node_id)        — phi > threshold
  - CircuitBreakerReset(node_id)       — phi back under warning

Outside the deterministic apply layer (AD-52 §15) so monotonic time is
fine here. The Raft REMOVE proposal that follows is on the apply layer
and uses log-supplied timestamps.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from .phi_accrual import PhiAccrualConfig, PhiAccrualDetector

if TYPE_CHECKING:
    pass


_DEFAULT_PHI_FAILURE_THRESHOLD: float = 8.0
_DEFAULT_PHI_WARNING_THRESHOLD: float = 4.0
_DEFAULT_TOMBSTONE_RETENTION_SECONDS: float = 10 * 60.0


class SwimStateTransition(str, Enum):
    """SWIM state events the detector emits to the caller."""

    NONE = "none"
    DECLARED_SUSPECT = "declared_suspect"
    DECLARED_DEAD = "declared_dead"
    PROPOSE_REMOVE = "propose_remove"
    REVIVED = "revived"


@dataclass(slots=True)
class PeerDetectionState:
    """Per-peer state held by the failure detector."""

    node_id: str
    phi_detector: PhiAccrualDetector
    swim_state: str = "alive"
    declared_suspect_at_monotonic: float | None = None
    declared_dead_at_monotonic: float | None = None
    last_circuit_breaker_state: str = "closed"


@dataclass(frozen=True, slots=True)
class DetectionDecision:
    """One emitted decision from the detector."""

    node_id: str
    swim_transition: SwimStateTransition = SwimStateTransition.NONE
    circuit_breaker_to: str | None = None
    phi_value: float = 0.0


class FailureDetector:
    """
    Composes SWIM gossip events (fed in via on_swim_event) and phi-accrual
    per-edge values (fed in via on_heartbeat). Emits DetectionDecision
    objects on tick().
    """

    __slots__ = (
        "_phi_failure_threshold",
        "_phi_warning_threshold",
        "_tombstone_retention_seconds",
        "_peers",
        "_phi_config",
    )

    def __init__(
        self,
        phi_failure_threshold: float = _DEFAULT_PHI_FAILURE_THRESHOLD,
        phi_warning_threshold: float = _DEFAULT_PHI_WARNING_THRESHOLD,
        tombstone_retention_seconds: float = _DEFAULT_TOMBSTONE_RETENTION_SECONDS,
        phi_config: PhiAccrualConfig | None = None,
    ) -> None:
        if phi_warning_threshold >= phi_failure_threshold:
            raise ValueError(
                "phi_warning_threshold must be < phi_failure_threshold"
            )

        self._phi_failure_threshold = phi_failure_threshold
        self._phi_warning_threshold = phi_warning_threshold
        self._tombstone_retention_seconds = tombstone_retention_seconds
        self._peers: dict[str, PeerDetectionState] = {}
        self._phi_config = phi_config or PhiAccrualConfig()

    def track_peer(
        self,
        node_id: str,
        baseline_rtt_ms: float | None = None,
    ) -> None:
        """Start tracking a peer. Idempotent."""
        if node_id in self._peers:
            return
        self._peers[node_id] = PeerDetectionState(
            node_id=node_id,
            phi_detector=PhiAccrualDetector(
                config=self._phi_config,
                baseline_rtt_ms=baseline_rtt_ms,
            ),
        )

    def untrack_peer(self, node_id: str) -> None:
        self._peers.pop(node_id, None)

    def on_heartbeat(self, node_id: str) -> None:
        """Record a heartbeat for a tracked peer. No-op for unknown
        peers (e.g., one that just left the membership table)."""
        state = self._peers.get(node_id)
        if state is None:
            return
        state.phi_detector.heartbeat()
        # An arriving heartbeat resets the SWIM state to alive (the SWIM
        # layer will confirm via its own gossip).
        if state.swim_state in ("suspect", "dead"):
            state.swim_state = "alive"
            state.declared_suspect_at_monotonic = None
            state.declared_dead_at_monotonic = None

    def on_swim_event(self, node_id: str, new_state: str) -> None:
        """Feed an external SWIM state change in. The detector mirrors
        it and starts the tombstone timer on first DEAD."""
        state = self._peers.get(node_id)
        if state is None:
            return
        now_monotonic = time.monotonic()
        if new_state == "suspect" and state.swim_state != "suspect":
            state.swim_state = "suspect"
            state.declared_suspect_at_monotonic = now_monotonic
        elif new_state == "dead" and state.swim_state != "dead":
            state.swim_state = "dead"
            state.declared_dead_at_monotonic = now_monotonic
        elif new_state == "alive":
            state.swim_state = "alive"
            state.declared_suspect_at_monotonic = None
            state.declared_dead_at_monotonic = None

    def tick(self) -> list[DetectionDecision]:
        """
        Sweep all peers. Returns decisions: tombstone-window expirations
        → ProposeRemove; phi threshold crossings → circuit breaker
        transitions.
        """
        decisions: list[DetectionDecision] = []
        now_monotonic = time.monotonic()
        # Deterministic ordering for replay debuggability.
        for node_id in sorted(self._peers.keys()):
            state = self._peers[node_id]
            self._evaluate_circuit_breaker(state, decisions)
            self._evaluate_tombstone(state, now_monotonic, decisions)
        return decisions

    def known_peers(self) -> frozenset[str]:
        return frozenset(self._peers.keys())

    def _evaluate_circuit_breaker(
        self,
        state: PeerDetectionState,
        decisions: list[DetectionDecision],
    ) -> None:
        phi_value = state.phi_detector.phi()
        if phi_value >= self._phi_failure_threshold:
            new_breaker_state = "open"
        elif phi_value >= self._phi_warning_threshold:
            new_breaker_state = "half_open"
        else:
            new_breaker_state = "closed"
        if new_breaker_state != state.last_circuit_breaker_state:
            state.last_circuit_breaker_state = new_breaker_state
            decisions.append(
                DetectionDecision(
                    node_id=state.node_id,
                    circuit_breaker_to=new_breaker_state,
                    phi_value=phi_value,
                )
            )

    def _evaluate_tombstone(
        self,
        state: PeerDetectionState,
        now_monotonic: float,
        decisions: list[DetectionDecision],
    ) -> None:
        if state.swim_state != "dead":
            return
        if state.declared_dead_at_monotonic is None:
            return
        elapsed = now_monotonic - state.declared_dead_at_monotonic
        if elapsed >= self._tombstone_retention_seconds:
            decisions.append(
                DetectionDecision(
                    node_id=state.node_id,
                    swim_transition=SwimStateTransition.PROPOSE_REMOVE,
                )
            )
            # Reset the timer so we don't emit ProposeRemove repeatedly.
            # The caller is expected to propose Remove via Raft; once
            # committed, on_membership_remove() clears the peer entirely.
            state.declared_dead_at_monotonic = None

    def on_membership_remove(self, node_id: str) -> None:
        """Caller signals that Raft committed Remove for this peer."""
        self.untrack_peer(node_id)
