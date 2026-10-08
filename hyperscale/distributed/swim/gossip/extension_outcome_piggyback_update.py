"""``ExtensionOutcomePiggybackUpdate`` -- pickled under the namespace
``hyperscale.distributed.swim.gossip.extension_outcome_gossip_buffer`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass
from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeEvent


@dataclass(slots=True, kw_only=True)
class ExtensionOutcomePiggybackUpdate:
    """A single outcome event queued for SWIM piggyback.

    Mirror of ``ExtensionDecisionPiggybackUpdate`` (H7b) for the
    outcome channel. Tracks broadcast count per AD-48's
    lambda * log(n+1) budget.
    """

    event: ExtensionOutcomeEvent
    timestamp: float
    broadcast_count: int = 0
    max_broadcasts: int = 10

    def should_broadcast(self) -> bool:
        return self.broadcast_count < self.max_broadcasts

    def mark_broadcast(self) -> None:
        self.broadcast_count += 1
