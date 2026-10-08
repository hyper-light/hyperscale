"""``ExtensionDecisionPiggybackUpdate`` -- pickled under the namespace
``hyperscale.distributed.swim.gossip.extension_decision_gossip_buffer`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass
from hyperscale.distributed.health.extension_ledger import ExtensionDecisionEvent


@dataclass(slots=True, kw_only=True)
class ExtensionDecisionPiggybackUpdate:
    """A single ``ExtensionDecisionEvent`` queued for SWIM piggyback.

    Tracks broadcast count per AD-48 so each event leaves the
    buffer once it's been disseminated λ × log(n+1) times. Mirrors
    ``WorkerStatePiggybackUpdate`` exactly so the two channels
    share the same bookkeeping discipline.
    """

    event: ExtensionDecisionEvent
    timestamp: float
    broadcast_count: int = 0
    max_broadcasts: int = 10

    def should_broadcast(self) -> bool:
        return self.broadcast_count < self.max_broadcasts

    def mark_broadcast(self) -> None:
        self.broadcast_count += 1
