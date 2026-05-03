"""Extension decision gossip buffer for AD-26 Phase H7b dissemination.

Mirrors ``WorkerStateGossipBuffer`` (AD-48 worker-state channel) but
carries ``ExtensionDecisionEvent`` records so peer managers learn
which workers/workflows have requested an AD-26 healthcheck
extension and the witness evidence behind every grant or denial.

Wire-format separator ``b"#|x"`` runs alongside the existing
membership ``#|h``, manager-state ``#|m``, vivaldi ``#|v``, and
worker-state ``#|w`` channels — extending the SWIM piggyback
multiplexer is a one-line addition in ``HealthAwareServer``.

Per-event broadcast budget follows AD-48's λ × log(n+1) formula so
the dissemination cost is sub-linear in cluster size. Idempotency
on ``event_id`` (workflow_id | fence_token | timestamp) keeps the
buffer's working set bounded even when multiple peers redundantly
re-disseminate the same event.
"""

from __future__ import annotations

import heapq
import math
import time
from dataclasses import dataclass, field
from typing import Any

from hyperscale.distributed.health.extension_ledger import (
    ExtensionDecisionEvent,
)


MAX_EXTENSION_DECISION_PIGGYBACK_SIZE: int = 800

EXTENSION_DECISION_SEPARATOR: bytes = b"#|x"

ENTRY_SEPARATOR: bytes = b"|"


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


@dataclass(slots=True)
class ExtensionDecisionGossipBuffer:
    """Buffer of ``ExtensionDecisionEvent`` to be piggybacked on SWIM.

    Identical strategy to ``WorkerStateGossipBuffer``:

    * ``add_event`` deduplicates on ``event_id`` so the same decision
      received from multiple peers occupies a single buffer slot.
    * ``encode_piggyback`` emits up to ``max_count`` events bounded
      by ``max_size`` bytes, prefixed with ``#|x`` so the receiver
      can dispatch on the multiplex tag.
    * ``decode_piggyback`` is the symmetric receiver-side helper.
    * Per-event broadcast budget = ``broadcast_multiplier × log(n+1)``
      where ``n`` is the cluster manager count.

    Thread-safety: NOT thread-safe. The owning ``HealthAwareServer``
    serializes through its existing piggyback lock.
    """

    updates: dict[str, ExtensionDecisionPiggybackUpdate] = field(
        default_factory=dict
    )
    broadcast_multiplier: int = 3
    max_updates: int = 500
    stale_age_seconds: float = 60.0
    max_piggyback_size: int = MAX_EXTENSION_DECISION_PIGGYBACK_SIZE

    _evicted_count: int = 0
    _stale_removed_count: int = 0
    _size_limited_count: int = 0
    _oversized_updates_count: int = 0
    _overflow_count: int = 0

    _on_overflow: Any = None

    def set_overflow_callback(self, callback: Any) -> None:
        self._on_overflow = callback

    def add_event(
        self, event: ExtensionDecisionEvent, number_of_managers: int = 1
    ) -> bool:
        """Queue an event for dissemination.

        Returns True if the buffer was modified (new event or a
        higher-leader-term replacement of an existing one).

        Same-``event_id`` re-adds with equal-or-lower
        ``leader_term`` are no-ops — the existing slot continues
        broadcasting unchanged. A higher-term version replaces the
        slot and resets the broadcast counter so the cluster sees
        the more-authoritative record.
        """
        event_id = event.event_id

        if event_id not in self.updates and len(self.updates) >= self.max_updates:
            self.cleanup_stale()
            self.cleanup_broadcast_complete()
            if len(self.updates) >= self.max_updates:
                self._evict_oldest()

        max_broadcasts = max(
            1, int(self.broadcast_multiplier * math.log(number_of_managers + 1))
        )

        existing = self.updates.get(event_id)
        if existing is None:
            self.updates[event_id] = ExtensionDecisionPiggybackUpdate(
                event=event,
                timestamp=time.monotonic(),
                max_broadcasts=max_broadcasts,
            )
            return True

        if event.leader_term > existing.event.leader_term:
            self.updates[event_id] = ExtensionDecisionPiggybackUpdate(
                event=event,
                timestamp=time.monotonic(),
                max_broadcasts=max_broadcasts,
            )
            return True

        return False

    def get_events_to_piggyback(
        self, max_count: int = 5
    ) -> list[ExtensionDecisionPiggybackUpdate]:
        max_count = max(1, min(max_count, 100))
        candidates = (u for u in self.updates.values() if u.should_broadcast())
        return heapq.nsmallest(max_count, candidates, key=lambda u: u.broadcast_count)

    def mark_broadcasts(
        self, updates: list[ExtensionDecisionPiggybackUpdate]
    ) -> None:
        for update in updates:
            event_id = update.event.event_id
            if event_id in self.updates:
                self.updates[event_id].mark_broadcast()
                if not self.updates[event_id].should_broadcast():
                    del self.updates[event_id]

    MAX_ENCODE_COUNT = 100

    def encode_piggyback(
        self,
        max_count: int = 5,
        max_size: int | None = None,
    ) -> bytes:
        """Encode a piggyback frame, ``#|x``-prefixed.

        Honors ``max_size`` (per-frame budget — typically the
        remaining UDP payload), skipping any single event larger
        than the cap and stopping the frame when the running total
        would exceed it.
        """
        max_count = max(1, min(max_count, self.MAX_ENCODE_COUNT))

        if max_size is None:
            max_size = self.max_piggyback_size

        events = self.get_events_to_piggyback(max_count)
        if not events:
            return b""

        result_parts: list[bytes] = []
        total_size = len(EXTENSION_DECISION_SEPARATOR)
        included_updates: list[ExtensionDecisionPiggybackUpdate] = []

        for piggyback_update in events:
            encoded = piggyback_update.event.to_bytes()
            update_size = len(encoded) + len(ENTRY_SEPARATOR)

            if update_size > max_size:
                self._oversized_updates_count += 1
                continue

            if total_size + update_size > max_size:
                self._size_limited_count += 1
                break

            result_parts.append(encoded)
            total_size += update_size
            included_updates.append(piggyback_update)

        if not result_parts:
            return b""

        self.mark_broadcasts(included_updates)
        return EXTENSION_DECISION_SEPARATOR + ENTRY_SEPARATOR.join(result_parts)

    def encode_piggyback_with_base(
        self,
        base_message: bytes,
        max_count: int = 5,
    ) -> bytes:
        """Encode bounded by the remaining UDP-payload budget."""
        from .gossip_buffer import MAX_UDP_PAYLOAD

        remaining = MAX_UDP_PAYLOAD - len(base_message)
        if remaining <= 0:
            return b""
        return self.encode_piggyback(max_count, max_size=remaining)

    MAX_DECODE_UPDATES = 100

    @classmethod
    def decode_piggyback(
        cls, data: bytes, max_updates: int = 100
    ) -> list[ExtensionDecisionEvent]:
        """Decode a ``#|x``-prefixed frame into events.

        Malformed events are silently skipped (defensive: a single
        corrupt entry shouldn't sink the whole frame). The caller
        feeds successful events into ``WorkerHealthManager.
        ingest_remote_decision_event`` which idempotently merges
        them into the H7a ledger.
        """
        if not data or not data.startswith(EXTENSION_DECISION_SEPARATOR):
            return []

        bounded_max = min(max_updates, cls.MAX_DECODE_UPDATES)
        events: list[ExtensionDecisionEvent] = []
        parts = data[len(EXTENSION_DECISION_SEPARATOR):].split(ENTRY_SEPARATOR)
        for part in parts:
            if len(events) >= bounded_max:
                break
            if part:
                event = ExtensionDecisionEvent.from_bytes(part)
                if event is not None:
                    events.append(event)
        return events

    def clear(self) -> None:
        self.updates.clear()

    def remove_event(self, event_id: str) -> bool:
        if event_id in self.updates:
            del self.updates[event_id]
            return True
        return False

    def _evict_oldest(self, count: int = 10) -> int:
        if not self.updates:
            return 0

        oldest = heapq.nsmallest(
            count, self.updates.items(), key=lambda x: x[1].timestamp
        )

        evicted = 0
        for event_id, _ in oldest:
            del self.updates[event_id]
            self._evicted_count += 1
            evicted += 1

        if evicted > 0:
            self._overflow_count += 1
            if self._on_overflow is not None:
                try:
                    self._on_overflow(evicted, self.max_updates)
                except Exception:
                    pass

        return evicted

    def cleanup_stale(self) -> int:
        now = time.monotonic()
        cutoff = now - self.stale_age_seconds

        to_remove = [
            event_id
            for event_id, update in self.updates.items()
            if update.timestamp < cutoff
        ]
        for event_id in to_remove:
            del self.updates[event_id]
            self._stale_removed_count += 1
        return len(to_remove)

    def cleanup_broadcast_complete(self) -> int:
        to_remove = [
            event_id
            for event_id, update in self.updates.items()
            if not update.should_broadcast()
        ]
        for event_id in to_remove:
            del self.updates[event_id]
        return len(to_remove)

    def cleanup(self) -> dict[str, int]:
        stale = self.cleanup_stale()
        complete = self.cleanup_broadcast_complete()
        return {
            "stale_removed": stale,
            "complete_removed": complete,
            "pending_updates": len(self.updates),
        }

    def get_stats(self) -> dict[str, Any]:
        return {
            "pending_updates": len(self.updates),
            "total_evicted": self._evicted_count,
            "total_stale_removed": self._stale_removed_count,
            "size_limited_count": self._size_limited_count,
            "oversized_updates": self._oversized_updates_count,
            "overflow_events": self._overflow_count,
            "max_piggyback_size": self.max_piggyback_size,
            "max_updates": self.max_updates,
        }
