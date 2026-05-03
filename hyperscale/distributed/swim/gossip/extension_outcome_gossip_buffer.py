"""Extension outcome gossip buffer for AD-26 Phase H8b dissemination.

Mirrors ``ExtensionDecisionGossipBuffer`` (H7b) but carries
``ExtensionOutcomeEvent`` records — the workflow-termination
training signal that feeds the H8 Bayesian alpha-tuner. Wire
separator ``b"#|o"`` runs alongside the existing ``#|h``,
``#|m``, ``#|w``, ``#|x``, and ``#|v`` channels.

A workflow has at most one outcome (it terminates exactly once),
so dedup is keyed on ``workflow_id`` alone. Higher-leader-term
events supersede existing slots so a new leader's authoritative
outcome wins over a stale one.
"""

from __future__ import annotations

import heapq
import math
import time
from dataclasses import dataclass, field
from typing import Any

from hyperscale.distributed.health.extension_outcome import (
    ExtensionOutcomeEvent,
)


MAX_EXTENSION_OUTCOME_PIGGYBACK_SIZE: int = 600

EXTENSION_OUTCOME_SEPARATOR: bytes = b"#|o"

ENTRY_SEPARATOR: bytes = b"|"


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


@dataclass(slots=True)
class ExtensionOutcomeGossipBuffer:
    """Buffer of ``ExtensionOutcomeEvent`` to be piggybacked on SWIM.

    Same dissemination strategy as H7b's decision buffer. Differs
    in the dedup key (``workflow_id`` since outcomes are unique
    per workflow) and the wire separator (``#|o``).
    """

    updates: dict[str, ExtensionOutcomePiggybackUpdate] = field(
        default_factory=dict
    )
    broadcast_multiplier: int = 3
    max_updates: int = 500
    stale_age_seconds: float = 60.0
    max_piggyback_size: int = MAX_EXTENSION_OUTCOME_PIGGYBACK_SIZE

    _evicted_count: int = 0
    _stale_removed_count: int = 0
    _size_limited_count: int = 0
    _oversized_updates_count: int = 0
    _overflow_count: int = 0

    _on_overflow: Any = None

    def set_overflow_callback(self, callback: Any) -> None:
        self._on_overflow = callback

    def add_event(
        self, event: ExtensionOutcomeEvent, number_of_managers: int = 1
    ) -> bool:
        """Queue an outcome event for dissemination.

        Returns True if the buffer was modified — i.e. this is a
        new workflow_id or a higher-leader-term replacement.
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
            self.updates[event_id] = ExtensionOutcomePiggybackUpdate(
                event=event,
                timestamp=time.monotonic(),
                max_broadcasts=max_broadcasts,
            )
            return True

        if event.leader_term > existing.event.leader_term:
            self.updates[event_id] = ExtensionOutcomePiggybackUpdate(
                event=event,
                timestamp=time.monotonic(),
                max_broadcasts=max_broadcasts,
            )
            return True

        return False

    def get_events_to_piggyback(
        self, max_count: int = 5
    ) -> list[ExtensionOutcomePiggybackUpdate]:
        max_count = max(1, min(max_count, 100))
        candidates = (u for u in self.updates.values() if u.should_broadcast())
        return heapq.nsmallest(max_count, candidates, key=lambda u: u.broadcast_count)

    def mark_broadcasts(
        self, updates: list[ExtensionOutcomePiggybackUpdate]
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
        max_count = max(1, min(max_count, self.MAX_ENCODE_COUNT))
        if max_size is None:
            max_size = self.max_piggyback_size

        events = self.get_events_to_piggyback(max_count)
        if not events:
            return b""

        result_parts: list[bytes] = []
        total_size = len(EXTENSION_OUTCOME_SEPARATOR)
        included_updates: list[ExtensionOutcomePiggybackUpdate] = []

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
        return EXTENSION_OUTCOME_SEPARATOR + ENTRY_SEPARATOR.join(result_parts)

    def encode_piggyback_with_base(
        self,
        base_message: bytes,
        max_count: int = 5,
    ) -> bytes:
        from .gossip_buffer import MAX_UDP_PAYLOAD

        remaining = MAX_UDP_PAYLOAD - len(base_message)
        if remaining <= 0:
            return b""
        return self.encode_piggyback(max_count, max_size=remaining)

    MAX_DECODE_UPDATES = 100

    @classmethod
    def decode_piggyback(
        cls, data: bytes, max_updates: int = 100
    ) -> list[ExtensionOutcomeEvent]:
        if not data or not data.startswith(EXTENSION_OUTCOME_SEPARATOR):
            return []

        bounded_max = min(max_updates, cls.MAX_DECODE_UPDATES)
        events: list[ExtensionOutcomeEvent] = []
        parts = data[len(EXTENSION_OUTCOME_SEPARATOR):].split(ENTRY_SEPARATOR)
        for part in parts:
            if len(events) >= bounded_max:
                break
            if part:
                event = ExtensionOutcomeEvent.from_bytes(part)
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
