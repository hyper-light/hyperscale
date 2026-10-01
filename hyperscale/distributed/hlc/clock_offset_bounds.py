from __future__ import annotations

from dataclasses import dataclass

# Physical clock readings are whole milliseconds, truncated.
_READING_RESOLUTION_MS = 1


@dataclass(slots=True, frozen=True)
class ClockOffsetBounds:
    """What one round trip proves about a peer's clock: its offset from
    this node's (peer minus self, milliseconds) lies in
    ``[lower_ms, upper_ms]``, as of ``measured_at`` (this node's
    monotonic seconds).

    The peer read its clock somewhere inside the round trip, so the bound
    is the round trip wide -- no assumption about where -- plus the
    millisecond each truncated clock reading may have dropped.
    """

    lower_ms: int
    upper_ms: int
    measured_at: float

    @classmethod
    def from_round_trip(
        cls,
        sent_physical_ms: int,
        round_trip_ms: int,
        peer_physical_ms: int,
        measured_at: float,
    ) -> ClockOffsetBounds:
        """``round_trip_ms`` is measured on the monotonic clock: a wall
        clock stepped during the exchange cannot narrow or invert the
        bound."""
        return cls(
            lower_ms=peer_physical_ms - (sent_physical_ms + round_trip_ms) - _READING_RESOLUTION_MS,
            upper_ms=peer_physical_ms - sent_physical_ms + _READING_RESOLUTION_MS,
            measured_at=measured_at,
        )

    def certainly_beyond(self, threshold_ms: int) -> bool:
        """Whether the offset is beyond ``threshold_ms`` in either
        direction for every value the bound allows."""
        return self.lower_ms > threshold_ms or self.upper_ms < -threshold_ms
