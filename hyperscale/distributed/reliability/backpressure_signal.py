"""``BackpressureSignal`` -- pickled under the namespace
``hyperscale.distributed.reliability.backpressure`` (see that module)."""

from dataclasses import dataclass

from .backpressure_level import BackpressureLevel


@dataclass(slots=True)
class BackpressureSignal:
    """
    Backpressure signal to include in responses.

    This signal tells the sender how to adjust their behavior.
    """

    level: BackpressureLevel
    suggested_delay_ms: int = 0
    batch_only: bool = False
    drop_non_critical: bool = False

    @property
    def delay_ms(self) -> int:
        return self.suggested_delay_ms

    @classmethod
    def from_level(
        cls,
        level: BackpressureLevel,
        throttle_delay_ms: int = 100,
        batch_delay_ms: int = 500,
        reject_delay_ms: int = 1000,
    ) -> "BackpressureSignal":
        """
        Create signal from backpressure level.

        Args:
            level: The backpressure level to signal.
            throttle_delay_ms: Suggested delay for THROTTLE level (default: 100ms).
            batch_delay_ms: Suggested delay for BATCH level (default: 500ms).
            reject_delay_ms: Suggested delay for REJECT level (default: 1000ms).
        """
        if level == BackpressureLevel.NONE:
            return cls(level=level)
        elif level == BackpressureLevel.THROTTLE:
            return cls(level=level, suggested_delay_ms=throttle_delay_ms)
        elif level == BackpressureLevel.BATCH:
            return cls(level=level, suggested_delay_ms=batch_delay_ms, batch_only=True)
        else:  # REJECT
            return cls(
                level=level,
                suggested_delay_ms=reject_delay_ms,
                batch_only=True,
                drop_non_critical=True,
            )

    def to_dict(self) -> dict:
        """Serialize to dictionary for embedding in messages."""
        return {
            "level": self.level.value,
            "suggested_delay_ms": self.suggested_delay_ms,
            "batch_only": self.batch_only,
            "drop_non_critical": self.drop_non_critical,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BackpressureSignal":
        """Deserialize from dictionary."""
        return cls(
            level=BackpressureLevel(data.get("level", 0)),
            suggested_delay_ms=data.get("suggested_delay_ms", 0),
            batch_only=data.get("batch_only", False),
            drop_non_critical=data.get("drop_non_critical", False),
        )
