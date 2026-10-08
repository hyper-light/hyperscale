"""Wire model ``NetworkCoordinate`` -- pickled under the wire namespace
``hyperscale.distributed.models.coordinates`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()


@dataclass(slots=True)
class NetworkCoordinate:
    """Network coordinate for RTT estimation (AD-35)."""

    vec: list[float]
    height: float
    adjustment: float
    error: float
    updated_at: float = field(default_factory=lambda: _DEFAULT_CLOCK.monotonic())
    sample_count: int = 0

    def to_dict(self) -> dict[str, float | list[float] | int]:
        """
        Serialize coordinate to dictionary for message embedding (AD-35 Task 12.2.1).

        Returns:
            Dict with position, height, adjustment, error, and sample_count
        """
        return {
            "vec": self.vec,
            "height": self.height,
            "adjustment": self.adjustment,
            "error": self.error,
            "sample_count": self.sample_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "NetworkCoordinate":
        """
        Deserialize coordinate from dictionary (AD-35 Task 12.2.1).

        Args:
            data: Dictionary from message with coordinate fields

        Returns:
            NetworkCoordinate instance (updated_at set to current time)

        Raises:
            TypeError, KeyError, ValueError: ``data`` is not a complete
                coordinate. Every field ``to_dict`` writes is required -- a
                defaulted vector or error would place the peer somewhere
                it never claimed to be.
        """
        if not isinstance(data, dict):
            raise TypeError(f"a coordinate is a mapping, not {type(data).__name__}")
        return cls(
            vec=[float(component) for component in data["vec"]],
            height=float(data["height"]),
            adjustment=float(data["adjustment"]),
            error=float(data["error"]),
            updated_at=_DEFAULT_CLOCK.monotonic(),
            sample_count=int(data["sample_count"]),
        )
