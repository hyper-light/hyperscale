from dataclasses import dataclass, field

# A series' points: (time, value) pairs.
SeriesPointList = list[tuple[int | float, int | float]]


@dataclass(slots=True, frozen=True)
class SeriesUpdate:
    """A multi-series plot's update: each series' points by its name, each
    series' current reading by its name -- shown after its name in the
    legend (``dispatched 59.9/s``), so the plot needs no table of readings
    beside it -- and one more reading drawn right-aligned on the legend's
    line (``dispatch p95 50 ms``; None draws none)."""

    points: dict[str, SeriesPointList]
    readings: dict[str, str] = field(default_factory=dict)
    extra_reading: str | None = None
