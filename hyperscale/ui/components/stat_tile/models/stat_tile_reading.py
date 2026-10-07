from dataclasses import dataclass, field

from hyperscale.ui.components.meter.models import MeterReading
from hyperscale.ui.styling.tones import StatusTone

from .stat_tile_secondary import StatTileSecondary


@dataclass(slots=True, frozen=True)
class StatTileReading:
    """What a stat tile shows under its label: its one prominent value
    (drawn in ``value_tone``'s color, or the palette's value color with
    none), a meter before it where the value is a ratio, and its secondary
    counts (each drawn only while nonzero)."""

    value: str
    value_tone: StatusTone | None = None
    meter: MeterReading | None = None
    secondaries: tuple[StatTileSecondary, ...] = field(default_factory=tuple)
