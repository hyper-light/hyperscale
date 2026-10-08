from dataclasses import dataclass

from hyperscale.ui.styling.tones import StatusTone


@dataclass(slots=True, frozen=True)
class StatTileSecondary:
    """A secondary count under a tile's value (``2 failed``): its text,
    the count it shows -- the tile draws it only while the count is above
    zero -- and the tone it is drawn in (None: the dim label color)."""

    text: str
    count: float
    tone: StatusTone | None = None
