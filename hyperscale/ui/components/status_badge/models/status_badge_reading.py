from dataclasses import dataclass

from hyperscale.ui.styling.tones import StatusTone


@dataclass(slots=True, frozen=True)
class StatusBadgeReading:
    """One status badge: its short label (``leader 127.0.0.1:8231``) and
    the tone its dot is drawn in."""

    label: str
    tone: StatusTone
