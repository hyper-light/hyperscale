from dataclasses import dataclass

from hyperscale.ui.styling.tones import StatusTone


@dataclass(slots=True, frozen=True)
class MeterReading:
    """One ratio a meter shows: ``used`` of ``total`` (cores in use of a
    worker's cores, healthy workers of those registered), the label drawn
    after the bar (``"6/8"``; None draws none), and the tone its fill is
    drawn in (None: the meter's own fill color)."""

    used: float
    total: float
    label: str | None = None
    tone: StatusTone | None = None

    @property
    def ratio(self) -> float:
        """``used`` over ``total``, within 0 and 1 (0 with no total)."""
        if self.total <= 0:
            return 0.0

        return min(max(self.used / self.total, 0.0), 1.0)
