from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class NiceTicks:
    """An axis' tick values at a "nice" step (1, 2 or 5 times a power of
    ten), from at or below its low end to at or above its high end, and
    the decimals that tell adjacent ticks apart."""

    values: list[float]
    step: float
    decimals: int
