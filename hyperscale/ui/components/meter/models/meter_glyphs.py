from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class MeterGlyphs:
    """The glyphs a meter draws its bar with: ``cell_steps`` are a cell's
    fill from empty up to one step short of full (a terminal cell divided
    into ``len(cell_steps)`` steps), ``full`` a filled cell and ``empty``
    an unfilled one."""

    cell_steps: tuple[str, ...]
    full: str
    empty: str
