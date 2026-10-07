"""A meter's bar as text: the filled cells and the unfilled ones, drawn
to the nearest step of a cell (an eighth in extended mode, a whole cell
in compatibility mode)."""

import math

from .models import MeterGlyphs, MeterReading

# A step half filled or more rounds up: the bar is the ratio rounded to
# the nearest step, never always down (which shows 7.6/8 as 7/8).
HALF_STEP = 0.5
# The space between a meter's bar and its label.
LABEL_GAP = " "


def meter_bar(ratio: float, width: int, glyphs: MeterGlyphs) -> tuple[str, str]:
    """A bar ``width`` cells wide filled to ``ratio`` (clamped to 0..1):
    (its filled cells -- the last one partly filled where the ratio ends
    inside it -- and its unfilled cells)."""
    steps_per_cell = len(glyphs.cell_steps)
    filled_steps = math.floor(min(max(ratio, 0.0), 1.0) * width * steps_per_cell + HALF_STEP)
    full_cells, partial_steps = divmod(filled_steps, steps_per_cell)
    partial_cell = glyphs.cell_steps[partial_steps]
    return glyphs.full * full_cells + partial_cell, glyphs.empty * (width - full_cells - len(partial_cell))


def meter_label(reading: MeterReading) -> str:
    """The label drawn after a reading's bar, gap included ("" with none)."""
    return "" if reading.label is None else LABEL_GAP + reading.label


def meter_text(reading: MeterReading, width: int, glyphs: MeterGlyphs) -> str:
    """A reading's meter ``width`` columns wide as plain text: its bar in
    every column its label leaves (none where the label takes them all),
    then its label."""
    label = meter_label(reading)
    filled, unfilled = meter_bar(reading.ratio, max(width - len(label), 0), glyphs)
    return filled + unfilled + label
