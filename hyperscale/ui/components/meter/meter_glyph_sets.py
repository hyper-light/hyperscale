"""The glyphs a meter's bar is drawn with, one set per terminal mode."""

from hyperscale.ui.config.mode import TerminalMode

from .models import MeterGlyphs

# Extended mode: a cell fills in eighths, left to right (the Unicode left
# block elements U+258F..U+2589), so a bar shows its ratio to an eighth of
# a cell; an unfilled cell is a light shade.
EXTENDED_METER_GLYPHS = MeterGlyphs(
    cell_steps=("", "▏", "▎", "▍", "▌", "▋", "▊", "▉"),
    full="█",
    empty="░",
)
# Compatibility mode: ASCII, whole cells only.
COMPATIBILITY_METER_GLYPHS = MeterGlyphs(cell_steps=("",), full="#", empty="-")
METER_GLYPHS: dict[TerminalMode, MeterGlyphs] = {
    TerminalMode.EXTENDED: EXTENDED_METER_GLYPHS,
    TerminalMode.COMPATIBILITY: COMPATIBILITY_METER_GLYPHS,
}
