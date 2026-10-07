from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.styling import stylize
from hyperscale.ui.styling.colors import ColorName, ExtendedColorName
from hyperscale.ui.styling.tones import TONE_PALETTES

from .meter_bar import meter_bar, meter_label
from .meter_glyph_sets import METER_GLYPHS
from .models import MeterReading


async def styled_meter(
    reading: MeterReading,
    width: int,
    fill_color: ColorName | ExtendedColorName,
    mode: TerminalMode,
    show_label: bool = True,
) -> str:
    """A reading's meter ``width`` columns wide, styled: its fill in its
    tone's color (``fill_color`` when it has none), its unfilled cells in
    the palette's rule color -- a track, not a value -- and its label in
    the label color."""
    palette = TONE_PALETTES[mode]
    label = meter_label(reading) if show_label else ""
    filled, unfilled = meter_bar(reading.ratio, max(width - len(label), 0), METER_GLYPHS[mode])
    return "".join(
        [
            await stylize(filled, color=palette.tone_colors.get(reading.tone, fill_color), mode=mode),
            await stylize(unfilled, color=palette.rule_color, mode=mode),
            await stylize(label, color=palette.label_color, mode=mode),
        ]
    )
