from dataclasses import dataclass

from hyperscale.ui.styling.colors import ColorName, ExtendedColorName

from ..status_tone import StatusTone

PaletteColor = ColorName | ExtendedColorName


@dataclass(slots=True, frozen=True)
class TonePalette:
    """The colors a calm terminal UI draws in: each status tone's color,
    the dim color of labels and secondary text, the dimmer color of the
    rules between groups, and the color of a prominent value."""

    tone_colors: dict[StatusTone, PaletteColor]
    label_color: PaletteColor
    rule_color: PaletteColor
    value_color: PaletteColor
