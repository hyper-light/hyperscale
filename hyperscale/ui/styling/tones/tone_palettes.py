"""The palettes the status components draw in, one per terminal mode."""

from hyperscale.ui.config.mode import TerminalMode

from .models import TonePalette

# Extended mode: 256-color indices. Status reads as green, amber and red;
# labels sit a step below the values and the rules a further step below
# the labels, so the groups separate without the rules drawing the eye.
EXTENDED_TONE_PALETTE = TonePalette(
    tone_colors={"ok": "sea_green", "degraded": "light_goldenrod", "failing": "indian_red_3"},
    label_color="grey_21",
    rule_color="grey_15",
    value_color="grey_29",
)
# Compatibility mode: the sixteen ANSI colors every terminal draws.
COMPATIBILITY_TONE_PALETTE = TonePalette(
    tone_colors={"ok": "light_green", "degraded": "yellow", "failing": "red"},
    label_color="dark_grey",
    rule_color="dark_grey",
    value_color="white",
)
TONE_PALETTES: dict[TerminalMode, TonePalette] = {
    TerminalMode.EXTENDED: EXTENDED_TONE_PALETTE,
    TerminalMode.COMPATIBILITY: COMPATIBILITY_TONE_PALETTE,
}
