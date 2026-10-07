"""What separates a tile's value from its secondary counts, one per
terminal mode: a middle dot in extended mode, a comma in ASCII."""

from hyperscale.ui.config.mode import TerminalMode

STAT_TILE_SEPARATORS: dict[TerminalMode, str] = {
    TerminalMode.EXTENDED: " · ",
    TerminalMode.COMPATIBILITY: ", ",
}
