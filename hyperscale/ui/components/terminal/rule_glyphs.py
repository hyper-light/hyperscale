"""The glyph a section's bottom rule is drawn with, one per terminal mode:
a light horizontal line in extended mode, a hyphen in ASCII."""

from hyperscale.ui.config.mode import TerminalMode

RULE_GLYPHS: dict[TerminalMode, str] = {
    TerminalMode.EXTENDED: "─",
    TerminalMode.COMPATIBILITY: "-",
}
