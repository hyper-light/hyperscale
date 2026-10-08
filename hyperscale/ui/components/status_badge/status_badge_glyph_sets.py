"""The glyph opening a status badge, by tone, one set per terminal mode.

Each tone has a glyph of its own shape as well as its own color, so a
badge reads the same where color is not drawn (a pipe, a capture, a
color-blind reader)."""

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.styling.tones import StatusTone

# Extended mode: a full dot, a half dot, a heavy cross.
EXTENDED_BADGE_GLYPHS: dict[StatusTone, str] = {"ok": "●", "degraded": "◐", "failing": "✖"}
# Compatibility mode: ASCII -- plus, tilde, bang.
COMPATIBILITY_BADGE_GLYPHS: dict[StatusTone, str] = {"ok": "+", "degraded": "~", "failing": "!"}
BADGE_GLYPHS: dict[TerminalMode, dict[StatusTone, str]] = {
    TerminalMode.EXTENDED: EXTENDED_BADGE_GLYPHS,
    TerminalMode.COMPATIBILITY: COMPATIBILITY_BADGE_GLYPHS,
}
# Each mode's tones by their glyph: a badge's text names its tone.
BADGE_TONES_BY_GLYPH: dict[TerminalMode, dict[str, StatusTone]] = {
    mode: {glyph: tone for tone, glyph in glyphs.items()} for mode, glyphs in BADGE_GLYPHS.items()
}
# Between a badge's glyph and its label.
GLYPH_GAP = " "
