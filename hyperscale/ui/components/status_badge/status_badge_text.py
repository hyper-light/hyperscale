from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.styling.tones import StatusTone

from .flow_into_lines import flow_into_lines
from .models import StatusBadgeReading
from .status_badge_glyph_sets import BADGE_GLYPHS, BADGE_TONES_BY_GLYPH, GLYPH_GAP


def badge_text(reading: StatusBadgeReading, mode: TerminalMode) -> str:
    """A badge as plain text: its tone's glyph, then its label."""
    return BADGE_GLYPHS[mode][reading.tone] + GLYPH_GAP + reading.label


def tone_of_badge_text(text: str, mode: TerminalMode) -> StatusTone | None:
    """The tone of a badge's plain text (``badge_text``), or None for text
    that is not a badge's."""
    glyph, gap, _ = text.partition(GLYPH_GAP)
    return BADGE_TONES_BY_GLYPH[mode].get(glyph) if gap else None


def badge_line_count(badges: list[StatusBadgeReading], line_width: int, gap_width: int, mode: TerminalMode) -> int:
    """The lines ``badges`` flow onto in lines ``line_width`` wide, laid
    out ``gap_width`` apart as a ``StatusBadge`` lays them out."""
    return len(flow_into_lines([len(badge_text(badge, mode)) for badge in badges], line_width, gap_width))
