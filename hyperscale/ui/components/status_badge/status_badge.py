import asyncio

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize
from hyperscale.ui.styling.tones import TONE_PALETTES

from .flow_into_lines import flow_into_lines
from .models import StatusBadgeReading
from .status_badge_config import StatusBadgeConfig
from .status_badge_glyph_sets import BADGE_GLYPHS, GLYPH_GAP
from .status_badge_text import badge_text

# The space between two badges: wider than the space inside one, so each
# badge reads as one unit.
BADGE_GAP = "    "


class StatusBadge:
    """A line of status badges: each a dot in its tone's color -- green,
    amber or red in extended mode, ``+ ~ !`` in compatibility mode -- and
    a short label, laid out left to right in their order (the most
    important first). Badges that pass the end of a line wrap onto the
    next where the component has one; past its last line they are not
    drawn, so the first badges always are.

    It is updated with the whole list of badges; only the newest queued
    list is drawn.
    """

    def __init__(
        self,
        name: str,
        config: StatusBadgeConfig,
        subscriptions: list[str] | None = None,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_Y_AXIS
        self.name = name
        self.subscriptions = subscriptions if subscriptions is not None else []
        self._config = config
        self._mode = TerminalMode.to_mode(config.terminal_mode)
        self._palette = TONE_PALETTES[self._mode]
        self._max_width = 0
        self._max_height = 0
        self._badges: list[StatusBadgeReading] = []
        self._updates: asyncio.Queue[list[StatusBadgeReading]] = asyncio.Queue()
        self._last_frame: list[str] | None = None

    @property
    def raw_size(self) -> int:
        return self._max_width

    @property
    def size(self) -> int:
        return self._max_width

    async def fit(self, max_width: int | None = None, max_height: int | None = None) -> None:
        """Take ``max_width`` x ``max_height``; the next frame redraws."""
        self._max_width = max_width or 0
        self._max_height = max_height or 0
        self._last_frame = None

    async def update(self, badges: list[StatusBadgeReading]) -> None:
        """Show ``badges`` from the next frame."""
        self._updates.put_nowait(badges)

    async def get_next_frame(self) -> tuple[list[str], bool]:
        """The badges' lines, and whether they changed since the last."""
        rerender = self._take_newest_update() or self._last_frame is None
        if rerender:
            self._last_frame = await self._render()

        return self._last_frame, rerender

    def _take_newest_update(self) -> bool:
        queued = False
        while not self._updates.empty():
            self._badges = self._updates.get_nowait()
            queued = True

        return queued

    async def _render(self) -> list[str]:
        """Each line's badges, styled and padded to the width."""
        badges = self._fitted_badges()
        lines = flow_into_lines(self._badge_widths(badges), self._max_width, len(BADGE_GAP))
        return [await self._render_line([badges[index] for index in line]) for line in lines[: self._max_height]]

    def _fitted_badges(self) -> list[StatusBadgeReading]:
        """The badges, a badge wider than a whole line (a width the layout
        never gives) with its label cut to the line rather than pass the
        section's edge."""
        label_width = max(self._max_width - len(GLYPH_GAP) - 1, 0)
        return [StatusBadgeReading(badge.label[:label_width], badge.tone) for badge in self._badges]

    def _badge_widths(self, badges: list[StatusBadgeReading]) -> list[int]:
        return [len(badge_text(badge, self._mode)) for badge in badges]

    async def _render_line(self, badges: list[StatusBadgeReading]) -> str:
        styled_badges = [await self._styled_badge(badge) for badge in badges]
        line_width = len(BADGE_GAP.join(badge_text(badge, self._mode) for badge in badges))
        return BADGE_GAP.join(styled_badges) + " " * max(self._max_width - line_width, 0)

    async def _styled_badge(self, badge: StatusBadgeReading) -> str:
        glyph = await stylize(
            BADGE_GLYPHS[self._mode][badge.tone], color=self._palette.tone_colors[badge.tone], mode=self._mode
        )
        return glyph + GLYPH_GAP + await stylize(badge.label, color=self._palette.value_color, mode=self._mode)

    async def pause(self) -> None:
        pass

    async def resume(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def abort(self) -> None:
        pass
