import asyncio

from hyperscale.ui.components.meter.meter_bar import LABEL_GAP, meter_label
from hyperscale.ui.components.meter.styled_meter import styled_meter
from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize
from hyperscale.ui.styling.tones import TONE_PALETTES

from .fitting_parts import TextPart, fitting_parts
from .models import StatTileReading
from .stat_tile_config import StatTileConfig
from .stat_tile_separators import STAT_TILE_SEPARATORS

# The lines a tile stacks its label over its value in; given fewer, it
# draws both on one line.
STACKED_LINES = 2
# Before its first reading a tile shows no value.
NO_READING = StatTileReading(value="")
# Between a meter and the text after it, and an inline label and its value.
PART_GAP = " "
# A tile's meter keeps at least this share of the value line: text after it
# shows only where it leaves the meter that much.
METER_LINE_SHARE = 0.5


class StatTile:
    """A dim small-caps label over one prominent value, as a row of tiles
    in a btop-style dashboard shows its headline numbers: ``WORKERS`` over
    ``1 healthy``. A ratio value draws a meter before its text (the meter
    takes every column the text leaves); secondary counts follow the value
    only while nonzero (``2 failed`` in red appears only when there were
    failures), each while it fits the tile's width, in their order.

    Given a single line it draws its label and value on that one line. It
    is updated with a ``StatTileReading``; only the newest queued is drawn.
    """

    def __init__(
        self,
        name: str,
        config: StatTileConfig,
        subscriptions: list[str] | None = None,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_Y_AXIS
        self.name = name
        self.subscriptions = subscriptions if subscriptions is not None else []
        self._config = config
        self._mode = TerminalMode.to_mode(config.terminal_mode)
        self._palette = TONE_PALETTES[self._mode]
        self._separator = STAT_TILE_SEPARATORS[self._mode]
        self._max_width = 0
        self._max_height = 0
        self._reading = NO_READING
        self._updates: asyncio.Queue[StatTileReading] = asyncio.Queue()
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

    async def update(self, reading: StatTileReading) -> None:
        """Show ``reading`` from the next frame."""
        self._updates.put_nowait(reading)

    async def get_next_frame(self) -> tuple[list[str], bool]:
        """The tile's lines, and whether they changed since the last."""
        rerender = self._take_newest_update() or self._last_frame is None
        if rerender:
            self._last_frame = await self._render()

        return self._last_frame, rerender

    def _take_newest_update(self) -> bool:
        queued = False
        while not self._updates.empty():
            self._reading = self._updates.get_nowait()
            queued = True

        return queued

    async def _render(self) -> list[str]:
        label = self._config.label[: self._max_width]
        styled_label = await stylize(label, color=self._palette.label_color, mode=self._mode)
        if self._max_height < STACKED_LINES:
            return [await self._render_inline(label, styled_label)]

        return [styled_label + " " * (self._max_width - len(label)), await self._value_line(self._max_width)]

    async def _render_inline(self, label: str, styled_label: str) -> str:
        value_width = max(self._max_width - len(label) - len(PART_GAP), 0)
        return styled_label + PART_GAP + await self._value_line(value_width)

    async def _value_line(self, width: int) -> str:
        """The meter (if any), the value and the nonzero secondaries that
        fit, filling ``width``."""
        parts = [*self._value_parts(), *self._secondary_parts()]
        shown = fitting_parts(parts, width - self._meter_reserve(width), len(self._separator))
        text_width = len(self._separator.join(text for text, _ in shown))
        gap = self._meter_gap(shown)
        meter = await self._styled_meter(width - text_width - len(gap))
        return meter + gap + await self._styled_text(shown) + " " * self._padding(width, text_width)

    async def _styled_text(self, shown: list[TextPart]) -> str:
        """The shown parts in their colors, the separators between them dim."""
        separator = await stylize(self._separator, color=self._palette.label_color, mode=self._mode)
        return separator.join([await stylize(text, color=color, mode=self._mode) for text, color in shown])

    def _value_parts(self) -> list[TextPart]:
        """The value in its tone's color (or the value color); none where
        the tile has no value text (a meter alone)."""
        if not self._reading.value:
            return []

        value_color = self._palette.tone_colors.get(self._reading.value_tone, self._palette.value_color)
        return [(self._reading.value, value_color)]

    def _secondary_parts(self) -> list[TextPart]:
        """Each nonzero secondary, in its tone's color or the label color."""
        return [
            (secondary.text, self._palette.tone_colors.get(secondary.tone, self._palette.label_color))
            for secondary in self._reading.secondaries
            if secondary.count > 0
        ]

    def _meter_reserve(self, width: int) -> int:
        """The columns of a ``width`` line the text leaves a meter at least:
        its share of the line, and never less than its label, a cell of its
        bar and the gap after it."""
        if self._reading.meter is None:
            return 0

        least_width = len(meter_label(self._reading.meter)) + len(LABEL_GAP) + len(PART_GAP)
        return max(int(width * METER_LINE_SHARE), least_width)

    def _meter_gap(self, shown: list[TextPart]) -> str:
        """The gap between the meter and the text after it: none without
        either."""
        return PART_GAP if shown and self._reading.meter is not None else ""

    def _padding(self, width: int, text_width: int) -> int:
        """The blank columns after the text: none where a meter fills the
        columns the text leaves."""
        return max(width - text_width, 0) if self._reading.meter is None else 0

    async def _styled_meter(self, width: int) -> str:
        """The meter in ``width`` columns (nothing without one)."""
        if self._reading.meter is None:
            return ""

        return await styled_meter(self._reading.meter, max(width, 0), self._config.meter_fill_color, self._mode)

    async def pause(self) -> None:
        pass

    async def resume(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def abort(self) -> None:
        pass
