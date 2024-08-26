import asyncio
import itertools

from hyperscale.terminal.components.spinner.spinner_factory import SpinnerFactory
from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.styling import stylize

from .progress_bar_chars import ProgressBarChars
from .progress_bar_color_config import ProgressBarColorConfig
from .segment_status import SegmentStatus
from .segment_type import SegmentType


class Segment:
    def __init__(
        self,
        sement_chars: str | ProgressBarChars,
        segment_type: SegmentType,
        segment_colors: ProgressBarColorConfig,
        segment_default_char: str | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ) -> None:
        self.segment_chars = sement_chars
        self.segment_type = segment_type
        self.segment_default_char = segment_default_char or "-"
        self.segment_colors = segment_colors
        self.status = SegmentStatus.READY
        self.mode = mode

        self._styled_ready_state: str | None = None
        self._styled_active_state: list[str] | str | None = None
        self._styled_ok_state: str | None = None
        self._styled_failed_state: str | None = None
        self._styled_start: str | None = None
        self._styled_end: str | None = None
        self.interval: int | None = None

    @property
    def ok(self):
        return self._styled_ok_state

    @property
    def active(self):
        return (
            next(self._styled_active_state)
            if isinstance(self._styled_active_state, itertools.cycle)
            else None
        )

    @property
    def ready(self):
        return self._styled_ready_state

    @property
    def failed(self):
        return self._styled_failed_state

    @property
    def next(self):
        if self.segment_type == SegmentType.START:
            return self._styled_start

        elif self.segment_type == SegmentType.END:
            return self._styled_end

        match self.status:
            case SegmentStatus.ACTIVE:
                return self.active

            case SegmentStatus.READY:
                return self.ready

            case SegmentStatus.FAILED:
                return self.failed

            case SegmentStatus.OK:
                return self.ok

    async def style(self):
        if self.segment_type == SegmentType.BAR:
            await self._style_bar_segment()

        elif self.segment_type == SegmentType.START:
            self._styled_start = await stylize(
                self.segment_chars.start_char,
                color=self.segment_colors.border_color,
                highlight=self.segment_colors.border_color_highlight,
                mode=self.mode,
            )

        else:
            self._styled_end = await stylize(
                self.segment_chars.end_char,
                color=self.segment_colors.border_color,
                highlight=self.segment_colors.border_color_highlight,
                mode=self.mode,
            )

    async def _style_bar_segment(self):
        if self.segment_chars.active_char:
            await self._style_spinner()

        else:
            self._styled_ok_state = await stylize(
                self.segment_chars.ok_char,
                color=self.segment_colors.ok_color,
                highlight=self.segment_colors.ok_color_highlight,
                mode=self.mode,
            )

        if self.segment_colors.ready_color:
            self._styled_ready_state = await stylize(
                self.segment_default_char,
                color=self.segment_colors.ready_color,
                highlight=self.segment_colors.ready_color_highlight,
            )

        else:
            self._styled_ready_state = self.segment_default_char

    async def _style_spinner(self) -> str:
        factory = SpinnerFactory()
        spinner = factory.get(self.segment_chars.active_char)
        self.interval = spinner.interval

        # We can avoid a lot of overhead by pre-styling the frames
        # to render
        self._styled_active_state = itertools.cycle(
            await asyncio.gather(
                *[
                    stylize(
                        frame,
                        color=self.segment_colors.active_color,
                        highlight=self.segment_colors.active_color_highlight,
                        mode=TerminalMode.to_mode(self.mode),
                    )
                    for frame in spinner.frames
                ]
            )
        )

        self._styled_ok_state = await stylize(
            self.segment_chars.ok_char,
            color=self.segment_colors.ok_color,
            highlight=self.segment_colors.ok_color_highlight,
            mode=TerminalMode.to_mode(self.mode),
        )

        self._styled_failed_state = await stylize(
            self.segment_chars.fail_char,
            color=self.segment_colors.fail_color,
            highlight=self.segment_colors.fail_color_highlight,
            mode=TerminalMode.to_mode(self.mode),
        )
