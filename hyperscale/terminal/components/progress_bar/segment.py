import asyncio

from hyperscale.terminal.components.spinner.spinner_factory import SpinnerFactory
from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.styling import stylize

from .bar_type import BarType
from .progress_bar_chars import ProgressBarChars
from .progress_bar_color_config import ProgressBarColorConfig
from .segment_status import SegmentStatus
from .segment_type import SegmentType


class Segment:
    def __init__(
        self,
        sement_chars: ProgressBarChars,
        segment_type: SegmentType,
        segment_colors: ProgressBarColorConfig,
        segment_default_char: str | None = None,
        bar_type: BarType = BarType.DEFAULT,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ) -> None:
        self.segment_chars = sement_chars
        self.segment_type = segment_type
        self.segment_default_char = segment_default_char or "-"
        self.segment_colors = segment_colors
        self.status = SegmentStatus.READY
        self.bar_type = bar_type
        self.mode = mode

        self._styled_ready_state: str | None = None
        self._styled_active_state: list[str] | str | None = None
        self._styled_ok_state: str | None = None
        self._styled_failed_state: str | None = None
        self.interval: int | None = None

    @property
    def ok(self):
        return self._styled_ok_state

    @property
    def active(self):
        return (
            next(self._styled_active_state)
            if isinstance(self._styled_active_state, list)
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
        if self.bar_type == BarType.SPINNER and self.segment_chars.active_char:
            await self._style_spinner()

        else:
            self._styled_ok_state = await stylize(
                self.segment_chars.ok_char,
                color=self.segment_colors.ok_color,
                highlight=self.segment_colors.ok_color_highlight,
                mode=self.mode,
            )

        self._styled_ready_state = await stylize(
            self.segment_default_char,
            color=self.segment_colors.ready_color,
            highlight=self.segment_colors.ready_color_highlight,
        )

    async def _style_spinner(self) -> str:
        factory = SpinnerFactory()
        spinner = factory.get(self.segment_chars.active_char)
        self.interval = spinner.interval

        # We can avoid a lot of overhead by pre-styling the frames
        # to render
        self._styled_active_state = await asyncio.gather(
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
