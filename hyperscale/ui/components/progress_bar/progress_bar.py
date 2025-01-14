from __future__ import annotations

import asyncio
import math
from typing import Any
from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
from .progress_bar_config import ProgressBarConfig
from .progress_bar_status import ProgressBarStatus


class ProgressBar:
    def __init__(
        self,
        name: str,
        config: ProgressBarConfig,
        subscriptions: list[str] | None = None,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._total = config.total

        (
            complete_char,
            end_char,
            failed_char,
            incomplete_char,
            start_char,
        ) = config.get_static_chars()

        active_spinner_frames = config.get_active_spinner()

        self._active = active_spinner_frames
        self._complete = complete_char
        self._end = end_char
        self._failed = failed_char
        self._incomplete = incomplete_char
        self._start = start_char

        self._bar_status = ProgressBarStatus.READY

        self._mode = TerminalMode.to_mode(config.terminal_mode)

        self._last_completed: int = 0
        self._next_spinner_frame = 0

        self._updates: asyncio.Queue[int | float] | None = None
        self._update_lock: asyncio.Lock = None

        self._size: int = 0
        self._max_width: int = 0
        self._bar_width: float = 0

        self._last_completed_segments: str = ""
        self._last_ready_segments: str = ""
        self._stylized_start_border: str | None = None
        self._stylized_end_border: str | None = None
        self._stylized_fail: str | None = None
        self._stylized_complete: str | None = None
        self._stylized_active: list[str] | None = None

    @property
    def raw_size(self):
        return self._max_width

    @property
    def size(self):
        return self._size

    async def fit(
        self,
        max_width: int | None = None,
    ):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        self._max_width = max_width
        bar_width = max_width

        if self._start:
            bar_width -= len(self._start)

        if self._end:
            bar_width -= len(self._end)

        self._max_width = max_width
        self._bar_width = bar_width

        self._last_completed_segments: str = ""
        self._stylized_start_border: str | None = None
        self._stylized_end_border: str | None = None

        self._last_ready_segments = await self._rerender_incomplete(0, 0)

        if (
            isinstance(self._config.failed_color, str)
            or self._config.failed_color is None
        ) and (
            isinstance(self._config.failed_highlight, str)
            or self._config.failed_highlight is None
        ):
            self._stylized_fail = await stylize(
                self._failed,
                color=get_style(self._config.failed_color),
                highlight=get_style(self._config.failed_highlight),
                mode=self._mode,
            )

        if (
            isinstance(self._config.complete_color, str)
            or self._config.complete_color is None
        ) and (
            isinstance(self._config.complete_highlight, str)
            or self._config.complete_highlight is None
        ):
            self._stylized_complete = await stylize(
                "".join([self._complete for _ in range(self._bar_width)]),
                color=get_style(self._config.complete_color),
                highlight=get_style(self._config.complete_highlight),
                mode=self._mode,
            )

        if (
            isinstance(self._config.active_color, str)
            or self._config.active_color is None
        ) and (
            isinstance(self._config.active_color, str)
            or self._config.active_highlight is None
        ):
            self._stylized_active = []

            for frame in self._active:
                self._stylized_active.append(
                    await stylize(
                        frame,
                        color=get_style(self._config.active_color),
                        highlight=get_style(self._config.active_highlight),
                        mode=self._mode,
                    )
                )

    async def get_next_frame(self):
        if self._bar_status == ProgressBarStatus.READY:
            self._bar_status == ProgressBarStatus.ACTIVE

        if self._bar_status in [ProgressBarStatus.COMPLETE, ProgressBarStatus.FAILED]:
            frame = await self._create_last_bar()
            self._size = len(frame)

        else:
            frame = await self._create_bar()
            self._size = len(frame)

        return [frame], True

    async def update(self, amount: int | float):
        await self._update_lock.acquire()

        if amount >= self._total:
            amount = self._total
            self._bar_status = ProgressBarStatus.COMPLETE

        self._updates.put_nowait(amount)

        self._update_lock.release()

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def stop(self):
        if self._update_lock.locked():
            self._update_lock.release()

        await self._update_lock.acquire()
        if self._last_completed >= self._total:
            self._bar_status = ProgressBarStatus.COMPLETE

        else:
            self._bar_status = ProgressBarStatus.FAILED

        self._update_lock.release()

    async def abort(self):
        if self._update_lock.locked():
            self._update_lock.release()

        await self._update_lock.acquire()
        self._bar_status = ProgressBarStatus.FAILED
        self._update_lock.release()

    async def ok(self):
        await self._update_lock.acquire()
        self._bar_status = ProgressBarStatus.COMPLETE
        self._update_lock.release()

    async def fail(self):
        await self._update_lock.acquire()
        self._bar_status = ProgressBarStatus.FAILED
        self._update_lock.release()

    async def _create_last_bar(self):
        completed = await self._check_if_should_rerender()
        if completed is None:
            completed = self._last_completed

        active_idx = self._completed_to_active_idx(completed)

        segments: list[str] = []

        if self._start and self._stylized_start_border is None:
            self._stylized_start_border = await self._render_start_border(completed)

        if self._start:
            segments.append(self._stylized_start_border)

        if self._bar_status == ProgressBarStatus.FAILED:
            segments.extend(await self._rerender_completed(active_idx, completed))

            segments.append(
                await stylize(
                    self._failed,
                    color=get_style(self._config.failed_color, completed),
                    highlight=get_style(self._config.failed_highlight, completed),
                    mode=self._mode,
                )
                if self._stylized_fail is None
                else self._stylized_fail
            )

            segments.extend(await self._rerender_incomplete(active_idx, completed))

        else:
            segments.append(
                await stylize(
                    "".join([self._complete for _ in range(self._bar_width)]),
                    color=get_style(self._config.complete_color, completed),
                    highlight=get_style(self._config.complete_highlight, completed),
                    mode=self._mode,
                )
                if self._stylized_complete is None
                else self._stylized_complete
            )

        if self._end and self._stylized_end_border is None:
            self._stylized_end_border = await self._render_end_border(completed)

        if self._end:
            segments.append(self._stylized_end_border)

        return "".join(segments)

    async def _create_bar(self):
        completed = await self._check_if_should_rerender()

        active_idx = 0
        if completed:
            active_idx = self._completed_to_active_idx(completed)

        segments: list[str] = []

        if self._start and self._stylized_start_border is None:
            self._stylized_start_border = await self._render_start_border(completed)

        if self._start:
            segments.append(self._stylized_start_border)

        if completed is not None:
            self._last_completed_segments = await self._rerender_completed(
                active_idx, completed
            )

        segments.extend(self._last_completed_segments)

        segments.append(
            await stylize(
                self._active[self._next_spinner_frame],
                color=get_style(self._config.active_color, completed),
                highlight=get_style(self._config.active_highlight, completed),
                mode=self._mode,
            )
            if self._stylized_active is None
            else self._stylized_active[self._next_spinner_frame]
        )

        self._next_spinner_frame = (self._next_spinner_frame + 1) % len(self._active)

        if completed is not None:
            self._last_ready_segments = await self._rerender_incomplete(
                active_idx, completed
            )
            self._last_completed = completed

        segments.extend(self._last_ready_segments)

        if self._end and self._stylized_end_border is None:
            self._stylized_end_border = await self._render_end_border(completed)

        if self._end:
            segments.append(self._stylized_end_border)

        return "".join(segments)

    async def _rerender_completed(
        self,
        active_idx: int,
        completed: int,
    ):
        return await stylize(
            "".join([self._complete for _ in range(0, active_idx)]),
            color=get_style(self._config.complete_color, completed),
            highlight=get_style(self._config.complete_highlight, completed),
            mode=self._mode,
        )

    async def _rerender_incomplete(
        self,
        active_idx: int,
        completed: int,
    ):
        return await stylize(
            "".join([self._incomplete for _ in range(active_idx + 1, self._bar_width)]),
            color=get_style(self._config.incomplete_color, completed),
            highlight=get_style(self._config.incomplete_highlight, completed),
            mode=self._mode,
        )

    async def _render_start_border(self, completed: int):
        return await stylize(
            self._start,
            color=get_style(self._config.border_color, completed),
            highlight=get_style(self._config.border_highlight, completed),
            mode=self._mode,
        )

    async def _render_end_border(self, completed: int):
        return await stylize(
            self._end,
            color=get_style(self._config.border_color, completed),
            highlight=get_style(self._config.border_color, completed),
            mode=self._mode,
        )

    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        amount: int | float | None = None
        if self._updates.empty() is False:
            amount = await self._updates.get()

        self._update_lock.release()

        return amount

    def _completed_to_active_idx(self, completed: int | float) -> int:
        active_idx = math.floor(completed * (self._bar_width / self._total))

        if active_idx >= self._bar_width:
            active_idx = self._bar_width - 1

        return active_idx
