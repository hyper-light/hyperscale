from __future__ import annotations

import asyncio
import functools
import inspect
import math
from typing import Any
from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.terminal.styling import stylize, get_style
from .progress_bar_config import ProgressBarConfig
from .progress_bar_status import ProgressBarStatus


class ProgressBar:
    def __init__(
        self,
        config: ProgressBarConfig,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS
        self._config = config

        data, total = config.get_data_and_total()

        self._data = data
        self._total = total

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

        self._completed: int = 0
        self._next_spinner_frame = 0

        self._update_lock: asyncio.Lock = None

        self._size: int = 0
        self._max_width: int = 0
        self._bar_width: float = 0

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

        self._max_width = max_width
        bar_width = max_width

        if self._start:
            bar_width -= len(self._start)

        if self._end:
            bar_width -= len(self._end)

        self._max_width = max_width
        self._bar_width = bar_width

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, traceback):
        # Avoid stop() execution for the 2nd time

        await self.ok()

        return False  # nothing is handled

    def __call__(self, fn):
        @functools.wraps(fn)
        async def inner(*args, **kwargs):
            async with self:
                if inspect.iscoroutinefunction(fn):
                    return await fn(*args, **kwargs)

                else:
                    return fn(*args, **kwargs)

        return inner

    async def get_next_frame(self) -> str:

        if self._bar_status == ProgressBarStatus.READY:
            self._bar_status == ProgressBarStatus.ACTIVE

        if self._bar_status in [ProgressBarStatus.COMPLETE, ProgressBarStatus.FAILED]:
            frame = await self._create_last_bar()
            self._size = len(frame)

        else:
            frame = await self._create_bar()
            self._size = len(frame)

        return frame

    async def __anext__(self):
        item: Any | None = None

        if inspect.isasyncgen(self._data) and self._completed <= self._total:
            item = await anext(self._data)

            await self.update()

        elif self._completed <= self._total:
            item = next(self._data)

            if inspect.isawaitable(item):
                item = await item

            await self.update()

        else:
            raise StopAsyncIteration(
                "Err. - async bar iterable is exhausted. No more data!"
            )

        if self._completed >= self._total:
            await self.ok()

        return item

    async def __aiter__(self):
        if inspect.isasyncgen(self._data):
            async for item in self._data:
                yield item

                await self.update()

        else:
            for item in self._data:
                if inspect.isawaitable(item):
                    item = await item

                yield item

                await self.update()

        if self._completed >= self._total:
            await self.ok()

    async def update(self, amount: int | float = 1):
        await self._update_lock.acquire()

        next_amount = self._completed + amount

        if next_amount >= self._total:
            next_amount = self._total

        self._completed = next_amount

        self._update_lock.release()

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def stop(self):
        if self._update_lock.locked():
            self._update_lock.release()

        await self._update_lock.acquire()
        if self._completed >= self._total:
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
        active_idx = math.floor(self._completed * (self._bar_width / self._total))

        segments: list[str] = []

        if self._start:
            segments.append(
                await stylize(
                    self._start,
                    color=get_style(self._config.border_color, self._data),
                    highlight=get_style(self._config.border_highlight, self._data),
                    mode=self._mode,
                )
            )

        if self._bar_status == ProgressBarStatus.FAILED:

            segments.extend(
                await stylize(
                    "".join([self._complete for _ in range(0, active_idx)]),
                    color=get_style(self._config.complete_color, self._data),
                    highlight=get_style(self._config.complete_highlight, self._data),
                    mode=self._mode,
                )
            )

            segments.append(
                await stylize(
                    self._failed,
                    color=get_style(self._config.failed_color, self._data),
                    highlight=get_style(self._config.failed_highlight, self._data),
                    mode=self._mode,
                )
            )

            segments.extend(
                await stylize(
                    "".join(
                        [
                            self._incomplete
                            for _ in range(active_idx + 1, self._bar_width)
                        ]
                    ),
                    color=get_style(self._config.incomplete_color, self._data),
                    highlight=get_style(self._config.incomplete_highlight, self._data),
                    mode=self._mode,
                )
            )

        else:
            segments.append(
                await stylize(
                    "".join([self._complete for _ in range(self._bar_width)]),
                    color=get_style(self._config.complete_color, self._data),
                    highlight=get_style(self._config.complete_highlight, self._data),
                    mode=self._mode,
                )
            )

        if self._end:
            segments.append(
                await stylize(
                    self._end,
                    color=get_style(self._config.border_color, self._data),
                    highlight=get_style(self._config.border_color, self._data),
                    mode=self._mode,
                )
            )

        return "".join(segments)

    async def _create_bar(self):
        active_idx = min(
            math.ceil(self._completed * self._bar_width / self._total),
            self._bar_width,
        )

        if active_idx >= self._bar_width:
            active_idx = self._bar_width - 1

        segments: list[str] = []

        if self._start:
            segments.append(
                await stylize(
                    self._start,
                    color=get_style(self._config.border_color, self._data),
                    highlight=get_style(self._config.border_highlight, self._data),
                    mode=self._mode,
                )
            )

        else:
            segments.extend(
                await stylize(
                    "".join([self._complete for _ in range(0, active_idx)]),
                    color=get_style(self._config.complete_color, self._data),
                    highlight=get_style(self._config.complete_highlight, self._data),
                    mode=self._mode,
                )
            )

            segments.append(
                await stylize(
                    self._active[self._next_spinner_frame],
                    color=get_style(self._config.active_color, self._data),
                    highlight=get_style(self._config.active_highlight, self._data),
                    mode=self._mode,
                )
            )

            self._next_spinner_frame = (self._next_spinner_frame + 1) % len(self._active)

            segments.extend(
                await stylize(
                    "".join(
                        [
                            self._incomplete
                            for _ in range(active_idx + 1, self._bar_width)
                        ]
                    ),
                    color=get_style(self._config.incomplete_color, self._data),
                    highlight=get_style(self._config.incomplete_highlight, self._data),
                    mode=self._mode,
                )
            )

        if self._end:
            segments.append(
                await stylize(
                    self._end,
                    color=get_style(self._config.border_color, self._data),
                    highlight=get_style(self._config.border_color, self._data),
                    mode=self._mode,
                )
            )

        return "".join(segments)
