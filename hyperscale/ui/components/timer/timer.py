import asyncio
import time
import math
from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
from typing import Any, Literal, Tuple, Dict
from .timer_config import TimerConfig
from .timer_status import TimerStatus


UnitGranularity = Literal["seconds", "minutes", "hours", "days", "weeks", "years"]


TimerSignal = Tuple[float | None, TimerStatus]


class Timer:
    def __init__(
        self,
        name: str,
        config: TimerConfig,
        subscriptions: list[str] | None = None,
    ):
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._start: float | None = None
        self._refresh_start: float | None = None
        self._refresh_elapsed: float = 0

        self._max_width: int | None = None
        self._status = TimerStatus.STOPPED

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[TimerSignal] | None = None

        self._last_frame: str | None = None
        self._current_unit_granularity: UnitGranularity = "seconds"
        self._granularity_map: dict[UnitGranularity, float] = {
            "seconds": 0.1,
            "minutes": 1,
            "hours": 60,
            "days": 3600,
            "weeks": 86400,
            "years": 604800,
        }

        self._mode = TerminalMode.to_mode(config.terminal_mode)

    @property
    def raw_size(self):
        return self._max_width

    @property
    def size(self):
        return self._max_width

    async def fit(
        self,
        max_width: int | None = None,
    ):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        self._max_width = max_width

        self._updates.put_nowait((None, self._status))

    async def update(self, run_timer: bool):
        await self._update_lock.acquire()

        if run_timer and self._status != TimerStatus.RUNNING:
            self._updates.put_nowait((time.monotonic(), TimerStatus.STARTING))

        elif run_timer is False:
            self._updates.put_nowait((None, TimerStatus.STOPPING))

        else:
            self._updates.put_nowait((None, self._status))

        if self._update_lock.locked():
            self._update_lock.release()

    async def get_next_frame(self):
        if self._refresh_start is None:
            self._refresh_start = time.monotonic()

        timer, action = await self._check_if_should_rerender()

        self._status = action
        elapsed = 0.00

        match action:
            case TimerStatus.STOPPED:
                elapsed = 0.00

            case TimerStatus.STOPPING:
                elapsed = time.monotonic() - self._start if self._start else 0.00
                self._start = None

            case TimerStatus.STARTING:
                self._start = timer
                elapsed = time.monotonic() - self._start
                self._status = TimerStatus.RUNNING

            case TimerStatus.RUNNING:
                elapsed = time.monotonic() - self._start

        timer_granularity = self._granularity_map.get(
            self._current_unit_granularity, 0.1
        )
        self._refresh_elapsed = time.monotonic() - self._refresh_start

        if self._status == TimerStatus.STOPPED and self._last_frame:
            self._refresh_start = time.monotonic()
            return [self._last_frame], False

        elif self._status == TimerStatus.STOPPING:
            self._refresh_start = time.monotonic()
            self._status = TimerStatus.STOPPED

        elif self._refresh_elapsed >= timer_granularity and self._last_frame:
            self._refresh_start = time.monotonic()
            return [self._last_frame], False

        time_string = self._create_time_string(elapsed)

        return await self._format_time_string(
            time_string,
            elapsed,
        )

    def _create_time_string(
        self,
        elapsed: float,
    ):
        self._current_unit_granularity = self._set_current_unit_granularity(elapsed)

        match self._current_unit_granularity:
            case "seconds":
                return self._create_seconds_string(elapsed)

            case "minutes":
                return self._create_minutes_string(elapsed)

            case "hours":
                return self._create_hours_string(elapsed)

            case "days":
                return self._create_days_string(elapsed)

            case "weeks":
                return self._create_weeks_string(elapsed)

            case _:
                return self._create_seconds_string(elapsed)

    def _set_current_unit_granularity(self, elapsed: float):
        if elapsed < 60:
            return "seconds"

        elif elapsed < 3600:
            return "minutes"

        elif elapsed < 86400:
            return "hours"

        elif elapsed < 604800:
            return "days"

        elif elapsed < 3.154e7:
            return "weeks"

        else:
            return "years"

    def _create_seconds_string(self, elapsed: float):
        seconds_whole = int(elapsed) % 60
        seconds_decimal = int((elapsed % 1) * 10)

        if seconds_whole < 10:
            time_string = f"{seconds_whole:01d}.{seconds_decimal:1d}s"

        elif seconds_whole >= 10 and seconds_whole < 60:
            time_string = f"{seconds_whole:02d}.{seconds_decimal:1d}s"

        return time_string

    def _create_minutes_string(self, elapsed: float):
        minutes = elapsed / 60
        minutes_whole = int(minutes) % 60

        seconds_whole = int(elapsed) % 60

        if minutes_whole > 0 and minutes_whole < 10:
            time_string = f"{minutes_whole:01d}m" + f"{seconds_whole:02d}s"

        elif minutes_whole >= 10 and minutes_whole < 60:
            time_string = f"{minutes_whole:02d}m" + f"{seconds_whole:02d}s"

        return time_string

    def _create_hours_string(self, elapsed: float):
        hours = elapsed / 3600
        hours_whole = int(hours) % 24

        minutes_whole = int(elapsed / 60) % 60

        self._carry = minutes_whole

        if hours_whole > 0 and hours_whole < 10:
            time_string = f"{hours_whole:01d}h" + f"{minutes_whole:02d}m"

        elif hours_whole >= 10 and hours_whole < 24:
            time_string = f"{hours_whole:02d}h" + f"{minutes_whole:02d}m"

        return time_string

    def _create_days_string(self, elapsed: float):
        days = elapsed / 86400
        days_whole = int(days) % 7

        hours_whole = int(elapsed / 3600) % 24

        if days_whole > 0 and days_whole < 7:
            time_string = f"{days_whole:01d}d" + f"{hours_whole:02d}h"

        return time_string

    def _create_weeks_string(self, elapsed: float):
        weeks = elapsed / 604800
        weeks_whole = int(weeks) % 52

        days_whole = int(elapsed / 86400) % 7

        if weeks_whole > 0 and weeks_whole < 10:
            time_string = f"{weeks_whole:01d}w" + f"{days_whole:02d}d"

        elif weeks_whole >= 10 and weeks_whole < 52:
            time_string = f"{weeks_whole:02d}w" + f"{days_whole:02d}d"

        return time_string

    def _create_years_string(self, elapsed: float):
        years = elapsed / 3.154e7
        years_whole = int(years)
        years_decimal = int((years % 1) * 10)

        weeks_whole = int(elapsed / 604800) % 52

        if years_whole > 0 and years_whole < 10:
            time_string = f"{years_whole:01d}y" + f"{weeks_whole:02d}w"

        elif years_whole >= 10:
            time_string = f"{years_whole}." + f"{years_decimal:02d}y"

        return time_string

    async def _format_time_string(
        self,
        time_string: str,
        elapsed: float,
    ):
        time_string = time_string[: self._max_width]
        remainder = self._max_width - len(time_string)

        if self._config.color or self._config.highlight or self._config.attributes:
            time_string = await stylize(
                time_string,
                color=get_style(
                    self._config.color,
                    elapsed,
                ),
                highlight=get_style(
                    self._config.highlight,
                    elapsed,
                ),
                attrs=[get_style(attr, elapsed) for attr in self._config.attributes]
                if self._config.attributes
                else None,
                mode=self._mode,
            )

        if remainder > 0:
            time_string = self._pad_status_text_horizontal(time_string, remainder)

        self._last_frame = time_string

        return [self._last_frame], True

    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        data: TimerSignal = (None, self._status)

        if self._updates.empty() is False:
            data = await self._updates.get()

        if self._update_lock.locked():
            self._update_lock.release()

        return data

    def _pad_status_text_horizontal(
        self,
        status_text: str,
        remainder: int,
    ):
        match self._config.horizontal_alignment:
            case "left":
                return status_text + (remainder * " ")

            case "center":
                left_pad = math.ceil(remainder / 2)
                right_pad = math.floor(remainder / 2)

                return " " * left_pad + status_text + " " * right_pad

            case "right":
                return (remainder * " ") + status_text

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def stop(self):
        if self._update_lock.locked():
            self._update_lock.release()

    async def abort(self):
        if self._update_lock.locked():
            self._update_lock.release()
