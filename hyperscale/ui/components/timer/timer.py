import asyncio
import time

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
from typing import Any, Literal
from .timer_config import TimerConfig


UnitGranularity = Literal[
    'seconds',
    'minutes',
    'hours',
    'days',
    'weeks',
    'years'
]


class Timer:

    def __init__(
        self,
        name: str,
        config: TimerConfig,
    ):
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.name = name

        self._config = config

        self._start: float | None = None

        self._max_width: int | None = None

        self._time_width = 6

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[int | float] | None = None


        self._last_frame: str | None = None
        self._current_unit_granularity: UnitGranularity = 'seconds'
        self._granularity_map: dict[
            UnitGranularity,
            float
        ] = {
            'seconds': 0.01,
            'minutes': 1,
            'hours': 60,
            'days': 3600,
            'weeks': 86400,
            'years': 604800
        }

        self._mode = TerminalMode.to_mode(config.terminal_mode)

    @property
    def raw_size(self):
        return self._time_width

    @property
    def size(self):
        return self._time_width
    
    async def fit(
        self,
        max_width: int | None = None,
    ):

        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        self._max_width = max_width

        self._updates.put_nowait(0)

    async def update(
        self,
        _: Any = None,
    ):
        await self._update_lock.acquire()

        if self._start is None:
            self._start = time.monotonic()

        self._updates.put_nowait(time.monotonic())

        self._update_lock.release()

    async def get_next_frame(self):

        if self._start is None:
            self._start = time.monotonic()

        
        timer_start = await self._check_if_should_rerender()
        if timer_start:
            self._start = timer_start


        elapsed = time.monotonic() - self._start

        if elapsed < self._granularity_map.get(self._current_unit_granularity, 0.01) and self._last_frame:
            return [self._last_frame], False

        seconds_whole = int(elapsed)%60
        seconds_decimal = int((elapsed % 1) * 100)

        time_string = ""

        if seconds_whole > 0 and seconds_whole < 10:
            time_string = f' {seconds_whole:01d}.{seconds_decimal:02d}s'

        elif seconds_whole >= 10 and seconds_whole < 60:
            time_string = f'{seconds_whole:02d}.{seconds_decimal:02d}s'

        minutes = (elapsed/60)
        minutes_whole = int(minutes)%60

        if minutes_whole > 0 and self._current_unit_granularity == 'seconds':
            self._current_unit_granularity = 'minutes'

        if minutes_whole > 0 and minutes_whole < 10:
            time_string = f' {minutes_whole:01d}m' + f'{seconds_whole:02d}s'

        elif minutes_whole >= 10 and minutes_whole < 60:
            time_string = f'{minutes_whole:02d}m' + f'{seconds_whole:02d}s'

        hours = (elapsed/3600)
        hours_whole = int(hours)%24

        if hours_whole > 0 and self._current_unit_granularity == 'minutes':
            self._current_unit_granularity = 'hours'

        if hours_whole > 0 and hours_whole < 10:
            time_string = f' {hours_whole:01d}h' + f'{minutes_whole:02d}m'

        elif hours_whole >= 10 and hours_whole < 24:
            time_string = f'{hours_whole:02d}h' + f'{minutes_whole:02d}m'
        
        days = elapsed/86400
        days_whole = int(days)

        if days_whole > 0 and self._current_unit_granularity == 'hours':
            self._current_unit_granularity = 'days'

        if days_whole > 0 and days_whole < 7:
            time_string = f' {days_whole:01d}d' + f'{hours_whole:02d}h'
        
        weeks = elapsed/604800
        weeks_whole = int(weeks)

        if weeks_whole > 0 and self._current_unit_granularity == 'days':
            self._current_unit_granularity = 'weeks'

        if weeks_whole > 0 and weeks_whole < 10:
            time_string = f' {weeks_whole:01d}w' + f'{days_whole:02d}d'

        elif weeks_whole >= 10 and weeks_whole < 52:
            time_string = f'{weeks_whole:02d}w' + f'{days_whole:02d}d'

        years = elapsed/3.154e7
        years_whole = int(years)
        years_decimal = int((years % 1) * 10)

        if years_whole > 0 and self._current_unit_granularity == 'weeks':
            self._current_unit_granularity = 'years'

        if years_whole > 0 and years_whole < 10:
            time_string = f' {years_whole:01d}y' + f'{weeks_whole:02d}w'

        elif years_whole >= 10:
            time_string = f'{years_whole}.' +  f'{years_decimal:02d}y'

        return await self._format_time_string(
            time_string,
            elapsed,
        )


    async def _format_time_string(
        self, 
        time_string: str, 
        elapsed: float,
    ):

        time_string = time_string[:self._max_width]
        
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
                attrs=[
                    get_style(
                        attr,
                        elapsed
                    ) for attr in self._config.attributes
                ] if self._config.attributes else None,
                mode=self._mode,
            )

        self._last_frame = time_string

        return [self._last_frame], True
    
        
    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        data: float | None = None
        
        if self._updates.empty() is False:
            data = await self._updates.get()

        self._update_lock.release()

        return data
    
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
