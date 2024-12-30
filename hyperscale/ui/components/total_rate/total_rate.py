import asyncio
import time

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style

from .total_rate_config import TotalRateConfig


class TotalRate:
    
    def __init__(
        self,
        name: str,
        config: TotalRateConfig,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.name = name

        self._config = config

        self._unit = config.unit
        self._precision = config.precision

        self._elapsed: int | float = 0
        self._start: float | None = None

        self._max_width: int | None = None
        self._total_rate_width = 0

        self._places_map = {"t": 1e12, "b": 1e9, "m": 1e6, "k": 1e3}

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[int | float] | None = None


        self._last_frame: str | None = None

        self._mode = TerminalMode.to_mode(config.terminal_mode)

    @property
    def raw_size(self):
        return self._total_rate_width

    @property
    def size(self):
        return self._total_rate_width

    async def fit(
        self,
        max_width: int | None = None,
    ):

        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        numerator_units_length = 2
        denominator_units_lenght = 1
        count_size = (
            self._precision + numerator_units_length + denominator_units_lenght + 1
        )

        self._max_width = max_width
        total_rate_width = count_size

        if self._unit:
            space_between = 1
            unit_width = max_width - count_size - space_between

            self._unit = self._unit[:unit_width]
            unit_size = len(self._unit) + space_between

            total_rate_width += unit_size

        self._total_rate_width = total_rate_width
        self._updates.put_nowait(0)

    async def update(
        self,
        amount: int | float,
    ):
        await self._update_lock.acquire()

        if self._start is None:
            self._start = time.monotonic()

        self._updates.put_nowait(amount)

        self._update_lock.release()

    async def get_next_frame(self):
        
        count = await self._check_if_should_rerender()
        rerender = False

        if count is not None:
            frame = await self._rerender(count)
            self._last_frame = [frame]
            rerender = True
        
        elif self._last_frame is None:
            frame = await self._rerender(0)
            self._last_frame = [frame]
            rerender = True
        
        return self._last_frame, rerender

    async def _rerender(self, count: int | float):

        if self._start is None:
            self._start = time.monotonic()

        rate = self._format_rate(count)


        if self._unit:
            rate = f"{rate} {self._unit}"

        rate = f"{rate}/s"


        return await stylize(
            rate,
            color=get_style(
                self._config.color, 
                count,
                self._elapsed, 
            ),
            highlight=get_style(
                self._config.highlight,
                count,
                self._elapsed, 
            ),
            attrs=[
                get_style(
                    attr,
                    count,
                    self._elapsed, 
                ) for attr in self._config.attributes
            ] if self._config.attributes else None
        )

    def _format_rate(self, count: int | float):
        selected_place_adjustment: int | None = None
        selected_place_unit: str | None = None

        sorted_places = sorted(
            self._places_map.items(),
            key=lambda adjustment: adjustment[1],
            reverse=True,
        )

        self._elapsed = time.monotonic() - self._start

        last_rate = count / self._elapsed

        adjustment_idx = 0
        for place_unit, adjustment in sorted_places:
            if last_rate / adjustment >= 1:
                selected_place_adjustment = adjustment
                selected_place_unit = place_unit
                adjustment_idx += 1
                break

        if selected_place_adjustment is None:
            selected_place_adjustment = 1

        full_rate = str(last_rate)
        full_rate_size = len(full_rate)
        rate = f"%.{self._precision}g" % (last_rate / selected_place_adjustment)
        rate_size = len(rate)

        if "." not in rate:
            rate += "."

        formatted_rate_size = len(rate)
        max_size = self._precision + 1

        if formatted_rate_size > max_size:
            rate = rate[: self._precision + 1]

        elif formatted_rate_size < max_size:
            current_digit = max_size
            while len(rate) < max_size:
                if current_digit < rate_size:
                    rate += full_rate[current_digit]

                else:
                    rate += "0"

                current_digit += 1

        if selected_place_unit:
            rate += selected_place_unit

        elif rate_size + 1 < full_rate_size:
            rate += full_rate[rate_size + 1]

        else:
            rate += "0"

        return rate
    
    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        data: int | float | None = None
        
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
