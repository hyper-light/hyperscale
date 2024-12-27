import asyncio
from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.terminal.styling import stylize, get_style
from .counter_config import CounterConfig


class Counter:
    def __init__(
        self,
        config: CounterConfig,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS
        self._config = config

        self._styled: str | None = None
        self._mode = TerminalMode.to_mode(config.terminal_mode)

        self._unit = config.unit

        self._max_width: int | None = None
        self._counter_size = config.precision + 1

        if config.unit:
            self._counter_size += len(config.unit) + 1

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[int] | None = None

        self._last_count_frame: str | None = None

        self._places_map = {"t": 1e12, "b": 1e9, "m": 1e6, "k": 1e3}

    @property
    def raw_size(self):
        return self._counter_size

    @property
    def size(self):
        return self._counter_size

    async def fit(
        self,
        max_width: int | None = None,
    ):
        
        self._max_width = max_width
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        count_size = self._config.precision + 2
        max_width -= count_size

        self._max_width = max_width
        self._counter_size = count_size

        if self._unit:
            space_size = 1
            unit_width = max_width - count_size - space_size

            self._unit = self._unit[:unit_width]

            unit_size = len(self._unit) + 1

            self._counter_size += unit_size

        self._updates.put_nowait(self._config.initial_amount)

    async def update(
        self,
        amount: int,
    ):
        await self._update_lock.acquire()

        self._updates.put_nowait(amount)

        self._update_lock.release()

    async def get_next_frame(self):

        amount = await self._check_if_should_rerender()

        if amount is None and self._last_count_frame:
            return self._last_count_frame, False
        
        elif amount is None:
            return await self._rerender(self._config.initial_amount), True
        
        return await self._rerender(amount), True
        
    async def _rerender(self, amount: int):

        count = self._format_count(amount)

        return await stylize(
            count,
            color=get_style(
                self._config.color, 
                amount,
            ),
            highlight=get_style(
                self._config.highlight, 
                amount,
            ),
            attrs=[
                get_style(
                    attr,
                    amount,
                ) for attr in self._config.attributes
            ] if self._config.attributes else None,
            mode=TerminalMode.to_mode(self._mode),
        )

    def _format_count(self, amount: int):
        selected_place_adjustment: int | None = None
        selected_place_unit: str | None = None

        sorted_places = sorted(
            self._places_map.items(),
            key=lambda adjustment: adjustment[1],
            reverse=True,
        )

        for place_unit, adjustment in sorted_places:
            if amount / adjustment >= 1:
                selected_place_adjustment = adjustment
                selected_place_unit = place_unit
                break

        if selected_place_adjustment is None:
            selected_place_adjustment = 1

        full_count = str(amount)
        full_count_size = len(full_count)
        count = f"%.{self._config.precision}g" % (
            amount / selected_place_adjustment
        )

        count_size = len(count)

        if "." not in count:
            count += "."

        formatted_count_size = len(count)
        max_size = self._config.precision + 1

        if formatted_count_size > max_size:
            count = count[: self._config.precision + 1]

        elif formatted_count_size < max_size:
            current_digit = max_size
            while len(count) < max_size:
                if current_digit < count_size:
                    count += full_count[current_digit]

                else:
                    count += "0"

                current_digit += 1

        if selected_place_unit:
            count += selected_place_unit

        elif count_size - 1 < full_count_size:
            count += full_count[count_size + 1]

        else:
            count += "0"

        return str(count)
    
    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        data: int | None = None
        
        if self._updates.empty() is False:
            data = await self._updates.get()

        self._update_lock.release()

        return data

    async def pause(self):
        pass

    async def resumse(self):
        pass

    async def stop(self):
        if self._update_lock.locked():
            self._update_lock.release()

    async def abort(self):
        if self._update_lock.locked():
            self._update_lock.release()
