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

        self._count = config.initial_amount
        self._unit = config.unit

        self._max_width: int | None = None
        self._counter_size = config.precision + 1

        if config.unit:
            self._counter_size += len(config.unit) + 1

        self._update_lock: asyncio.Lock | None = None

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
        
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        count_size = self._config.precision + 2
        max_width -= count_size

        self._max_width = count_size
        self._counter_size = count_size

        if self._unit:
            max_width -= 1

            self._unit = self._unit[:max_width]

            unit_size = len(self._unit) + 1

            self._max_width += unit_size
            self._counter_size += unit_size

    async def update(
        self,
        amount: int = 1,
    ):
        await self._update_lock.acquire()

        self._count += amount

        self._update_lock.release()

    async def set(
        self,
        amount: int,
    ):
        await self._update_lock.acquire()

        self._count = amount

        self._update_lock.release()

    async def get(self):
        return await self.get_next_frame()

    async def get_next_frame(self) -> str:
        await self._update_lock.acquire()
        count = await self._style()
        self._update_lock.release()

        return count

    async def _style(self):
        count = self._format_count()

        return await stylize(
            count,
            color=get_style(
                self._config.color, 
                self._count,
            ),
            highlight=get_style(
                self._config.highlight, 
                self._count,
            ),
            attrs=[
                get_style(
                    attr,
                    self._count,
                ) for attr in self._config.attributes
            ] if self._config.attributes else None,
            mode=TerminalMode.to_mode(self._mode),
        )

    def _format_count(self):
        selected_place_adjustment: int | None = None
        selected_place_unit: str | None = None

        sorted_places = sorted(
            self._places_map.items(),
            key=lambda adjustment: adjustment[1],
            reverse=True,
        )

        for place_unit, adjustment in sorted_places:
            if self._count / adjustment >= 1:
                selected_place_adjustment = adjustment
                selected_place_unit = place_unit
                break

        if selected_place_adjustment is None:
            selected_place_adjustment = 1

        full_count = str(self._count)
        full_count_size = len(full_count)
        count = f"%.{self._config.precision}g" % (
            self._count / selected_place_adjustment
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
