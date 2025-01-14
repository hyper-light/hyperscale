import asyncio
import math
from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
from typing import List
from .counter_config import CounterConfig


class Counter:
    def __init__(
        self,
        name: str,
        config: CounterConfig,
        subscriptions: list[str] | None = None,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._styled: str | None = None
        self._mode = TerminalMode.to_mode(config.terminal_mode)

        self._unit = config.unit

        self._max_width: int | None = None
        self._counter_size = 0

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[int] | None = None

        self._last_count_frame: List[str] | None = None

        self._places_map = {"t": 1e12, "b": 1e9, "m": 1e6, "k": 1e3}

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
        self._max_width = max_width
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        count_size = self._config.precision + 2

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

        if self._update_lock.locked():
            self._update_lock.release()

    async def get_next_frame(self):
        amount = await self._check_if_should_rerender()
        rerender = False

        if amount is not None:
            frame = await self._rerender(amount)
            self._last_count_frame = [frame]
            rerender = True

        elif self._last_count_frame is None:
            frame = await self._rerender(self._config.initial_amount)
            self._last_count_frame = [frame]
            rerender = True

        return self._last_count_frame, rerender

    async def _rerender(self, amount: int):
        count = self._format_count(amount)
        remainder = self._max_width - len(count)

        stylized_count = await stylize(
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
                )
                for attr in self._config.attributes
            ]
            if self._config.attributes
            else None,
            mode=TerminalMode.to_mode(self._mode),
        )

        return self._pad_count_horizontal(stylized_count, remainder)

    def _format_count(self, amount: int):
        amount = float(amount)
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
        count = f"%.{self._config.precision}g" % (amount / selected_place_adjustment)

        count_size = len(count)

        if "." not in count:
            count += "."

        formatted_count_size = len(count)
        max_size = self._config.precision + 1

        try:
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

            elif count_size + 1 < full_count_size:
                count += full_count[count_size + 1]

            else:
                count += "0"

        except Exception:
            return ""

        if self._unit:
            count = f"{count} {self._unit}"

        return count

    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        data: int | None = None

        if self._updates.empty() is False:
            data = await self._updates.get()

        if self._update_lock.locked():
            self._update_lock.release()

        return data

    def _pad_count_horizontal(
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

    async def resumse(self):
        pass

    async def stop(self):
        if self._update_lock.locked():
            self._update_lock.release()

    async def abort(self):
        if self._update_lock.locked():
            self._update_lock.release()
