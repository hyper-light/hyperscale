import asyncio

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions

from .meter_config import MeterConfig
from .models import MeterReading
from .styled_meter import styled_meter

# Before its first reading a meter draws an empty bar.
NO_READING = MeterReading(used=0.0, total=0.0)


class Meter:
    """An htop/btop-style bar gauge of one ratio -- cores in use, workers
    healthy, a quorum -- as wide as its section: ``██████████░░░░ 6/8``,
    filled to the eighth of a cell in extended mode (``#`` and ``-`` in
    whole cells in compatibility mode), with its reading's label after it
    when it has one and the config shows it.

    It is updated with a ``MeterReading``; each update is the whole
    reading, so only the newest queued is drawn.
    """

    def __init__(
        self,
        name: str,
        config: MeterConfig,
        subscriptions: list[str] | None = None,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.name = name
        self.subscriptions = subscriptions if subscriptions is not None else []
        self._config = config
        self._mode = TerminalMode.to_mode(config.terminal_mode)
        self._max_width = 0
        self._reading = NO_READING
        self._updates: asyncio.Queue[MeterReading] = asyncio.Queue()
        self._last_frame: list[str] | None = None

    @property
    def raw_size(self) -> int:
        return self._max_width

    @property
    def size(self) -> int:
        return self._max_width

    async def fit(self, max_width: int | None = None) -> None:
        """Take ``max_width`` columns; the next frame redraws."""
        self._max_width = max_width or 0
        self._last_frame = None

    async def update(self, reading: MeterReading) -> None:
        """Show ``reading`` from the next frame."""
        self._updates.put_nowait(reading)

    async def get_next_frame(self) -> tuple[list[str], bool]:
        """The meter's one line, and whether it changed since the last."""
        rerender = self._take_newest_update() or self._last_frame is None
        if rerender:
            self._last_frame = [
                await styled_meter(
                    self._reading, self._max_width, self._config.fill_color, self._mode, self._config.show_label
                )
            ]

        return self._last_frame, rerender

    def _take_newest_update(self) -> bool:
        """Take the newest queued reading, if any; whether there was one."""
        queued = False
        while not self._updates.empty():
            self._reading = self._updates.get_nowait()
            queued = True

        return queued

    async def pause(self) -> None:
        pass

    async def resume(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def abort(self) -> None:
        pass
