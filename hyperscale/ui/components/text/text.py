import asyncio
import math

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
from .text_config import TextConfig


class Text:
    def __init__(
        self,
        name: str,
        config: TextConfig,
        subscriptions: list[str] | None = None,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._max_width: int | None = None
        self._text_width = 0

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[str] | None = None

        self._last_frame: list[str] | None = None

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

        self._last_frame = None

        self._max_width = max_width

    async def update(self, text: str):
        await self._update_lock.acquire()
        self._updates.put_nowait(text)

        if self._update_lock.locked():
            self._update_lock.release()

    async def get_next_frame(self):
        text = await self._check_if_should_rerender()
        rerender = False

        if text is not None:
            frame = await self._rerender(text)
            self._last_frame = [frame]
            rerender = True

        elif self._last_frame is None:
            frame = await self._rerender(self._config.text)
            self._last_frame = [frame]
            rerender = True

        return self._last_frame, rerender

    async def _rerender(self, text: str):
        text_length = len(text)

        if text_length >= self._max_width:
            text = text[:self._max_width - 3] + '...'

        remainder = self._max_width - len(text)

        styled_text = await stylize(
            text,
            color=get_style(
                self._config.color,
                text,
            ),
            highlight=get_style(
                self._config.highlight,
                text,
            ),
            attrs=[
                get_style(
                    attr,
                    text,
                )
                for attr in self._config.attributes
            ]
            if self._config.attributes
            else None,
            mode=self._mode,
        )

        return self._pad_text_horizontal(styled_text, remainder)

    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        text: str | None = None
        if self._updates.empty() is False:
            text = await self._updates.get()

        if self._update_lock.locked():
            self._update_lock.release()

        return text

    def _pad_text_horizontal(
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
