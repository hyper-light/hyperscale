import asyncio
import math
import time

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
from .multiline_text_config import MultilineTextConfig


class MultilineText:
    def __init__(
        self,
        name: str,
        config: MultilineTextConfig,
        subscriptions: list[str] | None = None,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_Y_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._max_width: int | None = None
        self._text_width = 0

        self._start: float | None = None
        self._elapsed: float = 0

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[list[str]] | None = None

        self._last_rendered_frames: list[str] = []
        self._last_state: list[str] = []

        self._mode = TerminalMode.to_mode(config.terminal_mode)

    @property
    def raw_size(self):
        return self._max_width

    @property
    def size(self):
        return self._max_width

    async def fit(self, max_width: int | None = None, max_height: int | None = None):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        self._last_rendered_frames = None

        text = self._config.text
        text_length = max([len(line) for line in text])

        if text_length > max_width:
            text = [line[:max_width] for line in text]

        if len(text) > max_height:
            text = text[:max_height]

        self._text_width = text_length
        self._max_width = max_width
        self._max_height = max_height
        self._updates.put_nowait(text)

    async def update(self, text: list[str]):
        await self._update_lock.acquire()
        self._updates.put_nowait(text)

        if self._update_lock.locked():
            self._update_lock.release()

    async def get_next_frame(self):
        if self._start is None:
            self._start = time.monotonic()

        text = await self._check_if_should_rerender()
        rerender = False

        if text is not None:
            self._last_state = text
            frames = await self._rerender(text)
            self._last_rendered_frames = frames

            rerender = True

        elif self._config.text:
            self._last_state = self._config.text
            frames = await self._rerender(self._config.text)
            self._last_rendered_frames = frames
            rerender = True

        elif self._elapsed > self._config.pagination_refresh_rate:
            frames = await self._rerender(self._last_state)
            self._last_rendered_frames = frames

            self._start = time.monotonic()
            rerender = True

        self._elapsed = time.monotonic() - self._start

        return self._last_rendered_frames, rerender

    async def _rerender(self, text: list[str]):
        status_text = list(text)

        remainders: list[int] = [
            max(self._max_width - len(line), 0) for line in status_text
        ]

        status_text = self._cycle_text_rows(status_text)

        for idx, line in enumerate(status_text):
            status_text[idx] = await stylize(
                line,
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

        status_text = [
            self._pad_text_horizontal(line, remainder)
            for line, remainder in zip(status_text, remainders)
        ]

        return status_text

    def _cycle_text_rows(
        self,
        text: list[str],
    ):
        text_length = len(text)

        if text_length < 1:
            return text

        if (
            text_length > self._max_height
            and self._elapsed > self._config.pagination_refresh_rate
        ):
            difference = text_length - self._max_height
            self.offset = (self.offset + 1) % (difference + 1)
            text = text[self.offset : self._max_height + self.offset]

        elif text_length > self._max_height:
            text = text[self.offset : self._max_height + self.offset]

        return text

    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        text: list[str] | None = None
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
        if remainder < 1:
            return status_text

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
