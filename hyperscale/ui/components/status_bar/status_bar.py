import asyncio
import math
from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
from .status_bar_config import StatusBarConfig, StylingMap


class StatusBar:
    def __init__(
        self,
        name: str,
        config: StatusBarConfig,
        subscriptions: list[str] | None = None,
    ):
        self.fit_type = WidgetFitDimensions.X_AXIS
        self._name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._default_status = config.default_status

        status_styling_map = self._config.status_styles
        if status_styling_map is None:
            status_styling_map: StylingMap = {}

        self._status_styles = status_styling_map

        self._max_width: int = 0

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[str] | None = None

        self._last_frame: list[str] | None = None

        self._mode = TerminalMode.to_mode(self._config.terminal_mode)

    @property
    def raw_size(self):
        return self._max_width

    @property
    def size(self):
        return self._max_width

    async def fit(self, max_width: int | None = None):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        self._last_frame = None
        self._max_width = max_width
        self._updates.put_nowait(self._default_status)

    async def update(self, status: str):
        await self._update_lock.acquire()

        self._updates.put_nowait(status)

        self._update_lock.release()

    async def get_next_frame(self):
        status = await self._check_if_should_rerender()

        rerender = False

        if status:
            frame = await self._rerender(status)
            self._last_frame = frame
            rerender = True

        elif self._last_frame is None:
            frame = await self._rerender(self._default_status)
            self._last_frame = frame
            rerender = True

        return [self._last_frame], rerender

    async def _rerender(self, status: str):
        status_text = (
            " " * self._config.horizontal_padding
            + status
            + " " * self._config.horizontal_padding
        )

        status_text_width = len(status_text)

        if status_text_width > self._max_width:
            status_text = self._trim_status_text(status_text)

        if status_styles := self._status_styles.get(status):
            status_text = await stylize(
                status_text,
                color=get_style(
                    status_styles.get("color"),
                    status,
                ),
                highlight=get_style(
                    status_styles.get("highlight"),
                    status,
                ),
                attrs=[
                    get_style(
                        attr,
                        status,
                    )
                    for attr in status_styles.get("attrs", [])
                ],
                mode=self._mode,
            )

        remainder = self._max_width - status_text_width
        return self._pad_status_text_horizontal(status_text, remainder)

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

    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        status: str | None = None
        if self._updates.empty() is False:
            status = await self._updates.get()

        self._update_lock.release()

        return status

    def _trim_status_text(
        self,
        status_text: str,
        status_text_width: int,
    ):
        difference = status_text_width - self._max_width

        match self._config.horizontal_alignment:
            case "left":
                return status_text[difference:]

            case "center":
                left_adjust = math.ceil(difference / 2)
                right_adjust = math.floor(difference / 2)

                return status_text[left_adjust : status_text_width - right_adjust]

            case "right":
                return status_text[: status_text_width - right_adjust]

    def _pad_status_text_horizontal(
        self,
        status_text: str,
        remainder: int,
    ):
        left_pad = math.ceil(remainder / 2)
        right_pad = math.floor(remainder / 2)

        return " " * left_pad + status_text + " " * right_pad
