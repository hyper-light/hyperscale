import asyncio
import math
from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.terminal.styling import stylize, get_style
from .status_bar_config import StatusBarConfig, StylingMap


class StatusBar:
    def __init__(
        self,
        config: StatusBarConfig,
    ):
        self.fit_type = WidgetFitDimensions.X_AXIS
        self._config = config
        self._default_status = config.default_status

        status_styling_map = self._config.status_styles
        if status_styling_map is None:
            status_styling_map: StylingMap = {}

        self._status_styles = status_styling_map

        self._status: str | None = None
        self._max_width: int = 0

        self._update_lock: asyncio.Lock | None = None

        self._mode = TerminalMode.to_mode(self._config.terminal_mode)

    @property
    def raw_size(self):
        return self._max_width

    @property
    def size(self):
        return self._max_width

    async def update(self, status: str):
        await self._update_lock.acquire()

        self._status = status

        self._update_lock.release()

    async def fit(self, max_width: int | None = None):

        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        self._max_width = max_width

    async def get_next_frame(self):
        status_text = (
            " " * self._config.horizontal_padding
            + self._status
            + " " * self._config.horizontal_padding
        )

        status_text_width = len(status_text)

        if status_text_width > self._max_width:
            status_text = self._trim_status_text(status_text)

        if status_styles := self._status_styles.get(self._status):
            status_text = await stylize(
                status_text,
                color=get_style(
                    status_styles.get("color"),
                    self._status,
                ),
                highlight=get_style(
                    status_styles.get("highlight"),
                    self._status,
                ),
                attrs=[
                    get_style(
                        attr,
                        self._status,
                    ) for attr in status_styles.get("attrs", [])
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
