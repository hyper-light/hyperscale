import asyncio

from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.terminal.styling import stylize, get_style
from .text_config import TextConfig


class Text:
    def __init__(
        self,
        config: TextConfig
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS

        self._config = config
        self._text: str | None = None

        self._max_width: int | None = None
        self._text_width = 0


        self._update_lock: asyncio.Lock | None = None
        self._mode = TerminalMode.to_mode(config.terminal_mode)

    @property
    def raw_size(self):
        return self._text_width

    @property
    def size(self):
        return self._text_width
    
    async def update(self, text: str):
        await self._update_lock.acquire()
        self._text = text

        self._update_lock.release()

    async def fit(
        self,
        max_width: int | None = None,
    ):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if max_width <= len(self._text):
            self._text = self._text[:max_width]

        self._max_width = max_width
        self._text_width = len(self._text)

    async def get_next_frame(self) -> str:
        return await stylize(
            self._text if self._text else self._config.default_text,
            color=get_style(
                self._config.color,
                self._text,
            ),
            highlight=get_style(
                self._config.highlight,
                self._text,
            ),
            attrs=[
                get_style(
                    attr,
                    self._text,
                ) for attr in self._config.attributes
            ] if self._config.attributes else None,
            mode=self._mode,
        )
    
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
