import asyncio

from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.terminal.styling import stylize, get_style
from typing import Dict, Literal
from .link_config import LinkConfig
from .link_validator import LinkValidator


URLTextPair = Dict[
    Literal[
        "url",
        "text",
    ],
    str,
]


class Link:
    def __init__(self, config: LinkConfig) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS

        self._config = config

        self._text: str | None = config.default_link_text
        self._link: LinkValidator | None = config.default_url

        self._max_width: int | None = None

        self._link_fmt_str = "\x1b]8;;%s\x1b\\%s\x1b]8;;\x1b\\"

        self._update_lock: asyncio.Lock | None = None
        self._mode = TerminalMode.to_mode(self._config.terminal_mode)

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

        self._max_width = max_width

    async def update(
        self,
        link_and_text: URLTextPair,
    ):
        await self._update_lock.acquire()

        if url := link_and_text.get("url"):
            self._link = LinkValidator(url=url)

        if text := link_and_text.get("text"):
            self._text = text

        self._update_lock.release()

    async def get_next_frame(self):
        link_text = (self._text or self._config.fallback_text)[: self._max_width]

        styled_link_text = await stylize(
            link_text,
            color=get_style(
                self._config.color, 
                self._link.url,
                self._text,
            ),
            highlight=get_style(
                self._config.highlight,
                self._link.url,
                self._text,
            ),
            attrs=[
                get_style(
                    attr,
                    self._link.url,
                    self._text,
                ) for attr in self._config.attributes
            ] if self._config.attributes else None,
            mode=self._mode,
        )

        if self._link is None:
            return styled_link_text

        return self._link_fmt_str % (str(self._link.url), styled_link_text)

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
