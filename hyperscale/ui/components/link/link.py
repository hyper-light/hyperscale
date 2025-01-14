import asyncio

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
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
    def __init__(
        self,
        name: str,
        config: LinkConfig,
        subscriptions: list[str] | None = None,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._text: str = config.link_text
        self._link: LinkValidator | None = config.link_url

        self._max_width: int | None = None

        self._link_fmt_str = "\x1b]8;;%s\x1b\\%s\x1b]8;;\x1b\\"

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[URLTextPair] | None = None
        self._mode = TerminalMode.to_mode(self._config.terminal_mode)
        self._last_frame: str | None = None

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

        self._updates.put_nowait(
            {
                "url": self._config.link_url,
                "text": self._config.link_text,
            }
        )

    async def update(
        self,
        link_and_text: URLTextPair,
    ):
        await self._update_lock.acquire()
        self._updates.put_nowait(link_and_text)

        self._update_lock.release()

    async def get_next_frame(self):
        (link, text) = await self._check_if_should_rerender()
        rerender = False

        if link or text:
            frame = await self._rerender(
                link if link else self._config.link_url,
                text if text else self._config.link_text,
            )

            self._last_frame = [frame]
            rerender = True

        elif self._last_frame is None:
            frame = await self._rerender(
                self._config.link_url,
                self._config.link_text,
            )

            self._last_frame = [frame]
            rerender = True

        return self._last_frame, rerender

    async def _rerender(
        self,
        link: LinkValidator | None,
        text: str,
    ):
        link_text = text[: self._max_width]

        styled_link_text = await stylize(
            link_text,
            color=get_style(
                self._config.color,
                link,
                text,
            ),
            highlight=get_style(
                self._config.highlight,
                link,
                text,
            ),
            attrs=[
                get_style(
                    attr,
                    link,
                    text,
                )
                for attr in self._config.attributes
            ]
            if self._config.attributes
            else None,
            mode=self._mode,
        )

        if link is None:
            return styled_link_text

        return self._link_fmt_str % (str(link.url), styled_link_text)

    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        link: LinkValidator | None = None
        text: str | None = None

        if self._updates.empty() is False:
            update: URLTextPair = await self._updates.get()

            if update_url := update.get("url"):
                link = LinkValidator(url=update_url)

            if update_text := update.get("text"):
                text = update_text

        self._update_lock.release()

        return (link, text)

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
