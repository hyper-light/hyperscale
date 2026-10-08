import asyncio
from collections import deque
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
)

try:
    from playwright.async_api import (
        BrowserContext,
        Geolocation,
        Playwright,
        async_playwright,
    )
except Exception:
    class BrowserContext:
        pass

    class Geolocation:
        pass

    class Playwright:
        pass

    async def async_playwright(*args, **kwargs):
        pass

from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.testing.models import URL, Auth, Cookies, Data, Headers, Params

from .browser_page import BrowserPage
from .browser_session import BrowserSession
from .models.results import PlaywrightResult


class MercurySyncPlaywrightConnection:
    def __init__(
        self,
        pool_size: int = 10**3,
        pages: int = 1,
        timeouts: Timeouts | None = None,
    ) -> None:
        self._concurrency = pool_size
        self._max_pages = pages
        self.config = {}
        self.context: Optional[BrowserContext] = None
        self.sessions: Deque[BrowserSession] = deque()
        self._semaphore: asyncio.Semaphore = None
        # Each engine gets its own Timeouts: a default argument would be one
        # instance shared by every engine built without timeouts.
        self.timeouts = timeouts if timeouts is not None else Timeouts()
        self.results: List[PlaywrightResult] = []
        # The session and page each step's task has checked out: a page goes
        # back with the task that took it, whatever order steps finish in.
        self._active: Dict[asyncio.Task, Tuple[BrowserSession, BrowserPage]] = {}
        # Playwright and its browser sessions, started once on the first
        # page any step asks for; every step waits on the same start.
        self._playwright: Optional[Playwright] = None
        self._starting: Optional[asyncio.Task] = None
        # Session closes scheduled by the synchronous close(): held until
        # each finishes, since the event loop keeps only weak references.
        self._closing_sessions: set[asyncio.Task] = set()

    async def _optimize(self, optimized_param: URL | Params | Headers | Cookies | Data | Auth):
        """
        Nothing to prepare: the browser resolves and connects to every
        address itself, so a step's URL is used as given.
        """
        return None

    async def open_page(self):
        await self._semaphore.acquire()

        try:
            if self._starting is None:
                self._starting = asyncio.ensure_future(self.start())

            # Shielded: a step cancelled while waiting does not cancel the
            # start every other step is waiting on.
            await asyncio.shield(self._starting)

            session = self.sessions.popleft()
            page = await session.next_page()

        except BaseException:
            # No page was taken: the slot goes back.
            self._semaphore.release()
            raise

        self._active[asyncio.current_task()] = (session, page)

        return page

    def close_page(self):
        session, page = self._active.pop(asyncio.current_task())

        session.return_page(page)
        self.sessions.append(session)

        self._semaphore.release()

    async def __aenter__(self):
        return await self.open_page()

    async def __aexit__(self, exc_t: Type[Exception], exc_v: Exception, exc_tb: str):
        self.close_page()

    async def start(
        self,
        browser_type: Literal["safari", "webkit", "firefox", "chrome"] = None,
        device_type: str = None,
        locale: str = None,
        geolocation: Geolocation = None,
        permissions: List[str] = None,
        color_scheme: str = None,
        options: Dict[str, Any] = {},
    ):
        self._playwright = playwright = await async_playwright().start()

        sessions = [
            BrowserSession(playwright, self._max_pages, self.timeouts)
            for _ in range(self._concurrency)
        ]
        session_options = {
            "browser_type": browser_type,
            "device_type": device_type,
            "locale": locale,
            "geolocation": geolocation,
            "permissions": permissions,
            "color_scheme": color_scheme,
            "options": options,
            "timeout": self.timeouts.request_timeout,
        }

        # One browser for every session, each in a context of its own:
        # contexts keep cookies, cache and storage apart as separate
        # browsers would, at a fraction of the memory and start-up time.
        # The first session launches it; the rest open their contexts in it.
        first_session, *other_sessions = sessions
        await first_session.open(**session_options)
        await asyncio.gather(
            *[
                session.open(browser=first_session.browser, **session_options)
                for session in other_sessions
            ]
        )

        self.sessions.extend(sessions)

    def close(
        self,
        run_before_unload: Optional[bool] = None,
        reason: Optional[str] = None,
        timeout: Optional[int | float] = None,
    ):
        """
        Stop Playwright's driver, closing the browser the sessions share.
        Synchronous, as every engine's close is: the shutdown is scheduled,
        and held here until it finishes.
        """
        starting, self._starting = self._starting, None
        if starting is None:
            return

        if timeout is None:
            timeout = self.timeouts.request_timeout

        close_task = asyncio.ensure_future(self._shutdown(starting, timeout))
        self._closing_sessions.add(close_task)
        close_task.add_done_callback(self._release_closed_session)

    async def _shutdown(self, starting: asyncio.Task, timeout: int | float) -> None:
        """
        Stop Playwright's driver: the browser it launched, with every
        context and page in it, closes with it. A start still under way is
        waited for first, so nothing it launches outlives the engine.
        """
        await asyncio.wait([starting], timeout=timeout)
        if not starting.done():
            starting.cancel()

        self.sessions.clear()
        self._active.clear()

        playwright, self._playwright = self._playwright, None
        if playwright is not None:
            await asyncio.wait_for(playwright.stop(), timeout=timeout)

    def _release_closed_session(self, close_task: asyncio.Task) -> None:
        """Drop a finished close and retrieve its outcome, so a failed
        close is never reported as an unretrieved task exception."""
        self._closing_sessions.discard(close_task)
        if not close_task.cancelled():
            close_task.exception()
