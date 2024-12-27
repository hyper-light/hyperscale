from __future__ import annotations

import asyncio
import math
import shutil
import signal
import sys
import time
from typing import (
    Any,
    Callable,
    Dict,
    List,
    TypeVar,
)
from hyperscale.terminal.state import Action, ActionData, observe
from .canvas import Canvas
from .engine_config import EngineConfig
from .section import Section
from .refresh_rate import RefreshRateMap, RefreshRate

SignalHandlers = Callable[[int], Any] | int | None


K = TypeVar('K')
T = TypeVar('T', bound=ActionData)


async def default_handler(_: str, engine: Terminal):  # pylint: disable=unused-argument
    """Signal handler, used to gracefully shut down the ``spinner`` instance
    when specified signal is received by the process running the ``spinner``.

    ``signum`` and ``frame`` are mandatory arguments. Check ``signal.signal``
    function for more details.
    """

    await engine.abort()


async def handle_resize(engine: Terminal):
    try:
        await engine.pause()
        loop = asyncio.get_event_loop()

        terminal_size = await loop.run_in_executor(None, shutil.get_terminal_size)

        width = int(math.floor(terminal_size.columns * 0.75))

        height = terminal_size.lines - 5

        width_threshold = 1
        height_threshold = 1

        width_difference = abs(width - engine.canvas.total_width)
        height_difference = abs(height - engine.canvas.total_height)

        width = max(width - (width % 3), 1)

        if width_difference > width_threshold and height_difference > height_threshold:
            await engine.resize(
                width=width,
                height=height,
            )

        elif width_difference > width_threshold:
            await engine.resize(
                width=width,
                height=engine.canvas.height,
            )

        elif height_difference > height_threshold:
            await engine.resize(
                width=engine.canvas.width,
                height=height,
            )


        await engine.resume()

    except Exception:
        pass


class SubscriptionSet:

    def __init__(self):
        self.updates: Dict[str, List[Callable[[ActionData], None]]] = {}

    def add_topic(self, topic: str, update_funcs: List[Callable[[ActionData], None]]):
        self.updates[topic] = update_funcs


class Terminal:
    actions: List[Action[Any, ActionData]] = []
    _updates = SubscriptionSet()
    events: asyncio.Queue[tuple[str, ActionData]] = asyncio.Queue()


    def __init__(
        self,
        sections: List[Section],
        config: EngineConfig | None = None,
        sigmap: Dict[signal.Signals, asyncio.Coroutine] = None,
    ) -> None:
        self.config = config
        self.canvas = Canvas()

        refresh_rate = RefreshRate.MEDIUM.value

        if config and config.override_refresh_rate is None:
            refresh_rate = RefreshRateMap.to_refresh_rate(config.refresh_profile).value
        
        elif config and config.override_refresh_rate:
            refresh_rate = config.override_refresh_rate

        self._interval = round(1 / refresh_rate, 4)

        self._stop_run: asyncio.Event | None = None
        self._hide_run: asyncio.Event | None = None
        self._stdout_lock: asyncio.Lock | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._run_engine: asyncio.Future | None = None
        self._terminal_size: int = 0
        self._spin_thread: asyncio.Future | None = None
        self._frame_height: int = 0
        self._horizontal_padding: int = 0
        self._vertical_padding: int = 0

        self._sigmap = (
            sigmap
            if sigmap
            else {
                signal.SIGINT: default_handler,
                signal.SIGTERM: default_handler,
                signal.SIG_IGN: default_handler,
            }
        )
        # Maps signals to their default handlers in order to reset
        # custom handlers set by ``sigmap`` at the cleanup phase.
        self._dfl_sigmap: dict[signal.Signals, SignalHandlers] = {}
        self._sections = sections

        self._components = {
            component.name: component
            for section in sections
            for component in section.components
        }

        for action in self.actions:
            topic = action.__name__

            subscriptions = [
                component.update_func 
                for component in self._components.values() 
                if topic in component.subscriptions
            ]

            if len(subscriptions) > 0:
                self._updates.add_topic(topic, subscriptions)

    @classmethod
    def action(cls, func: Action[K, T]):
        cls.actions.append(func)
        return observe(
            func,
            cls._updates
        )

    async def resize(
        self,
        width: int,
        height: int,
    ):
        await self.canvas.initialize(
            self.canvas._sections,
            width=width,
            height=height,
            horizontal_padding=self._horizontal_padding,
            vertical_padding=self._vertical_padding,
        )

    async def render(
        self,
        horizontal_padding: int = 0,
        vertical_padding: int = 0,
    ):
        if self._run_engine is None:
            await self._initialize_canvas(
                horizontal_padding=horizontal_padding,
                vertical_padding=vertical_padding,
            )

            self._run_engine = asyncio.ensure_future(self._run())

    async def _initialize_canvas(
        self,
        horizontal_padding: int = 0,
        vertical_padding: int = 0,
    ):
        width: int | None = None
        height: int | None = None

        if horizontal_padding != self._vertical_padding:
            self._horizontal_padding = horizontal_padding

        if vertical_padding != self._vertical_padding:
            self._vertical_padding = vertical_padding

        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        terminal_size = await self._loop.run_in_executor(None, shutil.get_terminal_size)

        if self.config:
            width = self.config.width - self._horizontal_padding
            height = self.config.height - self._vertical_padding

        if width is None:
            width = (
                int(math.floor(terminal_size.columns * 0.75)) - self._horizontal_padding
            )

        width = max(width - (width % 3), 1)

        if height is None:
            height = terminal_size.lines - 5 - self._vertical_padding


        await self.canvas.initialize(
            self._sections,
            width=width,
            height=height,
            horizontal_padding=self._horizontal_padding,
            vertical_padding=self._vertical_padding,
        )

    async def _run(self):
        self._loop = asyncio.get_event_loop()
        await self._hide_cursor()

        self._register_signal_handlers()

        self._start_time = time.time()
        self._stop_time = None  # Reset value to properly calculate subsequent spinner starts (if any)  # pylint: disable=line-too-long
        self._stop_run = asyncio.Event()
        self._hide_run = asyncio.Event()
        self._stdout_lock = asyncio.Lock()

        try:
            self._spin_thread = asyncio.ensure_future(self._execute_render_loop())
        except Exception:
            # Ensure cursor is not hidden if any failure occurs that prevents
            # getting it back
            await self._show_cursor()

    async def _execute_render_loop(self):

        await self._clear_terminal(force=True)

        while not self._stop_run.is_set():
            try:

                await self._stdout_lock.acquire()

                frame = await self.canvas.render()

                frame = f"\033[3J\033[H\n{frame}"

                await self._loop.run_in_executor(None, sys.stdout.write, frame)

                if self._stdout_lock.locked():
                    self._stdout_lock.release()

            except Exception:
                import traceback
                print(traceback.format_exc())

                # Wait
            await asyncio.sleep(self._interval)

    @staticmethod
    async def _show_cursor():
        loop = asyncio.get_event_loop()
        if sys.stdout.isatty():
            # ANSI Control Sequence DECTCEM 1 does not work in Jupyter
            await loop.run_in_executor(None, sys.stdout.write, "\033[?25h")
            await loop.run_in_executor(None, sys.stdout.flush)

    @staticmethod
    async def _hide_cursor():
        loop = asyncio.get_event_loop()
        if sys.stdout.isatty():
            # ANSI Control Sequence DECTCEM 1 does not work in Jupyter
            await loop.run_in_executor(None, sys.stdout.write, "\033[?25l")
            await loop.run_in_executor(None, sys.stdout.flush)

    async def _clear_terminal(
        self,
        force: bool = False,
    ):
        if force:
            await self._loop.run_in_executor(
                None,
                sys.stdout.write,
                "\033[2J\033H",
            )

        else:
            await self._loop.run_in_executor(
                None,
                sys.stdout.write,
                "\033[3J\033[H",
            )
        
    async def pause(self):
        await self.canvas.pause()

        if self._stdout_lock.locked():
            self._stdout_lock.release()

        await self._stdout_lock.acquire()

        if not self._stop_run.is_set():
            self._stop_run.set()

        try:
            await self._spin_thread

        except Exception:
            pass

        try:
            await self._run_engine
        except Exception:
            pass

        await self._clear_terminal(force=True)

    async def resume(self):
        try:
            self._start_time = time.time()
            self._stop_time = None
            self._stop_run = asyncio.Event()

            if self._stdout_lock.locked():
                self._stdout_lock.release()

            self._spin_thread = asyncio.ensure_future(self._execute_render_loop())
        except Exception:
            # Ensure cursor is not hidden if any failure occurs that prevents
            # getting it back
            await self._show_cursor()

    async def stop(self):
        self._stop_time = time.time()

        if self._dfl_sigmap:
            # Reset registered signal handlers to default ones
            self._reset_signal_handlers()

        self._stop_run.set()

        if self._stdout_lock.locked():
            self._stdout_lock.release()

        try:
            self._spin_thread.set_result(None)
            await asyncio.sleep(0)

        except Exception:
            pass

        try:
            self._run_engine.set_result(None)
            await asyncio.sleep(0)
        except Exception:
            pass

        await self._stdout_lock.acquire()

        frame = await self.canvas.render()

        frame = f"\033[3J\033[H{frame}\n"

        await self._loop.run_in_executor(None, sys.stdout.write, frame)
        await self._loop.run_in_executor(None, sys.stdout.flush)

        if self._stdout_lock.locked():
            self._stdout_lock.release()

        await self._show_cursor()

    async def abort(self):
        self._stop_time = time.time()

        await self.canvas.stop()

        if self._dfl_sigmap:
            # Reset registered signal handlers to default ones
            self._reset_signal_handlers()

        self._stop_run.set()

        try:
            self._spin_thread.cancel()
            await asyncio.sleep(0)

        except (
            asyncio.CancelledError,
            asyncio.InvalidStateError,
            asyncio.TimeoutError,
        ):
            pass

        if self._stdout_lock.locked():
            self._stdout_lock.release()

        await self._stdout_lock.acquire()
        
        frame = await self.canvas.render()

        frame = f"\033[3J\033[H\n{frame}"

        await self._loop.run_in_executor(None, sys.stdout.write, frame)

        try:
            self._run_engine.cancel()
            await asyncio.sleep(0)
        except (
            asyncio.CancelledError,
            asyncio.InvalidStateError,
            asyncio.TimeoutError,
        ):
            pass

        self._stdout_lock.release()

        await self._show_cursor()

    def _reset_signal_handlers(self):
        for sig, sig_handler in self._dfl_sigmap.items():
            if sig and sig_handler:
                signal.signal(sig, sig_handler)

    def _register_signal_handlers(self):
        # SIGKILL cannot be caught or ignored, and the receiving
        # process cannot perform any clean-up upon receiving this
        # signal.
        if signal.SIGKILL in self._sigmap:
            raise ValueError(
                "Trying to set handler for SIGKILL signal. "
                "SIGKILL cannot be caught or ignored in POSIX systems."
            )

        for sig in self._sigmap:
            dfl_handler = signal.getsignal(sig)
            self._dfl_sigmap[sig] = dfl_handler

            self._loop.add_signal_handler(
                getattr(signal, sig.name),
                lambda signame=sig.name: asyncio.create_task(
                    default_handler(signame, self)
                ),
            )

        self._loop.add_signal_handler(
            signal.SIGWINCH, lambda: asyncio.create_task(handle_resize(self))
        )
