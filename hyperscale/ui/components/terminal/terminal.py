from __future__ import annotations

import asyncio
import functools
import io
import math
import os
import shutil
import signal
import sys
import time
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    TypeVar,
)

from hyperscale.ui.state import Action, ActionData, SubscriptionSet, observe
try:
    import uvloop as uvloop
    has_uvloop = True

except Exception:
    has_uvloop = False


from .canvas import Canvas
from .engine_config import EngineConfig
from .refresh_rate import RefreshRate, RefreshRateMap
from .section import Section
from .terminal_protocol import TerminalProtocol, patch_transport_close

SignalHandlers = Callable[[int], Any] | int | None
Notification = Callable[[], Awaitable[None]]


K = TypeVar("K")
T = TypeVar("T", bound=ActionData)


async def handle_resize(engine: Terminal):
    try:
        await engine.pause()
        loop = asyncio.get_event_loop()

        terminal_size = await loop.run_in_executor(None, shutil.get_terminal_size)

        width = int(math.floor(terminal_size.columns * 0.75))

        height = terminal_size.lines - 5

        width_threshold = 0
        height_threshold = 0

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

        if len(engine._updates.triggers) > 0:
            await asyncio.gather(
                *[
                    engine._updates.rerender_last(trigger)
                    for trigger in engine._updates.triggers.values()
                ]
            )

        await engine.resume()

    except Exception:
        pass


class Terminal:
    _actions: List[tuple[Action[Any, ActionData], str | None]] = []
    _updates = SubscriptionSet()

    def __init__(
        self,
        sections: List[Section],
        config: EngineConfig | None = None,
        sigmap: Dict[signal.Signals, asyncio.Coroutine] = None,
    ) -> None:
        self.config = config
        self.canvas = Canvas(sections)

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
        self._stdout: io.TextIOBase | None = None
        self._transport: asyncio.Transport | None = None
        self._protocol: asyncio.Protocol | None = None
        self._writer: asyncio.StreamWriter | None = None

        # Maps signals to their default handlers in order to reset
        # custom handlers set by ``sigmap`` at the cleanup phase.
        self._dfl_sigmap: dict[signal.Signals, SignalHandlers] = {}

        components: dict[str, tuple[list[str], Action[ActionData, ActionData]]] = {}

        for action, default_channel in self._actions:
            if default_channel is None:
                default_channel = action.__name__

            components.update(
                {
                    component.name: (component.subscriptions, component.update)
                    for section in sections
                    for component in section.components.values()
                }
            )

            subscriptions = [
                section.component.update
                for section in sections
                if section.has_component
                and default_channel in section.component.subscriptions
            ]

            if len(subscriptions) > 0:
                self._updates.add_topic(default_channel, subscriptions)

        for subscriptions, update in components.values():
            for subscription in subscriptions:
                self._updates.add_topic(subscription, [update])

    @classmethod
    def wrap_action(
        cls,
        func: Action[K, T],
        default_channel: str | None = None,
    ):
        cls._actions.append((func, default_channel))
        return observe(
            func,
            cls._updates,
            default_channel=default_channel,
        )

    async def set_component_active(self, component_name: str):
        if self._stdout_lock is None:
            self._stdout_lock = asyncio.Lock()

        await self._stdout_lock.acquire()

        if section := self.canvas.get_section(component_name):
            section.set_active(component_name)

        if self._stdout_lock.locked():
            self._stdout_lock.release()

    def add_channel(
        self,
        component_name: str,
        channel: str,
    ):
        if component := self.canvas.get_component(component_name):
            component.subscriptions.append(channel)
            self._updates.add_topic(channel, [component.update])

    async def resize(
        self,
        width: int,
        height: int,
    ):
        await self.canvas.initialize(
            width=width,
            height=height,
            horizontal_padding=self._horizontal_padding,
            vertical_padding=self._vertical_padding,
        )

    async def render_once(
        self,
        horizontal_padding: int = 0,
        vertical_padding: int = 0,
    ):
        await self._initialize_canvas(
            horizontal_padding=horizontal_padding,
            vertical_padding=vertical_padding,
        )

        self._stop_run = asyncio.Event()
        self._hide_run = asyncio.Event()

        if self._stdout_lock is None:
            self._stdout_lock = asyncio.Lock()

        await self._stdout_lock.acquire()

        frame = await self.canvas.render()

        if self._stdout_lock.locked():
            self._stdout_lock.release()

        return frame

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

    async def _dup_stdout(self):
        stdout_fileno = await self._loop.run_in_executor(None, sys.stdout.fileno)

        stdout_dup = await self._loop.run_in_executor(
            None,
            os.dup,
            stdout_fileno,
        )

        return await self._loop.run_in_executor(
            None, functools.partial(os.fdopen, stdout_dup, mode=sys.stdout.mode)
        )

    async def _initialize_canvas(
        self,
        horizontal_padding: int = 0,
        vertical_padding: int = 0,
    ):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        self._stdout = await self._dup_stdout()

        transport, protocol = await self._loop.connect_write_pipe(
            lambda: TerminalProtocol(),
            self._stdout,
        )

        try:
            if has_uvloop:
                transport.close = patch_transport_close(transport, self._loop)

        except Exception:
            pass

        self._transport = transport
        self._protocol = protocol
        self._writer = asyncio.StreamWriter(
            transport,
            protocol,
            None,
            self._loop,
        )

        width: int | None = None
        height: int | None = None

        if horizontal_padding != self._vertical_padding:
            self._horizontal_padding = horizontal_padding

        if vertical_padding != self._vertical_padding:
            self._vertical_padding = vertical_padding

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

        self._stop_run = asyncio.Event()
        self._hide_run = asyncio.Event()

        if self._stdout_lock is None:
            self._stdout_lock = asyncio.Lock()

        await self.canvas.initialize(
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

                frame = f"\033[3J\033[H{frame}\n".encode()
                await self._loop.run_in_executor(None, self._writer.write, frame)

                if self._stdout_lock.locked():
                    self._stdout_lock.release()

            except Exception:
                pass

                # Wait
            await asyncio.sleep(self._interval)

    async def _show_cursor(self):
        loop = asyncio.get_event_loop()
        if await self._loop.run_in_executor(None, self._stdout.isatty):
            # ANSI Control Sequence DECTCEM 1 does not work in Jupyter

            await self._stdout_lock.acquire()

            await loop.run_in_executor(None, self._writer.write, b"\033[?25h")

            if self._stdout_lock.locked():
                self._stdout_lock.release()

    async def _hide_cursor(self):
        loop = asyncio.get_event_loop()
        if await self._loop.run_in_executor(None, self._stdout.isatty):
            await self._stdout_lock.acquire()
            # ANSI Control Sequence DECTCEM 1 does not work in Jupyter
            await loop.run_in_executor(None, self._writer.write, b"\033[?25l")

            if self._stdout_lock.locked():
                self._stdout_lock.release()

    async def _clear_terminal(
        self,
        force: bool = False,
    ):
        if force:
            await self._loop.run_in_executor(
                None,
                self._writer.write,
                b"\033[2J\033H",
            )

        else:
            await self._loop.run_in_executor(
                None,
                self._writer.write,
                b"\033[3J\033[H",
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

        await self.canvas.stop()

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

        await self._stdout_lock.acquire()

        frame = await self.canvas.render()

        frame = f"\033[3J\033[H{frame}\n".encode()

        await self._loop.run_in_executor(None, self._writer.write, frame)

        try:
            self._run_engine.set_result(None)
            await asyncio.sleep(0)
        except Exception:
            pass

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

        frame = f"\033[3J\033[H{frame}\n".encode()

        await self._loop.run_in_executor(None, self._writer.write, frame)

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
        self._loop.add_signal_handler(
            signal.SIGWINCH, lambda: asyncio.create_task(handle_resize(self))
        )
