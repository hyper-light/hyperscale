from __future__ import annotations

import asyncio
import contextlib
import functools
import io
import math
import os
import shutil
import signal
import stat
import sys
import time
from collections.abc import AsyncIterator
from types import FrameType
from typing import (
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    List,
    Never,
    TypeVar,
)

from hyperscale.logging.streams.regular_file_stream_writer import RegularFileStreamWriter
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
from .writer import Writer

# What ``signal.getsignal`` returns: a handler (its result is ignored),
# SIG_DFL/SIG_IGN, or None for a handler not installed from Python.
SignalHandlers = Callable[[int, FrameType | None], object] | int | None
Notification = Callable[[], Awaitable[None]]


K = TypeVar("K")
T = TypeVar("T", bound=ActionData)

# The share of the terminal's columns a canvas spans (its lines are that
# wide plus the horizontal padding on each side, so no line reaches the
# terminal's last column).
TERMINAL_WIDTH_SHARE = 0.75
# The terminal's last column, which no line of a frame reaches.
LAST_COLUMN = 1


def canvas_size(
    columns: int,
    lines: int,
    horizontal_padding: int,
    vertical_padding: int,
    width_share: float = TERMINAL_WIDTH_SHARE,
) -> tuple[int, int]:
    """The canvas a terminal of ``columns`` x ``lines`` holds: its
    ``width_share`` of the columns less the horizontal padding -- never so
    wide that a line (the canvas and the padding on each side) reaches the
    terminal's last column, where a terminal wraps the cursor -- rounded
    down to a multiple of three (the sections' thirds), and every line but
    the vertical padding above and below it -- so a frame fills the
    terminal's rows exactly and never passes its bottom."""
    width = min(
        math.floor(columns * width_share) - horizontal_padding,
        columns - 2 * horizontal_padding - LAST_COLUMN,
    )
    return max(width - width % 3, 1), max(lines - 2 * vertical_padding, 1)


async def handle_resize(engine: Terminal):
    try:
        await engine.pause()
        loop = asyncio.get_event_loop()

        terminal_size = await loop.run_in_executor(None, shutil.get_terminal_size)

        # Every section is laid out again for the terminal's new size; the
        # render loop's restart clears the screen and redraws.
        width, height = canvas_size(
            terminal_size.columns,
            terminal_size.lines,
            engine._horizontal_padding,
            engine._vertical_padding,
            engine.width_share,
        )
        if (width, height) != (engine.canvas.width, engine.canvas.height):
            await engine.resize(width=width, height=height)

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
    # Every wrapped action, whatever its argument (``Never``: a one-argument
    # callable of any parameter type is an ``Action[Never, ...]``).
    _actions: List[tuple[Action[Never, ActionData], str | None]] = []
    _updates = SubscriptionSet()
    _render_event: asyncio.Event | None = None

    def __init__(
        self,
        sections: List[Section],
        config: EngineConfig | None = None,
        sigmap: Dict[signal.Signals, Coroutine[None, None, None]] | None = None,
        width_share: float = TERMINAL_WIDTH_SHARE,
    ) -> None:
        self.config = config
        # The share of the terminal's columns the canvas spans (canvas_size).
        self.width_share = width_share
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
        self._writer: Writer | None = None

        # Maps signals to their default handlers in order to reset
        # custom handlers set by ``sigmap`` at the cleanup phase.
        self._dfl_sigmap: dict[signal.Signals, SignalHandlers] = {}

        # Tasks the SIGWINCH and SIGINT handlers start, held until they end:
        # the loop keeps only a weak reference to a task.
        self._resize_tasks: set[asyncio.Task[None]] = set()
        self._keyboard_interrupt_task: asyncio.Task[None] | None = None

        # Each frame reaches the terminal in one write, drawn over the last
        # one in place: the cursor goes home and every line (padded to the
        # frame's width by its sections) overwrites the line under it --
        # never cleared first, which shows a blank screen between frames,
        # and never cleared of scrollback (a full repaint on some
        # terminals): the screen is cleared once, when the render loop
        # starts, and again after a resize. A frame fills the terminal's
        # rows (canvas_size) and ends on its last row with a carriage
        # return, never a newline: a newline there scrolls the screen up a
        # line, and every frame would step the header down. The frame is
        # wrapped in a synchronized update (DEC private mode 2026:
        # "Synchronized Output", contour-terminal's specification adopted
        # from the terminal-wg proposal,
        # https://gist.github.com/christianparpart/d8a62cc1ab659194337d73e399004036):
        # a terminal that supports it shows the frame only once all of it
        # has arrived; one that does not ignores the unknown mode.
        self._frame_prefix = b"\033[?2026h\033[H"
        self._frame_suffix = b"\033[?2026l"
        # The last frame, drawn as the terminal stops, leaves the cursor on
        # the line below it for whatever the shell writes next.
        self._final_frame_suffix = b"\033[?2026l\n"

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

    @property
    def refresh_interval(self) -> float:
        """Seconds between the terminal's refreshes, from its refresh rate."""
        return self._interval

    @classmethod
    def trigger_render(cls):
        """Signal the render loop to wake up and re-render immediately."""
        if cls._render_event is not None and not cls._render_event.is_set():
            cls._render_event.set()

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
            on_update=cls.trigger_render,
        )

    @classmethod
    def subscribe(cls, channel: str, update: Callable[[ActionData], Awaitable[None]]) -> None:
        """Receive every update a wrapped action publishes on ``channel``,
        as a component does, without rendering: a reader of the actions
        other than a terminal (the CI-safe summary of a run)."""
        cls._updates.add_topic(channel, [update])

    @classmethod
    def unsubscribe(cls, updates: list[Callable[[ActionData], Awaitable[None]]]) -> None:
        """Stop ``updates`` receiving any channel's updates, and release
        them."""
        cls._updates.remove_updates(updates)

    @contextlib.asynccontextmanager
    async def updating(self) -> AsyncIterator[None]:
        """Hold the next frame while a batch of updates is published, so
        no frame shows part of the batch: the render loop draws once the
        batch is whole."""
        await self._stdout_lock.acquire()
        try:
            yield

        finally:
            self._stdout_lock.release()

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
            # Return once the terminal has hidden the cursor, claimed its
            # signals and started its render loop: a caller that routes
            # SIGINT itself (ShutdownSignals) must claim it after this, not
            # race the terminal's own registration.
            await self._run_engine

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
        self._writer = await self._create_writer(self._stdout)

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

        terminal_width, terminal_height = canvas_size(
            terminal_size.columns,
            terminal_size.lines,
            self._horizontal_padding,
            self._vertical_padding,
            self.width_share,
        )
        if width is None:
            width = terminal_width

        width = max(width - (width % 3), 1)

        if height is None:
            height = terminal_height

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

        # Initialize the class-level render event
        Terminal._render_event = asyncio.Event()

        # Initial render
        try:
            await self._stdout_lock.acquire()

            frame = await self.canvas.render()

            self._writer.write(self._frame_prefix + frame.encode() + self._frame_suffix)
            await self._writer.drain()

        except Exception:
            pass

        finally:
            if self._stdout_lock.locked():
                self._stdout_lock.release()

        # Wait for action triggers to re-render
        while not self._stop_run.is_set():
            await Terminal._render_event.wait()
            Terminal._render_event.clear()

            if self._stop_run.is_set():
                break

            # Coalesce rapid triggers - wait briefly to batch multiple events
            await asyncio.sleep(0)
            Terminal._render_event.clear()

            try:
                await self._stdout_lock.acquire()

                frame = await self.canvas.render()

                self._writer.write(self._frame_prefix + frame.encode() + self._frame_suffix)
                await self._writer.drain()

            except Exception:
                pass

            finally:
                if self._stdout_lock.locked():
                    self._stdout_lock.release()

    async def _show_cursor(self):
        if await self._loop.run_in_executor(None, self._stdout.isatty):
            # ANSI Control Sequence DECTCEM 1 does not work in Jupyter

            await self._stdout_lock.acquire()
            self._writer.write(b"\033[?25h")
            await self._writer.drain()

            if self._stdout_lock.locked():
                self._stdout_lock.release()

    async def _hide_cursor(self):
        if await self._loop.run_in_executor(None, self._stdout.isatty):
            await self._stdout_lock.acquire()
            self._writer.write(b"\033[?25l")
            await self._writer.drain()

            if self._stdout_lock.locked():
                self._stdout_lock.release()

    async def _clear_terminal(
        self,
        force: bool = False,
    ):
        if force:
            self._writer.write(b"\033[2J\033[H")

        else:
            self._writer.write(b"\033[3J\033[H")
        
        await self._writer.drain()

    async def pause(self):
        await self.canvas.pause()

        if self._stdout_lock.locked():
            self._stdout_lock.release()

        await self._stdout_lock.acquire()

        if not self._stop_run.is_set():
            self._stop_run.set()

        # Wake up the render loop so it can exit
        Terminal.trigger_render()

        try:
            await self._spin_thread

        except Exception:
            pass

        try:
            await self._run_engine
        except Exception:
            pass

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
            await self._cancel_resize_tasks()

        self._stop_run.set()

        # Wake up the render loop so it can exit
        Terminal.trigger_render()

        try:
            await self._spin_thread

        except Exception:
            pass

        if self._stdout_lock.locked():
            self._stdout_lock.release()

        await self._stdout_lock.acquire()

        frame = await self.canvas.render()

        self._writer.write(self._frame_prefix + frame.encode() + self._final_frame_suffix)
        await self._writer.drain()

        try:
            await self._run_engine
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
            await self._cancel_resize_tasks()

        self._stop_run.set()

        # Wake up the render loop so it can exit
        Terminal.trigger_render()

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

        self._writer.write(self._frame_prefix + frame.encode() + self._final_frame_suffix)
        await self._writer.drain()

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
        # A resize after the terminal stopped must not restart its render
        # loop (handle_resize resumes it).
        self._loop.remove_signal_handler(signal.SIGWINCH)

        for sig, sig_handler in self._dfl_sigmap.items():
            if sig and sig_handler:
                signal.signal(sig, sig_handler)

    async def _create_writer(self, stdout: io.TextIOWrapper) -> Writer | RegularFileStreamWriter:
        """A writer for the duplicated stdout: asyncio's pipe transport for a
        pipe, socket or terminal, or a buffered off-loop writer for a
        regular file (``> run.log``), which the pipe transport rejects."""
        stdout_mode = (await self._loop.run_in_executor(None, os.fstat, stdout.fileno())).st_mode
        if stat.S_ISREG(stdout_mode):
            return RegularFileStreamWriter(stdout.fileno(), self._loop)

        return await self._create_pipe_writer(stdout)

    async def _create_pipe_writer(self, stdout: io.TextIOWrapper) -> Writer:
        """A writer over asyncio's pipe transport for the duplicated stdout."""
        transport, protocol = await self._loop.connect_write_pipe(
            lambda: TerminalProtocol(),
            stdout,
        )

        try:
            if has_uvloop:
                transport.close = patch_transport_close(transport, self._loop)

        except Exception:
            pass

        self._transport = transport
        self._protocol = protocol
        return Writer(
            transport,
            protocol,
            None,
            self._loop,
        )

    async def close(self):
        """Release what the terminal holds past stop() or abort(): its
        components' subscriptions to the actions, and its duplicate of
        stdout. Call it once the terminal will render no more."""
        self._updates.remove_updates(
            [component.update for section in self.canvas.sections for component in section.components.values()]
        )

        # Closed only once the transport has flushed what the terminal
        # wrote: its process may exit right after.
        self._writer.close()
        await self._writer.wait_closed()
        # A pipe transport closed the duplicate with itself (closing it
        # again does nothing); a regular file's writer does not own it.
        self._stdout.close()

    def _register_signal_handlers(self):
        self._loop.add_signal_handler(signal.SIGWINCH, self._on_resize_signal)

        # Store the original SIGINT handler so we can restore and re-raise
        self._dfl_sigmap[signal.SIGINT] = signal.getsignal(signal.SIGINT)

        self._loop.add_signal_handler(signal.SIGINT, self._on_keyboard_interrupt_signal)

    def _on_resize_signal(self) -> None:
        """SIGWINCH: resize in a task the terminal holds until it ends, so
        stop() and abort() can cancel it before it resumes the render loop."""
        resize_task = self._loop.create_task(handle_resize(self))
        self._resize_tasks.add(resize_task)
        resize_task.add_done_callback(self._resize_tasks.discard)

    def _on_keyboard_interrupt_signal(self) -> None:
        """SIGINT: abort the terminal in a task it holds. A repeat while
        that abort runs is ignored so it cannot interrupt the abort midway
        (as ShutdownSignals does); the abort re-sends SIGINT when done."""
        if self._keyboard_interrupt_task is None or self._keyboard_interrupt_task.done():
            self._keyboard_interrupt_task = self._loop.create_task(self._handle_keyboard_interrupt())

    async def _cancel_resize_tasks(self) -> None:
        """Cancel and wait out the resizes still running: a resize resumes
        the render loop, which stop() and abort() end."""
        for resize_task in self._resize_tasks:
            resize_task.cancel()

        await asyncio.gather(*self._resize_tasks, return_exceptions=True)

    async def _handle_keyboard_interrupt(self):
        """Handle keyboard interrupt by aborting the terminal and re-sending SIGINT."""
        try:
            await self.abort()
        except Exception:
            pass

        # Restore the default SIGINT handler
        if signal.SIGINT in self._dfl_sigmap:
            original_handler = self._dfl_sigmap[signal.SIGINT]
            if original_handler is not None:
                signal.signal(signal.SIGINT, original_handler)

        # Re-send SIGINT to ourselves so the signal propagates correctly
        os.kill(os.getpid(), signal.SIGINT)
