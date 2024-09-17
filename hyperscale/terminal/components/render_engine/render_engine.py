from __future__ import annotations

import asyncio
import signal
import sys
import time
from typing import (
    Any,
    Callable,
    Dict,
    List,
)

from .canvas import Canvas
from .engine_config import EngineConfig
from .section import Section

SignalHandlers = Callable[[int], Any] | int | None


async def default_handler(signame: str, engine: RenderEngine):  # pylint: disable=unused-argument
    """Signal handler, used to gracefully shut down the ``spinner`` instance
    when specified signal is received by the process running the ``spinner``.

    ``signum`` and ``frame`` are mandatory arguments. Check ``signal.signal``
    function for more details.
    """

    await engine.abort()


class RenderEngine:
    def __init__(
        self,
        config: EngineConfig | None = None,
        sigmap: Dict[signal.Signals, asyncio.Coroutine] = None,
    ) -> None:
        self.config = config
        self.canvas = Canvas()

        self._interval = 80 * 0.001
        if self.config:
            self._interval = config.refresh_rate * 0.001
        self._stop_run: asyncio.Event | None = None
        self._hide_run: asyncio.Event | None = None
        self._stdout_lock: asyncio.Lock | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._run_engine: asyncio.Future | None = None
        self._terminal_size: int = 0
        self._spin_thread: asyncio.Future | None = None
        self._frame_height: int = 0

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

    async def initialize(
        self,
        sections: List[Section],
    ):
        width: int | None = None
        height: int | None = None
        if self.config:
            width = self.config.width
            height = self.config.height

        await self.canvas.initialize(
            sections,
            width=width,
            height=height,
        )

    async def render(self):
        if self._run_engine is None:
            self._run_engine = asyncio.ensure_future(self._run())

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
        while not self._stop_run.is_set():
            if self._hide_run.is_set():
                # Wait a bit to avoid wasting cycles
                await asyncio.sleep(self._interval)
                continue

            await self._stdout_lock.acquire()
            await self._clear_terminal()

            frame = await self.canvas.render()

            await self._loop.run_in_executor(None, sys.stdout.write, "\r\n")
            await self._loop.run_in_executor(None, sys.stdout.write, frame)
            await self._loop.run_in_executor(None, sys.stdout.write, "\n\r")
            await self._loop.run_in_executor(None, sys.stdout.flush)

            # Wait
            try:
                await asyncio.wait_for(self._stop_run.wait(), timeout=self._interval)

            except asyncio.TimeoutError:
                pass

            self._stdout_lock.release()

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

    async def _clear_terminal(self):
        if sys.stdout.isatty():
            # ANSI Control Sequence EL does not work in Jupyter
            await self._loop.run_in_executor(
                None,
                sys.stdout.write,
                f"\033[{self.canvas.height + 3}A\033[4K",
            )

        else:
            await self._loop.run_in_executor(
                None,
                sys.stdout.write,
                f"\033[{self._frame_height + 3}A\033[4K",
            )

    async def stop(self):
        self._stop_time = time.time()

        if self._dfl_sigmap:
            # Reset registered signal handlers to default ones
            self._reset_signal_handlers()

        await self.canvas.stop()

        self._stop_run.set()
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

        await self._show_cursor()

    async def abort(self):
        self._stop_time = time.time()

        await self.canvas.abort()

        if self._dfl_sigmap:
            # Reset registered signal handlers to default ones
            self._reset_signal_handlers()

        if not self._stop_run.is_set():
            self._stop_run.set()

            try:
                self._spin_thread.cancel()

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
            ):
                pass

        if self._stdout_lock.locked():
            self._stdout_lock.release()

        await self._stdout_lock.acquire()
        await self._clear_terminal()

        try:
            self._run_engine.cancel()
        except (
            asyncio.CancelledError,
            asyncio.InvalidStateError,
            asyncio.TimeoutError,
        ):
            pass

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
