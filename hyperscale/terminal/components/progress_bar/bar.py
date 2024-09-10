from __future__ import annotations

import asyncio
import functools
import inspect
import math
import signal
import sys
import time
from enum import Enum
from os import get_terminal_size
from typing import Any, AsyncGenerator, Callable, Dict, Iterable, Literal, Type

from hyperscale.logging.spinner import ProgressText
from hyperscale.logging_rewrite import Logger
from hyperscale.terminal.components.spinner.spinner_data import spinner_data
from hyperscale.terminal.components.spinner.spinner_factory import SpinnerFactory
from hyperscale.terminal.components.spinner.spinner_types import SpinnerType
from hyperscale.terminal.components.spinner.to_unicode import to_unicode
from hyperscale.terminal.config.mode import TerminalMode

from .progress_bar_chars import ProgressBarChars
from .progress_bar_color_config import ProgressBarColorConfig
from .segment import Segment
from .segment_status import SegmentStatus
from .segment_type import SegmentType

SignalHandlers = Callable[[int, SpinnerType | None], Any] | int | None
Spinners = Type[spinner_data]


class LoggerMode(Enum):
    CONSOLE = "console"
    SYSTEM = "system"


async def default_handler(signame: str, bar: Bar):  # pylint: disable=unused-argument
    """Signal handler, used to gracefully shut down the ``spinner`` instance
    when specified signal is received by the process running the ``spinner``.

    ``signum`` and ``frame`` are mandatory arguments. Check ``signal.signal``
    function for more details.
    """

    await bar.fail()
    await bar.cancel()


class Bar:
    def __init__(
        self,
        data: int | Iterable[Any] | AsyncGenerator[Any, Any],
        chars: ProgressBarChars = None,
        colors: ProgressBarColorConfig | None = None,
        sigmap: Dict[signal.Signals, asyncio.Coroutine] = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
        enabled: bool = True,
        text: str | None = None,
        max_width_percentage: float = 0.5,
        disable_output: bool = False,
    ) -> None:
        total: int = 0
        if isinstance(data, int):
            total = data
            data = range(data)

        elif not isinstance(data, int) and hasattr("__len__", data):
            total = len(data)

        elif not isinstance(data, int) and not inspect.isasyncgen(data):
            total = len(list(data))

        elif inspect.isasyncgen(data) and not hasattr("__len__", data):
            raise Exception(
                "Err. - cannot determine length of async generator without __len__ attribute."
            )

        self._data = data
        self._total = total
        self.mode = mode

        self.segments: list[Segment] = []

        interval = 80
        spinner_char = chars.active_char
        if spinner_char is not None:
            factory = SpinnerFactory()
            spinner = factory.get(spinner_char)

            interval = spinner.interval

        self._interval = interval * 0.001
        self._max_width_percentage = max_width_percentage
        self._bar_width: float = 0
        self._segment_size: float = 0
        self._next_segment: float = 0
        self._completed: int = 0
        self._enable_output = disable_output is False

        self._frame_queue: asyncio.Queue = None

        self.segments.append(
            Segment(
                chars,
                SegmentType.START,
                segment_colors=colors,
                mode=mode,
            )
        )

        self.segments.extend(
            [
                Segment(
                    chars,
                    SegmentType.BAR,
                    segment_default_char=chars.background_char,
                    segment_colors=colors,
                    mode=mode,
                )
                for _ in range(self._total)
            ]
        )

        self.segments.append(
            Segment(
                chars,
                SegmentType.END,
                colors,
                mode=mode,
            )
        )

        # Other
        self._text = text
        self._side = "left"
        self._start_time: float | None = None
        self._stop_time: float | None = None

        # Helper flags
        self._stop_bar: asyncio.Event | None = None
        self._hide_bar: asyncio.Event | None = None
        self._run_progress_bar: asyncio.Future | None = None
        self._last_frames: list[str] | None = None
        self._stdout_lock = asyncio.Lock()
        self._hidden_level = 0
        self._cur_line_len = 0
        self._active_segment_idx = 1
        self._completed_segment_idx = 0
        self._size: int = 0
        self._base_size = len(self.segments)

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

        self.logger = Logger()

        self.display = text

        self.enabled = enabled
        self.logger_mode = LoggerMode.CONSOLE

        self._stdout_lock = asyncio.Lock()
        self._loop = asyncio.get_event_loop()
        self._terminal_width: int | float = 0

    @property
    def raw_size(self):
        return self._base_size

    @property
    def size(self):
        return self._size

    @property
    def elapsed_time(self) -> float:
        if self._start_time is None:
            return 0
        if self._stop_time is None:
            return time.monotonic() - self._start_time
        return self._stop_time - self._start_time

    async def _compose_out(
        self,
        frame: str,
        text: str | bytes | ProgressText | None = None,
        compose_mode: str | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ) -> str:
        if text:
            text = str(text)

        # Timer
        # if self._timer:
        #     sec, fsec = divmod(round(100 * self.elapsed_time), 100)
        #     text += " ({}.{:02.0f})".format(  # pylint: disable=consider-using-f-string
        #         timedelta(seconds=sec), fsec
        #     )
        # Mode
        if compose_mode is None and text:
            out = f"\r{frame} {text}"
        elif text:
            out = f"{frame} {text}\n"

        elif compose_mode is None:
            out = f"{frame}"

        else:
            out = f"{frame}\n"

        return out

    async def __aenter__(self):
        if self._run_progress_bar is None:
            self._run_progress_bar = asyncio.create_task(self._run())

        return self

    async def __aexit__(self, exc_type, exc_val, traceback):
        # Avoid stop() execution for the 2nd time

        if (
            self.enabled
            and self._spin_thread.done() is False
            and self._spin_thread.cancelled() is False
        ):
            await self.stop()

        return False  # nothing is handled

    def __call__(self, fn):
        @functools.wraps(fn)
        async def inner(*args, **kwargs):
            async with self:
                if inspect.iscoroutinefunction(fn):
                    return await fn(*args, **kwargs)

                else:
                    return fn(*args, **kwargs)

        return inner

    async def get_next_frame(self) -> str:
        if self._run_progress_bar is None:
            await self.run()

        if self._frame_queue:
            frame = await self._frame_queue.get()
            self._size = len(frame)

            return frame

        return ""

    async def run(
        self,
        text: str | bytes | ProgressText | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        if self._enable_output is False:
            self._frame_queue = asyncio.Queue()

        if self._run_progress_bar is None:
            await asyncio.gather(*[segment.style() for segment in self.segments])
            terminal_size = await self._loop.run_in_executor(None, get_terminal_size)
            self._terminal_width = terminal_size[0]

            self._bar_width = math.ceil(
                self._terminal_width * self._max_width_percentage
            )

            # If we have 1000 items with a bar width of 100, every 10 items we should increment a segment.
            self._segment_size = self._total / self._bar_width
            self._next_segment = self._segment_size

            self._run_progress_bar = asyncio.create_task(
                self._run(
                    text,
                    mode=mode,
                )
            )

    async def __aiter__(self):
        if inspect.isasyncgen(self._data):
            async for item in self._data:
                yield item

                self.update()

            await self.ok()

        if not inspect.isasyncgen(self._data):
            for item in self._data:
                yield item

                self.update()

            await self.ok()

    def update(self, amount: int | float = 1):
        self._completed += amount * self._segment_size

    async def _run(
        self,
        text: str | bytes | ProgressText | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        if text is None:
            text = self._text

        if self.enabled and self._enable_output:
            await self._hide_cursor()

        if self.enabled:
            self._register_signal_handlers()

            self._start_time = time.time()
            self._stop_time = None  # Reset value to properly calculate subsequent spinner starts (if any)  # pylint: disable=line-too-long
            self._stop_spin = asyncio.Event()
            self._hide_spin = asyncio.Event()
            try:
                self._spin_thread = asyncio.create_task(
                    self._spin(
                        text=text,
                        mode=TerminalMode.to_mode(mode),
                    )
                )
            except Exception:
                # Ensure cursor is not hidden if any failure occurs that prevents
                # getting it back
                if self._enable_output:
                    await self._show_cursor()

    async def stop(self):
        if self.enabled:
            self._stop_time = time.time()

            if self._dfl_sigmap:
                # Reset registered signal handlers to default ones
                self._reset_signal_handlers()

            if self._spin_thread:
                self._stop_spin.set()
                await self._spin_thread

            await self._run_progress_bar

            if self._enable_output:
                await self._clear_line()
                await self._show_cursor()

    async def cancel(self):
        if self.enabled:
            self._stop_time = time.time()

            if self._dfl_sigmap:
                # Reset registered signal handlers to default ones
                self._reset_signal_handlers()

            if not self._stop_spin.is_set():
                self._stop_spin.set()

                try:
                    self._spin_thread.cancel()

                except (
                    asyncio.CancelledError,
                    asyncio.InvalidStateError,
                    asyncio.TimeoutError,
                ):
                    pass

            try:
                self._run_progress_bar.cancel()
            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError,
                asyncio.TimeoutError,
            ):
                pass

            if self._enable_output:
                await self._clear_line()
                await self._show_cursor()

    async def hide(self):
        """Hide the spinner to allow for custom writing to the terminal."""
        thr_is_alive = self._spin_thread and (
            self._spin_thread.done() is False and self._spin_thread.cancelled() is False
        )

        if thr_is_alive and not self._hide_spin.is_set():
            # set the hidden spinner flag
            self._hide_spin.set()

            if self._enable_output:
                await self._clear_line()

                # flush the stdout buffer so the current line
                # can be rewritten to
                await self._loop.run_in_executor(None, sys.stdout.flush)

    async def show(self):
        """Show the hidden spinner."""
        thr_is_alive = self._spin_thread and (
            self._spin_thread.done() is False and self._spin_thread.cancelled() is False
        )

        if thr_is_alive and self._hide_spin.is_set():
            # clear the hidden spinner flag
            self._hide_spin.clear()

            # clear the current line so the spinner is not appended to it
        if thr_is_alive and self._hide_spin.is_set() and self._enable_output:
            await self._clear_line()

    async def write(self, text):
        """Write text in the terminal without breaking the spinner."""
        # similar to tqdm.write()
        # https://pypi.python.org/pypi/tqdm#writing-messages
        await self._stdout_lock.acquire()

        if self._enable_output:
            await self._clear_line()

        if isinstance(text, (str, bytes)):
            _text = to_unicode(text)
        else:
            _text = str(text)

        # Ensure output is Unicode
        assert isinstance(_text, str)

        if self._enable_output:
            await self._loop.run_in_executor(
                None,
                sys.stdout.write,
            )

        self._cur_line_len = 0
        self._stdout_lock.release()

    async def ok(
        self,
        text: str | bytes | ProgressText | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        if self.enabled:
            """Set Ok (success) finalizer to a spinner."""
            await self._freeze(
                SegmentStatus.OK,
                text=text,
                mode=TerminalMode.to_mode(mode),
            )

    async def fail(
        self,
        text: str | bytes | ProgressText | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        if self.enabled:
            """Set fail finalizer to a spinner."""
            await self._freeze(
                SegmentStatus.FAILED,
                text=text,
                mode=TerminalMode.to_mode(mode),
            )

    async def _freeze(
        self,
        status: SegmentStatus.OK | SegmentStatus.FAILED,
        text: str | bytes | ProgressText | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
        active_segment_idx = min(self._active_segment_idx, self._total)
        self.segments[active_segment_idx].status = status

        chars = "".join([segment.next for segment in self.segments])

        char = to_unicode(chars)
        self._last_frame = await self._compose_out(
            char,
            text=text,
            compose_mode="last",
            mode=mode,
        )

        if self._frame_queue:
            self._frame_queue.put_nowait(self._last_frame)

        # Should be stopped here, otherwise prints after
        # self._freeze call will mess up the spinner
        await self.stop()

        if self._enable_output:
            await self._loop.run_in_executor(None, sys.stdout.write, self._last_frame)

        self._cur_line_len = 0

    async def _spin(
        self,
        text: str | bytes | ProgressText | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
        self.segments[self._active_segment_idx].status = SegmentStatus.ACTIVE

        while not self._stop_spin.is_set():
            if self._hide_spin.is_set():
                # Wait a bit to avoid wasting cycles
                await asyncio.sleep(self._interval)
                continue

            await self._stdout_lock.acquire()

            if self._active_segment_idx >= self._total:
                self.segments[self._active_segment_idx].status = SegmentStatus.OK
                self._completed_segment_idx = self._active_segment_idx

            elif self._completed >= self._next_segment:
                # First set the active segment to OK/Completed status
                self.segments[self._active_segment_idx].status = SegmentStatus.OK
                self._completed_segment_idx = self._active_segment_idx
                self._active_segment_idx += 1
                self._next_segment += self._segment_size

                self.segments[self._active_segment_idx].status = SegmentStatus.ACTIVE

            # Compose output
            spin_phase = "".join([segment.next for segment in self.segments])

            out = await self._compose_out(
                spin_phase,
                text=text,
                mode=mode,
            )

            if self._frame_queue:
                self._frame_queue.put_nowait(out)

            # Write

            if self._enable_output:
                await self._clear_line()

                await self._loop.run_in_executor(None, sys.stdout.write, out)

                await self._loop.run_in_executor(None, sys.stdout.flush)

            self._cur_line_len = max(self._cur_line_len, len(out))

            # Wait
            try:
                await asyncio.wait_for(self._stop_spin.wait(), timeout=self._interval)

            except asyncio.TimeoutError:
                pass

            self._stdout_lock.release()

            if self._next_segment >= self._total:
                await self.stop()

    async def _clear_line(self):
        if sys.stdout.isatty():
            # ANSI Control Sequence EL does not work in Jupyter
            await self._loop.run_in_executor(None, sys.stdout.write, "\r\033[K")

        else:
            fill = " " * self._cur_line_len
            await self._loop.run_in_executor(
                None, sys.stdout.write, sys.stdout.write, f"\r{fill}\r"
            )

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

    def _reset_signal_handlers(self):
        for sig, sig_handler in self._dfl_sigmap.items():
            if sig and sig_handler:
                signal.signal(sig, sig_handler)
