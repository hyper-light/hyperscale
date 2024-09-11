from __future__ import annotations

import asyncio
import functools
import inspect
import itertools
import signal
import sys
import time
from datetime import timedelta
from enum import Enum
from os import get_terminal_size
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Type,
    Union,
)

from hyperscale.logging.spinner import ProgressText
from hyperscale.logging_rewrite import Logger
from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.styling import stylize
from hyperscale.terminal.styling.attributes import Attribute, AttributeName
from hyperscale.terminal.styling.colors import (
    Color,
    ColorName,
    ExtendedColorName,
    Highlight,
    HighlightName,
)

from .spinner_config import SpinnerConfig
from .spinner_data import spinner_data
from .spinner_factory import SpinnerFactory
from .spinner_types import SpinnerName, SpinnerType
from .to_unicode import to_unicode

SignalHandlers = Union[Callable[[int, SpinnerType | None], Any], int, None]
Spinners = Type[spinner_data]


class LoggerMode(Enum):
    CONSOLE = "console"
    SYSTEM = "system"


async def default_handler(signame: str, spinner: Spinner):  # pylint: disable=unused-argument
    """Signal handler, used to gracefully shut down the ``spinner`` instance
    when specified signal is received by the process running the ``spinner``.

    ``signum`` and ``frame`` are mandatory arguments. Check ``signal.signal``
    function for more details.
    """
    spinner.color = "red"
    await spinner.fail()
    await spinner.stop()


class Spinner:
    def __init__(
        self,
        spinner: SpinnerName = None,
        ok_char: str = "✔",
        fail_char: str = "✘",
        text: ProgressText | str | bytes = None,
        color: ColorName = None,
        highlight: HighlightName = None,
        attrs: List[AttributeName] = None,
        ok_color: ColorName | ExtendedColorName | None = None,
        ok_highlight: HighlightName | ExtendedColorName | None = None,
        ok_attrs: Sequence[str] | None = None,
        fail_color: ColorName | ExtendedColorName | None = None,
        fail_highlight: HighlightName | ExtendedColorName | None = None,
        fail_attrs: Sequence[str] | None = None,
        reversal: bool = False,
        side: Literal["left", "right"] = "left",
        sigmap: Dict[signal.Signals, Coroutine] = None,
        timer: bool = False,
        enabled: bool = True,
        spinners: Dict[
            str,
            Dict[
                Literal["frames", "interval"],
                int | List[str],
            ],
        ]
        | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
        disable_output: bool = False,
    ):
        # Spinner
        self._mode = TerminalMode.to_mode(mode)
        self._factory = SpinnerFactory(spinners=spinners)
        self._spinner = self._factory.get(spinner)
        self._frames = self._set_frames(self._spinner, reversal)
        self._interval = self._set_interval(self._spinner)
        self._cycle = self._set_cycle(self._frames)

        self._ok_char = ok_char
        self._ok_char_color = ok_color
        self._ok_char_highlight = ok_highlight
        self._ok_char_attrs = ok_attrs

        self._fail_char = fail_char
        self._fail_char_color = fail_color
        self._fail_char_highlight = fail_highlight
        self._fail_char_attrs = fail_attrs

        # Color Specification
        self._color = color
        self._highlight = highlight
        self._attrs = self._set_attrs(attrs) if attrs else set()

        # Other
        self._text = text
        self._side = self._set_side(side)
        self._reversal = reversal
        self._timer = timer
        self._start_time: Optional[float] = None
        self._stop_time: Optional[float] = None

        # Helper flags
        self._stop_spin: Optional[asyncio.Event] = None
        self._hide_spin: Optional[asyncio.Event] = None
        self._spin_thread: Optional[asyncio.Task] = None
        self._last_frame: Optional[str] = None
        self._stdout_lock = asyncio.Lock()
        self._hidden_level = 0
        self._cur_line_len = 0
        self._enable_output = disable_output is False

        self._frame_queue: asyncio.Queue = None

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
        self._max_size: int | None = None
        self._loop = asyncio.get_event_loop()
        self._base_size = self._spinner.size + len(self._text)

    @property
    def raw_size(self):
        return self._base_size

    @property
    def size(self):
        return self._base_size

    async def fit(
        self,
        max_size: int | None = None,
    ):
        if max_size is None:
            terminal_size = await self._loop.run_in_executor(None, get_terminal_size)
            max_size = terminal_size[0]

        max_size -= self._spinner.size
        if max_size <= 0 and self._text:
            self._text = ""

        elif self._text:
            self._text = self._text[:max_size]

        self._max_size = max_size
        self._base_size = self._spinner.size + len(self._text)

    async def get_next_frame(self) -> str:
        if self._frame_queue:
            return await self._frame_queue.get()

        return ""

    async def get_fail_frame(
        self,
        char: str | None = None,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        if char is None:
            char = self._fail_char

        if color is None:
            color = self._fail_char_color

        if highlight is None:
            highlight = self._fail_char_highlight

        if attrs is None:
            attrs = self._fail_char_attrs

        if color or highlight:
            char = await stylize(
                char,
                color=color,
                attrs=attrs,
                highlight=highlight,
                mode=TerminalMode.to_mode(mode),
            )

        return char

    async def get_ok_frame(
        self,
        char: str | None = None,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        if char is None:
            char = self._ok_char

        if color is None:
            color = self._ok_char_color

        if highlight is None:
            highlight = self._ok_char_highlight

        if attrs is None:
            attrs = self._ok_char_attrs

        if color or highlight:
            char = await stylize(
                char,
                color=color,
                attrs=attrs,
                highlight=highlight,
                mode=TerminalMode.to_mode(mode),
            )

        return char

    async def get_spinner_frame(
        self,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        char = next(self._cycle)

        if color is None:
            color = self._color

        if highlight is None:
            highlight = self._highlight

        if attrs is None:
            attrs = self._attrs

        if color or highlight:
            char = await stylize(
                char,
                color=color,
                attrs=attrs,
                highlight=highlight,
                mode=TerminalMode.to_mode(mode),
            )

        return char

    @property
    def ok_char(self):
        return self._ok_char

    @property
    def interval(self):
        return self._interval

    @staticmethod
    def _set_side(side: str) -> str:
        if side not in ("left", "right"):
            raise ValueError(
                "'{0}': unsupported side value. Use either 'left' or 'right'."
            )
        return side

    @staticmethod
    def _set_frames(
        spinner: SpinnerConfig, reversal: bool
    ) -> Union[str, Sequence[str]]:
        uframes = None  # unicode frames
        uframes_seq = None  # sequence of unicode frames

        if isinstance(spinner.frames, str):
            uframes = spinner.frames

        # TODO (pavdmyt): support any type that implements iterable
        if isinstance(spinner.frames, (list, tuple)):
            if spinner.frames and isinstance(spinner.frames[0], bytes):
                uframes_seq = [to_unicode(frame) for frame in spinner.frames]
            else:
                uframes_seq = spinner.frames

        _frames = uframes or uframes_seq
        if not _frames:
            raise ValueError(f"{spinner!r}: no frames found in spinner")

        # Builtin ``reversed`` returns reverse iterator,
        # which adds unnecessary difficulty for returning
        # unicode value;
        # Hence using [::-1] syntax
        frames = _frames[::-1] if reversal else _frames

        return frames

    @staticmethod
    def _set_interval(spinner: SpinnerConfig) -> float:
        # Milliseconds to Seconds
        return spinner.interval * 0.001

    @staticmethod
    def _set_cycle(frames: Union[str, Sequence[str]]) -> Iterator[str]:
        return itertools.cycle(frames)

    @staticmethod
    def _set_color(
        value: str,
        default: int | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ) -> str:
        if (
            value not in Color.names
            or (value in Color.names and mode == TerminalMode.EXTENDED)
            and default is None
        ):
            raise ValueError(
                "'{0}': unsupported color value. Use one of the: {1}".format(  # pylint: disable=consider-using-f-string
                    value, ", ".join(Color.names.keys())
                )
            )
        return Color.by_name(value, default=default)

    @staticmethod
    def _set_highlight(
        value: str,
        default: int | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ) -> str:
        if (
            value not in Highlight.names
            or (value not in Color.extended_names and mode == TerminalMode.EXTENDED)
            and default is None
        ):
            raise ValueError(
                "'{0}': unsupported highlight value. "  # pylint: disable=consider-using-f-string
                "Use one of the: {1}".format(value, ", ".join(Highlight.names.keys()))
            )
        return Highlight.by_name(value, default=default)

    @staticmethod
    def _set_attrs(attrs: Sequence[str]) -> set[str]:
        for attr in attrs:
            if attr not in Attribute.names:
                raise ValueError(
                    "'{0}': unsupported attribute value. "  # pylint: disable=consider-using-f-string
                    "Use one of the: {1}".format(
                        attr, ", ".join(Attribute.names.keys())
                    )
                )
        return set(attrs)

    @property
    def color(self) -> Optional[str]:
        return self._color

    @color.setter
    def color(self, value: str) -> None:
        self._color = self._set_color(value, mode=self._mode) if value else value

    @property
    def highlight(self) -> Optional[str]:
        return self._highlight

    @highlight.setter
    def highlight(self, value: str) -> None:
        self._highlight = (
            self._set_highlight(value, mode=self._mode) if value else value
        )

    @property
    def attrs(self) -> Sequence[str]:
        return list(self._attrs)

    @attrs.setter
    def attrs(self, value: Sequence[str]) -> None:
        new_attrs = self._set_attrs(value) if value else set()
        self._attrs = self._attrs.union(new_attrs)

    @property
    def side(self) -> str:
        return self._side

    @side.setter
    def side(self, value: str) -> None:
        self._side = self._set_side(value)

    @property
    def reversal(self) -> bool:
        return self._reversal

    @reversal.setter
    def reversal(self, value: bool) -> None:
        self._reversal = value
        self._frames = self._set_frames(self._spinner, self._reversal)
        self._cycle = self._set_cycle(self._frames)

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
        color: ColorName | None = None,
        attrs: AttributeName | None = None,
        highlight: HighlightName | None = None,
        compose_mode: Optional[str] = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ) -> str:
        if text:
            text = str(text)

        if color is None:
            color = self._color

        if highlight is None:
            highlight = self._highlight

        if attrs is None:
            attrs = self._attrs

        # Colors
        if color:
            frame = await stylize(
                frame,
                color=color,
                attrs=attrs,
                highlight=highlight,
                mode=mode,
            )

        # Position
        if self._side == "right":
            frame, text = text, frame
        # Timer
        if self._timer:
            sec, fsec = divmod(round(100 * self.elapsed_time), 100)
            text += " ({}.{:02.0f})".format(  # pylint: disable=consider-using-f-string
                timedelta(seconds=sec), fsec
            )
        # Mode
        if compose_mode is None and text:
            out = f"\r{frame} {text}"
        elif text:
            out = f"{frame} {text}\n"

        elif compose_mode is None:
            out = f"\r{frame}"

        else:
            out = f"{frame}\n"

        return out

    async def __aenter__(self):
        await self.spin()

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

    async def spin(
        self,
        text: str | bytes | ProgressText | None = None,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        if self.enabled:
            if self._sigmap:
                self._register_signal_handlers()

            await self._hide_cursor()
            self._start_time = time.time()
            self._stop_time = None  # Reset value to properly calculate subsequent spinner starts (if any)  # pylint: disable=line-too-long
            self._stop_spin = asyncio.Event()
            self._hide_spin = asyncio.Event()
            try:
                self._spin_thread = asyncio.create_task(
                    self._spin(
                        text=text,
                        color=color,
                        highlight=highlight,
                        attrs=attrs,
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

        if thr_is_alive and not self._hide_spin.is_set() and self._enable_output:
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

        if thr_is_alive and self._hide_spin.is_set() and self._enable_output:
            # clear the current line so the spinner is not appended to it
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

        await self._loop.run_in_executor(
            None,
            sys.stdout.write,
        )

        self._cur_line_len = 0
        self._stdout_lock.release()

    async def ok(
        self,
        char: str | None = None,
        text: str | bytes | ProgressText | None = None,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        if char is None:
            char = self._ok_char

        if color is None:
            color = self._ok_char_color

        if highlight is None:
            highlight = self._ok_char_highlight

        if attrs is None:
            attrs = self._ok_char_attrs

        if self.enabled:
            """Set Ok (success) finalizer to a spinner."""
            _char = char if char else "✔"
            await self._freeze(
                _char,
                text=text,
                color=color,
                attrs=attrs,
                highlight=highlight,
                mode=TerminalMode.to_mode(mode),
            )

    async def fail(
        self,
        char: str | None = None,
        text: str | bytes | ProgressText | None = None,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        if char is None:
            char = self._fail_char

        if color is None:
            color = self._fail_char_color

        if highlight is None:
            highlight = self._fail_char_highlight

        if attrs is None:
            attrs = self._fail_char_attrs

        if self.enabled:
            """Set fail finalizer to a spinner."""
            _char = char if char else "✘"
            await self._freeze(
                _char,
                text=text,
                color=color,
                attrs=attrs,
                highlight=highlight,
                mode=TerminalMode.to_mode(mode),
            )

    async def _freeze(
        self,
        end_char: str,
        text: str | bytes | ProgressText | None = None,
        color: ColorName | None = None,
        attrs: AttributeName | None = None,
        highlight: HighlightName | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
        """Stop spinner, compose last frame and 'freeze' it."""
        char = to_unicode(end_char)
        self._last_frame = await self._compose_out(
            char,
            text=text,
            color=color,
            attrs=attrs,
            highlight=highlight,
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
        color: ColorName | None = None,
        highlight: HighlightName | None = None,
        attrs: Sequence[str] | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
        while not self._stop_spin.is_set():
            if self._hide_spin.is_set():
                # Wait a bit to avoid wasting cycles
                await asyncio.sleep(self._interval)
                continue

            await self._stdout_lock.acquire()
            terminal_size = await self._loop.run_in_executor(None, get_terminal_size)

            terminal_width = terminal_size[0]

            # Compose output
            spin_phase = next(self._cycle)
            out = await self._compose_out(
                spin_phase,
                text=text,
                color=color,
                attrs=attrs,
                highlight=highlight,
                mode=mode,
            )

            if len(out) > terminal_width:
                out = f"{out[:terminal_width-1]}..."

            if self._frame_queue:
                self._frame_queue.put_nowait(self._last_frame)

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

        for sig, sig_handler in self._sigmap.items():
            # A handler for a particular signal, once set, remains
            # installed until it is explicitly reset. Store default
            # signal handlers for subsequent reset at cleanup phase.
            dfl_handler = signal.getsignal(sig)
            self._dfl_sigmap[sig] = dfl_handler

            # ``signal.SIG_DFL`` and ``signal.SIG_IGN`` are also valid
            # signal handlers and are not callables.
            if callable(sig_handler):
                # ``signal.signal`` accepts handler function which is
                # called with two arguments: signal number and the
                # interrupted stack frame. ``functools.partial`` solves
                # the problem of passing spinner instance into the handler
                # function.
                sig_handler = functools.partial(sig_handler, spinner=self)

            self._loop.add_signal_handler(
                getattr(signal, sig.name),
                lambda signame=sig.name: asyncio.create_task(sig_handler(self)),
            )

    def _reset_signal_handlers(self):
        for sig, sig_handler in self._dfl_sigmap.items():
            if sig and sig_handler:
                self._loop.add_signal_handler(
                    getattr(signal, sig.name),
                    lambda signame=sig.name: asyncio.create_task(
                        asyncio.to_thread(
                            sig_handler,
                            signame,
                            self,
                        )
                    ),
                )
