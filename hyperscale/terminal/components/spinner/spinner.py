from __future__ import annotations

import asyncio
import itertools
from os import get_terminal_size
from typing import (
    Any,
    Callable,
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
from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.terminal.styling import stylize
from hyperscale.terminal.styling.attributes import AttributeName
from hyperscale.terminal.styling.colors import (
    ColorName,
    ExtendedColorName,
    HighlightName,
)

from .spinner_config import SpinnerConfig
from .spinner_data import spinner_data
from .spinner_factory import SpinnerFactory
from .spinner_types import SpinnerName, SpinnerType
from .to_unicode import to_unicode

SignalHandlers = Union[Callable[[int, SpinnerType | None], Any], int, None]
Spinners = Type[spinner_data]


class Spinner:
    def __init__(
        self,
        spinner: SpinnerName = None,
        ok_char: str = "✔",
        fail_char: str = "✘",
        text: ProgressText | str | bytes = None,
        color: ColorName | ExtendedColorName = None,
        highlight: HighlightName | ExtendedColorName = None,
        attrs: List[AttributeName] = None,
        ok_color: ColorName | ExtendedColorName | None = None,
        ok_highlight: HighlightName | ExtendedColorName | None = None,
        ok_attrs: Sequence[str] | None = None,
        fail_color: ColorName | ExtendedColorName | None = None,
        fail_highlight: HighlightName | ExtendedColorName | None = None,
        fail_attrs: Sequence[str] | None = None,
        reverse: bool = False,
        spinners: Dict[
            str,
            Dict[
                Literal["frames", "interval"],
                int | List[str],
            ],
        ]
        | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ):
        self.fit_type = WidgetFitDimensions.X_AXIS
        # Spinner
        self._mode = TerminalMode.to_mode(mode)
        self._factory = SpinnerFactory(spinners=spinners)
        self._spinner = self._factory.get(spinner)
        self._frames = self._set_frames(self._spinner, reverse)
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
        self._attrs = attrs

        # Other
        self._text = text

        self._last_frame: Optional[str] = None
        self._stdout_lock = asyncio.Lock()

        self._stdout_lock = asyncio.Lock()
        self._loop = asyncio.get_event_loop()
        self._loop = asyncio.get_event_loop()
        self._base_size = self._spinner.size + len(self._text)

    @property
    def raw_size(self):
        return self._base_size

    @property
    def size(self):
        return self._base_size

    async def update(self, text: str):
        self._text = text

    async def fit(self, max_size: int | None = None):
        remaining_size = max_size

        if max_size is None:
            terminal_size = await self._loop.run_in_executor(None, get_terminal_size)
            max_size = terminal_size[0]
            remaining_size = max_size

        remaining_size -= self._spinner.size
        if remaining_size <= 0 and self._text:
            self._text = ""

        elif self._text:
            self._text = self._text[:remaining_size]
            remaining_size -= len(self._text)

        self._base_size = max_size

    async def get_next_frame(self) -> str:
        if self._last_frame:
            return self._last_frame

        return await self._create_next_spin_frame()

    @staticmethod
    def _set_frames(
        spinner: SpinnerConfig,
        reversal: bool,
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
    def _set_cycle(frames: Union[str, Sequence[str]]) -> Iterator[str]:
        return itertools.cycle(frames)

    async def _compose_out(
        self,
        frame: str,
        text: str | bytes | ProgressText | None = None,
        color: ColorName | None = None,
        attrs: AttributeName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
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

        # Mode
        if self._text:
            return f"{frame} {text}"

        return frame

    async def stop(self):
        pass

    async def abort(self):
        await self.fail()

    async def ok(
        self,
        char: str | None = None,
        text: str | bytes | ProgressText | None = None,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
    ):
        if char is None:
            char = self._ok_char

        if color is None:
            color = self._ok_char_color

        if highlight is None:
            highlight = self._ok_char_highlight

        if attrs is None:
            attrs = self._ok_char_attrs

        _char = char if char else "✔"
        await self._create_last_frame(
            _char,
            text=text,
            color=color,
            attrs=attrs,
            highlight=highlight,
            mode=self._mode,
        )

    async def fail(
        self,
        char: str | None = None,
        text: str | bytes | ProgressText | None = None,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
    ):
        if char is None:
            char = self._fail_char

        if color is None:
            color = self._fail_char_color

        if highlight is None:
            highlight = self._fail_char_highlight

        if attrs is None:
            attrs = self._fail_char_attrs

        _char = char if char else "✘"
        await self._create_last_frame(
            _char,
            text=text,
            color=color,
            attrs=attrs,
            highlight=highlight,
            mode=self._mode,
        )

    async def _create_last_frame(
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
            mode=mode,
        )

    async def _create_next_spin_frame(
        self,
        text: str | bytes | ProgressText | None = None,
        color: ColorName | None = None,
        highlight: HighlightName | None = None,
        attrs: Sequence[str] | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ):
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

        return out
