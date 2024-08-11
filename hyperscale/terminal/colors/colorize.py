import asyncio
import os
import sys
from typing import Iterable

from .attribute import Attribute
from .color import Color
from .highlight import Highlight
from .mode import TerminalMode

RESET = "\033[0m"


async def _can_do_colour(
    *,
    no_color: bool | None = None,
    force_color: bool | None = None,
) -> bool:
    if no_color is not None and no_color:
        return False
    if force_color is not None and force_color:
        return True

    return (
        hasattr(sys.stdout, "isatty")
        and await asyncio.to_thread(sys.stdout.isatty)
        and await asyncio.to_thread(os.environ.get, "TERM") != "dumb"
    )


async def colorize(
    text: str,
    color: str | int | None = None,
    highlight: str | int | None = None,
    attrs: Iterable[str] | None = None,
    *,
    no_color: bool | None = None,
    force_color: bool | None = None,
    mode: TerminalMode = TerminalMode.COMPATIBILITY,
) -> str:
    """Colorize text.

    Available text colors:
        black, red, green, yellow, blue, magenta, cyan, white,
        light_grey, dark_grey, light_red, light_green, light_yellow, light_blue,
        light_magenta, light_cyan.

    Available text highlights:
        on_black, on_red, on_green, on_yellow, on_blue, on_magenta, on_cyan, on_white,
        on_light_grey, on_dark_grey, on_light_red, on_light_green, on_light_yellow,
        on_light_blue, on_light_magenta, on_light_cyan.

    Available attributes:
        bold, dark, underline, blink, reverse, concealed.

    Example:
        colored('Hello, World!', 'red', 'on_black', ['bold', 'blink'])
        colored('Hello, World!', 'green')
    """
    if (await _can_do_colour(no_color=no_color, force_color=force_color)) is False:
        return text

    ansi_string = ""

    if color is not None and mode == TerminalMode.COMPATIBILITY:
        ansi_color_string = "\033[%dm" % (
            Color.by_name(color) if isinstance(color, str) else color
        )

        ansi_string = f"{ansi_color_string}{text}"

    elif color is not None and mode == TerminalMode.EXTENDED:
        ansi_256_color_fmt_str = (
            "\033[38;5;%dm" % Color.by_name(color) if isinstance(color, str) else color
        )
        ansi_string = f"{ansi_256_color_fmt_str}{text}"

    if highlight is not None and mode == TerminalMode.COMPATIBILITY:
        ansi_256_highlight_fmt_str = "\033[%dm" % (
            Highlight.by_name(highlight) if isinstance(highlight, str) else highlight
        )

        ansi_string = f"{ansi_256_highlight_fmt_str}{ansi_string}"

    elif highlight and mode == TerminalMode.COMPATIBILITY:
        ansi_256_highlight_fmt_str = (
            ansi_256_highlight_fmt_str % Highlight.by_name(highlight)
            if isinstance(highlight, str)
            else color
        )
        ansi_string = f"{ansi_256_highlight_fmt_str}{ansi_string}"

    if attrs is not None:
        for attr in attrs:
            ansi_attribute_string = "\033[38;5;%dm" % Attribute.by_name(attr)
            ansi_string = f"{ansi_attribute_string}{ansi_string}"

    return ansi_string + RESET
