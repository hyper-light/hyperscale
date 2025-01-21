import asyncio
import os
import sys
from typing import Iterable

from hyperscale.ui.config.mode import TerminalMode

from .attributes.attribute import Attribute, AttributeName
from .colors.color import Color, ColorName
from .colors.extended_color import ExtendedColorName
from .colors.highlight import Highlight, HighlightName

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

    loop = asyncio.get_event_loop()

    return (
        hasattr(sys.stdout, "isatty")
        and await loop.run_in_executor(None, sys.stdout.isatty)
        and await loop.run_in_executor(None, os.environ.get, "TERM") != "dumb"
    )


async def stylize(
    text: str,
    color: ColorName | ExtendedColorName | int | None = None,
    highlight: HighlightName | ExtendedColorName | int | None = None,
    attrs: Iterable[AttributeName] | None = None,
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

    ansi_string = str(text)

    if color is not None and mode == TerminalMode.COMPATIBILITY:
        ansi_string = "\033[%dm%s" % (
            Color.by_name(color, mode=mode) if isinstance(color, str) else color,
            ansi_string,
        )

    elif color is not None and mode == TerminalMode.EXTENDED:
        ansi_string = "\033[38:5:%dm%s" % (
            Color.by_name(color, mode=mode) if isinstance(color, str) else color,
            ansi_string,
        )

    if highlight is not None and mode == TerminalMode.COMPATIBILITY:
        ansi_string = "\033[%dm%s" % (
            Highlight.by_name(highlight) if isinstance(highlight, str) else highlight,
            ansi_string,
        )

    elif highlight is not None and mode == TerminalMode.EXTENDED:
        ansi_string = "\033[48:5:%dm%s" % (
            Highlight.by_name(highlight, mode=mode)
            if isinstance(highlight, str)
            else color,
            ansi_string,
        )

    if attrs is not None:
        attrs = [attr for attr in attrs if attr is not None]

        for attr in attrs:
            ansi_string = "\033[%dm%s" % (Attribute.by_name(attr), ansi_string)

    return ansi_string + RESET
