import asyncio
import sys
from hyperscale.commands.cli import (
    CLI,
    Pattern,
    RawFile,
    CLIStyle,
    Operator,
    Paths,
    Map,
    ImportFile,
)
from pydantic import RootModel, StrictStr
from typing import Literal


class EmailsList(RootModel):
    root: list[StrictStr]


@CLI.root(
    global_styles=CLIStyle(
        flag_description_color="white",
        error_color="hot_pink_3",
        error_attributes=["italic"],
        flag_color="aquamarine_2",
        text_color="hot_pink_3",
        indentation=3,
        terminal_mode="extended",
    )
)
async def emailgrep(
    matches: Operator[
        Map[
            Paths[Literal["*", "../"]],
            RawFile[str],
            Pattern[
                Literal[r"([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)"], list[str]
            ],
        ],
        EmailsList,
    ] = None,
):
    """
    A ripgrep like example program that searches for emails.

    @param matches The pattern to match for.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, sys.stdout.write, str(matches.data.root))


asyncio.run(CLI.run(args=sys.argv[1:]))
