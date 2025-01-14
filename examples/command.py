import asyncio
import sys
from hyperscale.commands.cli import (
    Chain,
    CLI,
    Context,
    Env,
    Pattern,
    RawFile,
    CLIStyle,
    JsonData,
    Operator,
    Paths,
    Map,
)
from pydantic import BaseModel, StrictInt, RootModel, StrictStr
from typing import Literal
from examples.command_file_two import output


class UserList(RootModel):
    root: list[StrictStr]


class ConfigFile(BaseModel):
    workers: StrictInt


async def get_workers():
    return 2


@CLI.root(
    output,
    global_styles=CLIStyle(
        flag_description_color="white",
        error_color="hot_pink_3",
        error_attributes=["italic"],
        flag_color="aquamarine_2",
        text_color="hot_pink_3",
        indentation=3,
        terminal_mode="extended",
    ),
)
async def root(context: Context[str, str] = None, envar_path: str = None):
    """
    An example command program
    """
    context["test"] = "An example context value."


@CLI.group()
async def setup(quiet: bool = False):
    pass


@setup.command()
async def test(name: str):
    pass


@CLI.command()
async def run(
    script: str,
    workers: int | Env[int] = get_workers,
    context: Context[str, str] = None,
    additional: Pattern[Literal[r"^[0-9]+"], int] = None,
    config: Operator[
        Map[
            Paths,
            RawFile[str],
            Pattern[
                Literal[r"([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)"], list[str]
            ],
        ],
        UserList,
    ] = None,
):
    """
    Run the provided script with the specified number of workers.

    @param script The script to run.
    @param workers The number of workers to use.
    """
    print(config.data, type(config.data))


asyncio.run(CLI.run(args=sys.argv[1:]))
