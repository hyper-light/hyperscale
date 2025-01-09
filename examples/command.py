import asyncio
import sys
from hyperscale.commands.cli import (
    CLI, 
    Context, 
    Env, 
    Pattern, 
    JsonFile,
    CLIStyle
)
from pydantic import BaseModel, StrictInt
from typing import Literal
from examples.command_file_two import output


class ConfigFile(BaseModel):
    workers: StrictInt

async def get_workers():
    return 2


@CLI.root(
    output,
    global_styles=CLIStyle(
        error_color='blue_violet',
        error_attributes=['italic'],
        flag_color='aquamarine_2',
        text_color='hot_pink_3',
        indentation=3,
        terminal_mode='extended'
    )
)
async def root(
    context: Context[str, str] = None,
    envar_path: str = None
):
    '''
    An example command program
    '''
    context['test'] = 'An example context value.'


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
    additional: Pattern[
        Literal[r'^[0-9]+'], 
        int
    ] = None,
    config: JsonFile[ConfigFile] = None,
):
    '''
    Run the provided script with the specified number of workers.

    @param script The script to run.
    @param workers The number of workers to use.
    '''
    pass




asyncio.run(CLI.run(args=sys.argv[1:]))