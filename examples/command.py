import asyncio
import sys
from hyperscale.commands.cli import CLI, Context, Env, Pattern
from typing import Literal, ForwardRef

async def get_workers():
    return 2


@CLI.root()
async def root(
    context: Context[str, str] = None,
    envar_path: str = None
):
    '''
    An example command program
    '''
    context['test'] = 'A'


@CLI.group()
async def setup(quiet: bool = False):
    print('Is quiet? ', quiet)


@setup.command()
async def test(name: str):
    print(name)


@CLI.command()
async def run(
    script: str,
    workers: int | Env[int] = get_workers,
    context: Context[str, str] = None,
    additional: Pattern[
        Literal[r'^[0-9]+'], 
        int
    ] = None,
):
    '''
    Run the provided script with the specified number of workers.

    @param script The script to run.
    @param workers The number of workers to use.
    '''
    print(context['test'], script, workers, additional, type(additional))




asyncio.run(CLI.run(args=sys.argv[1:]))