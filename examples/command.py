import asyncio
import sys
from hyperscale.commands.cli import CLI
from examples.command_file_two import group, test, test_again


@CLI.root(group, test, test_again)
async def root(
    test: str, 
    skip: bool = None, 
    bloop: float = None,
):
    '''
    An example group

    @param test An example param
    @param skip Another example param
    @param bloop Yet another example param
    '''
    print('Hello world!')


@CLI.command()
async def test(bop: bool = None):
    '''
    A test command.

    @param bop A test param.
    '''
    print(bop, 'HI')





asyncio.run(CLI.run(args=sys.argv[1:]))