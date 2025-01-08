from hyperscale.commands.cli import CLI



@CLI.group()
async def group(
    test: str, 
    skip: bool = None,
    bloop: float = 0,
):
    '''
    An example group

    @param test An example param
    @param skip Another example param
    @param bloop Yet another example param
    '''
    print('Hello world!')


@CLI.group()
async def test(bop: bool = None):
    '''
    A test command.

    @param bop A test param.
    '''
    print(bop, 'HI')


@CLI.command()
async def test_again(message: int | float = None):
    pass

