from hyperscale.commands.cli import CLI, Context, Env, Pattern

async def get_workers():
    return 2


@CLI.command()
async def run(
    script: str,
    workers: int | Env[int] = get_workers,
    context: Context[str, str] = None,
    additional: Pattern[r'\w+'] = None,
):
    '''
    Run the provided script with the specified number of workers.

    @param script The script to run.
    @param workers The number of workers to use.
    '''
    print(context['test'], script)

