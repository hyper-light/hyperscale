import asyncio
import functools
import psutil
import uvloop

from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow
from .cli import CLI, ImportFile

uvloop.install()

async def get_default_workers():
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        functools.partial(
            psutil.cpu_count,
            logical=False
        ),
    )


@CLI.group()
async def run():
    '''
    Commands for running tests either locally or
    against a remote (cloud) instance.
    '''


@run.command()
async def test(
    path: ImportFile[Workflow],
    server_port: int = 15454,
    workers: int = get_default_workers,
    name: str = "default"
):
    '''
    Run the specified test file locally
    '''

    workflows = [
        workflow() for workflow in path.data.values()
    ]

    runner = LocalRunner(
        "127.0.0.1",
        server_port,
        workers=workers,
    )

    await runner.run(
        name,
        workflows,
    )


@run.command()
async def job(
    job_name: str
):
    '''
    Run the specified job on the specified remote Hyperscale
    installation.
    '''