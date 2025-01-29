import asyncio
import inspect
import textwrap
import pathlib
import shutil
import uvloop

from typing import Literal

from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow
from hyperscale.core.jobs.models import HyperscaleConfig
from .cli import CLI
from .workflow import test

uvloop.install()


async def create_test(
    loop: asyncio.AbstractEventLoop,
    path: str,
):
    test_file = await loop.run_in_executor(
        None,
        open,
        path,
        'w',
    )

    try:
        await loop.run_in_executor(
            None,
            test_file.write,
            textwrap.dedent(
                inspect.getsource(test)
            ),
        )

    except Exception:
        pass

    await loop.run_in_executor(
        None,
        test_file.close
    )

@CLI.command()
async def new(
    path: str,
    overwrite: bool = False,
):
    loop = asyncio.get_event_loop()

    test_path = await loop.run_in_executor(
        None,
        pathlib.Path,
        path,
    )

    absolute_path = await loop.run_in_executor(
        None,
        test_path.absolute
    )

    resolved_path = await loop.run_in_executor(
        None,
        absolute_path.resolve
    )

    filepath_exists = await loop.run_in_executor(
        None,
        resolved_path.exists
    )


    if filepath_exists and overwrite is True:
        await create_test(
            loop,
            str(resolved_path),
        )

    elif filepath_exists is False:
        await create_test(
            loop,
            str(resolved_path),
        )
        
