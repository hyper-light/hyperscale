import asyncio
import functools
import psutil
import uvloop


from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow
from .cli import (
    CLI,
    ImportFile,
    JsonFile,
    AssertSet,
)
from hyperscale.logging import LogLevelName
from .hyperscale_config import HyperscaleConfig

uvloop.install()


async def get_default_workers():
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        functools.partial(
            psutil.cpu_count,
            logical=False,
        ),
    )


def get_default_config():
    return HyperscaleConfig()


@CLI.command()
async def run(
    path: ImportFile[Workflow],
    config: JsonFile[HyperscaleConfig] = get_default_config,
    log_level: AssertSet[LogLevelName] = "info",
    workers: int = get_default_workers,
    name: str = "default",
    quiet: bool = False,
):
    """
    Run the specified test file locally
    """
    workflows = [workflow() for workflow in path.data.values()]

    runner = LocalRunner(
        "127.0.0.1",
        config.data.server_port,
        log_level,
        workers=workers,
    )

    terminal_ui_enabled = quiet is False

    try:
        await runner.run(
            name,
            workflows,
            terminal_ui_enabled=terminal_ui_enabled,
        )

    except Exception:
        await runner.abort(
            terminal_ui_enabled=terminal_ui_enabled,
        )
