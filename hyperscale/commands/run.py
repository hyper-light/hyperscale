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
from hyperscale.logging import LogLevelName, LoggingConfig
from hyperscale.core.jobs.models import HyperscaleConfig

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
    log_level: AssertSet[LogLevelName] = "fatal",
    workers: int = get_default_workers,
    name: str = "default",
    quiet: bool = False,
):
    """
    Run the specified test file locally

    @param path The path to the test file to run
    @param config A path to a valid .hyperscale.json config file
    @param log_level The log level to use for log files
    @param workers The number of parallel threads/processes to use
    @param name The name of the test
    @param quiet If specified, all GUI output will be disabled
    """
    workflows = [workflow() for workflow in path.data.values()]

    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output='stderr',
    )

    runner = LocalRunner(
        "127.0.0.1",
        config.data.server_port,
        workers=workers,
    )

    terminal_ui_enabled = quiet is False

    try:
        await runner.run(
            name,
            workflows,
            terminal_ui_enabled=terminal_ui_enabled,
        )

    except asyncio.CancelledError:
        return

    except Exception as e:
        await runner.abort(
            error=e,
            terminal_ui_enabled=terminal_ui_enabled,
        )
