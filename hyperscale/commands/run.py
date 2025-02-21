import asyncio
import cloudpickle
import functools
import json
import os
import sys

import psutil

try:

    import uvloop
    uvloop.install()

except Exception:
    pass

from hyperscale.core.jobs.models import HyperscaleConfig, TerminalMode
from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow
from hyperscale.logging import LoggingConfig, LogLevelName

from .cli import (
    CLI,
    AssertSet,
    ImportFile,
    JsonFile,
)


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
    config = HyperscaleConfig()
    config_path = ".hyperscale.config.json"
    if not os.path.exists(config_path):
        with open(config_path, "w") as config_file:
            json.dump(
                config.model_dump(),
                config_file,
                indent=4,
            )

    else:
        with open(config_path, "r") as config_file:
            config_data = json.load(config_file)
            config_data['logs_directory'] = os.path.join(
                os.getcwd(),
                'logs',
            )
            
            config = HyperscaleConfig(**config_data)

    return config


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

    for workflow in workflows:
        cloudpickle.register_pickle_by_value(sys.modules[workflow.__module__])

    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )

    runner = LocalRunner(
        "127.0.0.1",
        config.data.server_port,
        workers=workers,
    )

    terminal_mode: TerminalMode = config.data.terminal_mode
    if quiet:
        terminal_mode = "disabled"

    try:
        await runner.run(
            name,
            workflows,
            terminal_mode=terminal_mode,
        )

    except (
        Exception,
        KeyboardInterrupt,
        asyncio.CancelledError,
        asyncio.InvalidStateError,
    ) as e:
        await runner.abort(
            error=e,
            terminal_mode=terminal_mode,
        )
