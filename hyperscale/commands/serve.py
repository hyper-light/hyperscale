import asyncio
import functools
import json
import os
import sys

import cloudpickle
import psutil

try:
    import uvloop

    uvloop.install()

except Exception:
    pass


from hyperscale.core.jobs.models import HyperscaleConfig, TerminalMode
from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.logging import LoggingConfig, LogLevelName
from hyperscale.distributed.nodes import (
    WorkerServer,
    ManagerServer,
    GateServer,
)

from .cli import (
    CLI,
    AssertSet,
    ImportType,
    JsonFile,
)
from .config import get_default_config


@CLI.group()
async def serve():
    '''
    Manage and run hyperscale distributed worker, manager, and gate servers
    '''


@serve.command()
async def worker(
    host: str = '0.0.0.0',
    tcp_port: int = 6470,
    udp_port: int = 6570,
    config: JsonFile[HyperscaleConfig] = get_default_config,
    log_level: AssertSet[LogLevelName] = "fatal",
    quiet: bool = False,
):

    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )

    terminal_mode: TerminalMode = config.data.terminal_mode
    if quiet:
        terminal_mode = "disabled"

    worker = WorkerServer(
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
    )


@serve.command()
async def manager(
    host: str = '0.0.0.0',
    tcp_port: int = 6270,
    udp_port: int = 6370,
    log_level: AssertSet[LogLevelName] = "fatal",
    config: JsonFile[HyperscaleConfig] = get_default_config,
    quiet: bool = False,
):
    
    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )

    terminal_mode: TerminalMode = config.data.terminal_mode
    if quiet:
        terminal_mode = "disabled"

    manager = ManagerServer(
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
    )


@serve.command()
async def gate(
    host: str = '0.0.0.0',
    tcp_port: int = 6070,
    udp_port: int = 6170,
    datacenter_id: str | None = None,
    log_level: AssertSet[LogLevelName] = "fatal",
    config: JsonFile[HyperscaleConfig] = get_default_config,
    quiet: bool = False,
):
    
    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )
    
    terminal_mode: TerminalMode = config.data.terminal_mode
    if quiet:
        terminal_mode = "disabled"
        
    gate = GateServer(
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
    )