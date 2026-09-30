import asyncio
from hyperscale.commands.cli import JsonFile, command, AssertSet
from hyperscale.distributed.nodes import WorkerServer
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.distributed.env import Env as HyperscaleEnv

from hyperscale.core.jobs.models import HyperscaleConfig
from hyperscale.logging import LoggingConfig, LogLevelName

from .node_address import parse_node_address
from .shared import get_default_workers, get_default_config, resolve_auth_secret
from .shutdown_signals import ShutdownSignals


@command(
    display_help_on_error=False
)
async def worker(
    host: str = "127.0.0.1",
    tcp_port: int = 8111,
    udp_port: int = 8121,
    datacenter: str = 'default',
    workers: int = get_default_workers,
    boot_timeout: str = '5m',
    shutdown_timeout: str = "1m",
    acm_secret: str | None = None,
    config: JsonFile[HyperscaleConfig] = get_default_config,
    log_level: AssertSet[LogLevelName] = "fatal",
    managers: list[str] = [],
):
        
    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )


    env = HyperscaleEnv(
         MERCURY_SYNC_AUTH_SECRET=resolve_auth_secret(acm_secret),
         MERCURY_SYNC_LOG_LEVEL=log_level.data,
         WORKER_MAX_CORES=workers,
    )

    start_timeout_sec = TimeParser(boot_timeout).time
    shutdown_timeout_sec = TimeParser(shutdown_timeout).time

    worker = WorkerServer(
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
        env=env,
        dc_id=datacenter,
        seed_managers=[
             parse_node_address(manager) for manager in managers
        ],
    )

    try:
        await worker.start(timeout=start_timeout_sec)

    except BaseException:
        # A failed or interrupted boot has already spawned the executor
        # pool; abort it before propagating so no child process or
        # semaphore outlives the command.
        await worker.abort_and_wait(timeout=shutdown_timeout_sec)
        raise

    try:
        with ShutdownSignals(asyncio.current_task()):
            await worker.wait()

    except (
            KeyboardInterrupt,
            asyncio.CancelledError,
            asyncio.InvalidStateError,
            asyncio.TimeoutError,
    ):
            await worker.abort_and_wait(
                timeout=shutdown_timeout_sec,
            )