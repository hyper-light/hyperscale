import asyncio
from hyperscale.commands.cli import JsonFile, command, AssertSet
from hyperscale.distributed.nodes import GateServer
from hyperscale.core.engines.client.time_parser import TimeParser

from hyperscale.core.jobs.models import HyperscaleConfig
from hyperscale.logging import LoggingConfig, LogLevelName

from .shared import get_default_config, node_env, resolve_auth_secret
from .shutdown_signals import ShutdownSignals


@command(
    display_help_on_error=False,
    shortnames={"host": "H"},
)
async def gate(
    host: str = "127.0.0.1",
    tcp_port: int = 8431,
    udp_port: int = 8441,
    datacenter: str = 'global',
    boot_timeout: str = '5m',
    shutdown_timeout: str = "1m",
    acm_secret: str | None = None,
    config: JsonFile[HyperscaleConfig] = get_default_config,
    log_level: AssertSet[LogLevelName] = "fatal",
):
    """
    Run a Hyperscale gate. The gate starts standalone; use
    `hyperscale join` to connect datacenter managers to it.

    @param host The address to bind and advertise to peers
    @param tcp_port The TCP port for data operations
    @param udp_port The UDP port for SWIM health checks
    @param datacenter The gate's datacenter identifier
    @param boot_timeout How long to wait for the gate to boot
    @param shutdown_timeout How long to wait for the gate to shut down
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET)
    @param config A path to a valid .hyperscale.json config file
    @param log_level The log level to use
    """
    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )

    env = node_env(
        MERCURY_SYNC_AUTH_SECRET=resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
    )

    start_timeout_sec = TimeParser(boot_timeout).time
    shutdown_timeout_sec = TimeParser(shutdown_timeout).time

    gate = GateServer(
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
        env=env,
        dc_id=datacenter,
    )

    try:
        # GateServer.start takes no timeout of its own; bound the boot
        # here so --boot-timeout means the same thing for every role.
        await asyncio.wait_for(gate.start(), timeout=start_timeout_sec)

    except BaseException:
        await gate.abort_and_wait(timeout=shutdown_timeout_sec)
        raise

    try:
        with ShutdownSignals(asyncio.current_task()):
            await gate.wait()

    except (
            KeyboardInterrupt,
            asyncio.CancelledError,
            asyncio.InvalidStateError,
            asyncio.TimeoutError,
    ):
            await gate.abort_and_wait(
                timeout=shutdown_timeout_sec,
            )
