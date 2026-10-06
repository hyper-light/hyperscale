import functools

from hyperscale.commands.cli import JsonFile, command, AssertSet
from hyperscale.distributed.nodes import WorkerServer
from hyperscale.core.engines.client.time_parser import TimeParser

from hyperscale.core.jobs.models import HyperscaleConfig
from hyperscale.logging import Logger, LoggingConfig, LogLevelName
from hyperscale.ui.node_dashboard import (
    NodeDashboard,
    NodeDashboardConfig,
    StderrLogRedirect,
    WorkerDashboardReader,
)

from .node_address import parse_node_address, parse_node_host
from .node_lifecycle import run_node_until_stopped
from .seed_locators import is_dynamic_locator, resolve_seed_addresses
from .shared import (
    get_default_workers,
    get_default_config,
    node_env,
    node_log_path,
    node_terminal_mode,
    resolve_auth_secret,
)


@command(
    display_help_on_error=False,
    shortnames={"host": "H"},
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
    quiet: bool = False,
):
    """
    Run a Hyperscale worker. It registers with the managers given by
    --managers and runs the workflows they dispatch to it.

    @param host The address to bind and advertise to peers
    @param tcp_port The TCP port for data operations
    @param udp_port The UDP port for SWIM health checks
    @param datacenter The datacenter this worker belongs to
    @param workers The number of cores (executor processes) the worker runs workflows on
    @param boot_timeout How long to wait for the worker to boot
    @param shutdown_timeout How long to wait for the worker to shut down
    @param acm_secret The shared cluster secret (defaults to MERCURY_SYNC_AUTH_SECRET)
    @param config A path to a valid .hyperscale.json config file
    @param log_level The log level to use
    @param managers The TCP host:port of the managers this worker registers with
    @param quiet If specified, the live node dashboard is disabled
    """

    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )


    host = parse_node_host(host, tcp_port)

    env = node_env(
        MERCURY_SYNC_AUTH_SECRET=await resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level.data,
        WORKER_MAX_CORES=workers,
    )

    start_timeout_sec = TimeParser(boot_timeout).time
    shutdown_timeout_sec = TimeParser(shutdown_timeout).time

    # AD-52 section 2: the managers may be given as seed locators, resolved
    # at launch (retried while they do not yet resolve, within the boot
    # timeout).
    seed_managers = (
        await resolve_seed_addresses(
            managers,
            "--managers",
            start_timeout_sec,
            env.CLUSTER_FORMATION_INTERVAL_SECONDS,
            env.MANAGER_TCP_TIMEOUT_STANDARD,
        )
        if any(is_dynamic_locator(manager) for manager in managers)
        else [parse_node_address(manager) for manager in managers]
    )

    terminal_mode = await node_terminal_mode(config.data.terminal_mode, quiet)
    log_path = node_log_path(config.data.logs_directory, "worker", datacenter, host, tcp_port)

    # While the dashboard renders, stderr (the node's logs, and its executor
    # processes' output) goes to the log file.
    async with StderrLogRedirect(log_path, enabled=terminal_mode != "disabled"):
        worker = WorkerServer(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            env=env,
            dc_id=datacenter,
            seed_managers=seed_managers,
        )

        await run_node_until_stopped(
            worker,
            functools.partial(worker.start, timeout=start_timeout_sec),
            NodeDashboard(
                WorkerDashboardReader(worker),
                worker,
                terminal_mode,
                env,
                log_path,
                NodeDashboardConfig(),
                Logger(),
            ),
            drain_before_stop=False,
            shutdown_timeout_seconds=shutdown_timeout_sec,
        )
