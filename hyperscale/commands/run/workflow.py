import asyncio
import signal
import sys

import cloudpickle

try:
    import uvloop

    uvloop.install()

except Exception:
    pass

from hyperscale.core.jobs.models import HyperscaleConfig, TerminalMode
from hyperscale.core.jobs.runner.cluster_runner import ClusterRunner
from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.distributed.models import JobStatus
from hyperscale.graph import Workflow
from hyperscale.logging import LoggingConfig, LogLevelName

from hyperscale.commands.cli import (
    AssertSet,
    ImportType,
    JsonFile,
    command,
)

from .node_address import parse_node_address
from .shared import get_default_workers, get_default_config, node_env, resolve_auth_secret





@command(shortnames={"host": "H"})
async def workflow(
    path: ImportType[Workflow],
    config: JsonFile[HyperscaleConfig] = get_default_config,
    log_level: AssertSet[LogLevelName] = "fatal",
    workers: int = get_default_workers,
    name: str = "default",
    quiet: bool = False,
    gates: list[str] = [],
    managers: list[str] = [],
    host: str = "127.0.0.1",
    acm_secret: str | None = None,
):
    """
    Run the specified test file locally, or on a running cluster: through
    its gates, or straight to one datacenter's managers

    @param path The path to the test file to run
    @param config A path to a valid .hyperscale.json config file
    @param log_level The log level to use for log files
    @param workers The number of parallel threads/processes to use (local runs)
    @param name The name of the test
    @param quiet If specified, all GUI output will be disabled
    @param gates The TCP host:port of the cluster's gates to run the test through
    @param managers The TCP host:port of one datacenter's managers to run the test on directly
    @param host The address the cluster pushes the run's progress to (cluster runs; it listens on the config's server port)
    @param acm_secret The shared cluster secret (cluster runs; defaults to MERCURY_SYNC_AUTH_SECRET)
    """

    workflows = [(workflow._dependencies, workflow()) for workflow in path.data.values()]

    for _, workflow in workflows:
        cloudpickle.register_pickle_by_value(sys.modules[workflow.__module__])

    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )

    if gates or managers:
        terminal_mode: TerminalMode = config.data.terminal_mode
        if quiet:
            terminal_mode = "disabled"

        try:
            cluster_runner = ClusterRunner(
                host,
                config.data.server_port,
                node_env(
                    MERCURY_SYNC_AUTH_SECRET=resolve_auth_secret(acm_secret),
                    MERCURY_SYNC_LOG_LEVEL=log_level.data,
                ),
                gates=[parse_node_address(gate) for gate in gates],
                managers=[parse_node_address(manager) for manager in managers],
            )

        except ValueError as address_error:
            print(str(address_error), file=sys.stderr)
            raise SystemExit(1)

        try:
            result = await cluster_runner.run(name, workflows, terminal_mode=terminal_mode)

        except (KeyboardInterrupt, asyncio.CancelledError):
            # Stopped by the operator: the runner cancelled the job on the
            # cluster and shut down. Exit as a shell reports a process a
            # signal ended (128 + its number); a KeyboardInterrupt is SIGINT.
            raise SystemExit(128 + (cluster_runner.stopped_by or signal.SIGINT).value)

        if result.status != JobStatus.COMPLETED.value:
            print(
                f"{name}: job {result.job_id} {result.status}"
                + (f": {result.error}" if result.error else ""),
                file=sys.stderr,
            )
            raise SystemExit(1)

        print(f"{name}: job {result.job_id} completed")
        return

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
            terminal_mode=config.data.terminal_mode,
        )

