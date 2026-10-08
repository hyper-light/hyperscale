import asyncio
import functools
import operator
import signal
import sys
from collections.abc import Awaitable, Callable, Iterable

import cloudpickle

try:
    import uvloop

    uvloop.install()

except Exception:
    pass

from hyperscale.core.jobs.models import HyperscaleConfig, TerminalMode
from hyperscale.core.jobs.runner.cluster_runner import ClusterRunner
from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.distributed.models import ClientJobResult, JobStatus
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.graph import Workflow
from hyperscale.logging import LoggingConfig, LogLevelName
from hyperscale.reporting.common.results_types import RunResults, WorkflowStats
from hyperscale.ui.ci_safe import TerminalSelection
from hyperscale.ui.hyperscale_interface_config import HyperscaleInterfaceConfig
from hyperscale.ui.node_dashboard import StderrLogRedirect
from hyperscale.ui.run_summary import RunSummaryLines
from hyperscale.ui.run_summary.run_summary_text import start_text
from hyperscale.ui.run_summary.run_terminal_capability import select_run_terminal_mode

from hyperscale.commands.cli import (
    AssertSet,
    ImportType,
    JsonFile,
    command,
)

from .node_address import parse_node_address
from .models import RunOutcome
from .shared import (
    get_default_workers,
    get_default_config,
    node_env,
    node_log_path,
    requested_output_mode,
    resolve_auth_secret,
)

TestWorkflows = list[tuple[list[str], Workflow]]
RunCall = Callable[[], Awaitable[RunOutcome]]
# The exit status of a run that ended other than completing.
FAILED_EXIT_STATUS = 1
# The role a run's log file is named for, beside the nodes' log files.
RUN_LOG_ROLE = "run"


@command(shortnames={"host": "H", "output_mode": "o"})
async def workflow(
    path: ImportType[Workflow],
    config: JsonFile[HyperscaleConfig] = get_default_config,
    log_level: AssertSet[LogLevelName] = "fatal",
    workers: int = get_default_workers,
    name: str = "default",
    quiet: bool = False,
    output_mode: AssertSet[TerminalMode] | None = None,
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
    @param output_mode The terminal output mode -- full, ci-safe, ci or disabled -- overriding the config's terminal_mode (full still falls back to ci-safe where it cannot draw)
    @param gates The TCP host:port of the cluster's gates to run the test through
    @param managers The TCP host:port of one datacenter's managers to run the test on directly
    @param host The address the cluster pushes the run's progress to (cluster runs; it listens on the config's server port)
    @param acm_secret The shared cluster secret (cluster runs; defaults to MERCURY_SYNC_AUTH_SECRET, else the per-user cluster cookie)
    """
    workflows = instantiate_workflows(path.data.values())
    test_workflows = list(map(operator.itemgetter(1), workflows))

    logging_config = LoggingConfig()
    logging_config.update(
        log_directory=config.data.logs_directory,
        log_level=log_level.data,
        log_output="stderr",
    )

    # The full UI where stdout can show it, otherwise CI-safe lines; an
    # explicitly configured mode, and --quiet, win.
    selection = await select_run_terminal_mode(requested_output_mode(output_mode, config), quiet, test_workflows)
    run_call = (
        await cluster_run_call(
            name, workflows, selection.mode, config.data, log_level.data, gates, managers, host, acm_secret
        )
        if gates or managers
        else functools.partial(run_locally, name, workflows, selection.mode, config.data.server_port, workers)
    )
    log_path = node_log_path(config.data.logs_directory, RUN_LOG_ROLE, name, host, config.data.server_port)
    summary_lines = run_summary_lines(selection, test_workflows)

    # While the run's UI writes, stderr (its logs, and its local workers'
    # output) goes to the log file: no log line tears a frame or lands
    # among the summary lines.
    async with StderrLogRedirect(log_path, enabled=selection.mode != "disabled"):
        outcome = await run_reported(
            summary_lines,
            start_text(test_workflows, selection.degraded_reason, log_path),
            run_call,
            name,
        )

    report_outcome(outcome, summary_lines is None)


def instantiate_workflows(workflow_classes: Iterable[type[Workflow]]) -> TestWorkflows:
    """Each workflow of the test file, with its dependencies; the test
    file is pickled by value, as it is not importable where they run."""
    workflows = [(workflow_class._dependencies, workflow_class()) for workflow_class in workflow_classes]
    for _, test_workflow in workflows:
        cloudpickle.register_pickle_by_value(sys.modules[test_workflow.__module__])

    return workflows


def run_summary_lines(selection: TerminalSelection, test_workflows: list[Workflow]) -> RunSummaryLines | None:
    """The CI-safe summary lines a "ci-safe" run writes to stdout, checking
    its progress once per the full UI's update interval; None in any other
    mode."""
    if selection.mode != "ci-safe":
        return None

    return RunSummaryLines(
        sys.stdout.buffer,
        test_workflows,
        RealClock(),
        TaskRunner(),
        HyperscaleInterfaceConfig().update_interval,
    )


async def cluster_run_call(
    name: str,
    workflows: TestWorkflows,
    terminal_mode: TerminalMode,
    config: HyperscaleConfig,
    log_level: LogLevelName,
    gates: list[str],
    managers: list[str],
    host: str,
    acm_secret: str | None,
) -> RunCall:
    """The run on the cluster. The node addresses and the cluster secret
    are checked here, before the run's output starts: a bad one ends the
    command (exit status 1, the reason on stderr)."""
    try:
        gate_addresses = list(map(parse_node_address, gates))
        manager_addresses = list(map(parse_node_address, managers))

    except ValueError as address_error:
        print(str(address_error), file=sys.stderr)
        raise SystemExit(FAILED_EXIT_STATUS)

    env = node_env(
        MERCURY_SYNC_AUTH_SECRET=await resolve_auth_secret(acm_secret),
        MERCURY_SYNC_LOG_LEVEL=log_level,
    )
    return functools.partial(
        run_on_cluster,
        name,
        workflows,
        terminal_mode,
        functools.partial(
            ClusterRunner,
            host,
            config.server_port,
            env,
            gates=gate_addresses,
            managers=manager_addresses,
        ),
    )


async def run_on_cluster(
    name: str,
    workflows: TestWorkflows,
    terminal_mode: TerminalMode,
    create_cluster_runner: Callable[[], ClusterRunner],
) -> RunOutcome:
    """Run the workflows as one job on the cluster. Stopped by the
    operator, the runner cancels the job on the cluster and shuts down,
    and the run exits as a shell reports a process a signal ended (128 +
    its number; a KeyboardInterrupt is SIGINT)."""
    cluster_runner = create_cluster_runner()
    try:
        result = await cluster_runner.run(name, workflows, terminal_mode=terminal_mode)

    except (KeyboardInterrupt, asyncio.CancelledError):
        stopped_by = cluster_runner.stopped_by or signal.SIGINT
        return RunOutcome(f"{name}: stopped by {stopped_by.name}", 128 + stopped_by.value)

    return cluster_outcome(name, result)


def cluster_outcome(name: str, result: ClientJobResult) -> RunOutcome:
    """A cluster job's outcome: completed, or its status and error, with
    each workflow's final results."""
    if result.status == JobStatus.COMPLETED.value:
        return RunOutcome(f"{name}: job {result.job_id} completed", 0, cluster_workflow_stats(result))

    error_suffix = f": {result.error}" if result.error else ""
    return RunOutcome(
        f"{name}: job {result.job_id} {result.status}{error_suffix}",
        FAILED_EXIT_STATUS,
        cluster_workflow_stats(result),
    )


def cluster_workflow_stats(result: ClientJobResult) -> dict[str, WorkflowStats]:
    """Each workflow's final results as the client received them -- the
    stats it gave its local reporters -- by workflow name."""
    return {
        workflow_result.workflow_name: workflow_result.stats
        for workflow_result in result.workflow_results.values()
        if is_test_workflow_stats(workflow_result.stats)
    }


def is_test_workflow_stats(workflow_stats: WorkflowStats | None) -> bool:
    """Whether ``workflow_stats`` are a test workflow's final results (a
    workflow that runs no load returns its context instead)."""
    return "stats" in (workflow_stats or {})


async def run_locally(
    name: str,
    workflows: TestWorkflows,
    terminal_mode: TerminalMode,
    server_port: int,
    workers: int,
) -> RunOutcome:
    """Run the workflows on local worker processes. The runner returns the
    error a run ended on; one raised past it aborts the runner."""
    runner = LocalRunner("127.0.0.1", server_port, workers=workers)
    try:
        result = await runner.run(name, workflows, terminal_mode=terminal_mode)

    except (
        Exception,
        KeyboardInterrupt,
        asyncio.CancelledError,
        asyncio.InvalidStateError,
    ) as run_error:
        await runner.abort(error=run_error, terminal_mode=terminal_mode)
        result = run_error

    return local_outcome(name, result)


def local_outcome(name: str, result: RunResults | BaseException) -> RunOutcome:
    """A local run's outcome: completed, with each workflow's final
    results (those its reporters were given); failed, with each workflow
    that raised or timed out and why; or the error it ended on."""
    if isinstance(result, BaseException):
        return RunOutcome(f"{name}: failed: {type(result).__name__}: {result}", FAILED_EXIT_STATUS)

    if failed_workflows := failed_workflows_line(result):
        return RunOutcome(f"{name}: failed: {failed_workflows}", FAILED_EXIT_STATUS, local_workflow_stats(result))

    return RunOutcome(f"{name}: completed", 0, local_workflow_stats(result))


def failed_workflows_line(result: RunResults) -> str:
    """Each workflow of the run that raised or timed out, with its error,
    in workflow-name order; empty when none did."""
    return "; ".join(
        f"workflow {workflow_name}: {type(workflow_error).__name__}: {workflow_error}"
        for workflow_name, workflow_error in sorted(result.get("timeouts", {}).items())
    )


def local_workflow_stats(result: RunResults) -> dict[str, WorkflowStats]:
    """Each test workflow's final results from a local run -- the stats
    the runner gave its reporters -- by workflow name."""
    return {
        workflow_name: workflow_stats
        for workflow_name, workflow_stats in result["results"].items()
        if is_test_workflow_stats(workflow_stats)
    }


async def run_reported(
    summary_lines: RunSummaryLines | None,
    run_start_text: str,
    run_call: RunCall,
    name: str,
) -> RunOutcome:
    """Run ``run_call``, with the CI-safe summary lines around it when
    there are any: started first, and stopped on every exit path with the
    run's outcome as their last line."""
    if summary_lines is None:
        return await run_call()

    await summary_lines.start(run_start_text)
    outcome = RunOutcome(f"{name}: stopped", FAILED_EXIT_STATUS)
    try:
        outcome = await run_call()

    finally:
        await stop_summary_lines(summary_lines, outcome)

    return outcome


async def stop_summary_lines(summary_lines: RunSummaryLines, outcome: RunOutcome) -> None:
    """Stop the summary lines with ``outcome``; a write that failed (its
    reader went away) is reported on stderr: the run's log file."""
    if (write_error := await summary_lines.stop(outcome.line, outcome.workflow_stats)) is not None:
        print(
            f"the run's progress output stopped: {type(write_error).__name__}: {write_error}",
            file=sys.stderr,
        )


def report_outcome(outcome: RunOutcome, outcome_unwritten: bool) -> None:
    """End the command as the run ended: a run that did not complete
    reports why on stderr and exits non-zero; one that completed reports
    it on stdout, unless the summary lines already wrote it."""
    if outcome.exit_status != 0:
        print(outcome.line, file=sys.stderr)
        raise SystemExit(outcome.exit_status)

    if outcome_unwritten:
        print(outcome.line)
