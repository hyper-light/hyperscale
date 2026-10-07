"""
E2E: the progress output of a real local `hyperscale run workflow`.

- "full" mode where the full run UI cannot show -- stdout a pipe, TERM
  dumb, a CI environment, an ASCII-only encoding -- writes append-only
  plain ASCII lines instead, with no escape byte: a start line naming the
  test file and the workflow's VUs and duration, progress lines with the
  elapsed time, total actions, actions per second and the current step
  that change as the run goes, then the final summary, where results were
  written and the outcome. The run's logs go to its log file, never among
  the lines (stderr is merged into the output here, as in `docker logs`),
  and the run exits 0.
- "full" mode on a capable terminal (a 120x38 pty) still draws the full
  UI: every frame one synchronized update of the whole screen, the
  Hyperscale header, and the cursor shown again at the end.

Every workflow targets an HTTP server the test starts on 127.0.0.1, and
each run has a server port of its own.
"""

import asyncio
import fcntl
import json
import os
import pathlib
import pty
import re
import signal
import struct
import termios

import pytest

from hyperscale.ui.ci_safe.terminal_capability import CI_ENVIRONMENT_VARIABLES
from tests.integration.cli.node_processes import HYPERSCALE, LOCALHOST, command_environment, reserve_port_blocks

# The command a test runs (a test harness may run the source tree instead
# of the installed entry point).
RUN_WORKFLOW_COMMAND = [HYPERSCALE, "run", "workflow"]
WORKERS = 2
# The ports a local run binds from its server port: its own, then each
# worker's (LocalRunner._bin_and_check_socket_range), with room to spare.
RUN_PORT_BLOCK = 2 * (1 + WORKERS + WORKERS**2)
# Server ports other local runs on this host use: never ours.
OTHER_SESSIONS_PORTS = frozenset({8790, 8791})
WORKFLOW_VUS = 4
WORKFLOW_DURATION_SECONDS = 12
# The run's bound: its duration, worker startup and shutdown.
RUN_TIMEOUT_SECONDS = WORKFLOW_DURATION_SECONDS + 120.0
TERMINAL_COLUMNS = 120
TERMINAL_LINES = 38
TEST_NAME = "default"
ESCAPE = b"\x1b"
HIDE_CURSOR = b"\x1b[?25l"
SHOW_CURSOR = b"\x1b[?25h"
FRAME_START = b"\x1b[?2026h\x1b[H"
FRAME_END = b"\x1b[?2026l"
# A line of the Hyperscale header's art (hyperscale/ui/hyperscale_header.py).
HEADER_ART_LINE = b"//__ \\\\// //_// //_// // // //__  //    __// // //_//"
# What the run logs to stderr at info as its workflow starts: never in the output.
RUN_LOG_FRAGMENT = "Running workflow LocalTarget"
ELAPSED_PREFIX = re.compile(r"^\d+h\d{2}m\d{2}s \| ")
# Each case: why the full run UI cannot show, as the run's environment and
# its stdout (None: a pipe; else a pty of columns x lines).
CI_SAFE_CASES: dict[str, tuple[dict[str, str], tuple[int, int] | None]] = {
    "stdout piped": ({}, None),
    "TERM dumb": ({"TERM": "dumb"}, (TERMINAL_COLUMNS, TERMINAL_LINES)),
    "CI set": ({"CI": "true"}, (TERMINAL_COLUMNS, TERMINAL_LINES)),
    "ASCII encoding": ({"LANG": "C", "LC_ALL": "C", "PYTHONIOENCODING": "ascii"}, (TERMINAL_COLUMNS, TERMINAL_LINES)),
}

WORKFLOW_SOURCE = """
from hyperscale.graph import Workflow, step
from hyperscale.reporting.json import JSONConfig
from hyperscale.testing import URL, HTTPResponse


class LocalTarget(Workflow):
    vus: int = {vus}
    duration: str = "{duration_seconds}s"
    reporting = JSONConfig(
        workflow_results_filepath="{results_directory}/workflow_results.json",
        step_results_filepath="{results_directory}/step_results.json",
    )

    @step()
    async def get_target(self, url: URL = "http://{host}:{port}/") -> HTTPResponse:
        return await self.client.http.get(url)
"""


async def answer_requests(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Answer every GET on a keep-alive connection with a 2-byte 200."""
    try:
        while await reader.readuntil(b"\r\n\r\n"):
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            await writer.drain()

    except (asyncio.IncompleteReadError, ConnectionError):
        pass

    finally:
        writer.close()


async def start_target() -> tuple[asyncio.Server, int]:
    """The run's HTTP target on 127.0.0.1, at a port the OS picks."""
    server = await asyncio.start_server(answer_requests, LOCALHOST, 0)
    return server, server.sockets[0].getsockname()[1]


def reserve_run_port() -> int:
    """A free block of ports for one local run, clear of other sessions'."""
    while True:
        (run_port,) = reserve_port_blocks([RUN_PORT_BLOCK])
        if OTHER_SESSIONS_PORTS.isdisjoint(range(run_port, run_port + RUN_PORT_BLOCK)):
            return run_port


def write_run_files(directory: pathlib.Path, target_port: int) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
    """The test file (its results written in ``directory``), a "full"
    config with a server port and logs directory of its own; (test file,
    config, logs directory)."""
    test_file = directory / "local_target_test.py"
    test_file.write_text(
        WORKFLOW_SOURCE.format(
            vus=WORKFLOW_VUS,
            duration_seconds=WORKFLOW_DURATION_SECONDS,
            host=LOCALHOST,
            port=target_port,
            results_directory=directory,
        )
    )
    logs_directory = directory / "logs"
    config_path = directory / "hyperscale.json"
    config_path.write_text(
        json.dumps(
            {"logs_directory": str(logs_directory), "server_port": reserve_run_port(), "terminal_mode": "full"}
        )
    )
    return test_file, config_path, logs_directory


def run_environment(**extra_variables: str) -> dict[str, str]:
    """A capable terminal's environment -- a TERM naming one, none of the
    CI providers' variables (the test itself may run in CI) -- then
    ``extra_variables``."""
    environment = {
        name: value
        for name, value in command_environment(TERM="xterm-256color").items()
        if name not in CI_ENVIRONMENT_VARIABLES
    }
    environment.update(extra_variables)
    return environment


async def run_on_pipe(arguments: list[str], environment: dict[str, str]) -> tuple[int, bytes]:
    """Run the command with stdout and stderr one pipe; its exit status
    and everything it wrote."""
    process = await asyncio.create_subprocess_exec(
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
        env=environment,
    )
    try:
        output, _ = await asyncio.wait_for(process.communicate(), timeout=RUN_TIMEOUT_SECONDS)

    finally:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()

    return process.returncode, output


async def run_on_terminal(
    arguments: list[str], environment: dict[str, str], columns: int, lines: int
) -> tuple[int, bytes]:
    """Run the command with stdin, stdout and stderr a pty of ``columns``
    x ``lines``; its exit status and everything it wrote."""
    master_descriptor, slave_descriptor = pty.openpty()
    fcntl.ioctl(slave_descriptor, termios.TIOCSWINSZ, struct.pack("HHHH", lines, columns, 0, 0))
    os.set_blocking(master_descriptor, False)
    collected = bytearray()
    closed = asyncio.Event()
    loop = asyncio.get_running_loop()

    def on_readable() -> None:
        try:
            chunk = os.read(master_descriptor, 1 << 16)
        except OSError:
            chunk = b""
        if not chunk:
            loop.remove_reader(master_descriptor)
            closed.set()
        collected.extend(chunk)

    process = await asyncio.create_subprocess_exec(
        *arguments,
        stdin=slave_descriptor,
        stdout=slave_descriptor,
        stderr=slave_descriptor,
        start_new_session=True,
        env=environment,
    )
    os.close(slave_descriptor)
    loop.add_reader(master_descriptor, on_readable)
    try:
        await asyncio.wait_for(process.wait(), timeout=RUN_TIMEOUT_SECONDS)
        await asyncio.wait_for(closed.wait(), timeout=RUN_TIMEOUT_SECONDS)

    finally:
        loop.remove_reader(master_descriptor)
        os.close(master_descriptor)
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()

    return process.returncode, bytes(collected)


async def run_local_workflow(
    directory: pathlib.Path, extra_environment: dict[str, str], stdout_terminal: tuple[int, int] | None
) -> tuple[int, bytes, pathlib.Path]:
    """Run a local workflow against a target the test serves, logging at
    info; its exit status, its output, and its logs directory."""
    server, target_port = await start_target()
    test_file, config_path, logs_directory = write_run_files(directory, target_port)
    arguments = [
        *RUN_WORKFLOW_COMMAND,
        str(test_file),
        "--config", str(config_path),
        "--workers", str(WORKERS),
        "--log-level", "info",
    ]
    environment = run_environment(**extra_environment)
    try:
        if stdout_terminal is None:
            exit_status, output = await run_on_pipe(arguments, environment)
        else:
            exit_status, output = await run_on_terminal(arguments, environment, *stdout_terminal)

    finally:
        server.close()
        await server.wait_closed()

    return exit_status, output, logs_directory


def assert_ci_safe_output(exit_status: int, output: bytes, logs_directory: pathlib.Path) -> list[str]:
    """The run completed and wrote only plain ASCII lines: its start,
    changing progress, its end and outcome; its logs are in its log file.
    The lines."""
    assert exit_status == 0, output.decode("ascii", errors="replace")
    assert ESCAPE not in output, f"the output carries escape sequences:\n{output!r}"
    assert output.isascii(), f"the output is not ASCII:\n{output!r}"
    lines = [line for line in output.decode("ascii").replace("\r", "").split("\n") if line]

    start_line, *progress_lines, end_line, outcome_line = lines
    assert start_line.startswith(
        "0h00m00s | run start | file local_target_test.py | workflows LocalTarget 4 VUs for 12s"
    ), lines
    assert len(progress_lines) >= 2, lines
    assert all(ELAPSED_PREFIX.match(line) and "actions/s" in line for line in progress_lines), lines
    assert len({ELAPSED_PREFIX.sub("", line) for line in progress_lines}) >= 2, (
        f"the progress lines never changed: {progress_lines}"
    )
    results_directory = logs_directory.parent
    assert " | run end | LocalTarget: " in end_line, end_line
    assert end_line.endswith(
        f" | results JSON {results_directory}/workflow_results.json {results_directory}/step_results.json"
    ), end_line
    assert outcome_line == f"{TEST_NAME}: completed", lines
    assert_end_line_matches_results(end_line, results_directory)

    assert RUN_LOG_FRAGMENT not in output.decode("ascii"), "a log line was written among the summary lines"
    run_logs = "".join(log_path.read_text() for log_path in logs_directory.glob("run-*.log"))
    assert RUN_LOG_FRAGMENT in run_logs, "the run's logs never reached its log file"
    return lines


def reported_count(metrics: list[dict[str, str | int | float]], metric_name: str, step: str | None = None) -> int:
    """A count the JSON reporter wrote: ``metric_name`` of the workflow
    (``step`` None) or of ``step``."""
    (value,) = [
        metric["metric_value"]
        for metric in metrics
        if metric["metric_type"] == "COUNT"
        and metric["metric_name"] == metric_name
        and metric.get("metric_step") == step
    ]
    return value


def assert_end_line_matches_results(end_line: str, results_directory: pathlib.Path) -> None:
    """The run-end line's action count and each step's total, ok and err
    are exactly those the run's reporter wrote."""
    workflow_metrics = json.loads((results_directory / "workflow_results.json").read_text())
    step_metrics = json.loads((results_directory / "step_results.json").read_text())
    steps = sorted({metric["metric_step"] for metric in step_metrics})
    expected_steps = ", ".join(
        f"{step} total {reported_count(step_metrics, 'executed', step)} "
        f"ok {reported_count(step_metrics, 'succeeded', step)} err {reported_count(step_metrics, 'failed', step)}"
        for step in steps
    )
    assert f"LocalTarget: {reported_count(workflow_metrics, 'executed')} actions in " in end_line, end_line
    assert f"steps [{expected_steps}]" in end_line, (end_line, expected_steps)


def assert_full_ui_frames(exit_status: int, output: bytes) -> list[bytes]:
    """The run completed drawing the full UI: hidden cursor, frames each
    one whole synchronized update of the screen, the header, and the
    cursor shown again. The frames."""
    assert exit_status == 0, output[-3000:]
    assert HIDE_CURSOR in output and output.rfind(SHOW_CURSOR) > output.rfind(HIDE_CURSOR)
    assert HEADER_ART_LINE in output
    frames = [chunk.split(FRAME_END)[0] for chunk in output.split(FRAME_START)[1:]]
    assert len(frames) >= 2, output[-3000:]
    # Each frame arrives whole, between its own start and end, and draws
    # the same number of rows: none is cut or merged with another.
    assert all(chunk.count(FRAME_END) == 1 for chunk in output.split(FRAME_START)[1:])
    assert len({frame.count(b"\n") for frame in frames}) == 1
    assert RUN_LOG_FRAGMENT.encode() not in output, "a log line tore the full UI's frames"
    return frames


@pytest.mark.parametrize("case", list(CI_SAFE_CASES))
async def test_where_the_full_run_ui_cannot_show_a_run_writes_ascii_lines(case: str, tmp_path: pathlib.Path) -> None:
    extra_environment, stdout_terminal = CI_SAFE_CASES[case]
    exit_status, output, logs_directory = await run_local_workflow(tmp_path, extra_environment, stdout_terminal)
    assert_ci_safe_output(exit_status, output, logs_directory)


async def test_a_capable_terminal_still_draws_the_full_run_ui(tmp_path: pathlib.Path) -> None:
    exit_status, output, _ = await run_local_workflow(tmp_path, {}, (TERMINAL_COLUMNS, TERMINAL_LINES))
    assert_full_ui_frames(exit_status, output)
