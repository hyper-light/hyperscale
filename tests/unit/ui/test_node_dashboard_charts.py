"""
The node dashboards render as `run workflow`'s UI does: the Hyperscale
header with the node's identity, the role's panels, a chart per series the
role's reader samples, and the role's table.

Each test builds a real node (constructed, never started) on a clock the
test moves by hand, puts known state into it -- a registered worker,
dispatches and their round trips, jobs, datacenter heartbeats -- and reads
it twice through the role's reader: once directly, asserting each chart's
exact value, and once through a "ci" dashboard rendering into a pipe that
stands in for the terminal, asserting the header, each chart (its title
and the top of its value axis, which the scatter plot scales to 1.1 times
the largest value it plots, rounded up) and the table rows.

The clock stands still while the dashboard renders: the rates the first
sample computes (over the interval the test advanced) are the only rate
points, and every later sample at the same instant adds none. So each test
gives the dashboard a reader of its own, constructed with the one it reads
directly (a reader's rates are over the interval since its own last
sample).
"""

import asyncio
import math
import pathlib
import re
from collections.abc import Awaitable, Callable
from typing import TypeVar

from hyperscale.distributed.jobs.dispatch_outcome import DispatchOutcome
from hyperscale.distributed.models import (
    GlobalJobStatus,
    JobInfo,
    ManagerHeartbeat,
    NodeInfo,
    TrackingToken,
    WorkerRegistration,
    WorkflowProgress,
)
from hyperscale.distributed.nodes import GateServer, ManagerServer, WorkerServer
from hyperscale.distributed.reliability.backpressure_level import BackpressureLevel
from hyperscale.distributed.swim.health_aware_server import HealthAwareServer
from hyperscale.logging import Logger
from hyperscale.ui.components.scatter_plot import PlotConfig, ScatterPlot
from hyperscale.ui.hyperscale_header import create_hyperscale_header
from hyperscale.ui.node_dashboard import (
    GateDashboardReader,
    ManagerDashboardReader,
    NodeDashboard,
    NodeDashboardConfig,
    NodeDashboardReader,
    WorkerDashboardReader,
)
from hyperscale.ui.node_dashboard.node_dashboard_chart_series import NodeDashboardChartSeries
from tests.integration.cli.node_processes import reserve_port_blocks, worker_port_span
from tests.integration.ui.node_dashboard_harness import (
    dashboard_env,
    dashboard_tasks,
    roomy_terminal,
    terminal_pipe,
    wait_for_frame,
)

__all__ = ["roomy_terminal"]

AwaitedResult = TypeVar("AwaitedResult")
ANSI_SEQUENCE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
DATACENTER = "DC-DASH"
# The interval the test's clock advances between the readers' baseline and
# their first sample: every rate is a count over it.
SAMPLED_INTERVAL_SECONDS = 2.0
# The scatter plot scales its value axis to this multiple of the largest
# value it plots (ScatterPlot._generate_x_and_y_vals), as the run UI's chart.
VALUE_AXIS_HEADROOM = 1.1


class HandClock:
    """A monotonic and wall clock the test moves by hand; its waits are
    the event loop's own, so the dashboard's terminal renders on."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def monotonic_ns(self) -> int:
        return int(self.now * 1_000_000_000)

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def wait_for(self, awaitable: Awaitable[AwaitedResult], timeout: float | None) -> AwaitedResult:
        return await asyncio.wait_for(awaitable, timeout)


def plain_lines(frame: str) -> list[str]:
    """The frame's lines without their color sequences."""
    return ANSI_SEQUENCE.sub("", frame).split("\n")


def chart_axis_top(frame: str, title: str) -> str:
    """The top label of the value axis of the chart titled ``title``: the
    line below its title, from the title's column."""
    lines = plain_lines(frame)
    title_marker = f"({title}) ^"
    title_line_indexes = [index for index, line in enumerate(lines) if title_marker in line]
    assert len(title_line_indexes) == 1, f"no chart titled {title!r}:\n" + "\n".join(lines)
    title_line_index = title_line_indexes[0]
    title_column = lines[title_line_index].index(title_marker)
    axis_line = lines[title_line_index + 1][title_column:]
    return axis_line.split("|", 1)[0].strip()


def expected_axis_top(largest_value: float) -> str:
    return str(math.ceil(largest_value * VALUE_AXIS_HEADROOM))


async def header_lines() -> list[str]:
    """The Hyperscale header's art, as the run UI renders it."""
    header = create_hyperscale_header("compatability")
    await header.fit()
    lines, _ = await header.get_next_frame()
    return [stripped for line in lines if (stripped := ANSI_SEQUENCE.sub("", line).strip())]


async def render_dashboard(
    reader: NodeDashboardReader,
    node: HealthAwareServer,
    tmp_path: pathlib.Path,
    *fragments: str,
) -> str:
    """Render ``reader``'s dashboard until a frame shows every fragment;
    return that frame. The dashboard is stopped and leaves no task."""
    async with terminal_pipe() as collected:
        dashboard = NodeDashboard(
            reader,
            node,
            "ci",
            dashboard_env(tmp_path),
            tmp_path / "node.log",
            NodeDashboardConfig(),
            Logger(),
        )
        await dashboard.start()
        try:
            frame = await wait_for_frame(collected, *fragments)
        finally:
            await dashboard.stop()

    assert dashboard_tasks() == []
    return frame


async def assert_header_and_charts(frame: str, reader: NodeDashboardReader, chart_tops: dict[str, str]) -> None:
    plain_frame = "\n".join(plain_lines(frame))
    for header_line in await header_lines():
        assert header_line in plain_frame, f"the Hyperscale header is missing {header_line!r}:\n{plain_frame}"

    for chart in reader.layout.charts:
        assert chart_axis_top(frame, chart.title) == chart_tops[chart.name], (
            f"chart {chart.title!r} plots the wrong values:\n{plain_frame}"
        )


async def registered_manager(tmp_path: pathlib.Path, clock: HandClock) -> tuple[ManagerServer, int]:
    """A manager with one registered worker of eight cores, six in use."""
    manager_port, worker_port = reserve_port_blocks([2, 2])
    manager = ManagerServer(
        host="127.0.0.1",
        tcp_port=manager_port,
        udp_port=manager_port + 1,
        env=dashboard_env(tmp_path),
        dc_id=DATACENTER,
        clock=clock,
    )
    registration = WorkerRegistration(
        node=NodeInfo(
            node_id="worker-dash",
            role="worker",
            host="127.0.0.1",
            port=worker_port,
            datacenter=DATACENTER,
            udp_port=worker_port + 1,
        ),
        total_cores=8,
        available_cores=2,
        memory_mb=1024,
    )
    await manager._registry.register_worker(registration)
    await manager._worker_pool.register_worker(registration)
    return manager, worker_port


def add_job(manager: ManagerServer, job_id: str) -> JobInfo:
    job = JobInfo(token=TrackingToken.for_job(DATACENTER, manager.node_id.full, job_id), submission=None)
    manager._job_manager._jobs[str(job.token)] = job
    return job


async def test_the_manager_dashboard_charts_its_dispatches_completions_cores_and_latency(
    tmp_path: pathlib.Path,
) -> None:
    clock = HandClock()
    manager, worker_port = await registered_manager(tmp_path, clock)
    job = add_job(manager, "job-dash")
    reader = ManagerDashboardReader(manager)
    rendered_reader = ManagerDashboardReader(manager)

    clock.now += SAMPLED_INTERVAL_SECONDS
    manager._dispatch._dispatch_outcome_counts[DispatchOutcome.ACCEPTED] += 100
    job.workflows_completed = 20
    job.workflows_failed = 8
    for _ in range(manager._manager_state._slo_config.min_sample_count):
        manager._manager_state.record_dispatch_latency("worker-dash", 40.0, clock.now)

    chart_values = reader.read().chart_values
    assert chart_values == [50.0, 10.0, 4.0, 75.0, 40.0]

    frame = await render_dashboard(
        rendered_reader, manager, tmp_path, "(dispatches /s) ^", "(dispatch p95 ms) ^", f"127.0.0.1:{worker_port}"
    )
    await assert_header_and_charts(
        frame,
        reader,
        {
            "dispatches": expected_axis_top(50.0),
            "completions": expected_axis_top(10.0),
            "failures": expected_axis_top(4.0),
            "cores_in_use": expected_axis_top(75.0),
            "dispatch_latency": expected_axis_top(40.0),
        },
    )
    worker_row = next(line for line in plain_lines(frame) if f"127.0.0.1:{worker_port}" in line)
    assert worker_row.split() == ["|", f"127.0.0.1:{worker_port}", "healthy", "8", "2", "healthy", "40.0", "|"]
    assert f"MANAGER {manager.node_id.short}" in frame


async def test_a_chart_with_no_value_is_not_plotted_as_zero(tmp_path: pathlib.Path) -> None:
    # No dispatch has had its round trip timed: the latency chart has no
    # point to plot (a gap, not a zero latency), and the worker's p95 cell
    # keeps its default.
    clock = HandClock()
    manager, worker_port = await registered_manager(tmp_path, clock)
    reader = ManagerDashboardReader(manager)
    rendered_reader = ManagerDashboardReader(manager)
    clock.now += SAMPLED_INTERVAL_SECONDS

    assert reader.read().chart_values == [0.0, 0.0, 0.0, 75.0, None]

    frame = await render_dashboard(
        rendered_reader,
        manager,
        tmp_path,
        "(dispatches /s) ^",
        "(cores in use %) ^",
        "dispatch p95 ms: no value yet",
        f"127.0.0.1:{worker_port}",
    )
    assert "(dispatch p95 ms) ^" not in frame
    worker_row = next(line for line in plain_lines(frame) if f"127.0.0.1:{worker_port}" in line)
    assert worker_row.split()[-2] == "-"


async def test_the_worker_dashboard_charts_its_workflows_cores_throughput_and_backpressure(
    tmp_path: pathlib.Path,
) -> None:
    clock = HandClock()
    (worker_port,) = reserve_port_blocks([2 + worker_port_span(4)])
    worker = WorkerServer(
        host="127.0.0.1",
        tcp_port=worker_port,
        udp_port=worker_port + 1,
        env=dashboard_env(tmp_path),
        total_cores=4,
        dc_id=DATACENTER,
        clock=clock,
    )
    reader = WorkerDashboardReader(worker)
    await worker._core_allocator.allocate("workflow-dash", 3)
    progress = WorkflowProgress(
        job_id="job-dash",
        workflow_id="workflow-dash",
        workflow_name="DashWorkflow",
        status="running",
        completed_count=42,
        failed_count=1,
        rate_per_second=7.5,
        elapsed_seconds=1.0,
    )
    worker._worker_state.add_active_workflow("workflow-dash", progress, ("127.0.0.1", worker_port))
    worker._worker_state._throughput_last_value = 2.5
    worker._worker_state.set_manager_backpressure("manager-dash", BackpressureLevel.BATCH)

    assert reader.read().chart_values == [1.0, 75.0, 2.5, float(BackpressureLevel.BATCH)]

    frame = await render_dashboard(reader, worker, tmp_path, "(backpressure level) ^", "DashWorkflow")
    await assert_header_and_charts(
        frame,
        reader,
        {
            "active_workflows": expected_axis_top(1.0),
            "cores_busy": expected_axis_top(75.0),
            "throughput": expected_axis_top(2.5),
            "backpressure": expected_axis_top(float(BackpressureLevel.BATCH)),
        },
    )
    workflow_row = next(line for line in plain_lines(frame) if "DashWorkflow" in line)
    assert workflow_row.split() == ["|", "DashWorkflow", "running", "42", "1", "7.5", "0", "|"]
    assert f"WORKER {worker.node_id.short}" in frame


def manager_heartbeat(datacenter_id: str, slo_p95_ms: float, slo_updated_at: float) -> ManagerHeartbeat:
    """A heartbeat of a datacenter's leader with two healthy workers and
    free cores, reporting a dispatch latency SLO."""
    return ManagerHeartbeat(
        node_id=f"manager-{datacenter_id}",
        datacenter=datacenter_id,
        is_leader=True,
        term=1,
        version=1,
        active_jobs=0,
        active_workflows=0,
        worker_count=2,
        healthy_worker_count=2,
        available_cores=8,
        total_cores=16,
        slo_p95_ms=slo_p95_ms,
        slo_sample_count=20,
        slo_updated_at=slo_updated_at,
    )


def report_datacenter(gate: GateServer, datacenter_id: str, manager_port: int, slo_p95_ms: float) -> None:
    """A datacenter's leader heartbeat reaching the gate: its health
    classification and its SLO view."""
    heartbeat = manager_heartbeat(datacenter_id, slo_p95_ms, gate._clock.monotonic())
    manager_address = ("127.0.0.1", manager_port)
    gate._dc_health_manager.update_manager(datacenter_id, manager_address, heartbeat)
    gate._modular_state._datacenter_manager_status.setdefault(datacenter_id, {})[manager_address] = heartbeat


def hold_job(gate: GateServer, job_id: str, status: str) -> None:
    gate._job_manager.set_job(job_id, GlobalJobStatus(job_id=job_id, status=status))


async def test_the_gate_dashboard_charts_its_jobs_datacenters_and_their_latency(tmp_path: pathlib.Path) -> None:
    clock = HandClock()
    gate_port, east_manager_port, west_manager_port = reserve_port_blocks([2, 2, 2])
    gate = GateServer(
        host="127.0.0.1",
        tcp_port=gate_port,
        udp_port=gate_port + 1,
        env=dashboard_env(tmp_path),
        datacenter_managers={
            "DC-EAST": [("127.0.0.1", east_manager_port)],
            "DC-WEST": [("127.0.0.1", west_manager_port)],
        },
        clock=clock,
    )
    hold_job(gate, "job-running", "running")
    hold_job(gate, "job-finishing", "running")
    reader = GateDashboardReader(gate)
    rendered_reader = GateDashboardReader(gate)

    clock.now += SAMPLED_INTERVAL_SECONDS
    report_datacenter(gate, "DC-EAST", east_manager_port, 30.0)
    report_datacenter(gate, "DC-WEST", west_manager_port, 90.0)
    hold_job(gate, "job-finishing", "completed")
    hold_job(gate, "job-failing", "failed")
    for admitted_index in range(3):
        hold_job(gate, f"job-admitted-{admitted_index}", "running")

    # Four jobs admitted (three running, one already failed), one completed
    # and one failed over the interval; both datacenters accept jobs.
    assert reader.read().chart_values == [2.0, 0.5, 0.5, 2.0, 90.0]

    frame = await render_dashboard(
        rendered_reader, gate, tmp_path, "(jobs admitted /s) ^", "(worst DC p95 ms) ^", "DC-EAST", "DC-WEST"
    )
    await assert_header_and_charts(
        frame,
        reader,
        {
            "admitted": expected_axis_top(2.0),
            "completed": expected_axis_top(0.5),
            "failed": expected_axis_top(0.5),
            "accepting": expected_axis_top(2.0),
            "dispatch_latency": expected_axis_top(90.0),
        },
    )
    datacenter_rows = {
        cells[1]: cells for line in plain_lines(frame) if len(cells := line.split()) > 2 and cells[1].startswith("DC-")
    }
    assert datacenter_rows["DC-EAST"][2] == "healthy" and datacenter_rows["DC-EAST"][-2] == "30.0"
    assert datacenter_rows["DC-WEST"][2] == "healthy" and datacenter_rows["DC-WEST"][-2] == "90.0"
    assert f"GATE {gate.node_id.short}" in frame


def collect_rates(reader: ManagerDashboardReader, clock: HandClock, advance: Callable[[], None]) -> list[float | None]:
    clock.now += SAMPLED_INTERVAL_SECONDS
    advance()
    return reader.read().chart_values


async def test_manager_rates_count_only_what_happened_since_the_last_sample(tmp_path: pathlib.Path) -> None:
    # A job cleaned up between samples takes its counts with it, and a
    # job's counts never move backwards into negative rates.
    clock = HandClock()
    manager, _ = await registered_manager(tmp_path, clock)
    job = add_job(manager, "job-dash")
    reader = ManagerDashboardReader(manager)

    def complete_ten() -> None:
        job.workflows_completed = 10

    def clean_up_job() -> None:
        manager._job_manager._jobs.clear()

    assert collect_rates(reader, clock, complete_ten)[:3] == [0.0, 5.0, 0.0]
    assert collect_rates(reader, clock, clean_up_job)[:3] == [0.0, 0.0, 0.0]
    # The clock did not move: no rate is plotted for this sample.
    assert reader.read().chart_values[:3] == [None, None, None]


def test_chart_series_hold_one_window_and_plot_the_newest_at_its_end() -> None:
    # A window of ten seconds sampled each second holds ten samples, however
    # long the node runs: the newest plots at the window's end, the oldest
    # held one interval past its start, and a gap is left out.
    series = NodeDashboardChartSeries(window_seconds=10.0, sample_interval_seconds=1.0)
    for second in range(25):
        series.record(float(second), [float(second), None if second % 2 else float(second)])

    assert series.points(0) == [(float(place), float(second)) for place, second in zip(range(1, 11), range(15, 25))]
    assert series.points(1) == [
        (float(place), float(second)) for place, second in zip(range(2, 11, 2), range(16, 25, 2))
    ]


async def test_a_largest_value_under_five_is_drawn() -> None:
    # The value axis ends at 1.1 times the largest value rounded up, never
    # on it: plotille draws no point on an axis' maximum, so an axis
    # rounded down to the value (1.1 x 1 to 1) left a datacenter count of
    # one, or a single active workflow, undrawn.
    plot = ScatterPlot(
        "largest_value_plot",
        PlotConfig(plot_name="count", x_axis_name="s", y_axis_name="count", point_char="dot"),
    )
    await plot.fit(max_width=40, max_height=10)
    await plot.get_next_frame()
    await plot.update([(1.0, 1.0), (2.0, 1.0)])
    lines, rendered = await plot.get_next_frame()

    assert rendered
    value_row = next(line for line in lines if line.split("|", 1)[0].strip() == "1")
    assert "\u25cf" in ANSI_SEQUENCE.sub("", value_row), (
        "the points at the largest value were not drawn:\n" + "\n".join(lines)
    )


async def test_a_chart_whose_window_empties_shows_it_waits_again(tmp_path: pathlib.Path) -> None:
    # The latency chart plots while its window holds observations; once the
    # last one is older than the window (the node went idle) it shows that
    # it waits again, never its stale points.
    clock = HandClock()
    manager, _ = await registered_manager(tmp_path, clock)
    for _ in range(manager._manager_state._slo_config.min_sample_count):
        manager._manager_state.record_dispatch_latency("worker-dash", 40.0, clock.now)
    reader = ManagerDashboardReader(manager)
    env = dashboard_env(tmp_path)

    async with terminal_pipe() as collected:
        dashboard = NodeDashboard(reader, manager, "ci", env, tmp_path / "node.log", NodeDashboardConfig(), Logger())
        await dashboard.start()
        try:
            await wait_for_frame(collected, "(dispatch p95 ms) ^")
            clock.now += 2 * env.SLO_EVALUATION_WINDOW_SECONDS
            frame = await wait_for_frame(collected, "dispatch p95 ms: no value yet")
        finally:
            await dashboard.stop()

    assert "(dispatch p95 ms) ^" not in frame
    assert dashboard_tasks() == []
