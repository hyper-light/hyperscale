"""
The node dashboards render as `run workflow`'s UI does: the Hyperscale
header with the node's identity, the role's panels, the role's one chart
of its series in a shared unit (a legend naming each) beside each series'
newest value and the role's values in other units, and the role's table --
every section sized from the canvas so the whole fits the terminal.

Each test builds a real node (constructed, never started) on a clock the
test moves by hand, puts known state into it -- a registered worker,
dispatches and their round trips, jobs, datacenter heartbeats -- and reads
it twice through the role's reader: once directly, asserting each series'
exact value and the other values' lines, and once through a "ci" dashboard
rendering into a pipe that stands in for the terminal, asserting the
header, the legend, every reading, the top of the chart's value axis (the
scatter plot scales it to 1.1 times the largest value of any series,
rounded up) and the table rows.

The clock stands still while the dashboard renders: the rates the first
sample computes (over the interval the test advanced) are the only rate
points, and every later sample at the same instant adds none. So each test
gives the dashboard a reader of its own, constructed with the one it reads
directly (a reader's rates are over the interval since its own last
sample).
"""

import asyncio
import itertools
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
from hyperscale.ui.components.scatter_plot import PlotConfig, PlotSeries, ScatterPlot
from hyperscale.ui.components.scatter_plot.point_char import PointChar
from hyperscale.ui.components.terminal.canvas import Canvas
from hyperscale.ui.hyperscale_header import create_hyperscale_header
from hyperscale.ui.node_dashboard import (
    GateDashboardReader,
    ManagerDashboardReader,
    NodeDashboard,
    NodeDashboardConfig,
    NodeDashboardReader,
    WorkerDashboardReader,
)
from hyperscale.ui.node_dashboard.models import NodeDashboardLayout
from hyperscale.ui.node_dashboard.node_dashboard_chart_series import NodeDashboardChartSeries
from hyperscale.ui.node_dashboard.node_dashboard_rows import IDENTITY_LINE_COUNT, table_rows
from hyperscale.ui.node_dashboard.node_dashboard_sections import (
    CHART_COMPONENT_NAME,
    IDENTITY_COMPONENT_NAME,
    generate_node_dashboard_sections,
    node_dashboard_table_config,
)
from hyperscale.ui.components.terminal.terminal import canvas_size as terminal_canvas_size
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
# The terminal sizes the layout must fit without clipping: the smallest it
# is built for, a shorter one, and a roomy one.
TERMINAL_SIZES = ((120, 38), (120, 30), (120, 22), (160, 48))
# The padding the dashboard renders with (NodeDashboard).
HORIZONTAL_PADDING = 4
VERTICAL_PADDING = 1
LAYOUTS = (ManagerDashboardReader.layout, WorkerDashboardReader.layout, GateDashboardReader.layout)


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


def value_axis_labels(frame: str, title: str) -> list[str]:
    """The value-axis labels of the chart titled ``title``, top down (the
    rows without a label left out)."""
    lines = plain_lines(frame)
    # The plot centers a short title in its axis' label column.
    title_marker = re.compile(rf"\({re.escape(title)}\)\s*\^")
    title_line_indexes = [index for index, line in enumerate(lines) if title_marker.search(line)]
    assert len(title_line_indexes) == 1, f"no chart titled {title!r}:\n" + "\n".join(lines)
    title_line_index = title_line_indexes[0]
    title_column = title_marker.search(lines[title_line_index]).start()
    axis_lines = itertools.takewhile(
        lambda line: not line[title_column:].split("|", 1)[0].strip().startswith("-"), lines[title_line_index + 1 :]
    )
    return [label for line in axis_lines if (label := line[title_column:].split("|", 1)[0].strip())]


def assert_nice_value_axis(labels: list[str], largest_value: float) -> None:
    """The labels step evenly at a nice step (1, 2 or 5 times a power of
    ten) with just the decimals that tell them apart, from 0 to a top at
    least 1.1 times the largest value (plotille draws no point on an
    axis' maximum)."""
    values = [float(label) for label in labels]
    steps = {round(upper - lower, 9) for upper, lower in zip(values, values[1:])}
    assert len(steps) == 1, labels
    (step,) = steps
    mantissa = step / 10 ** math.floor(math.log10(step))
    assert round(mantissa, 9) in (1.0, 2.0, 5.0), labels
    decimals = max(-math.floor(math.log10(step)), 0)
    assert labels == [f"{value:.{decimals}f}" for value in values], labels
    assert values[-1] == 0.0 and values[0] >= largest_value * VALUE_AXIS_HEADROOM, labels


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


def legend(reader: NodeDashboardReader) -> str:
    """The chart's legend: each series' point character and title."""
    return "  ".join(f"{PointChar.by_name(chart.point_char)} {chart.title}" for chart in reader.layout.charts)


async def assert_header_chart_and_readings(
    frame: str,
    reader: NodeDashboardReader,
    readings: list[str],
    largest_value: float | None,
) -> None:
    """The frame shows the Hyperscale header, the chart's legend (and the
    nice value axis above ``largest_value``, where it plots any value), and
    every reading; where it plots any value, a nice value axis above
    ``largest_value``."""
    plain_frame = "\n".join(plain_lines(frame))
    for header_line in await header_lines():
        assert header_line in plain_frame, f"the Hyperscale header is missing {header_line!r}:\n{plain_frame}"

    assert legend(reader) in plain_frame, f"the legend is missing:\n{plain_frame}"
    for reading in readings:
        assert reading in plain_frame, f"the reading {reading!r} is missing:\n{plain_frame}"

    if largest_value is not None:
        assert_nice_value_axis(value_axis_labels(frame, reader.layout.chart_unit), largest_value)


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


async def test_the_manager_dashboard_charts_its_dispatches_and_lists_cores_and_latency(
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

    frame_read = reader.read()
    assert frame_read.chart_values == [4.0, 10.0, 50.0]
    assert frame_read.value_lines == ["cores in use % 75.0", "dispatch p95 ms 40.0"]

    frame = await render_dashboard(
        rendered_reader, manager, tmp_path, "(wf /s)", "dispatched 50.0", f"127.0.0.1:{worker_port}"
    )
    await assert_header_chart_and_readings(
        frame,
        reader,
        ["dispatched 50.0", "completed 10.0", "failed 4.0", "cores in use % 75.0", "dispatch p95 ms 40.0"],
        50.0,
    )
    worker_row = next(line for line in plain_lines(frame) if f"127.0.0.1:{worker_port}" in line)
    assert worker_row.split() == ["|", f"127.0.0.1:{worker_port}", "healthy", "8", "2", "healthy", "40.0", "|"]
    assert f"MANAGER {manager.node_id.short}" in frame


async def test_a_chart_with_no_value_is_not_plotted_as_zero(tmp_path: pathlib.Path) -> None:
    # No dispatch has had its round trip timed: the latency chart has no
    # point to plot (a gap, not a zero latency), its reading shows no value,
    # and the worker's p95 cell keeps its default.
    clock = HandClock()
    manager, worker_port = await registered_manager(tmp_path, clock)
    reader = ManagerDashboardReader(manager)
    rendered_reader = ManagerDashboardReader(manager)
    clock.now += SAMPLED_INTERVAL_SECONDS

    frame_read = reader.read()
    assert frame_read.chart_values == [0.0, 0.0, 0.0]
    assert frame_read.value_lines == ["cores in use % 75.0", "dispatch p95 ms -"]

    frame = await render_dashboard(
        rendered_reader,
        manager,
        tmp_path,
        "(wf /s)",
        "cores in use % 75.0",
        "dispatch p95 ms -",
        f"127.0.0.1:{worker_port}",
    )
    worker_row = next(line for line in plain_lines(frame) if f"127.0.0.1:{worker_port}" in line)
    assert worker_row.split()[-2] == "-"


async def test_the_worker_dashboard_charts_its_ended_workflows_and_lists_its_load(
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

    # Its clock has not moved since the reader began: no rate yet.
    frame_read = reader.read()
    assert frame_read.chart_values == [None, None]
    assert frame_read.value_lines == ["active workflows 1", "cores busy % 75.0"]

    frame = await render_dashboard(reader, worker, tmp_path, "(wf /s)", "cores busy % 75.0", "DashWorkflow")
    await assert_header_chart_and_readings(
        frame, reader, ["completed -", "failed -", "active workflows 1", "cores busy % 75.0"], None
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


async def test_the_gate_dashboard_charts_its_jobs_and_lists_its_datacenters(tmp_path: pathlib.Path) -> None:
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
    frame_read = reader.read()
    assert frame_read.chart_values == [0.5, 0.5, 2.0]
    assert frame_read.value_lines == ["DCs accepting 2", "worst DC p95 ms 90.0"]

    frame = await render_dashboard(
        rendered_reader, gate, tmp_path, "(jobs /s)", "worst DC p95 ms 90.0", "DC-EAST", "DC-WEST"
    )
    await assert_header_chart_and_readings(
        frame,
        reader,
        ["admitted 2.0", "completed 0.5", "failed 0.5", "DCs accepting 2", "worst DC p95 ms 90.0"],
        2.0,
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


def test_chart_series_hold_one_window_and_plot_seconds_since_its_oldest_sample() -> None:
    # A window of ten seconds sampled each second holds ten samples, however
    # long the node runs; each plots at its seconds since the oldest sample
    # in the window (as the run UI plots seconds since its run began), every
    # chart from the same sample, and a gap is left out.
    series = NodeDashboardChartSeries(window_seconds=10.0, sample_interval_seconds=1.0)
    for second in range(25):
        series.record(float(second), [float(second), None if second % 2 else float(second)])

    assert series.points(0) == [(float(place), float(second)) for place, second in zip(range(10), range(15, 25))]
    assert series.points(1) == [
        (float(place), float(second)) for place, second in zip(range(1, 10, 2), range(16, 25, 2))
    ]


def test_a_dashboard_started_moments_ago_spreads_its_samples_from_zero() -> None:
    # Three samples of a ten second window: they span 0 to 2 seconds, not
    # the window's last two seconds (which crams them into one column).
    series = NodeDashboardChartSeries(window_seconds=10.0, sample_interval_seconds=1.0)
    for second in range(100, 103):
        series.record(float(second), [float(second)])

    assert series.points(0) == [(0.0, 100.0), (1.0, 101.0), (2.0, 102.0)]


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
    value_row = next(line for line in lines if line.split("|", 1)[0].strip() in ("1", "1.0"))
    assert "\u25cf" in ANSI_SEQUENCE.sub("", value_row), (
        "the points at the largest value were not drawn:\n" + "\n".join(lines)
    )


def canvas_size(columns: int, lines: int) -> tuple[int, int]:
    """The canvas a terminal of ``columns`` x ``lines`` gives the
    dashboard, as Terminal sizes it."""
    return terminal_canvas_size(columns, lines, HORIZONTAL_PADDING, VERTICAL_PADDING)


async def dashboard_canvas(layout: NodeDashboardLayout, columns: int, lines: int) -> Canvas:
    """``layout``'s dashboard laid out for a ``columns`` x ``lines``
    terminal, before any sample."""
    canvas = Canvas(
        generate_node_dashboard_sections(
            layout, node_dashboard_table_config(layout, "compatability"), "compatability"
        )
    )
    canvas_width, canvas_height = canvas_size(columns, lines)
    await canvas.initialize(width=canvas_width, height=canvas_height)
    return canvas


async def test_the_layout_fills_the_terminal_exactly_and_gives_the_table_what_is_left() -> None:
    # Every section takes its rows of the canvas: together they fill it
    # (nothing passes its bottom), the header row holds the identity column
    # unpaged, and the table takes every row the others leave.
    for layout in LAYOUTS:
        for columns, lines in TERMINAL_SIZES:
            canvas = await dashboard_canvas(layout, columns, lines)
            canvas_width, canvas_height = canvas_size(columns, lines)
            frame_lines = plain_lines((await canvas.render()).replace("\r", ""))
            assert len(frame_lines) == canvas_height, (layout.role, columns, lines, "\n".join(frame_lines))
            assert max(map(len, frame_lines)) == canvas_width
            assert canvas.get_section(IDENTITY_COMPONENT_NAME).height >= IDENTITY_LINE_COUNT
            assert canvas.get_section(f"node_dashboard_{layout.role}_table").height == table_rows(canvas_height)


async def test_a_resize_lays_the_sections_out_again_for_the_new_size() -> None:
    layout = ManagerDashboardReader.layout
    canvas = await dashboard_canvas(layout, 160, 48)
    for columns, lines in TERMINAL_SIZES:
        canvas_width, canvas_height = canvas_size(columns, lines)
        await canvas.initialize(width=canvas_width, height=canvas_height)
        frame_lines = plain_lines((await canvas.render()).replace("\r", ""))
        assert len(frame_lines) == canvas_height, (columns, lines)


async def test_the_chart_holds_every_series_of_the_role_with_a_legend() -> None:
    for layout in LAYOUTS:
        canvas = await dashboard_canvas(layout, 120, 38)
        chart_section = canvas.get_section(CHART_COMPONENT_NAME)
        assert chart_section.component_names == [CHART_COMPONENT_NAME]
        plot_lines, _ = await chart_section.component.get_next_frame()
        plain_plot = [ANSI_SEQUENCE.sub("", line) for line in plot_lines]
        assert plain_plot[0].strip() == "  ".join(
            f"{PointChar.by_name(chart.point_char)} {chart.title}" for chart in layout.charts
        )
        assert any(f"({layout.chart_unit})" in line and line.rstrip().endswith("^") for line in plain_plot)
        assert any(line.strip().startswith("Time (sec) |") for line in plain_plot)


def series_plot(series_names: tuple[str, ...]) -> ScatterPlot:
    return ScatterPlot(
        "series_plot",
        PlotConfig(
            plot_name="per second",
            x_axis_name="Time (sec)",
            y_axis_name="per second",
            series=[
                PlotSeries(name=series_name, point_char=point_char)
                for series_name, point_char in zip(series_names, ("dot", "x", "circle_toggle"))
            ],
        ),
    )


async def test_a_multi_series_plot_scales_to_every_series_and_the_later_series_wins_a_shared_cell() -> None:
    plot = series_plot(("low", "high"))
    await plot.fit(max_width=40, max_height=12)
    await plot.get_next_frame()
    await plot.update({"low": [(1.0, 1.0), (5.0, 2.0)], "high": [(5.0, 2.0), (3.0, 40.0)]})
    lines, rendered = await plot.get_next_frame()
    plain_plot = [ANSI_SEQUENCE.sub("", line) for line in lines]

    assert rendered
    assert plain_plot[0].strip() == f"{PointChar.by_name('dot')} low  {PointChar.by_name('x')} high"
    # The value axis covers the higher series: a nice top above 1.1 x 40.
    assert_nice_value_axis(value_axis_labels("\n".join(plain_plot), "per second"), 40.0)
    plotted = "".join(plain_plot[3:])
    # (5, 2) is in both series: the later one ("high", X) wins its cell,
    # so "low" shows one point and "high" two.
    assert plotted.count(PointChar.by_name("dot")) == 1, "\n".join(plain_plot)
    assert plotted.count(PointChar.by_name("x")) == 2, "\n".join(plain_plot)
    assert all(len(line) == 40 for line in plain_plot), "\n".join(plain_plot)


async def test_a_multi_series_plot_with_no_points_draws_its_axes_and_legend() -> None:
    plot = series_plot(("dispatched", "completed", "failed"))
    await plot.fit(max_width=40, max_height=12)
    lines, rendered = await plot.get_next_frame()
    plain_plot = [ANSI_SEQUENCE.sub("", line) for line in lines]

    assert rendered
    assert "dispatched" in plain_plot[0] and "failed" in plain_plot[0]
    assert any("(per second) ^" in line for line in plain_plot), "\n".join(plain_plot)
    assert any(line.strip().startswith("Time (sec) |") for line in plain_plot)
    assert len(plain_plot) <= 12
