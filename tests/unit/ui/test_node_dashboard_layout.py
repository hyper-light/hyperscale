"""
The node dashboard's geometry: every section's rows add up to the canvas
at every height; the table takes only the rows it needs (its header and
rows, or its empty state's one line) and the chart every row left; on a
shorter canvas the chart gives up rows first, then the table (down to its
header and a row), then the tiles' second line, and only then is the chart
dropped. A dashboard whose table gains rows lays its sections out again in
the frame that shows them, and the frame still fits the terminal.
"""

import pathlib
import re

import pytest

from hyperscale.distributed.nodes import ManagerServer
from hyperscale.logging import Logger
from hyperscale.ui.components.meter import MeterReading
from hyperscale.ui.components.stat_tile import StatTileReading
from hyperscale.ui.components.status_badge import StatusBadgeReading
from hyperscale.ui.components.terminal.canvas import Canvas
from hyperscale.ui.components.terminal.terminal import canvas_size
from hyperscale.ui.node_dashboard import ManagerDashboardReader, NodeDashboard, NodeDashboardConfig
from hyperscale.ui.node_dashboard.models import NodeDashboardFrame, NodeDashboardLayout, TableRow
from hyperscale.ui.node_dashboard.node_dashboard import HORIZONTAL_PADDING, VERTICAL_PADDING, WIDTH_SHARE
from hyperscale.ui.node_dashboard.node_dashboard_rows import (
    CHART_MIN_ROWS,
    HEADER_ROWS,
    STATUS_ROWS,
    TABLE_MIN_ROWS,
    TILE_INLINE_ROWS,
    TILE_STACKED_ROWS,
    NodeDashboardRows,
    table_rows_needed,
)
from hyperscale.ui.node_dashboard.node_dashboard_sections import (
    BADGES_COMPONENT_NAME,
    CHART_COMPONENT_NAME,
    STATUS_COMPONENT_NAME,
    generate_node_dashboard_sections,
    node_dashboard_table_config,
    table_component_name,
    tile_component_name,
)
from tests.integration.cli.node_processes import reserve_port_blocks
from tests.integration.ui.node_dashboard_harness import dashboard_env, dashboard_tasks, terminal_pipe, wait_for_frame

ANSI_SEQUENCE = re.compile(r"\x1b\[[0-9;:?]*[A-Za-z]")
LAYOUT = ManagerDashboardReader.layout
# Canvas heights from too short for the dashboard to roomy.
CANVAS_HEIGHTS = range(10, 80)
TERMINAL_SIZES = ((120, 38), (120, 30), (100, 30))


def section_rows(rows: NodeDashboardRows, canvas_height: int) -> dict[str, int]:
    return {
        "header": rows.header_rows(canvas_height),
        "badges": rows.badge_rows(canvas_height),
        "tiles": rows.tile_rows(canvas_height),
        "chart": rows.chart_rows(canvas_height),
        "table": rows.table_rows(canvas_height),
        "status": rows.status_rows(canvas_height),
    }


def sized_rows(badge_lines: int, table_row_count: int) -> NodeDashboardRows:
    rows = NodeDashboardRows()
    rows.need(badge_lines, table_row_count)
    return rows


def test_the_sections_rows_add_up_to_every_canvas_that_fits() -> None:
    for badge_lines in (1, 2, 3):
        for table_row_count in (0, 1, 4, 30):
            rows = sized_rows(badge_lines, table_row_count)
            for canvas_height in CANVAS_HEIGHTS:
                if not rows.fits(canvas_height):
                    continue

                taken = section_rows(rows, canvas_height)
                assert sum(taken.values()) == canvas_height, (badge_lines, table_row_count, canvas_height, taken)
                assert taken["table"] >= min(table_rows_needed(table_row_count), TABLE_MIN_ROWS), taken
                assert taken["chart"] == 0 or taken["chart"] >= CHART_MIN_ROWS, taken


def test_the_table_takes_only_the_rows_it_needs_and_the_chart_the_rest() -> None:
    canvas_height = 36
    for table_row_count in (0, 1, 2, 5):
        rows = sized_rows(1, table_row_count)
        taken = section_rows(rows, canvas_height)
        # Its header and each row (or its empty state's line), and its rule.
        assert taken["table"] == max(table_row_count + 1, 1) + 1, taken
        assert taken["chart"] == canvas_height - HEADER_ROWS - 2 - TILE_STACKED_ROWS - taken["table"] - STATUS_ROWS


def test_a_shorter_canvas_shrinks_the_chart_then_the_table_then_the_tiles_then_drops_the_chart() -> None:
    rows = sized_rows(1, 6)
    stages: list[str] = []
    for canvas_height in range(60, 9, -1):
        taken = section_rows(rows, canvas_height)
        stage = (
            "chart shrinks" if taken["table"] == table_rows_needed(6) and taken["chart"] > CHART_MIN_ROWS
            else "table shrinks" if taken["chart"] == CHART_MIN_ROWS and taken["tiles"] == TILE_STACKED_ROWS
            else "tiles inline" if taken["chart"] == CHART_MIN_ROWS
            else "chart dropped" if taken["chart"] == 0 and rows.fits(canvas_height)
            else "too short"
        )
        if not stages or stages[-1] != stage:
            stages.append(stage)

        if stage == "tiles inline":
            assert taken["tiles"] == TILE_INLINE_ROWS and taken["table"] == TABLE_MIN_ROWS, taken

    assert stages == ["chart shrinks", "table shrinks", "tiles inline", "chart dropped", "too short"], stages


def test_need_reports_a_change_only_when_the_rows_change() -> None:
    rows = NodeDashboardRows()
    assert rows.need(1, 0) is False, "the first frame's need is the initial layout's"
    assert rows.need(1, 2) is True
    assert rows.need(1, 2) is False
    assert rows.need(2, 2) is True
    assert rows.need(2, 0) is True


async def laid_out_canvas(layout: NodeDashboardLayout, rows: NodeDashboardRows, columns: int, lines: int) -> Canvas:
    canvas = Canvas(
        generate_node_dashboard_sections(layout, node_dashboard_table_config(layout, "compatability"), "compatability", rows)
    )
    canvas_width, canvas_height = canvas_size(columns, lines, HORIZONTAL_PADDING, VERTICAL_PADDING, WIDTH_SHARE)
    await canvas.initialize(width=canvas_width, height=canvas_height)
    return canvas


async def test_every_section_spans_the_canvas_and_the_groups_are_set_apart_by_rules() -> None:
    for columns, lines in TERMINAL_SIZES:
        rows = sized_rows(1, 1)
        canvas = await laid_out_canvas(LAYOUT, rows, columns, lines)
        canvas_width, canvas_height = canvas_size(columns, lines, HORIZONTAL_PADDING, VERTICAL_PADDING, WIDTH_SHARE)
        for component_name in (BADGES_COMPONENT_NAME, CHART_COMPONENT_NAME, table_component_name(LAYOUT)):
            assert canvas.get_section(component_name).width == canvas_width, (columns, component_name)

        tile_widths = [canvas.get_section(tile_component_name(index)).width for index in range(len(LAYOUT.tile_labels))]
        assert sum(tile_widths) == canvas_width and max(tile_widths) - min(tile_widths) <= canvas_width % 4 + 1
        frame_lines = (await canvas.render()).replace("\r", "").split("\n")
        rule_lines = [line for line in frame_lines if set(line.strip()) == {"-"}]
        # Below the header, the badges, the tiles, the chart and the table:
        # five rules, each across the canvas; nothing boxed in.
        assert len(rule_lines) == 5 and all(len(line.strip()) == canvas_width for line in rule_lines), frame_lines
        assert not any("|" in line[:2] for line in frame_lines), "a section is boxed in"
        assert len(frame_lines) == canvas_height
        assert canvas.get_section(STATUS_COMPONENT_NAME).height == STATUS_ROWS


class GrowingTableReader:
    """A manager layout's reader whose table gains a row each sample."""

    layout: NodeDashboardLayout = LAYOUT

    def __init__(self, final_row_count: int) -> None:
        self._final_row_count = final_row_count
        self.reads = 0

    def read(self) -> NodeDashboardFrame:
        row_count = min(self.reads, self._final_row_count)
        self.reads += 1
        return NodeDashboardFrame(
            identity_lines=["MANAGER DC-DASH-50-9000", "tcp 127.0.0.1:9000", "udp 127.0.0.1:9001"],
            lifecycle_state="active",
            uptime_seconds=float(self.reads),
            cluster_lines=[],
            summary_lines=[],
            detail_lines=[],
            table_rows=[worker_row(index) for index in range(row_count)],
            chart_values=[0.0, float(row_count), float(row_count)],
            value_lines=[],
            sampled_at=float(self.reads),
            badges=[StatusBadgeReading("leader 127.0.0.1:9000", "ok")],
            tiles=[StatTileReading(value=f"{row_count} healthy") for _ in LAYOUT.tile_labels],
            chart_extra_reading="dispatch p95 -",
        )


def worker_row(index: int) -> TableRow:
    return {
        "worker": f"127.0.0.1:{9100 + index}",
        "state": StatusBadgeReading("healthy", "ok"),
        "cores": MeterReading(used=index, total=8, label=f"{index}/8"),
        "load": StatusBadgeReading("healthy", "ok"),
    }


@pytest.mark.parametrize("columns,lines", TERMINAL_SIZES)
async def test_a_table_that_gains_rows_is_laid_out_again_in_the_frame_that_shows_them(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, columns: int, lines: int
) -> None:
    monkeypatch.setenv("COLUMNS", str(columns))
    monkeypatch.setenv("LINES", str(lines))
    (manager_port,) = reserve_port_blocks([2])
    env = dashboard_env(tmp_path)
    manager = ManagerServer(host="127.0.0.1", tcp_port=manager_port, udp_port=manager_port + 1, env=env, dc_id="DC-DASH")
    async with terminal_pipe() as collected:
        dashboard = NodeDashboard(
            GrowingTableReader(3),
            manager,
            "ci",
            env,
            tmp_path / "manager.log",
            NodeDashboardConfig(sample_interval_seconds=0.05),
            Logger(),
        )
        await dashboard.start()
        try:
            frame = await wait_for_frame(collected, "127.0.0.1:9102", "3 healthy")
        finally:
            await dashboard.stop()

    assert dashboard_tasks() == []
    frame_lines = ANSI_SEQUENCE.sub("", frame).replace("\r", "").rstrip("\n").split("\n")
    _, canvas_height = canvas_size(columns, lines, HORIZONTAL_PADDING, VERTICAL_PADDING, WIDTH_SHARE)
    assert len(frame_lines) == canvas_height + 2 * VERTICAL_PADDING, "\n".join(frame_lines)
    assert max(map(len, frame_lines)) < columns, [repr(line) for line in frame_lines if len(line) >= columns]
    # The table: its header, its three rows and its rule, right above the
    # status line -- no blank rows between.
    header_index = next(index for index, line in enumerate(frame_lines) if line.split()[:2] == ["worker", "state"])
    assert [line.split()[0] for line in frame_lines[header_index + 1 : header_index + 4]] == [
        "127.0.0.1:9100",
        "127.0.0.1:9101",
        "127.0.0.1:9102",
    ]
    assert set(frame_lines[header_index + 4].strip()) == {"-"}
    assert "ctrl-c stops the node" in frame_lines[header_index + 5]
