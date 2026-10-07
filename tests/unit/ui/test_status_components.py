"""
The status components a calm dashboard is built of, each rendered alone:

- Meter: a bar filled to the nearest eighth of a cell (extended) or whole
  cell (compatibility), clamped to empty and full, exactly as wide as it is
  given, its label after it;
- StatusBadge: a glyph of its tone's own shape and a label, laid out on one
  line and wrapped onto the next where the line runs out, never past the
  component's lines or width;
- StatTile: a dim label over a value, secondary counts only while nonzero,
  a meter taking the columns its text leaves, label and value on one line
  when it is given one;
- Table: one dim centered line in place of its header while it has no rows;
- Section: a single rule below it (``bottom_rule``) in the mode's glyph;
- ScatterPlot: a legend carrying each series' reading, an extra reading
  right-aligned where it fits.

Frames are compared without their color sequences: stdout is not a
terminal here, so nothing is colored.
"""

import re

from hyperscale.ui.components.meter import (
    METER_GLYPHS,
    Meter,
    MeterConfig,
    MeterReading,
    meter_bar,
    meter_text,
)
from hyperscale.ui.components.meter.meter_glyph_sets import COMPATIBILITY_METER_GLYPHS, EXTENDED_METER_GLYPHS
from hyperscale.ui.components.scatter_plot import PlotConfig, PlotSeries, ScatterPlot, SeriesUpdate
from hyperscale.ui.components.stat_tile import StatTile, StatTileConfig, StatTileReading, StatTileSecondary
from hyperscale.ui.components.status_badge import (
    StatusBadge,
    StatusBadgeConfig,
    StatusBadgeReading,
    badge_line_count,
    badge_text,
    tone_of_badge_text,
)
from hyperscale.ui.components.status_badge.flow_into_lines import flow_into_lines
from hyperscale.ui.components.status_badge.status_badge import BADGE_GAP
from hyperscale.ui.components.table import Table, TableConfig
from hyperscale.ui.components.table.table_config import HeaderOptions
from hyperscale.ui.components.terminal import Section, SectionConfig
from hyperscale.ui.components.text import Text, TextConfig
from hyperscale.ui.config.mode import TerminalMode

ANSI_SEQUENCE = re.compile(r"\x1b\[[0-9;:?]*[A-Za-z]")
FULL = EXTENDED_METER_GLYPHS.full
EMPTY = EXTENDED_METER_GLYPHS.empty
# The eighth-block glyphs, one eighth to seven eighths.
EIGHTHS = EXTENDED_METER_GLYPHS.cell_steps


def plain(lines: list[str]) -> list[str]:
    return [ANSI_SEQUENCE.sub("", line) for line in lines]


def test_a_meter_fills_to_the_nearest_eighth_of_a_cell() -> None:
    width = 4
    # Four cells hold 32 eighths: each ratio is a count of eighths.
    for filled_eighths in range(width * 8 + 1):
        filled, unfilled = meter_bar(filled_eighths / (width * 8), width, EXTENDED_METER_GLYPHS)
        full_cells, partial_eighths = divmod(filled_eighths, 8)
        assert filled == FULL * full_cells + EIGHTHS[partial_eighths], filled_eighths
        assert len(filled + unfilled) == width, filled_eighths

    # Half an eighth rounds up, just under it rounds down.
    assert meter_bar(1.5 / 32, width, EXTENDED_METER_GLYPHS)[0] == EIGHTHS[2]
    assert meter_bar(1.49 / 32, width, EXTENDED_METER_GLYPHS)[0] == EIGHTHS[1]


def test_a_meter_is_empty_at_zero_full_at_one_and_clamped_outside() -> None:
    for glyphs in (EXTENDED_METER_GLYPHS, COMPATIBILITY_METER_GLYPHS):
        assert meter_bar(0.0, 10, glyphs) == ("", glyphs.empty * 10)
        assert meter_bar(-3.0, 10, glyphs) == ("", glyphs.empty * 10)
        assert meter_bar(1.0, 10, glyphs) == (glyphs.full * 10, "")
        assert meter_bar(7.5, 10, glyphs) == (glyphs.full * 10, "")
        assert meter_bar(0.5, 0, glyphs) == ("", "")


def test_a_compatibility_meter_fills_whole_cells_in_ascii() -> None:
    assert meter_bar(0.5, 4, COMPATIBILITY_METER_GLYPHS) == ("##", "--")
    assert meter_bar(0.6, 4, COMPATIBILITY_METER_GLYPHS) == ("##", "--")
    assert meter_bar(0.65, 4, COMPATIBILITY_METER_GLYPHS) == ("###", "-")
    for ratio in (index / 97 for index in range(98)):
        filled, unfilled = meter_bar(ratio, 13, COMPATIBILITY_METER_GLYPHS)
        assert len(filled + unfilled) == 13 and (filled + unfilled).isascii()


def test_a_meter_takes_exactly_its_width_with_its_label_after_the_bar() -> None:
    reading = MeterReading(used=6, total=8, label="6/8")
    for width in range(0, 30):
        text = meter_text(reading, width, EXTENDED_METER_GLYPHS)
        assert text.endswith(" 6/8")
        assert len(text) == max(width, len(" 6/8")), (width, text)

    assert meter_text(reading, 12, COMPATIBILITY_METER_GLYPHS) == "######-- 6/8"
    # No total: an empty bar, never a division by zero.
    assert meter_text(MeterReading(used=3, total=0, label="3/0"), 8, COMPATIBILITY_METER_GLYPHS) == "---- 3/0"


async def test_the_meter_component_draws_its_newest_reading_at_its_width() -> None:
    meter = Meter("cores", MeterConfig(terminal_mode="extended"))
    await meter.fit(20)
    (line,), rendered = await meter.get_next_frame()
    assert rendered and plain([line]) == [EMPTY * 20]

    await meter.update(MeterReading(used=1, total=4, label="1/4"))
    await meter.update(MeterReading(used=3, total=4, label="3/4"))
    (line,), rendered = await meter.get_next_frame()
    assert rendered and plain([line]) == [FULL * 12 + EMPTY * 4 + " 3/4"]

    _, rendered = await meter.get_next_frame()
    assert not rendered, "an unchanged meter redrew"
    unlabelled = Meter("cores", MeterConfig(terminal_mode="compatability", show_label=False))
    await unlabelled.fit(8)
    await unlabelled.update(MeterReading(used=1, total=2, label="1/2"))
    assert plain((await unlabelled.get_next_frame())[0]) == ["####----"]


def test_each_tone_has_a_glyph_of_its_own_shape_and_a_badge_names_its_tone() -> None:
    for mode in (TerminalMode.EXTENDED, TerminalMode.COMPATIBILITY):
        texts = {tone: badge_text(StatusBadgeReading("load healthy", tone), mode) for tone in ("ok", "degraded", "failing")}
        assert len({text.split(" ", 1)[0] for text in texts.values()}) == 3, texts
        for tone, text in texts.items():
            assert text.endswith(" load healthy")
            assert tone_of_badge_text(text, mode) == tone

        assert tone_of_badge_text("healthy", mode) is None
        assert tone_of_badge_text("", mode) is None

    assert badge_text(StatusBadgeReading("swim 3 ok", "ok"), TerminalMode.COMPATIBILITY) == "+ swim 3 ok"
    assert badge_text(StatusBadgeReading("swim 3 ok, 1 suspect", "degraded"), TerminalMode.COMPATIBILITY)[0] == "~"
    assert badge_text(StatusBadgeReading("leader none", "failing"), TerminalMode.COMPATIBILITY)[0] == "!"


def test_badges_flow_left_to_right_and_wrap_where_the_line_runs_out() -> None:
    assert flow_into_lines([5, 5, 5], 19, 2) == [[0, 1, 2]]
    assert flow_into_lines([5, 5, 5], 18, 2) == [[0, 1], [2]]
    assert flow_into_lines([30, 2], 10, 2) == [[0], [1]]
    assert flow_into_lines([], 10, 2) == [[]]


async def test_a_badge_line_lays_out_its_badges_and_wraps_within_its_lines() -> None:
    badges = [
        StatusBadgeReading("leader 127.0.0.1:8231", "ok"),
        StatusBadgeReading("standalone 1/1 voters", "ok"),
        StatusBadgeReading("swim 0 ok", "ok"),
        StatusBadgeReading("load stressed, lhm 2", "degraded"),
    ]
    one_line_width = len(BADGE_GAP.join(badge_text(badge, TerminalMode.COMPATIBILITY) for badge in badges))
    for width, height, expected_lines in ((one_line_width, 1, 1), (one_line_width - 1, 2, 2), (one_line_width - 1, 1, 1)):
        line = StatusBadge("badges", StatusBadgeConfig(terminal_mode="compatability"))
        await line.fit(width, height)
        await line.update(badges)
        lines = plain((await line.get_next_frame())[0])
        assert len(lines) == expected_lines and all(len(text) == width for text in lines), (width, lines)
        assert lines[0].startswith("+ leader 127.0.0.1:8231" + BADGE_GAP + "+ standalone"), lines

    assert badge_line_count(badges, one_line_width, len(BADGE_GAP), TerminalMode.COMPATIBILITY) == 1
    assert badge_line_count(badges, one_line_width - 1, len(BADGE_GAP), TerminalMode.COMPATIBILITY) == 2

    # A badge wider than the whole line is cut to it, never past the edge.
    narrow = StatusBadge("badges", StatusBadgeConfig(terminal_mode="compatability"))
    await narrow.fit(10, 1)
    await narrow.update([StatusBadgeReading("leader 127.0.0.1:8231", "ok")])
    assert [len(text) for text in plain((await narrow.get_next_frame())[0])] == [10]


async def rendered_tile(reading: StatTileReading, width: int, height: int, mode: str = "compatability") -> list[str]:
    tile = StatTile("tile", StatTileConfig(label="JOBS", terminal_mode=mode))
    await tile.fit(width, height)
    await tile.update(reading)
    lines, rendered = await tile.get_next_frame()
    assert rendered
    return plain(lines)


async def test_a_tile_shows_secondary_counts_only_while_they_are_nonzero() -> None:
    quiet = StatTileReading(
        value="2 running",
        secondaries=(StatTileSecondary("0 failed", 0, "failing"), StatTileSecondary("0 queued", 0)),
    )
    busy = StatTileReading(
        value="2 running",
        secondaries=(StatTileSecondary("1 failed", 1, "failing"), StatTileSecondary("3 queued", 3)),
    )
    assert await rendered_tile(quiet, 30, 2) == ["JOBS".ljust(30), "2 running".ljust(30)]
    assert await rendered_tile(busy, 30, 2) == ["JOBS".ljust(30), "2 running, 1 failed, 3 queued".ljust(30)]
    # Secondaries that do not fit are left out from the last; the value stays.
    assert await rendered_tile(busy, 20, 2) == ["JOBS".ljust(20), "2 running, 1 failed".ljust(20)]
    assert (await rendered_tile(busy, 32, 2, "extended"))[1] == "2 running · 1 failed · 3 queued".ljust(32)


async def test_a_tile_given_one_line_draws_label_and_value_on_it() -> None:
    reading = StatTileReading(value="2 running", secondaries=(StatTileSecondary("1 failed", 1, "failing"),))
    assert await rendered_tile(reading, 30, 1) == ["JOBS 2 running, 1 failed".ljust(30)]


async def test_a_meter_tile_gives_its_meter_the_columns_its_text_leaves() -> None:
    meter_only = StatTileReading(value="", meter=MeterReading(used=6, total=8, label="6/8"))
    assert await rendered_tile(meter_only, 20, 2) == ["JOBS".ljust(20), "############---- 6/8"]

    with_text = StatTileReading(
        value="", meter=MeterReading(used=1, total=2, label="1/2"), secondaries=(StatTileSecondary("2 queued", 2),)
    )
    (_, value_line) = await rendered_tile(with_text, 30, 2)
    assert len(value_line) == 30 and value_line.endswith(" 1/2 2 queued"), value_line
    # The meter keeps at least half the line: text that would take more is
    # left out.
    crowded = StatTileReading(
        value="", meter=MeterReading(used=1, total=2, label="1/2"), secondaries=(StatTileSecondary("a" * 12, 1),)
    )
    (_, value_line) = await rendered_tile(crowded, 20, 2)
    assert value_line == "########-------- 1/2", value_line


async def test_a_table_with_no_rows_shows_its_message_on_one_dim_centered_line() -> None:
    table = Table(
        "workers",
        TableConfig(
            headers={"worker": HeaderOptions(default="none"), "cores": HeaderOptions(default=0)},
            table_format="plain",
            size_columns_to_content=True,
            empty_message="waiting for workers to register",
        ),
    )
    await table.fit(41, 5)
    lines = plain((await table.get_next_frame())[0])
    assert lines == ["     waiting for workers to register     "]

    await table.update([{"worker": "127.0.0.1:8331", "cores": 8}])
    lines = plain((await table.get_next_frame())[0])
    assert len(lines) == 2 and "worker" in lines[0] and "127.0.0.1:8331" in lines[1]

    await table.update([])
    assert plain((await table.get_next_frame())[0]) == ["     waiting for workers to register     "]


async def test_a_section_draws_a_single_rule_below_it_in_the_modes_glyph() -> None:
    for mode, glyph in (("extended", "─"), ("compatability", "-")):
        section = Section(
            SectionConfig(width="full", height_rows=lambda canvas_height: 3, bottom_rule=True, mode=mode),
            components=[Text("status", TextConfig(text="ready", horizontal_alignment="left", terminal_mode=mode))],
        )
        await section.resize(12, 10)
        await section.create_blocks()
        lines = plain(await section.render())
        assert lines == ["ready".ljust(12), " " * 12, glyph * 12], (mode, lines)


async def test_the_legend_carries_each_series_reading_and_the_extra_reading_right_aligned() -> None:
    plot = ScatterPlot(
        "plot",
        PlotConfig(
            plot_name="wf /s",
            x_axis_name="Time (sec)",
            y_axis_name="wf /s",
            series=[PlotSeries(name="failed", point_char="x"), PlotSeries(name="dispatched", point_char="dot")],
            legend_order=["dispatched", "failed"],
        ),
    )
    await plot.fit(max_width=60, max_height=10)
    await plot.update(
        SeriesUpdate(
            points={"dispatched": [(1.0, 59.9)], "failed": []},
            readings={"dispatched": "59.9/s", "failed": "0.0/s"},
            extra_reading="dispatch p95 50.0 ms",
        )
    )
    legend = plain((await plot.get_next_frame())[0])[0]
    assert legend == "● dispatched 59.9/s   X failed 0.0/s".ljust(40) + "dispatch p95 50.0 ms", legend

    # An extra reading kept at least the entries' separator apart from them
    # shows; one that would come closer, or does not fit, is left out.
    entries = "● dispatched 59.9/s   X failed 0.0/s"
    extra_reading = "dispatch p95 50.0 ms"
    for width, expected in (
        (len(entries) + 3 + len(extra_reading), entries + "   " + extra_reading),
        (len(entries) + 2 + len(extra_reading), entries.ljust(len(entries) + 2 + len(extra_reading))),
        (40, entries.ljust(40)),
    ):
        await plot.fit(max_width=width, max_height=10)
        await plot.update(
            SeriesUpdate(points={}, readings={"dispatched": "59.9/s", "failed": "0.0/s"}, extra_reading=extra_reading)
        )
        legend = plain((await plot.get_next_frame())[0])[0]
        assert legend == expected, (width, legend)
