"""
The terminal components a node dashboard updates live, held to what a live
panel needs: an updated MultilineText keeps its text between updates (it
reverted to the configured text on the next render), clips lines wider
than itself, and pads each row it shows by that row's own width; a Table
given no rows clears (an empty update was taken for "no update").
"""

from hyperscale.ui.components.multiline_text import MultilineText, MultilineTextConfig
from hyperscale.ui.components.table import Table, TableConfig
from hyperscale.ui.components.table.table_config import HeaderOptions

PANEL_WIDTH = 20
PANEL_HEIGHT = 3


async def fitted_panel(pagination_refresh_rate: float = 3) -> MultilineText:
    panel = MultilineText(
        "panel",
        MultilineTextConfig(
            text=["waiting"],
            horizontal_alignment="left",
            pagination_refresh_rate=pagination_refresh_rate,
        ),
    )
    await panel.fit(max_width=PANEL_WIDTH, max_height=PANEL_HEIGHT)
    # The first render shows the configured text, as a terminal's first
    # frame does.
    first_frames, _ = await panel.get_next_frame()
    assert "waiting" in first_frames[0]
    return panel


async def test_an_updated_panel_keeps_its_text_until_the_next_update() -> None:
    panel = await fitted_panel()

    await panel.update(["WORKERS 1"])
    updated_frames, rerendered = await panel.get_next_frame()
    assert rerendered and "WORKERS 1" in updated_frames[0]

    for _ in range(3):
        frames, _ = await panel.get_next_frame()
        assert "WORKERS 1" in frames[0], f"the panel lost its update: {frames}"


async def test_a_panel_clips_lines_wider_than_itself() -> None:
    panel = await fitted_panel()
    await panel.update(["x" * (PANEL_WIDTH * 2), "short"])
    frames, _ = await panel.get_next_frame()

    assert [len(frame) for frame in frames] == [PANEL_WIDTH, PANEL_WIDTH]


async def test_a_paged_panel_pads_every_row_it_shows_to_its_width() -> None:
    # Five rows of different widths in a three-row panel: once it pages,
    # each shown row is still exactly the panel's width.
    panel = await fitted_panel(pagination_refresh_rate=0)
    await panel.update(["a", "bbbbbbbbbbbb", "cc", "dddddddd", "e"])

    for _ in range(4):
        frames, _ = await panel.get_next_frame()
        assert [len(frame) for frame in frames] == [PANEL_WIDTH] * PANEL_HEIGHT, frames


async def test_a_table_given_no_rows_clears() -> None:
    table = Table(
        "table",
        TableConfig(
            headers={"worker": HeaderOptions(default="none"), "cores": HeaderOptions(default=0)},
            minimum_column_width=8,
        ),
    )
    await table.fit(max_width=40, max_height=6)

    await table.update([{"worker": "10.0.0.7:8111", "cores": 4}])
    frames, _ = await table.get_next_frame()
    assert any("10.0.0.7:8111" in frame for frame in frames)

    await table.update([])
    frames, rerendered = await table.get_next_frame()
    assert rerendered
    assert not any("10.0.0.7:8111" in frame for frame in frames), frames
