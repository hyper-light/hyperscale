import asyncio
import itertools
import math
from typing import (
    Any,
    Dict,
    Literal,
)

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.styling import stylize, get_style
from hyperscale.ui.styling.colors import (
    ColorName,
    ExtendedColorName,
    Colorizer,
)

from .cell_alignment import CellAlignment, CellAlignmentMap, CellAlignmentType
from .charset_position_type import CharsetPositionType
from .table_border_lines import TableBorderCharset, TableBorderLines


HeaderColorMap = Dict[str, Colorizer]


DataColorMap = Dict[str, Colorizer]

TableBorderType = Literal[
    "simple",
    "plain",
    "grid",
    "simple_grid",
    "rounded_grid",
    "heavy_grid",
    "mixed_grid",
    "double_grid",
    "fancy_grid",
    "outline",
    "simple_outline",
    "rounded_outline",
    "heavy_outline",
    "mixed_outline",
    "double_outline",
    "fancy_outline",
    "github",
    "orgtbl",
    "jira",
    "presto",
    "pretty",
    "psql",
    "rst",
    "youtrack",
]


class TableAssembler:
    def __init__(
        self,
        border_type: TableBorderType,
        column_size: int,
        columns_count: int,
        max_width: int,
        header_alignment: CellAlignment = "LEFT",
        cell_alignment: CellAlignment = "LEFT",
        field_format_map: Dict[str, str] | None = None,
        field_default_map: Dict[str, Any] | None = None,
        header_color_map: HeaderColorMap | None = None,
        data_color_map: DataColorMap | None = None,
        border_color: ColorName | ExtendedColorName | None = None,
        terminal_mode: TerminalMode | None = None,
    ):
        self._border_type = border_type
        self._cell_alignment_map = CellAlignmentMap()
        self._header_alignment = self._cell_alignment_map.by_name(header_alignment)
        self._cell_alignment = self._cell_alignment_map.by_name(cell_alignment)
        self.columns_size = column_size
        self.columns_count = columns_count
        self.max_width = max_width

        if field_format_map is None:
            field_format_map = {}

        self._field_format_map = field_format_map

        if field_default_map is None:
            field_default_map = {}

        self._field_default_map = field_default_map

        self._header_color_map = header_color_map
        self._data_color_map = data_color_map
        self._border_color = border_color
        self._terminal_mode = terminal_mode

        self._row_size = columns_count * column_size
        self._charsets: Dict[
            TableBorderType,
            TableBorderLines,
        ] = {
            "simple": TableBorderLines(
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="",
                    fill_char="-",
                    separator_char=" ",
                    end_char="",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="",
                    separator_char=" ",
                    end_char="",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="",
                    separator_char=" ",
                    end_char="",
                ),
            ),
            "plain": TableBorderLines(
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="",
                    separator_char=" ",
                    end_char="",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="",
                    separator_char=" ",
                    end_char="",
                ),
            ),
            "grid": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="+",
                    fill_char="-",
                    separator_char="+",
                    end_char="+",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="+",
                    fill_char="=",
                    separator_char="+",
                    end_char="+",
                ),
                line_between_rows=TableBorderCharset(
                    charset_type="line_between_rows",
                    begin_char="+",
                    fill_char="-",
                    separator_char="+",
                    end_char="+",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="+",
                    fill_char="-",
                    separator_char="+",
                    end_char="+",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                padding=1,
            ),
            "simple_grid": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="┌",
                    fill_char="─",
                    separator_char="┬",
                    end_char="┐",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="├",
                    fill_char="─",
                    separator_char="┼",
                    end_char="┤",
                ),
                line_between_rows=TableBorderCharset(
                    charset_type="line_between_rows",
                    begin_char="├",
                    fill_char="─",
                    separator_char="┼",
                    end_char="┤",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="└",
                    fill_char="─",
                    separator_char="┴",
                    end_char="┘",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                padding=1,
            ),
            "rounded_grid": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="╭",
                    fill_char="─",
                    separator_char="┬",
                    end_char="╮",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="├",
                    fill_char="─",
                    separator_char="┼",
                    end_char="┤",
                ),
                line_between_rows=TableBorderCharset(
                    charset_type="line_between_rows",
                    begin_char="├",
                    fill_char="─",
                    separator_char="┼",
                    end_char="┤",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="╰",
                    fill_char="─",
                    separator_char="┴",
                    end_char="╯",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                padding=1,
            ),
            "heavy_grid": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="┏",
                    fill_char="━",
                    separator_char="┳",
                    end_char="┓",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="┣",
                    fill_char="━",
                    separator_char="╋",
                    end_char="┫",
                ),
                line_between_rows=TableBorderCharset(
                    charset_type="line_between_rows",
                    begin_char="┣",
                    fill_char="━",
                    separator_char="╋",
                    end_char="┫",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="┗",
                    fill_char="━",
                    separator_char="┻",
                    end_char="┛",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="┃",
                    separator_char="┃",
                    end_char="┃",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="┃",
                    separator_char="┃",
                    end_char="┃",
                ),
                padding=1,
            ),
            "mixed_grid": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="┍",
                    fill_char="━",
                    separator_char="┯",
                    end_char="┑",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="┝",
                    fill_char="━",
                    separator_char="┿",
                    end_char="┥",
                ),
                line_between_rows=TableBorderCharset(
                    charset_type="line_between_rows",
                    begin_char="┝",
                    fill_char="━",
                    separator_char="┿",
                    end_char="┥",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="┕",
                    fill_char="━",
                    separator_char="┷",
                    end_char="┙",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                padding=1,
            ),
            "double_grid": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="╔",
                    fill_char="═",
                    separator_char="╦",
                    end_char="╗",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="╠",
                    fill_char="═",
                    separator_char="╬",
                    end_char="╣",
                ),
                line_between_rows=TableBorderCharset(
                    charset_type="line_between_rows",
                    begin_char="╠",
                    fill_char="═",
                    separator_char="╬",
                    end_char="╣",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="╚",
                    fill_char="═",
                    separator_char="╩",
                    end_char="╝",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="║",
                    separator_char="║",
                    end_char="║",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="║",
                    separator_char="║",
                    end_char="║",
                ),
                padding=1,
            ),
            "fancy_grid": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="╒",
                    fill_char="═",
                    separator_char="╤",
                    end_char="╕",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="╞",
                    fill_char="═",
                    separator_char="╪",
                    end_char="╡",
                ),
                line_between_rows=TableBorderCharset(
                    charset_type="line_between_rows",
                    begin_char="├",
                    fill_char="─",
                    separator_char="┼",
                    end_char="┤",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="╘",
                    fill_char="═",
                    separator_char="╧",
                    end_char="╛",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                padding=1,
            ),
            "outline": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="+",
                    fill_char="-",
                    separator_char="+",
                    end_char="+",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="+",
                    fill_char="=",
                    separator_char="+",
                    end_char="+",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="+",
                    fill_char="-",
                    separator_char="+",
                    end_char="+",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                padding=1,
            ),
            "simple_outline": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="┌",
                    fill_char="─",
                    separator_char="┬",
                    end_char="┐",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="├",
                    fill_char="─",
                    separator_char="┼",
                    end_char="┤",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="└",
                    fill_char="─",
                    separator_char="┴",
                    end_char="┘",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                padding=1,
            ),
            "rounded_outline": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="╭",
                    fill_char="─",
                    separator_char="┬",
                    end_char="╮",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="├",
                    fill_char="─",
                    separator_char="┼",
                    end_char="┤",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="╰",
                    fill_char="─",
                    separator_char="┴",
                    end_char="╯",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                padding=1,
            ),
            "heavy_outline": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="┏",
                    fill_char="━",
                    separator_char="┳",
                    end_char="┓",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="┣",
                    fill_char="━",
                    separator_char="╋",
                    end_char="┫",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="┗",
                    fill_char="━",
                    separator_char="┻",
                    end_char="┛",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="┃",
                    separator_char="┃",
                    end_char="┃",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="┃",
                    separator_char="┃",
                    end_char="┃",
                ),
                padding=1,
            ),
            "mixed_outline": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="┍",
                    fill_char="━",
                    separator_char="┯",
                    end_char="┑",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="┝",
                    fill_char="━",
                    separator_char="┿",
                    end_char="┥",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="┕",
                    fill_char="━",
                    separator_char="┷",
                    end_char="┙",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                padding=1,
            ),
            "double_outline": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="╔",
                    fill_char="═",
                    separator_char="╦",
                    end_char="╗",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="╠",
                    fill_char="═",
                    separator_char="╬",
                    end_char="╣",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="╚",
                    fill_char="═",
                    separator_char="╩",
                    end_char="╝",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="║",
                    separator_char="║",
                    end_char="║",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="║",
                    separator_char="║",
                    end_char="║",
                ),
                padding=1,
            ),
            "fancy_outline": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="╒",
                    fill_char="═",
                    separator_char="╤",
                    end_char="╕",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="╞",
                    fill_char="═",
                    separator_char="╪",
                    end_char="╡",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="╘",
                    fill_char="═",
                    separator_char="╧",
                    end_char="╛",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="│",
                    separator_char="│",
                    end_char="│",
                ),
                padding=1,
            ),
            "github": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="|",
                    fill_char="-",
                    separator_char="|",
                    end_char="|",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="|",
                    fill_char="-",
                    separator_char="|",
                    end_char="|",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                padding=1,
                hide_lines=["line_above"],
            ),
            "orgtbl": TableBorderLines(
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="|",
                    fill_char="-",
                    separator_char="+",
                    end_char="|",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                padding=1,
            ),
            "jira": TableBorderLines(
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="||",
                    separator_char="||",
                    end_char="||",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                padding=1,
            ),
            "presto": TableBorderLines(
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="",
                    fill_char="-",
                    separator_char="+",
                    end_char="",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="",
                    separator_char="|",
                    end_char="",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="",
                    separator_char="|",
                    end_char="",
                ),
                padding=1,
            ),
            "pretty": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="+",
                    fill_char="-",
                    separator_char="+",
                    end_char="+",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="+",
                    fill_char="-",
                    separator_char="+",
                    end_char="+",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="+",
                    fill_char="-",
                    separator_char="+",
                    end_char="+",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                padding=1,
            ),
            "psql": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="+",
                    fill_char="-",
                    separator_char="+",
                    end_char="+",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="|",
                    fill_char="-",
                    separator_char="+",
                    end_char="|",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="+",
                    fill_char="-",
                    separator_char="+",
                    end_char="+",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="|",
                    separator_char="|",
                    end_char="|",
                ),
                padding=1,
            ),
            "rst": TableBorderLines(
                line_above=TableBorderCharset(
                    charset_type="line_above",
                    begin_char="",
                    fill_char="=",
                    separator_char="",
                    end_char="",
                ),
                line_below_header=TableBorderCharset(
                    charset_type="line_below_header",
                    begin_char="|",
                    fill_char="-",
                    separator_char="+",
                    end_char="|",
                ),
                line_below=TableBorderCharset(
                    charset_type="line_below",
                    begin_char="",
                    fill_char="=",
                    separator_char="",
                    end_char="",
                ),
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="",
                    separator_char=" ",
                    end_char="",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="",
                    separator_char=" ",
                    end_char="",
                ),
                padding=0,
            ),
            "youtrack": TableBorderLines(
                header_row=TableBorderCharset(
                    charset_type="header_row",
                    begin_char="||",
                    separator_char="||",
                    end_char="||",
                ),
                data_row=TableBorderCharset(
                    charset_type="data_row",
                    begin_char="| ",
                    separator_char=" | ",
                    end_char=" |",
                ),
                padding=1,
            ),
        }

        self._border_charset = self._charsets[border_type]

        self._height_offset = 0

    def calculate_height_offset(self, data: list[list[Any]]):
        spacer_lines_count = len(data) - 1

        height_offset = self._calculate_height_offset_for_line(
            self._border_charset.line_above
        )

        height_offset += self._calculate_height_offset_for_line(
            self._border_charset.line_below_header
        )

        height_offset += (
            self._calculate_height_offset_for_line(
                self._border_charset.line_between_rows
            )
            * spacer_lines_count
        )

        height_offset += self._calculate_height_offset_for_line(
            self._border_charset.line_below
        )

        return height_offset + 1

    async def create_table_lines(
        self,
        headers: list[str],
        data: list[list[Any]],
    ) -> list[str]:
        headers_count = len(headers)
        if headers_count < self.columns_count:
            self.columns_count = headers_count
            self.columns_size = math.floor(self.max_width / self.columns_count)

        lines: list[str] = []

        lines.append(await self._create_line_above())
        lines.append(await self._create_header_line(headers))
        lines.append(await self._create_line_below_header())

        if (
            self._border_charset.line_between_rows
            and self._border_charset.line_between_rows
            not in self._border_charset.hide_lines
        ):
            lines.extend(
                await self._create_data_and_spacer_lines(
                    headers,
                    data,
                )
            )

        else:
            lines.extend(
                await asyncio.gather(
                    *[
                        self._create_data_line(
                            headers,
                            row,
                        )
                        for row in data
                    ]
                )
            )

        lines.append(await self._create_line_below())

        return [line for line in lines if line]

    async def _create_data_and_spacer_lines(
        self,
        headers: list[str],
        data: list[list[Any]],
    ):
        data_and_spacer_lines: list[str] = []

        data_lines = await asyncio.gather(
            *[
                self._create_data_line(
                    headers,
                    row,
                )
                for row in data
            ]
        )

        data_lines_count = len(data_lines)

        data_row_spacer_lines = await asyncio.gather(
            *[self._create_line_between_rows() for _ in range(data_lines_count - 1)]
        )

        for data_line, spacer_line in itertools.zip_longest(
            data_lines,
            data_row_spacer_lines,
            fillvalue=None,
        ):
            data_and_spacer_lines.append(data_line)

            if spacer_line:
                data_row_spacer_lines.append(spacer_line)

        return data_row_spacer_lines

    async def _create_line_above(self) -> str | None:
        cells = await asyncio.gather(
            *[
                self._create_border_cell(
                    self._border_charset.line_above,
                    position_type=self._calculate_position_type(
                        idx,
                    ),
                )
                for idx in range(self.columns_count)
            ]
        )

        cells = [cell for cell in cells if cell is not None]

        if cells and len(cells) < 1:
            return None

        return "".join(cells)

    async def _create_line_below_header(self) -> str | None:
        cells = await asyncio.gather(
            *[
                self._create_border_cell(
                    self._border_charset.line_below_header,
                    position_type=self._calculate_position_type(
                        idx,
                    ),
                )
                for idx in range(self.columns_count)
            ]
        )

        cells = [cell for cell in cells if cell is not None]

        if cells and len(cells) < 1:
            return None

        return "".join(cells)

    async def _create_line_between_rows(self) -> str | None:
        cells = await asyncio.gather(
            *[
                self._create_border_cell(
                    self._border_charset.line_between_rows,
                    position_type=self._calculate_position_type(
                        idx,
                    ),
                )
                for idx in range(self.columns_count)
            ]
        )

        cells = [cell for cell in cells if cell is not None]

        if cells and len(cells) < 1:
            return None

        return "".join(cells)

    async def _create_line_below(self) -> str | None:
        cells = await asyncio.gather(
            *[
                self._create_border_cell(
                    self._border_charset.line_below,
                    position_type=self._calculate_position_type(
                        idx,
                    ),
                )
                for idx in range(self.columns_count)
            ]
        )

        cells = [cell for cell in cells if cell is not None]

        if cells and len(cells) < 1:
            return None

        return "".join(cells)

    async def _create_header_line(self, headers: list[str]) -> str | None:
        header_cells = await asyncio.gather(
            *[
                self._create_cell(
                    header,
                    self._border_charset.header_row,
                    position_type=self._calculate_position_type(
                        idx,
                    ),
                    header_key=header,
                    color_map=self._header_color_map,
                    is_header=True,
                )
                for idx, header in enumerate(headers[: self.columns_count])
            ]
        )

        return "".join(header_cells)

    async def _create_data_line(
        self,
        headers: list[str],
        row: list[Any],
    ) -> str | None:
        data_cells = await asyncio.gather(
            *[
                self._create_cell(
                    self._get_value_or_default(
                        headers[idx],
                        row,
                        idx,
                    ),
                    self._border_charset.data_row,
                    position_type=self._calculate_position_type(
                        idx,
                    ),
                    header_key=headers[idx],
                    color_map=self._data_color_map,
                )
                for idx in range(self.columns_count)
            ]
        )

        return "".join(data_cells)

    async def _create_border_cell(
        self,
        charset: TableBorderCharset | None,
        position_type: CharsetPositionType,
    ):
        if charset is None or (charset.charset_type in self._border_charset.hide_lines):
            return None

        border_length = self._calculate_border_length(
            charset,
            position_type,
        )

        fill_size = self.columns_size - border_length

        match position_type:
            case CharsetPositionType.START:
                cell = await self._format_border_cell(
                    fill_size,
                    charset.fill_char,
                    left_border_char=charset.begin_char,
                    right_border_char=charset.separator_char,
                )

            case CharsetPositionType.BETWEEN:
                cell = await self._format_border_cell(
                    fill_size,
                    charset.fill_char,
                    right_border_char=charset.separator_char,
                )

            case CharsetPositionType.END:
                cell = await self._format_border_cell(
                    fill_size,
                    charset.fill_char,
                    right_border_char=charset.end_char,
                )

        return cell

    async def _create_cell(
        self,
        data: Any,
        charset: TableBorderCharset,
        position_type: CharsetPositionType,
        header_key: str | None = None,
        color_map: HeaderColorMap | DataColorMap | None = None,
        is_header: bool = False,
    ):
        if data is None:
            data = self._field_default_map.get(header_key)

        converted_data = self._convert_cell_to_string(data, header_key)

        data_length = len(converted_data)
        border_length = self._calculate_border_length(
            charset,
            position_type,
        )

        (
            padding_left,
            padding_right,
            adjusted_data_length,
        ) = self._calculate_padding(
            data_length,
            border_length,
            is_header=is_header,
        )

        if adjusted_data_length > 0:
            converted_data = converted_data[:adjusted_data_length]

        cell: str = ""

        match position_type:
            case CharsetPositionType.START:
                cell = await self._format_cell(
                    data,
                    converted_data,
                    left_border_char=charset.begin_char,
                    right_border_char=charset.separator_char,
                    padding_left=padding_left,
                    padding_right=padding_right,
                    color_map_key=header_key,
                    color_map=color_map,
                )

            case CharsetPositionType.BETWEEN:
                cell = await self._format_cell(
                    data,
                    converted_data,
                    right_border_char=charset.separator_char,
                    padding_left=padding_left,
                    padding_right=padding_right,
                    color_map_key=header_key,
                    color_map=color_map,
                )

            case CharsetPositionType.END:
                cell = await self._format_cell(
                    data,
                    converted_data,
                    right_border_char=charset.end_char,
                    padding_left=padding_left,
                    padding_right=padding_right,
                    color_map_key=header_key,
                    color_map=color_map,
                )

        return cell

    def _convert_cell_to_string(
        self,
        data: Any,
        header_key: str,
    ) -> str:
        if data is None:
            return "None"

        elif isinstance(data, bytes):
            return data.decode()

        elif isinstance(data, int):
            return self._convert_numeric_type_to_string(
                data,
                header_key,
            )

        elif isinstance(data, float):
            return self._convert_numeric_type_to_string(
                data,
                header_key,
            )

        return data

    def _convert_numeric_type_to_string(
        self,
        data: int | float,
        header_key: str | None,
    ):
        field_format = self._field_format_map.get(header_key)
        if field_format:
            return format(data, field_format)

        return f"{data}"

    def _calculate_border_line_borders_length(
        self,
        charset: TableBorderCharset,
    ):
        inbetween_cells = self.columns_count - 2

        start_cell_border_length = self._calculate_start_border_length(charset)

        inbetween_cell_border_length = (
            self._calculate_inbetween_border_length(charset) * inbetween_cells
        )

        end_cell_border_length = self._calculate_end_border_length(charset)

        return (
            start_cell_border_length,
            inbetween_cell_border_length,
            end_cell_border_length,
        )

    def _calculate_border_length(
        self,
        charset: TableBorderCharset,
        position_type: CharsetPositionType,
    ):
        match position_type:
            case CharsetPositionType.START:
                return self._calculate_start_border_length(
                    charset,
                )

            case CharsetPositionType.BETWEEN:
                return self._calculate_inbetween_border_length(
                    charset,
                )

            case CharsetPositionType.END:
                return self._calculate_end_border_length(
                    charset,
                )

            case _:
                raise Exception("Err. Invalid position type.")

    def _calculate_padding(
        self,
        header_length: int,
        border_length: int,
        is_header: bool = False,
    ):
        padding_total = self.columns_size - header_length - border_length
        adjusted_header_length = 0

        if padding_total < 0:
            difference = abs(padding_total)
            adjusted_header_length = header_length - difference

            padding_total = self.columns_size - adjusted_header_length - border_length

        padding_left = 0
        padding_right = 0

        alignment = self._cell_alignment
        if is_header:
            alignment = self._header_alignment

        match alignment:
            case CellAlignmentType.LEFT:
                padding_right = padding_total

            case CellAlignmentType.CENTER:
                padding_left = int(math.ceil(padding_total / 2))
                padding_right = int(math.floor(padding_total / 2))

            case CellAlignmentType.RIGHT:
                padding_left = padding_total

        return (
            padding_left,
            padding_right,
            adjusted_header_length,
        )

    async def _format_border_cell(
        self,
        fill_size: int,
        fill_char: str,
        left_border_char: str | None = None,
        right_border_char: str | None = None,
    ):
        cell: str = ""

        if left_border_char:
            cell += left_border_char

        cell += fill_size * fill_char

        if right_border_char:
            cell += right_border_char

        if self._border_color:
            cell = await stylize(
                cell,
                color=self._border_color,
                mode=self._terminal_mode,
            )

        return cell

    async def _format_cell(
        self,
        raw_value: Any,
        converted_data: str,
        left_border_char: str | None = None,
        right_border_char: str | None = None,
        padding_left: int = 0,
        padding_right: int = 0,
        color_map_key: str | None = None,
        color_map: HeaderColorMap | DataColorMap | None = None,
    ):
        cell: str = ""

        if left_border_char and self._border_color:
            cell += await stylize(
                left_border_char,
                color=self._border_color,
                mode=self._terminal_mode,
            )

        elif left_border_char:
            cell += left_border_char

        if color_map and color_map_key:
            converted_data = await self._colorize_data(
                raw_value,
                converted_data,
                color_map,
                color_map_key,
            )

        cell += padding_left * " " + converted_data + padding_right * " "

        if right_border_char and self._border_color:
            cell += await stylize(
                right_border_char,
                color=self._border_color,
                mode=self._terminal_mode,
            )

        elif right_border_char:
            cell += right_border_char

        return cell

    async def _colorize_data(
        self,
        raw_value: Any,
        data: str,
        color_map: HeaderColorMap | DataColorMap,
        color_key: str,
    ):
        colorizer = color_map.get(color_key)
        if colorizer is None:
            return data

        return await stylize(
            data, color=get_style(colorizer, raw_value), mode=self._terminal_mode
        )

    def _calculate_start_border_length(self, charset: TableBorderCharset):
        if charset.begin_char and charset.separator_char:
            return len(charset.begin_char) + len(charset.separator_char)

        elif charset.begin_char:
            return len(charset.begin_char)

        elif charset.separator_char:
            return len(charset.separator_char)

        else:
            return 0

    def _calculate_inbetween_border_length(self, charset: TableBorderCharset):
        if charset.separator_char is None:
            return 0

        return len(charset.separator_char)

    def _calculate_end_border_length(self, charset: TableBorderCharset):
        if charset.end_char is None:
            return 0

        return len(charset.end_char)

    def _calculate_position_type(self, idx: int):
        last_idx = self.columns_count - 1

        if idx == 0:
            return CharsetPositionType.START

        elif idx == last_idx:
            return CharsetPositionType.END

        else:
            return CharsetPositionType.BETWEEN

    def _calculate_height_offset_for_line(self, charset: TableBorderCharset):
        if charset and charset.charset_type not in self._border_charset.hide_lines:
            return 1

        return 0

    def _get_value_or_default(
        self,
        header: str,
        row: list[Any],
        idx: int,
    ):
        row_length = len(row)

        if idx >= row_length:
            return self._field_default_map.get(header)

        return row[idx]
