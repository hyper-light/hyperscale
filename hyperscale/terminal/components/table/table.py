import asyncio
import math
import time
from typing import Any

from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions

from .table_config import HeaderOptions, TableConfig
from .tabulate import tabulate


class Table:
    def __init__(
        self,
        config: TableConfig,
    ):
        self.config = config
        self._mode = TerminalMode.to_mode(self.config.terminal_mode)
        self._header_keys = list(config.headers.keys())
        self.fit_type = WidgetFitDimensions.X_Y_AXIS

        self._max_height = 0
        self._max_width = 0

        self._max_rows = 0
        self._column_width = 0
        self._columns = len(self._header_keys)

        self.data: list[list[Any]] = []
        self._update_lock: asyncio.Lock | None = None
        self.offset = 0
        self._start: float | None = None
        self._elapsed: float = 0

    @property
    def raw_size(self):
        return self._max_width

    @property
    def size(self):
        return self._max_width

    async def fit(
        self,
        max_width: int | None = None,
        max_height: int | None = None,
    ):
        self._column_width = int(math.floor(max_width / len(self._header_keys)))

        self._max_width = self._max_width
        self._max_height = max_height
        self._max_width = max_width

        height_adjustment = await self._get_height_adjustment()

        self._max_rows = max_height - height_adjustment

    async def update(
        self,
        data: list[dict[str, Any]],
    ):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        await self._update_lock.acquire()

        self.data = [
            [row.get(header) for header in self._header_keys][: self._columns]
            for row in data
        ]

        self._update_lock.release()

    async def get_next_frame(self):
        if self._start is None:
            self._start = time.monotonic()

        float_precision = self._get_field_precision("float")
        integer_precision = self._get_field_precision("integer")

        data = self._format_data()

        length_adjustment = await self._get_table_length_adjustment(
            data,
            float_precision,
            integer_precision,
        )

        table: str = await tabulate(
            data,
            headers=self._header_keys,
            missingval=self.config.null_value,
            tablefmt=self.config.table_format,
            floatfmt=float_precision,
            intfmt=integer_precision,
            numalign=self.config.number_alignment_type,
            stralign=self.config.string_alignment_type,
            maxcolwidths=self._column_width,
            maxheadercolwidths=self._column_width,
            header_color_map=self.config.header_color_map,
            values_color_map=self.config.data_color_map,
            table_color=self.config.table_color,
            terminal_mode=self._mode,
        )

        table_lines = [line.strip("\n") for line in table.split("\n")]

        for idx, table_line in enumerate(table_lines):
            table_lines[idx] = table_line + (" " * length_adjustment)

        self._elapsed = time.monotonic() - self._start

        return table_lines

        # return await asyncio.gather(
        #     *[
        #         stylize(
        #             line,
        #             color=self.config.table_color,
        #             mode=self._mode,
        #         )
        #         for line in table_lines
        #     ]
        # )

    def _format_data(self):
        data = self.data

        if len(data) < 1:
            return [
                [
                    self._get_default_by_type(self.config.headers[header])
                    for header in self._header_keys
                ]
            ]

        data_length = len(data)
        if (
            data_length > self._max_rows
            and self._elapsed > self.config.pagination_refresh_rate
        ):
            difference = data_length - self._max_rows
            self.offset = (self.offset + 1) % difference
            data = data[self.offset : self._max_rows + self.offset]
            self._start = time.monotonic()

        elif data_length > self._max_rows:
            data = data[self.offset : self._max_rows + self.offset]

        return data

    async def _get_height_adjustment(self):
        float_precision = self._get_field_precision("float")
        integer_precision = self._get_field_precision("integer")

        data = self._format_data()

        table: str = await tabulate(
            data,
            headers=self._header_keys,
            missingval=self.config.null_value,
            tablefmt=self.config.table_format,
            floatfmt=float_precision,
            intfmt=integer_precision,
            numalign=self.config.number_alignment_type,
            stralign=self.config.string_alignment_type,
            maxcolwidths=self._column_width,
            maxheadercolwidths=self._column_width,
        )

        return max(len(table.split("\n")) - 1, 0)

    async def _get_table_length_adjustment(
        self,
        data: list[list[Any]],
        float_precision: tuple[str, ...] | None,
        integer_precision: tuple[str, ...] | None,
    ):
        table: str = await tabulate(
            data,
            headers=self._header_keys,
            missingval=self.config.null_value,
            tablefmt=self.config.table_format,
            floatfmt=float_precision,
            intfmt=integer_precision,
            numalign=self.config.number_alignment_type,
            stralign=self.config.string_alignment_type,
            maxcolwidths=self._column_width,
            maxheadercolwidths=self._column_width,
        )

        table_lines = table.split("\n")
        length_adjustments: list[int] = []

        for table_line in table_lines:
            difference = 0
            if len(table_line) <= self._max_width:
                difference = self._max_width - len(table_line)

            length_adjustments.append(difference)

        return max(length_adjustments)

    def _get_default_by_type(self, header: HeaderOptions):
        if header.default:
            return header.default

        match header.field_type:
            case "string":
                return ""

            case "integer":
                return 0

            case "float":
                return 0.0

            case "bool":
                return ""

    def _get_field_precision(self, field_type: str):
        return tuple(
            [
                header.precision
                for header in self.config.headers.values()
                if header.field_type == field_type
            ]
        )

    async def stop(self):
        pass

    async def abort(self):
        pass
