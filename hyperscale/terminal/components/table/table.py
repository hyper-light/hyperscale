import asyncio
import math
import time
from typing import Any

from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions

from .table_config import TableConfig
from .tabulate import TableAssembler


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

        self._column_width = 0
        self._columns_count = len(self._header_keys)
        self._width_adjust = 0

        self.data: list[list[Any]] = []
        self._update_lock: asyncio.Lock | None = None
        self.offset = 0
        self._start: float | None = None
        self._elapsed: float = 0

        self._assembler: TableAssembler | None = None

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
        self._column_width = int(math.floor(max_width / self._columns_count))

        table_size = self._column_width * self._columns_count

        if table_size < max_width:
            self._width_adjust = max_width - table_size

        self._max_height = max_height
        self._max_width = max_width

        self._assembler = TableAssembler(
            self.config.table_format,
            self._column_width,
            self._columns_count,
            cell_alignment=self.config.cell_alignment,
            field_format_map={
                header: header_config.precision_format
                for header, header_config in self.config.headers.items()
                if header_config.precision_format is not None
            },
            field_default_map={
                header: header_config.default
                for header, header_config in self.config.headers.items()
            },
            header_color_map={
                header: header_config.header_color
                for header, header_config in self.config.headers.items()
                if header_config.header_color is not None
            },
            data_color_map={
                header: header_config.data_color
                for header, header_config in self.config.headers.items()
                if header_config.data_color is not None
            },
            border_color=self.config.border_color,
            terminal_mode=self._mode,
        )

    async def update(
        self,
        data: list[dict[str, Any]],
    ):
        self.data = [
            [row.get(header) for header in self._header_keys][: self._columns_count]
            for row in data
        ]

    async def get_next_frame(self):
        if self._start is None:
            self._start = time.monotonic()

        height_adjustment = self._assembler.calculate_height_offset(self.data)

        data_rows = self._cycle_data_rows(height_adjustment)

        table_lines: list[str] = await self._assembler.create_table_lines(
            [header for header in self.config.headers.keys()],
            data_rows,
        )

        for idx, line in enumerate(table_lines):   
            column_size = self._column_width * self._columns_count
            difference = self._max_width - column_size

            if difference > 0 and self._width_adjust > 0:
                table_lines[idx] = line + self._width_adjust * " "
            
        self._elapsed = time.monotonic() - self._start

        return table_lines

    def _cycle_data_rows(
        self,
        height_adjustment: int,
    ):
        data_length = len(self.data)
        adjusted_max_rows = max(self._max_height - height_adjustment, 1)

        data = self.data

        if data_length < 1:
            return data

        if (
            data_length > adjusted_max_rows
            and self._elapsed > self.config.pagination_refresh_rate
        ):
            difference = data_length - adjusted_max_rows
            self.offset = (self.offset + 1) % difference
            data = data[self.offset : adjusted_max_rows + self.offset]
            self._start = time.monotonic()

        elif data_length > adjusted_max_rows:
            data = data[self.offset : adjusted_max_rows + self.offset]

        return data

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def stop(self):
        if self._update_lock.locked():
            self._update_lock.release()

    async def abort(self):
        if self._update_lock.locked():
            self._update_lock.release()
