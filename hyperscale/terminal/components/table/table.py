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

        self._last_state: list[list[Any]] = []
        self._last_rendered_frames: list[str] = []

        self._updates: asyncio.Queue | None = None
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
        
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

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

        self._last_rendered_frames.clear()

        self._updates.put_nowait([])

    async def update(
        self,
        data: list[dict[str, Any]],
    ):
        await self._update_lock.acquire()

        self._updates.put_nowait(data)

        self._update_lock.release()
        
    async def get_next_frame(self):
        if self._start is None:
            self._start = time.monotonic()


        data = await self._check_if_should_rerender()

        rerender = False

        if data:
            table_lines = await self._rerender(data)
            self._last_state = data
            rerender = True

        elif self._elapsed > self.config.pagination_refresh_rate:
            table_lines = await self._rerender(self._last_state)
            self._start = time.monotonic()
            rerender = True

        else:
            table_lines = self._last_rendered_frames

        self._elapsed = time.monotonic() - self._start
    
        return table_lines, rerender
    
    async def _rerender(self, data: list[list[Any]]):

        height_adjustment = self._assembler.calculate_height_offset(data)
        data_rows = self._cycle_data_rows(data, height_adjustment)

        table_lines: list[str] = await self._assembler.create_table_lines(
            [header for header in self.config.headers.keys()],
            data_rows,
        )

        for idx, line in enumerate(table_lines):   
            column_size = self._column_width * self._columns_count
            difference = self._max_width - column_size

            if difference > 0 and self._width_adjust > 0:
                table_lines[idx] = line + self._width_adjust * " "
        
        self._last_rendered_frames = table_lines

        return table_lines

    def _cycle_data_rows(
        self,
        data: list[list[Any]],
        height_adjustment: int,
    ):

        data_length = len(data)
        adjusted_max_rows = max(self._max_height - height_adjustment, 1)

        if data_length < 1:
            return data

        if (
            data_length > adjusted_max_rows
            and self._elapsed > self.config.pagination_refresh_rate
        ):
            difference = data_length - adjusted_max_rows
            self.offset = (self.offset + 1) % difference
            data = data[self.offset : adjusted_max_rows + self.offset]

        elif data_length > adjusted_max_rows:
            data = data[self.offset : adjusted_max_rows + self.offset]

        return data
    
    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        data: list[list[Any]] | None = None
        
        if self._updates.empty() is False:
            update_data: list[dict[str, Any]] = await self._updates.get()

            data = [
                [row.get(header) for header in self._header_keys][: self._columns_count]
                for row in update_data
            ]

        self._update_lock.release()

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
