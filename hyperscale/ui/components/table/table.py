import asyncio
import math
import time
from typing import Any

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions

from .table_config import TableConfig
from .tabulate import TableAssembler


class Table:
    def __init__(
        self,
        name: str,
        config: TableConfig,
        subscriptions: list[str] | None = None,
    ):
        self.fit_type = WidgetFitDimensions.X_Y_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._mode = TerminalMode.to_mode(self._config.terminal_mode)
        self._header_keys = list(config.headers.keys())

        self._max_height = 0
        self._max_width = 0

        self._column_width = 0
        self._total_columns = len(self._header_keys)
        self._columns_count = self._total_columns

        self._fixed_header_positions = [
            (idx, header)
            for idx, (header, header_config) in enumerate(config.headers.items())
            if header_config.fixed
        ]

        self._header_rotable_status = [
            (header_name, header.fixed)
            for header_name, header in config.headers.items()
        ]

        self._fixed_headers = [header for _, header in self._fixed_header_positions]

        self._headers_rotate_count: int = 1
        self._fixed_headers_count = len(self._fixed_headers)
        self._use_header_rotation = False
        self._width_adjust = 0

        self._last_state: list[dict[str, Any]] = []
        self._last_rendered_frames: list[str] = []

        self._updates: asyncio.Queue | None = None
        self._update_lock: asyncio.Lock | None = None

        self.offset = 0
        self.header_offset = 0
        self._start: float | None = None

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

        columns_count = len(self._header_keys)
        column_width = int(math.floor(max_width / columns_count))

        header_rotation_enabled = self._config.minimum_column_width is not None
        minimum_column_width = (
            self._config.minimum_column_width
            if self._config.minimum_column_width
            else self._column_width
        )
        minimum_total_width = minimum_column_width * columns_count

        if header_rotation_enabled and max_width < minimum_total_width:
            max_headers = max(
                int(math.floor(max_width / self._config.minimum_column_width)), 1
            )

            columns_count = min(max_headers, columns_count)

            self._headers_rotate_count = max(
                columns_count - self._fixed_headers_count, 0
            )

            column_width = int(math.floor(max_width / columns_count))

            self._use_header_rotation = True

        table_size = column_width * columns_count

        self._width_adjust = max(max_width - table_size, 0)
        self._max_height = max_height
        self._max_width = max_width
        self._columns_count = columns_count
        self._column_width = column_width

        self._assembler = TableAssembler(
            self._config.table_format,
            column_width,
            columns_count,
            max_width,
            header_alignment=self._config.header_alignment,
            cell_alignment=self._config.cell_alignment,
            field_format_map={
                header: header_config.precision_format
                for header, header_config in self._config.headers.items()
                if header_config.precision_format is not None
            },
            field_default_map={
                header: header_config.default
                for header, header_config in self._config.headers.items()
            },
            header_color_map={
                header: header_config.header_color
                for header, header_config in self._config.headers.items()
                if header_config.header_color is not None
            },
            data_color_map={
                header: header_config.data_color
                for header, header_config in self._config.headers.items()
                if header_config.data_color is not None
            },
            border_color=self._config.border_color,
            terminal_mode=self._mode,
        )

        self._last_rendered_frames.clear()

        self._start = time.monotonic()

    async def update(
        self,
        data: list[dict[str, Any]],
    ):
        await self._update_lock.acquire()

        self._updates.put_nowait(data)

        if self._update_lock.locked():
            self._update_lock.release()

    async def get_next_frame(self):
        if self._start is None:
            self._start = time.monotonic()

        data = await self._check_if_should_rerender()

        rerender = False

        elapsed = time.monotonic() - self._start

        if (
            data
            and self._config.no_update_on_push
            and len(self._last_rendered_frames) > 0
        ):
            self._last_state = data

        elif data:
            table_lines = await self._rerender(data)
            self._last_rendered_frames = table_lines

            self._last_state = data
            rerender = True

        elif len(self._last_rendered_frames) < 1:
            table_lines = await self._rerender(self._last_state)
            self._last_rendered_frames = table_lines

        elif elapsed > self._config.pagination_refresh_rate:
            table_lines = await self._rerender(self._last_state)
            self._last_rendered_frames = table_lines

            self._start = time.monotonic()
            rerender = True

        return self._last_rendered_frames, rerender

    async def _rerender(self, data: list[dict[str, Any]]):
        current_headers = list(self._header_keys[: self._columns_count])

        if self._use_header_rotation:
            current_headers = self._cycle_headers(current_headers)

        data = [[row.get(header) for header in current_headers] for row in data]

        height_adjustment = self._assembler.calculate_height_offset(data)
        data_rows = self._cycle_data_rows(data, height_adjustment)

        table_lines: list[str] = await self._assembler.create_table_lines(
            current_headers,
            data_rows,
        )

        for idx, line in enumerate(table_lines):
            table_lines[idx] = line + self._width_adjust * " "

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

        elapsed = time.monotonic() - self._start

        if (
            data_length > adjusted_max_rows
            and elapsed > self._config.pagination_refresh_rate
        ):
            difference = data_length - adjusted_max_rows
            self.offset = (self.offset + 1) % (difference + 1)
            data = data[self.offset : adjusted_max_rows + self.offset]

        elif data_length > adjusted_max_rows:
            data = data[self.offset : adjusted_max_rows + self.offset]

        return data

    def _cycle_headers(self, current_headers: list[str]):
        rotated_headers = current_headers

        table_size = self._column_width * self._total_columns

        elapsed = time.monotonic() - self._start

        if (
            table_size > self._max_width
            and elapsed >= self._config.pagination_refresh_rate
        ):
            self.header_offset = (self.header_offset + 1) % self._headers_rotate_count

            rotated_headers = [
                header
                for header in self._header_keys
                if self._config.headers[header].fixed
            ]

            rotable_headers = [
                header
                for header in self._header_keys
                if self._config.headers[header].fixed is False
            ][
                self.header_offset : self._columns_count
                + self.header_offset
                - self._fixed_headers_count
            ]

            rotated_headers.extend(rotable_headers)

        elif table_size > self._max_width:
            rotated_headers = [
                header
                for header in self._header_keys
                if self._config.headers[header].fixed
            ]

            rotable_headers = [
                header
                for header in self._header_keys
                if self._config.headers[header].fixed is False
            ][
                self.header_offset : self._columns_count
                + self.header_offset
                - self._fixed_headers_count
            ]

            rotated_headers.extend(rotable_headers)

        return rotated_headers

    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        data: list[dict[str, Any]] | None = None

        if self._updates.empty() is False:
            data: list[dict[str, Any]] = await self._updates.get()

        if self._update_lock.locked():
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
