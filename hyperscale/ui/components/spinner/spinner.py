from __future__ import annotations

import asyncio
import itertools
from typing import (
    Any,
    Optional,
)

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
from .spinner_config import SpinnerConfig
from .spinner_factory import SpinnerFactory
from .spinner_status import SpinnerStatus


class Spinner:
    def __init__(
        self,
        name: str,
        config: SpinnerConfig,
        subscriptions: list[str] | None = None,
    ):
        self.fit_type = WidgetFitDimensions.X_AXIS
        self._name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        # Spinner
        factory = SpinnerFactory()
        spinner = factory.get(config.spinner)

        self._spinner_size = spinner.size
        self._frames = (
            spinner.frames[::-1] if config.reverse_spinner_direction else spinner.frames
        )
        self._cycle = itertools.cycle(self._frames)

        self._last_frame: Optional[str] = None
        self._base_size: int = 0
        self._max_width: int = 0

        self._update_lock: asyncio.Lock | None = None
        self._spinner_status = SpinnerStatus.READY

        self._mode = TerminalMode.to_mode(config.terminal_mode)

    @property
    def raw_size(self):
        return self._base_size

    @property
    def size(self):
        return self._base_size

    async def update(self, _: Any):
        pass

    async def fit(
        self,
        max_width: int | None = None,
    ):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        remaining_size = max_width

        remaining_size -= self._spinner_size
        if remaining_size <= 0 and self._text:
            self._text = ""

        elif self._text:
            self._text = self._text[:remaining_size]
            remaining_size -= len(self._text)

        self._base_size = self._spinner_size + len(self._text)
        self._max_width = max_width

    async def get_next_frame(self):
        if self._spinner_status == SpinnerStatus.READY:
            self._spinner_size = SpinnerStatus.ACTIVE

        if self._spinner_status in [SpinnerStatus.OK, SpinnerStatus.FAILED]:
            frame = self._create_last_frame()
            return [frame], True

        frame = await self._create_next_spin_frame()
        return [frame], True

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def stop(self):
        if self._update_lock.locked():
            self._update_lock.release()

        await self.ok()

    async def abort(self):
        if self._update_lock.locked():
            self._update_lock.release()

        await self.fail()

    async def ok(self):
        await self._update_lock.acquire()
        self._spinner_status = SpinnerStatus.OK
        self._update_lock.release()

    async def fail(self):
        await self._update_lock.acquire()
        self._spinner_size = SpinnerStatus.FAILED
        self._update_lock.release()

    async def _create_last_frame(self):
        """Stop spinner, compose last frame and 'freeze' it."""

        if self._spinner_size == SpinnerStatus.FAILED:
            return await stylize(
                self._config.fail_char,
                color=get_style(self._config.fail_color),
                highlight=get_style(self._config.fail_highlight),
                attrs=[get_style(attr) for attr in self._config.fail_attrbutes]
                if self._config.fail_attrbutes
                else None,
                mode=self._mode,
            )

        return await stylize(
            self._config.ok_char,
            color=get_style(self._config.ok_color),
            highlight=get_style(self._config.ok_highlight),
            attrs=[get_style(attr) for attr in self._config.ok_attributes]
            if self._config.ok_attributes
            else None,
            mode=self._mode,
        )

    async def _create_next_spin_frame(self):
        # Compose output
        spin_phase = next(self._cycle)

        return await stylize(
            spin_phase,
            color=get_style(self._config.color),
            highlight=get_style(self._config.highlight),
            attrs=[get_style(attr) for attr in self._config.attributes]
            if self._config.attributes
            else None,
            mode=self._mode,
        )
