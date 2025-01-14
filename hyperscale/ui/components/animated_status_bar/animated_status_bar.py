import asyncio
import math
import time
import random
from typing import Literal

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
from hyperscale.ui.styling.attributes import Attributizer
from hyperscale.ui.styling.colors import Colorizer, HighlightColorizer
from typing import Dict, List

from .animated_status_bar_config import (
    AnimatedStatusBarConfig,
    AnimationConfig,
    AnimationDirection,
    AnimationType,
)


StylingMap = Dict[
    str,
    Dict[
        Literal[
            "primary_color",
            "primary_highlight",
            "primary_attrs",
            "secondary_color",
            "secondary_highlight",
            "secondary_attrs",
        ],
        Colorizer | HighlightColorizer | List[Attributizer] | None,
    ],
]


AnimationMap = Dict[str, List[AnimationType]]


AnimationDirectionMap = Dict[str, AnimationDirection]


class AnimatedStatusBar:
    def __init__(
        self,
        name: str,
        config: AnimatedStatusBarConfig,
        subscriptions: list[str] | None = None,
    ):
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._current_status = config.default_status

        duration_string = f"{config.animation_duration}{config.animation_duration_unit}"
        self._duration_seconds = TimeParser(duration_string).time

        self._current_direction: AnimationDirection = config.animation_direction
        if self._current_direction == "bounce":
            self._current_direction = "forward"

        self._space_padding = 0
        self._additional_padding = 0
        if self._config.horizontal_padding:
            self._space_padding = min(self._config.horizontal_padding, 1)
            self._additional_padding = max(self._config.horizontal_padding - 1, 0)

        elif self._config.horizontal_padding:
            self._additional_padding = self._config.horizontal_padding

        self._active_idx: int = 0
        self._active_color_idx: int = 0
        self._inactive_color_idx: int = 0
        self._start: float | None = None
        self._elapsed: float = 0
        self._active_segment: Literal["active", "inactive"] = "active"
        self._precomputed_frames: dict[tuple[str, str, str, int], str] = {}

        animations = self._config.animations
        if animations is None:
            animations: AnimationConfig = {}

        self._status_styles: StylingMap = {
            status: {
                style_key: style_value
                for style_key, style_value in config.items()
                if style_key
                in [
                    "primary_color",
                    "primary_highlight",
                    "primary_attrs",
                    "secondary_color",
                    "secondary_highlight",
                    "secondary_attrs",
                ]
            }
            for status, config in animations.items()
        }

        self._animations: AnimationMap = {
            status: config.get("animations", [])
            for status, config in animations.items()
        }

        self._animation_directions: AnimationDirectionMap = {
            status: config.get("direction", "forward")
            for status, config in animations.items()
        }

        self._max_width: int = 0

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[str] | None = None

        self._last_frame: list[str] | None = None

        self._mode = TerminalMode.to_mode(self._config.terminal_mode)

    @property
    def raw_size(self):
        return self._max_width

    @property
    def size(self):
        return self._max_width

    async def fit(self, max_width: int | None = None):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        self._last_frame = None
        self._max_width = max_width
        self._updates.put_nowait(self._current_status)

        await self._precompute_status_styles()

    async def update(self, status: str):
        await self._update_lock.acquire()

        self._updates.put_nowait(status)

        self._update_lock.release()

    async def get_next_frame(self):
        status = await self._check_if_should_rerender()

        if self._start is None:
            self._start = time.monotonic()

        rerender = False
        render_fraction = self._duration_seconds / len(self._current_status)
        if "blink" in self._animations.get(self._current_status):
            render_fraction = self._duration_seconds / 2

        elapsed = time.monotonic() - self._start

        if status:
            frame = await self._rerender(status)

            self._current_status = status
            self._start = time.monotonic()

            self._last_frame = frame
            rerender = True

        elif self._last_frame is None:
            frame = await self._rerender(self._current_status)
            self._last_frame = frame
            self._start = time.monotonic()
            rerender = True

        elif elapsed >= render_fraction:
            self._start = time.monotonic()

            frame = await self._rerender(self._current_status)

            self._last_frame = frame
            rerender = True

        return [self._last_frame], rerender

    async def _rerender(
        self,
        status: str,
    ):
        if cached_frame := self._precomputed_frames.get(
            (status, self._current_direction, self._active_segment, self._active_idx)
        ):
            status_size = len(status) + (2 * self._additional_padding)
            self._active_idx = self._update_active_idx(status_size)

            self._update_current_bounce_direction()
            self._update_active_segment()

            return cached_frame

        status = (
            " " * self._additional_padding + status + " " * self._additional_padding
        )

        status_text_width = len(status)
        if status_text_width > self._max_width:
            status = self._trim_status_text(status)

        animations = self._animations.get(self._current_status)

        if "rotate" in animations:
            status_text = await self._apply_rotate_animation(status)

        elif "swipe" in animations:
            status_text = await self._apply_swipe_animation(status)

        elif "stripe" in animations:
            status_text = await self._apply_stripe_animation(status)

        elif "blink" in animations:
            status_text = await self._apply_blink_animation(status)

        elif "vegas" in animations:
            status_text = await self._apply_vegas_animation(status)

        elif "color" in animations or "highlight" in animations:
            status_text = await self._apply_color_or_highlight_animation(status)

        else:
            status_styles = self._status_styles.get(self._current_status, {})

            status_text = await stylize(
                status,
                color=get_style(
                    status_styles.get("primary_color"),
                    status,
                ),
                highlight=get_style(
                    status_styles.get("primary_highlight"),
                    status,
                ),
                attrs=[
                    get_style(
                        attr,
                        status,
                    )
                    for attr in status_styles.get("primary_attrs", [])
                ],
                mode=self._mode,
            )

        remainder = self._max_width - status_text_width - self._space_padding
        return self._pad_status_text_horizontal(status_text, remainder)

    async def _precompute_status_styles(self):
        for status, status_styles in self._status_styles.items():
            style_strings = [
                stylizer
                for stylizer in status_styles.values()
                if isinstance(stylizer, str)
            ]

            if len(style_strings) == len(status_styles):
                await self._generate_status_precomputed_frames(status)

    async def _generate_status_precomputed_frames(self, status: str):
        status_length = (self._additional_padding * 2) + len(status)
        animation_length = status_length * 2

        if "rotate" in self._animations.get(self._current_status):
            animation_length = status_length * 4

        for _ in range(animation_length):
            cache_key = (
                status,
                self._current_direction,
                self._active_segment,
                self._active_idx,
            )
            frame = await self._rerender(status)

            self._precomputed_frames[cache_key] = frame

        self._active_idx = 0
        self._current_direction = (
            "reverse"
            if self._animation_directions.get(self._current_status) == "reverse"
            else "forward"
        )

    async def _apply_blink_animation(
        self,
        status: str,
    ):
        status_length = len(status)
        status_styles = self._status_styles.get(self._current_status, {})

        if self._active_idx == 0:
            blink_styled_status = " " * status_length

        else:
            blink_styled_status = await stylize(
                status,
                color=get_style(
                    status_styles.get("primary_color"),
                    status,
                )
                if "color" in self._animations.get(self._current_status)
                else None,
                highlight=get_style(
                    status_styles.get("primary_highlight"),
                    status,
                )
                if "highlight" in self._animations.get(self._current_status)
                else None,
                attrs=[
                    get_style(
                        attr,
                        status,
                    )
                    for attr in status_styles.get("primary_attrs", [])
                ],
                mode=self._mode,
            )

        self._active_idx = self._update_active_idx(status_length)

        return self._pad_status_text_horizontal(
            blink_styled_status, self._space_padding
        )

    async def _apply_stripe_animation(
        self,
        status: str,
    ):
        status_length = len(status)
        status_styles = self._status_styles.get(self._current_status, {})

        if self._current_direction == "forward":
            striped_styled_status = await self._apply_forward_stripe_animation(
                status,
                status_length,
                status_styles,
            )

        else:
            striped_styled_status = await self._apply_reverse_stripe_animation(
                status,
                status_length,
                status_styles,
            )

        self._update_current_bounce_direction()

        return self._pad_status_text_horizontal(
            striped_styled_status, self._space_padding
        )

    async def _apply_forward_stripe_animation(
        self,
        status: str,
        status_length: str,
        status_styles: StylingMap,
    ):
        striped_styled_status = ""

        for idx, char in enumerate(status):
            if idx == self._active_idx - 1:
                striped_styled_status += await stylize(
                    char,
                    color=get_style(
                        status_styles.get("primary_color"),
                        status,
                    )
                    if "color" in self._animations.get(self._current_status)
                    else None,
                    highlight=get_style(
                        status_styles.get("primary_highlight"),
                        status,
                    )
                    if "highlight" in self._animations.get(self._current_status)
                    else None,
                    attrs=[
                        get_style(
                            attr,
                            status,
                        )
                        for attr in status_styles.get("primary_attrs", [])
                    ],
                    mode=self._mode,
                )

            else:
                striped_styled_status += " "

        self._active_idx = self._update_active_idx(status_length)

        return striped_styled_status

    async def _apply_reverse_stripe_animation(
        self,
        status: str,
        status_length: str,
        status_styles: StylingMap,
    ):
        striped_styled_status = ""

        active_idx = status_length - self._active_idx

        for idx, char in enumerate(status):
            if idx == active_idx:
                striped_styled_status += await stylize(
                    char,
                    color=get_style(
                        status_styles.get("primary_color"),
                        status,
                    )
                    if "color" in self._animations.get(self._current_status)
                    else None,
                    highlight=get_style(
                        status_styles.get("primary_highlight"),
                        status,
                    )
                    if "highlight" in self._animations.get(self._current_status)
                    else None,
                    attrs=[
                        get_style(
                            attr,
                            status,
                        )
                        for attr in status_styles.get("primary_attrs", [])
                    ],
                    mode=self._mode,
                )

            else:
                striped_styled_status += " "

        self._active_idx = self._update_active_idx(status_length)

        return striped_styled_status

    async def _apply_vegas_animation(
        self,
        status: str,
    ):
        status_length = len(status)

        status_idxs = list(range(status_length))
        active_idxs = [random.randrange(0, status_length) for _ in status_idxs]
        inactive_idxs = [idx for idx in status_idxs if idx not in active_idxs]

        status_styles = self._status_styles.get(self._current_status, {})

        vegas_styled_status = ""

        for idx in status_idxs:
            if idx in inactive_idxs:
                vegas_styled_status += " "

            else:
                vegas_styled_status += await stylize(
                    status[idx],
                    color=get_style(
                        status_styles.get("primary_color"),
                        status,
                    )
                    if "color" in self._animations.get(self._current_status)
                    else None,
                    highlight=get_style(
                        status_styles.get("primary_highlight"),
                        status,
                    )
                    if "highlight" in self._animations.get(self._current_status)
                    else None,
                    attrs=[
                        get_style(
                            attr,
                            status,
                        )
                        for attr in status_styles.get("primary_attrs", [])
                    ],
                    mode=self._mode,
                )

        return self._pad_status_text_horizontal(
            vegas_styled_status, self._space_padding
        )

    async def _apply_swipe_animation(self, status: str):
        status_length = len(status)

        if self._current_direction == "forward" and self._active_segment == "active":
            active_text = status[: self._active_idx]
            inactive_text = " " * len(status[self._active_idx :])

        elif (
            self._current_direction == "forward" and self._active_segment == "inactive"
        ):
            active_text = " " * len(status[: self._active_idx])
            inactive_text = status[self._active_idx :]

        elif self._current_direction == "reverse" and self._active_segment == "active":
            active_text = status[status_length - self._active_idx :]
            inactive_text = " " * len(status[: status_length - self._active_idx])

        else:
            active_text = " " * len(status[status_length - self._active_idx :])
            inactive_text = status[: status_length - self._active_idx]

        (active_text, inactive_text) = await self._apply_styling(
            active_text,
            inactive_text,
            status,
            status_length,
        )

        return self._create_status_string(
            status,
            active_text,
            inactive_text,
            status_length,
        )

    async def _apply_color_or_highlight_animation(self, status: str):
        status_length = len(status)

        if self._current_direction == "forward":
            active_text = status[: self._active_idx]
            inactive_text = status[self._active_idx :]

        else:
            active_text = status[status_length - self._active_idx :]
            inactive_text = status[: status_length - self._active_idx]

        (active_text, inactive_text) = await self._apply_styling(
            active_text, inactive_text, status, status_length
        )

        return self._create_status_string(
            status,
            active_text,
            inactive_text,
            status_length,
        )

    async def _apply_styling(
        self,
        active_text: str,
        inactive_text: str,
        status: str,
        status_length: int,
    ):
        status_styles = self._status_styles.get(self._current_status, {})
        if self._active_idx < status_length and self._active_segment == "active":
            active_text = await stylize(
                active_text,
                color=get_style(
                    status_styles.get("primary_color"),
                    status,
                )
                if "color" in self._animations.get(self._current_status)
                else None,
                highlight=get_style(
                    status_styles.get("primary_highlight"),
                    status,
                )
                if "highlight" in self._animations.get(self._current_status)
                else None,
                attrs=[
                    get_style(
                        attr,
                        status,
                    )
                    for attr in status_styles.get("primary_attrs", [])
                ],
                mode=self._mode,
            )

            inactive_text = await stylize(
                inactive_text,
                color=get_style(
                    status_styles.get("secondary_color"),
                    status,
                )
                if "color" in self._animations.get(self._current_status)
                else None,
                highlight=get_style(
                    status_styles.get("secondary_highight"),
                    status,
                )
                if "highlight" in self._animations.get(self._current_status)
                else None,
                attrs=[
                    get_style(
                        attr,
                        status,
                    )
                    for attr in status_styles.get("secondary_attrs", [])
                ],
                mode=self._mode,
            )

        elif self._active_idx < status_length and self._active_segment == "inactive":
            inactive_text = await stylize(
                inactive_text,
                color=get_style(
                    status_styles.get("primary_color"),
                    status,
                )
                if "color" in self._animations.get(self._current_status)
                else None,
                highlight=get_style(
                    status_styles.get("primary_highlight"),
                    status,
                )
                if "highlight" in self._animations.get(self._current_status)
                else None,
                attrs=[
                    get_style(
                        attr,
                        status,
                    )
                    for attr in status_styles.get("primary_attrs", [])
                ],
                mode=self._mode,
            )

            active_text = await stylize(
                active_text,
                color=get_style(
                    status_styles.get("secondary_color"),
                    status,
                )
                if "color" in self._animations.get(self._current_status)
                else None,
                highlight=get_style(
                    status_styles.get("secondary_highlight"),
                    status,
                )
                if "highlight" in self._animations.get(self._current_status)
                else None,
                attrs=[
                    get_style(
                        attr,
                        status,
                    )
                    for attr in status_styles.get("secondary_attrs", [])
                ],
                mode=self._mode,
            )

        return (
            active_text,
            inactive_text,
        )

    def _create_status_string(
        self,
        status: str,
        active_text: str,
        inactive_text: str,
        status_length: int,
    ):
        self._active_idx = self._update_active_idx(status_length)

        if self._current_direction == "forward":
            status = active_text + inactive_text

        else:
            status = inactive_text + active_text

        if (
            self._active_idx == 0
            and self._animation_directions.get(self._current_status) == "bounce"
            and self._active_segment == "inactive"
        ):
            self._current_direction = (
                "reverse" if self._current_direction == "forward" else "forward"
            )

        self._update_active_segment()

        return self._pad_status_text_horizontal(status, self._space_padding)

    def _update_active_segment(self):
        if self._active_idx == 0 and self._active_segment == "active":
            self._active_segment = "inactive"

        elif self._active_idx == 0 and self._active_segment == "inactive":
            self._active_segment = "active"

    async def _apply_rotate_animation(
        self,
        status: str,
    ) -> str:
        chunks = ("", status)

        match self._animation_directions.get(self._current_status):
            case "forward":
                chunks = self._get_forward_rotation_animation_chunks(status)

            case "reverse":
                chunks = self._get_reverse_rotation_animation_chunks(status)

            case "bounce":
                chunks = self._get_bounce_rotation_animation_chunks(status)

            case _:
                chunks = self._get_forward_rotation_animation_chunks(status)

        active_text, inactive_text = chunks
        has_coloring_animation = "highlight" in self._animations.get(
            self._current_status
        ) or "color" in self._animations.get(self._current_status)

        if (
            status_styles := self._status_styles.get(self._current_status)
        ) and has_coloring_animation:
            active_text = await stylize(
                active_text,
                color=get_style(
                    status_styles.get("primary_color"),
                    status,
                )
                if "color" in self._animations.get(self._current_status)
                else None,
                highlight=get_style(
                    status_styles.get("primary_highlight"),
                    status,
                )
                if "highlight" in self._animations.get(self._current_status)
                else None,
                attrs=[
                    get_style(
                        attr,
                        status,
                    )
                    for attr in status_styles.get("primary_attrs", [])
                ],
                mode=self._mode,
            )

            inactive_text = await stylize(
                inactive_text,
                color=get_style(
                    status_styles.get(
                        "primary_color",
                        status_styles.get(
                            "secondary_status_styles := self._status_styles.get(self._current_status)color"
                        ),
                    ),
                    status,
                )
                if "color" in self._animations.get(self._current_status)
                else None,
                highlight=get_style(
                    status_styles.get(
                        "primary_highlight", status_styles.get("secondary_highlight")
                    ),
                    status,
                )
                if "highlight" in self._animations.get(self._current_status)
                else None,
                attrs=[
                    get_style(
                        attr,
                        status,
                    )
                    for attr in status_styles.get(
                        "primary_attrs", status_styles.get("secondary_attrs", [])
                    )
                ],
                mode=self._mode,
            )

        if self._current_direction == "forward":
            return self._apply_rotation_padding(active_text, inactive_text)

        return self._apply_rotation_padding(inactive_text, active_text)

    def _apply_rotation_padding(
        self,
        active_text: str,
        inactive_text: str,
    ):
        if len(active_text) == 0:
            return (
                active_text
                + " " * self._space_padding
                + inactive_text
                + " " * self._space_padding
            )

        elif len(inactive_text) == 0:
            return (
                " " * self._space_padding
                + active_text
                + " " * self._space_padding
                + inactive_text
            )

        else:
            return (
                active_text
                + " " * self._space_padding
                + " " * self._space_padding
                + inactive_text
            )

    def _get_forward_rotation_animation_chunks(
        self,
        status: str,
    ):
        status_length = len(status)

        self._active_idx = self._update_active_idx(status_length)

        active_idx = self._active_idx * -1

        active_text = str(status[active_idx:])
        inactive_text = str(status[:active_idx])

        return (
            active_text,
            inactive_text,
        )

    def _get_reverse_rotation_animation_chunks(
        self,
        status: str,
    ):
        status_length = len(status)

        self._active_idx = self._update_active_idx(status_length)

        active_idx = self._active_idx

        return (
            status[:active_idx],
            status[active_idx:],
        )

    def _get_bounce_rotation_animation_chunks(
        self,
        status: str,
    ):
        if self._current_direction == "forward":
            chunks = self._get_forward_rotation_animation_chunks(status)

        else:
            chunks = self._get_reverse_rotation_animation_chunks(status)

        self._update_current_bounce_direction()
        self._update_active_segment()

        return chunks

    def _update_active_idx(self, status_length: int):
        if "blink" in self._animations.get(self._current_status):
            return (self._active_idx + 1) % 2

        return (self._active_idx + 1) % status_length

    def _update_current_bounce_direction(self):
        if (
            self._active_idx == 0
            and self._active_segment == "inactive"
            and self._animation_directions.get(self._current_status) == "bounce"
        ):
            self._current_direction = (
                "reverse" if self._current_direction == "forward" else "forward"
            )

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

    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        status: str | None = None
        if self._updates.empty() is False:
            status = await self._updates.get()

        self._update_lock.release()

        return status

    def _trim_status_text(
        self,
        status_text: str,
        status_text_width: int,
    ):
        difference = status_text_width - self._max_width

        match self._config.horizontal_alignment:
            case "left":
                return status_text[difference:]

            case "center":
                left_adjust = math.ceil(difference / 2)
                right_adjust = math.floor(difference / 2)

                return status_text[left_adjust : status_text_width - right_adjust]

            case "right":
                return status_text[: status_text_width - right_adjust]

    def _pad_status_text_horizontal(
        self,
        status_text: str,
        remainder: int,
    ):
        left_pad = math.ceil(remainder / 2)
        right_pad = math.floor(remainder / 2)

        return " " * left_pad + status_text + " " * right_pad
