import math

from hyperscale.terminal.components.link import Link
from hyperscale.terminal.components.progress_bar import Bar
from hyperscale.terminal.components.spinner import Spinner
from hyperscale.terminal.components.text import Text

from .alignment import Alignment


class Component:
    def __init__(
        self,
        component: Text | Spinner | Link | Bar,
        alignment: Alignment | None = None,
        horizontal_padding: int = 0,
        vertical_padding: int = 0,
    ) -> None:
        if alignment is None:
            alignment = Alignment()

        self.component = component
        self.alignment = alignment
        self._horizontal_position: int | None = None
        self._vertical_position: int | None = None
        self._frame_width: int = component.size

        self._frame_height: int = 0
        self._horizontal_padding = horizontal_padding
        self._vertical_padding = vertical_padding

    @property
    def raw_size(self):
        return self.component.raw_size

    @property
    def size(self):
        return self.component.size

    async def fit(
        self,
        max_size: int,
    ):
        await self.component.fit(max_size)

    def _set_position(
        self,
        section_width: int,
        section_height: int,
        section_horizontal_center: int,
        section_vertical_center: int,
    ):
        match self.alignment.horizontal:
            case "left":
                self._horizontal_position = 0 + self._horizontal_padding

            case "center":
                self._horizontal_position = max(
                    section_horizontal_center - math.ceil(self._frame_width / 2), 0
                )

            case "right":
                self._horizontal_position = max(
                    section_width - self._frame_width - self._horizontal_padding, 0
                )

            case _:
                self._horizontal_position = 0 + self._horizontal_padding

        match self.alignment.vertical:
            case "top":
                self._vertical_position = 0 + self._vertical_padding

            case "center":
                self._vertical_position = section_vertical_center

            case "bottom":
                self._vertical_position = (
                    section_height - self._frame_height - self._vertical_padding
                )

            case _:
                self._vertical_position = 0 + self._vertical_padding

    async def render(
        self,
        section_width: int,
        section_height: int,
        section_horizontal_center: int,
        section_vertical_center: int,
    ):
        frame = await self.component.get_next_frame()
        self._frame_width = self.component.size

        if self._horizontal_position is None and self._vertical_position is None:
            self._frame_height = len(frame.split("\n"))

            self._set_position(
                section_width,
                section_height,
                section_horizontal_center,
                section_vertical_center,
            )

        return (
            frame,
            self._vertical_position,
            self.component.raw_size,
            self.alignment.horizontal,
        )
