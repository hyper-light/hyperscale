import asyncio
from typing import Dict

from hyperscale.terminal.components.link import Link
from hyperscale.terminal.components.progress_bar import Bar
from hyperscale.terminal.components.spinner import Spinner
from hyperscale.terminal.components.text import Text
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions


class Composite:
    def __init__(
        self,
        components: Dict[
            str,
            Link,
            Bar,
            Spinner,
            Text,
        ],
        space: int = 0,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.components = components
        self._space = space

        self._size = sum(
            [component.size for component in components.values()]
        ) + space * len(components)

    @property
    def size(self):
        return self._size

    async def get_next_frame(self):
        join_char = ""

        if self._space > 0:
            join_char = " " * self._space

        return join_char.join(
            await asyncio.gather(
                *[component.get_next_frame() for component in self.components.values()]
            )
        )
