import asyncio
from hyperscale.core_rewrite.graph import Workflow
from hyperscale.ui.components.terminal import Terminal, Section
from .generate_ui_sections import generate_ui_sections


class HyperscaleInterface:
    def __init__(
        self,
        padding: tuple[int, int] | None = None
    ):
        
        if padding is None:
            padding = (4, 1)


        self._terminal: Terminal | None = None

        horizontal_padding, vertical_padding = padding

        self._horizontal_padding = horizontal_padding
        self._vertical_padding = vertical_padding

        self._terminal_task: asyncio.Task | None = None

    def initialize(
        self,
        workflows: list[Workflow],
    ):
        sections: list[Section] = generate_ui_sections(workflows)
        self._terminal = Terminal(sections)

    def run(self):

        if self._terminal is None:
            raise Exception('Err. - Terminal not initialized.')

        if self._terminal_task is None:
            self._terminal_task = asyncio.ensure_future(
                self._terminal.render(
                    horizontal_padding=self._horizontal_padding,
                    vertical_padding=self._vertical_padding,
                )
            )

    async def stop(self):
        if self._terminal:
            await self._terminal.stop()
            await self._terminal_task

    async def abort(self):

        if self._terminal is None:
            return

        try:
            await self._terminal.abort()
        except Exception:
            pass

        if self._terminal_task is None:
            return
    
        try:
            self._terminal_task.cancel()
            self._terminal_task.set_result(None)

        except Exception:
            pass
    