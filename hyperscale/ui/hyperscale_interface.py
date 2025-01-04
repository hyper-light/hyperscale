import asyncio
import time
from hyperscale.core.graph import Workflow
from hyperscale.ui.components.terminal import Terminal, Section
from .generate_ui_sections import generate_ui_sections
from .hyperscale_interface_config import HyperscaleInterfaceConfig
from .interface_updates_controller import InterfaceUpdatesController


class HyperscaleInterface:
    def __init__(
        self,
        updates: InterfaceUpdatesController,
        config: HyperscaleInterfaceConfig | None = None,
        padding: tuple[int, int] | None = None
    ):
        
        if config is None:
            config = HyperscaleInterfaceConfig()
        
        if padding is None:
            padding = (4, 1)

        self._config = config
        self._terminal: Terminal | None = None

        horizontal_padding, vertical_padding = padding

        self._horizontal_padding = horizontal_padding
        self._vertical_padding = vertical_padding
        self._updates = updates

        self._active_workflow = 'initializing'
        self._active_workflows: list[str] = []

        self._terminal_task: asyncio.Task | None = None
        self._run_switch_loop: asyncio.Event | None = None
        self._active_workflows: list[str] = []

        self._component_names = [
            'run_progress',
            'run_message_display',
            'run_timer',
            'executions_counter',
            'total_executions',
            'executions_over_time',
            'execution_stats_table',

        ]

        self._current_active_idx: int = 0
        self._updated_active_workflows: asyncio.Event | None = None
        self._start: float | None = None

    def initialize(
        self,
        workflows: list[Workflow],
    ):
        sections: list[Section] = generate_ui_sections(workflows)
        self._terminal = Terminal(sections)

    async def run(self):

        if self._terminal is None:
            raise Exception('Err. - Terminal not initialized.')

        if self._terminal_task is None:
            self._run_switch_loop = asyncio.Event()
            self._initial_tasks_set = asyncio.Future()

            self._terminal_task = asyncio.ensure_future(self._run())

            await self._terminal.render(
                horizontal_padding=self._horizontal_padding,
                vertical_padding=self._vertical_padding,
            )

    async def _run(self):

        while not self._run_switch_loop.is_set():
            await asyncio.gather(*[
                self._terminal.set_component_active(
                    f'{component_name}_{self._active_workflow}' 
                ) for component_name in self._component_names
            ])

            active_workflows_update: list[str] | None = await self._updates.get_active_workflows(
                self._config.update_interval
            )

            if isinstance(active_workflows_update, list):
                self._active_workflows = active_workflows_update
                self._current_active_idx = 0
                self._active_workflow = active_workflows_update[self._current_active_idx]


            elif len(self._active_workflows) > 0:
                self._current_active_idx = (self._current_active_idx + 1)%len(self._active_workflows)
                self._active_workflow = self._active_workflows[self._current_active_idx]

    async def stop(self):

        if self._run_switch_loop.is_set() is False:
            self._run_switch_loop.set()

        self._updates.shutdown()

        if self._updated_active_workflows and self._updated_active_workflows.is_set() is False:
            self._updated_active_workflows.set()

        if self._terminal:
            await self._terminal.stop()
            await self._terminal_task
        
    async def abort(self):

        if not self._run_switch_loop.is_set():
            self._run_switch_loop.set()

        try:

            self._updates.shutdown()

        except Exception:
            pass
        
        try:

            if self._updated_active_workflows and self._updated_active_workflows.is_set() is False:
                self._updated_active_workflows.set()

        except Exception:
            pass

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
    