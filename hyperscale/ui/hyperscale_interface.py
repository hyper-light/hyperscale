import asyncio
import time
from hyperscale.core_rewrite.graph import Workflow
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

        self._terminal_task: asyncio.Task | None = None
        self._run_switch_loop: asyncio.Event | None = None
        self._active_workflows: list[str] = ['initializing']
        self._action_names: list[str] = [
            'update_run_progress',
            'update_run_message',
            'update_run_timer',
            'update_total_executions',
            'update_total_executions',
            'update_execution_timings',
            'update_execution_stats',
        ]

        self._current_active_idx: int = 0
        self._updated_active_workflows: asyncio.Future[list[str] | None] = None
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
                notifications=[self._check_for_updates],
                horizontal_padding=self._horizontal_padding,
                vertical_padding=self._vertical_padding,
            )

    async def _run(self):

        while not self._run_switch_loop.is_set():

            active_worklow = self._active_workflows[self._current_active_idx]
            await asyncio.gather(*[
                self._terminal.set_component_active(
                    f'{action_name}_{active_worklow}' 
                ) for action_name in self._action_names
            ])

            self._updated_active_workflows = asyncio.Future()
            active_workflows_update = await self._updated_active_workflows

            if active_workflows_update:
                self._active_workflows = active_workflows_update
                self._current_active_idx = 0

            else:
                self._current_active_idx = (self._current_active_idx)%len(self._active_workflows)

    async def _check_for_updates(self):
        
        if self._start is None:
            self._start = time.monotonic()


        elapsed = time.monotonic() - self._start

        
        active_workflows_update = await self._updates.get_active_workflows()

        can_update = self._updated_active_workflows is not None and self._updated_active_workflows.done() is False

        if active_workflows_update and can_update:
            self._updated_active_workflows.set_result(active_workflows_update)

        elif elapsed > self._config.update_interval and can_update:
            self._updated_active_workflows.set_result(None)  


    async def stop(self):

        if self._run_switch_loop.is_set() is False:
            self._run_switch_loop.set()

        self._updates.shutdown()

        if self._updated_active_workflows and self._updated_active_workflows.done() is False:
            self._updated_active_workflows.set_result(None)

        if self._terminal:
            await self._terminal.stop()
            await self._terminal_task
        
    async def abort(self):

        if self._run_switch_loop.is_set() is False:
            self._run_switch_loop.set()

        self._updates.shutdown()
        
        if self._updated_active_workflows and self._updated_active_workflows.done() is False:
            self._updated_active_workflows.set_result(None)

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
    