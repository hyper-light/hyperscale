import asyncio
import time
from hyperscale.core_rewrite.graph import Workflow
from hyperscale.ui.components.terminal import Terminal, Section
from .actions import update_active_workflow_message
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

        self._terminal_task: asyncio.Task | None = None
        self._run_switch_loop: asyncio.Event | None = None
        self._active_workflows: list[str] = []
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
            self._updated_active_workflows  = asyncio.Event()

            self._terminal_task = asyncio.ensure_future(self._run())

            await self._terminal.render(
                notifications=[self._check_for_updates],
                horizontal_padding=self._horizontal_padding,
                vertical_padding=self._vertical_padding,
            )

    async def _run(self):

        while not self._run_switch_loop.is_set():
            await asyncio.gather(*[
                self._terminal.set_component_active(
                    f'{action_name}_{self._active_workflow}' 
                ) for action_name in self._action_names
            ])

            await self._updated_active_workflows.wait()

            if len(self._active_workflows) > 0:
                self._current_active_idx = (self._current_active_idx)%len(self._active_workflows)
                self._active_workflow = self._active_workflows[self._current_active_idx]
            
                await update_active_workflow_message(
                    self._active_workflow,
                    f'Running - {self._active_workflow}',
                )


    async def _check_for_updates(self):
        try:

            if self._start is None:
                self._start = time.monotonic()


            elapsed = time.monotonic() - self._start
  
            active_workflows_update = await self._updates.get_active_workflows()

            if active_workflows_update:
                self._active_workflows = active_workflows_update
                self._current_active_idx = 0

                self._updated_active_workflows.set() 

            elif elapsed > self._config.update_interval:
                self._start = 0
                self._updated_active_workflows.set() 

        except Exception:
           import traceback
           print(traceback.format_exc())


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
    