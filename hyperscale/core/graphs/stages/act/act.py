from typing import Generic, Optional

from typing_extensions import TypeVarTuple, Unpack

from hyperscale.core.engines.client import Client
from hyperscale.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hyperscale.core.graphs.stages.base.stage import Stage
from hyperscale.core.graphs.stages.types.stage_types import StageTypes
from hyperscale.core.hooks.types.base.hook_type import HookType
from hyperscale.core.hooks.types.internal.decorator import Internal
from hyperscale.monitoring import CPUMonitor, MemoryMonitor

T = TypeVarTuple("T")


class Act(Stage, Generic[Unpack[T]]):
    stage_type = StageTypes.ACT
    priority: Optional[str] = None
    retries = 0

    def __init__(self) -> None:
        super().__init__()
        self.persona = None
        self.client: Client[Unpack[T]] = Client(
            self.graph_name, self.graph_id, self.name, self.stage_id
        )

        self.accepted_hook_types = [
            HookType.ACTION,
            HookType.CHANNEL,
            HookType.CHECK,
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT,
            HookType.LOAD,
            HookType.SAVE,
            HookType.TASK,
            HookType.TRANSFORM,
        ]

        self.priority = self.priority
        if self.priority is None:
            self.priority = "auto"

        self.priority_level: StagePriority = StagePriority.map(self.priority)

        self.stage_retries = self.retries

    @Internal()
    async def run(self):
        await self.setup_events()
        self.dispatcher.assemble_execution_graph()

        cpu_monitor = CPUMonitor()
        memory_monitor = MemoryMonitor()

        main_monitor_name = f"{self.name}.main"

        await cpu_monitor.start_background_monitor(main_monitor_name)
        await memory_monitor.start_background_monitor(main_monitor_name)

        await self.dispatcher.dispatch_events(self.name)

        await cpu_monitor.stop_background_monitor(main_monitor_name)
        await memory_monitor.stop_background_monitor(main_monitor_name)

        cpu_monitor.close()
        memory_monitor.close()

        cpu_monitor.stage_metrics[main_monitor_name] = cpu_monitor.collected[
            main_monitor_name
        ]
        memory_monitor.stage_metrics[main_monitor_name] = memory_monitor.collected[
            main_monitor_name
        ]

        self.context.update(
            {
                "act_stage_monitors": {
                    self.name: {"cpu": cpu_monitor, "memory": memory_monitor}
                }
            }
        )
