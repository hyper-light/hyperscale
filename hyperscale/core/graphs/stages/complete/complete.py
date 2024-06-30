from hyperscale.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hyperscale.core.graphs.stages.base.stage import Stage
from hyperscale.core.graphs.stages.types.stage_types import StageTypes
from hyperscale.core.hooks.types.internal.decorator import Internal


class Complete(Stage):
    stage_type = StageTypes.COMPLETE

    def __init__(self) -> None:
        super().__init__()

        self.retries: int = 0
        self.priority = None
        self.priority_level: StagePriority = StagePriority.map(self.priority)

    @Internal()
    async def run(self):
        await self.logger.filesystem.aio["hyperscale.core"].info(
            f"{self.metadata_string} - Graph has reached terminal point"
        )
