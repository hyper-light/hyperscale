from hyperscale.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hyperscale.core.graphs.stages.base.stage import Stage
from hyperscale.core.graphs.stages.types.stage_types import StageTypes
from hyperscale.core.hooks.types.internal.decorator import Internal


class Idle(Stage):
    stage_type = StageTypes.IDLE

    def __init__(self) -> None:
        super().__init__()
        self.name = self.__class__.__name__
        self.priority = None
        self.priority_level: StagePriority = StagePriority.map(self.priority)

        self.retries: int = 0

    @Internal()
    async def run(self):
        await self.logger.filesystem.aio["hyperscale.core"].debug(
            f"{self.metadata_string} - Starting graph execution"
        )
