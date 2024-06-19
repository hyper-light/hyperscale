from __future__ import annotations

from typing import List

from hyperscale.core.graphs.stages.base.stage import Stage
from hyperscale.core.graphs.stages.complete.complete import Complete
from hyperscale.core.graphs.stages.types.stage_states import StageStates
from hyperscale.core.graphs.transitions.common.base_edge import BaseEdge


class CompleteEdge(BaseEdge[Complete]):

    def __init__(self, source: Complete, destination: BaseEdge[Stage]) -> None:
        super(
            CompleteEdge,
            self
        ).__init__(
            source,
            destination
        )

    async def transition(self):

        await self.source.run()

        self.source.state = StageStates.COMPLETE

        self.visited.append(self.source.name)

        return None, None

    def _update(self, destination: Stage):
        self.next_history.update({
            (self.source.name, destination.name): {}
        })

    def split(self, edges: List[CompleteEdge]) -> None:
        pass
        

