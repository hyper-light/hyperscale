import math

from hyperscale.core_rewrite.engines.client.time_parser import TimeParser
from hyperscale.core_rewrite.graph import Workflow
from hyperscale.terminal.components.progress_bar import (
    BarFactory,
    ProgressBarColorConfig,
)
from hyperscale.terminal.components.render_engine import (
    RenderEngine,
    Section,
)


class HyperscaleInterface:
    def __init__(self):
        self._engine = RenderEngine()
        self._bar_factory = BarFactory()
        self.workflow_sections: dict[str, Section] = []

    async def initialize(self, workflows: list[Workflow]):
        for workflow in workflows:
            workflow_duration_seconds = math.ceil(TimeParser(workflow.duration).time)
            bar = self._bar_factory.create_bar(
                int(workflow_duration_seconds),
                colors=ProgressBarColorConfig(
                    active_color="royal_blue",
                    fail_color="white",
                    ok_color="hot_pink_3",
                ),
                mode="extended",
                disable_output=True,
            )
