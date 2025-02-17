from __future__ import annotations

from abc import ABC, abstractmethod
from hyperscale.reporting.common import (
    WorkflowMetricSet,
    StepMetricSet,
)


class CustomReporter(ABC):

    @abstractmethod
    async def connect(self):
        pass
    
    @abstractmethod
    async def submit_workflow_results(self, _: WorkflowMetricSet):
        pass

    @abstractmethod
    async def submit_step_results(self, _: StepMetricSet):
        pass

    @abstractmethod
    async def close(self):
        pass