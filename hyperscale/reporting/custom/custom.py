from hyperscale.reporting.common import (
    WorkflowMetricSet,
    StepMetricSet,
)


class CustomReporter:

    async def connect(self):
        pass

    async def submit_workflow_results(self, _: WorkflowMetricSet):
        pass


    async def submit_step_results(self, _: StepMetricSet):
        pass

    async def close(self):
        pass