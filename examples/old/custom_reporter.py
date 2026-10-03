import asyncio
from hyperscale.graph import Workflow, step
from hyperscale.plugin import (
    CustomReporter,
    StepMetricSet,
    WorkflowMetricSet, 
)
from hyperscale.testing import URL, HTTPResponse


class ExampleReporter(CustomReporter):

    def __init__(self):
        super().__init__()
        self._loop: asyncio.AbstractEventLoop | None = None

    async def connect(self):
        self._loop = asyncio.get_event_loop()

    async def submit_workflow_results(self, results: WorkflowMetricSet):
        await self._loop.run_in_executor(
            None,
            print,
            results,
        )
        
    async def submit_step_results(self, results: StepMetricSet):
        await self._loop.run_in_executor(
            None,
            print,
            results
        )

    async def close(self):
        pass


class Test(Workflow):
    vus = 1000
    duration = "1m"
    reporting=ExampleReporter()

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(
            url,
        )
