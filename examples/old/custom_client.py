import asyncio
import time
from hyperscale.graph import Workflow, step
from hyperscale.plugin import CustomResult

    
class ExampleResponse(CustomResult):
    message: str

    def context(self):
        return self.message

    @property
    def successful(self):
        return self.message is not None


class ExampleClient:

    async def go(self, message: str) -> ExampleResponse:

        start = time.monotonic()
        await asyncio.sleep(1)

        return ExampleResponse(
            message=message,
            timings={
                'total': time.monotonic() - start
            }
        )


class Test(Workflow):
    vus = 1000
    duration = "1m"
    example=ExampleClient()

    @step()
    async def login(self) -> ExampleResponse:
        return await self.example.go('Hello!')
