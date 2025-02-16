import asyncio
import time
from hyperscale.graph import Workflow, step
from hyperscale.plugin import CustomResponse

    
class ExampleResponse(CustomResponse):
    message: str

    @classmethod
    def response_type(cls):
        return 'Example'

    @property
    def successful(self):
        return self.message is not None


class ExampleEngine:

    def __init__(self):
        pass

    async def go(self, message: str) -> ExampleResponse:

        start = time.monotonic()
        await asyncio.sleep(10)

        return ExampleResponse(
            message=message,
            timings={
                'total': time.monotonic() - start
            }
        )


class Test(Workflow):
    vus = 1000
    duration = "1m"
    example=ExampleEngine()

    @step()
    async def login(self) -> ExampleResponse:
        return await self.example.go('Hello!')
