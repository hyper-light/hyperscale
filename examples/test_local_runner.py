import asyncio
import gc
import uvloop

uvloop.install()

from hyperscale.core_rewrite.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class Test(Workflow):
    vus = 1000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)


async def run():
    runner = LocalRunner("127.0.0.1", 15454)

    await runner.run(
        "test",
        [Test()],
    )

    runner.close()


if __name__ == "__main__":
    asyncio.run(run())
