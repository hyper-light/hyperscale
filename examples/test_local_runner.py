import asyncio

from hyperscale.core_rewrite.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTP2Response


class Test(Workflow):
    vus = 1000
    threads = 8
    duration = "1m"

    @step()
    async def login(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)


async def run():
    runner = LocalRunner(
        "0.0.0.0",
        15454,
    )

    results = await runner.run(
        "test",
        [Test()],
    )

    print(results)


if __name__ == "__main__":
    asyncio.run(run())
