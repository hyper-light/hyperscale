import asyncio

from hyperscale.core_rewrite.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse, Headers


class Test(Workflow):
    vus = 125
    threads = 8
    duration = "1m"

    @step()
    async def login(self, url: URL = "https://httpbin.org/get", headers: Headers= {}) -> HTTPResponse:
        return await self.client.http.get(url, headers=headers)


async def run():
    runner = LocalRunner(
        "0.0.0.0",
        12443,
    )

    results = await runner.run(
        "test",
        [Test()],
    )

    print(results)


if __name__ == "__main__":
    asyncio.run(run())
