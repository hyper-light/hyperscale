import asyncio

import uvloop

uvloop.install()

from hyperscale.core_rewrite.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class Test(Workflow):
    vus = 16000
    duration = "1h"

    @step()
    async def login(
        self,
        url: URL = "https://kiwifarms.st/webmanifest.php",
    ) -> HTTPResponse:
        response = await self.client.http.get(url)
        assert (
            response.status >= 200 and response.status < 300
        ), f"Err. - Request returned status - {response.status}"
        return response


async def run():
    runner = LocalRunner("0.0.0.0", 15454)

    results = await runner.run(
        "test",
        [Test()],
    )

    print(results)


if __name__ == "__main__":
    asyncio.run(run())
