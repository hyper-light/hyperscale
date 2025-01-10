import asyncio
import json
import uvloop

uvloop.install()

from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class Test(Workflow):
    vus = 4000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)


# async def run():
#     runner = LocalRunner("127.0.0.1", 15454)

#     results = await runner.run(
#         "test",
#         [Test()],
#     )

#     # runner.close()

#     return results


# if __name__ == "__main__":
#     results = asyncio.run(run())

#     with open('results.json', 'w+') as results_file:
#         json.dump(results, results_file, indent=4)
