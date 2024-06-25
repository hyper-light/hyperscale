import asyncio

from hyperscale.core_rewrite.graph import Graph
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTP2Response


class Test(Workflow):
    vus = 400
    threads = 4
    duration = "1m"

    @step()
    async def login(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)


async def run():
    g = Graph(
        [
            Test(),
        ]
    )

    await g.run()


loop = asyncio.new_event_loop()

loop.set_task_factory(asyncio.eager_task_factory)
asyncio.set_event_loop(loop)

loop.run_until_complete(run())
