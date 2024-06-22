import asyncio

import dill

from hyperscale.core_rewrite import Graph, Workflow, step
from hyperscale.core_rewrite.engines.client.http2 import HTTP2Response
from hyperscale.core_rewrite.optimized import URL, Headers


class Test(Workflow):
    vus = 400
    threads = 4
    duration = "1m"

    @step()
    async def one(
        self,
        url: URL = "https://http2.github.io/",
        headers: Headers = {"content-type": "application/json"},
    ) -> HTTP2Response:
        return await self.client.http2.get(url, headers=headers)

    @step("one")
    async def two(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)

    @step("one")
    async def three(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)

    @step("two", "three")
    async def check_statuses(
        self,
        one: HTTP2Response = None,
        two: HTTP2Response = None,
        three: HTTP2Response = None,
    ) -> Exception | None:
        assert (
            one.status >= 200 and one.status < 300
        ), f"Task One failed status check, got - {one.status}"
        assert (
            two.status >= 200 and three.status < 300
        ), f"Task Two failed status check, got - {two.status}"
        assert (
            three.status >= 200 and three.status < 300
        ), f"Task Three failed status check, got - {three.status}"


async def run():
    w = Test()
    dill.dumps(w)

    # for hook_name, hook in w.hooks.items():
    #     hook_pair = {hook_name: hook}
    #     d = dill.dumps(hook_pair, recurse=True)

    g = Graph([w])

    await g.setup()

    await g.run()


loop = asyncio.new_event_loop()

loop.set_task_factory(asyncio.eager_task_factory)
asyncio.set_event_loop(loop)

loop.run_until_complete(run())
