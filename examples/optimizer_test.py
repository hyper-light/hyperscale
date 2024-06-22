import asyncio
from typing import Literal

import dill

from hyperscale.core_rewrite import Graph, Workflow, step
from hyperscale.core_rewrite.engines.client.http2 import HTTP2Response
from hyperscale.core_rewrite.optimized import (
    URL,
)


class Test(Workflow):
    vus = 200
    threads = 4
    duration = "1m"
    udp_port = 8811

    def different(self) -> Literal["Hello there!"]:
        return "Hello there!"

    @step()
    async def one(self, url=URL("https://http2.github.io/")) -> HTTP2Response:
        return await self.client.http2.get(url)

    @step("one")
    async def two(self, url=URL("https://http2.github.io/")) -> HTTP2Response:
        return await self.client.http2.get(url)

    # @step("one")
    # async def three(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
    #     return await self.client.http2.get(url)


async def run():
    w = Test()
    dill.dumps(w)

    # for hook_name, hook in w.hooks.items():
    #     hook_pair = {hook_name: hook}
    #     d = dill.dumps(hook_pair, recurse=True)

    g = Graph([w])

    await g.setup()

    await g.run()


asyncio.run(run())
