from __future__ import annotations

import asyncio
from typing import Generic, Literal, TypeVar

import dill

from hyperscale.core_rewrite import Graph, Workflow, step
from hyperscale.core_rewrite.engines.client.http import HTTPResponse
from hyperscale.core_rewrite.hooks.optimized.models import URL


class Result:
    pass


T = TypeVar("T")
K = TypeVar("K")

State = Generic[T, K]


class Test(Workflow):
    vus = 1000
    threads = 4
    duration = "1m"
    udp_port = 8811

    def different(self) -> Literal["Hello there!"]:
        return "Hello there!"

    @step()
    async def one(self) -> HTTPResponse:
        return await self.client.http.get("https://httpbin.org/get")

    @step("one")
    async def two(self) -> HTTPResponse:
        boop = self.different()
        return await self.client.http.get(
            f"https://httpbin.org/get?beep={boop}", headers={"test": boop}
        )

    @step("one")
    async def three(self, url=URL("https://httpbin.org/get")) -> HTTPResponse:
        return await self.client.http.get(url)


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
