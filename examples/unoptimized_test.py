from __future__ import annotations

import asyncio
from typing import Generic, Literal, TypeVar

import dill

from hyperscale.core import Graph, Workflow, step
from hyperscale.core.engines.client.http import HTTPResponse


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
        return await self.client.http.get(
            "https://httpbin.org/get",
            {"beep": "boop"},
        )

    @step("one")
    async def three(self) -> HTTPResponse:
        return await self.client.http.post(
            "https://httpbin.org/post",
            params={"a": "b"},
            headers={"test": "boop"},
            cookies=[("a", "b")],
            data={"bop": "blep"},
        )


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
