from __future__ import annotations

import asyncio
from typing import Generic, Literal, TypeVar

import dill

from hyperscale.core_rewrite import Graph, Workflow, step
from hyperscale.core_rewrite.engines.client.http import HTTPResponse
from hyperscale.core_rewrite.hooks.optimized.models import (
    URL,
    Cookies,
    Data,
    Headers,
    Params,
)


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
    async def one(
        self,
        url=URL("https://httpbin.org/get"),
    ) -> HTTPResponse:
        return await self.client.http.get(url)

    @step("one")
    async def two(
        self,
        url=URL("https://httpbin.org/get"),
        params=Params(
            {"beep": "boop"},
        ),
    ) -> HTTPResponse:
        return await self.client.http.get(
            url,
            params=params,
        )

    @step("one")
    async def three(
        self,
        url=URL("https://httpbin.org/post"),
        params=Params(
            {"a": "b"},
        ),
        headers=Headers(
            {"test": "boop"},
        ),
        cookies=Cookies([("a", "b")]),
        data=Data({"bop": "blep"}),
    ) -> HTTPResponse:
        return await self.client.http.post(
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            data=data,
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
