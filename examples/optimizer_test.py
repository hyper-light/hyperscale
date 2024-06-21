from __future__ import annotations

import asyncio
from typing import Generic, Literal, TypeVar

import dill

from hyperscale.core_rewrite import Graph, Workflow, step
from hyperscale.core_rewrite.engines.client.graphql import GraphQLResponse
from hyperscale.core_rewrite.engines.client.http import HTTPResponse
from hyperscale.core_rewrite.engines.client.http2 import HTTP2Response
from hyperscale.core_rewrite.engines.client.udp import UDPResponse


class Result:
    pass


T = TypeVar("T")
K = TypeVar("K")

State = Generic[T, K]


class Test(Workflow):
    vus = 1000
    threads = 4
    duration = "1m"

    def different(self) -> Literal["Hello there!"]:
        return "Hello there!"

    @step()
    async def one(self) -> HTTPResponse:
        return await self.client.http.get("https://httpbin.org/get")

    @step("one")
    async def two(self) -> HTTPResponse:
        boop = self.different()
        return await self.client.http.get(
            f"https://httpbin.org/get?beep={boop}", headers={"test": (boop,)}
        )

    @step("one")
    async def three(self) -> HTTPResponse:
        return await self.client.http.get("https://httpbin.org/get")

    @step("two", "three")
    async def four(self) -> HTTP2Response:
        return await self.client.http2.post(
            "https://httpbin.org/post",
            headers={"test": "this"},
            cookies=[
                ("beep", "boop"),
                ("bop", "bap"),
            ],
            params={"sort": True},
            auth=("user", "pass"),
            data={"test": "this"},
            redirects=4,
        )

    @step("two", "three")
    async def five(self) -> UDPResponse:
        return await self.client.udp.send(f"127.0.0.1:{self.udp_port}", "Test this!")

    @step("two", "three")
    async def six(self) -> GraphQLResponse:
        return await self.client.graphql.query(
            "https://httpbin.org/get",
            """
            query getContinents {
                continents {
                    code
                    name
                }
                }
            """,
            headers={"test": "this"},
        )


async def run():
    w = Test()
    dill.dumps(w)

    # for hook_name, hook in w.hooks.items():
    #     hook_pair = {hook_name: hook}
    #     d = dill.dumps(hook_pair, recurse=True)

    g = Graph([w])

    await g.setup()

    # await g.run()


asyncio.run(run())
