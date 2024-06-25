import asyncio

from hyperscale.core_rewrite import (
    Workflow,
    step,
)
from hyperscale.core_rewrite.engines.client.http2 import HTTP2Response
from hyperscale.core_rewrite.graph import Graph, depends
from hyperscale.core_rewrite.state import Provide, Use, state
from hyperscale.core_rewrite.testing import (
    COUNT,
    URL,
    Headers,
    Metric,
)


class Test(Workflow):
    vus = 400
    threads = 4
    duration = "1m"

    @state()
    async def authed_headers(self) -> Use[Headers]:
        return Headers({"api_key": "ABCDEFG"})

    @step()
    async def login(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)

    @step("login")
    async def get_api_v1(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)

    @step("login")
    async def get_api_v2(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)

    @step("get_api_v1", "get_api_v2")
    async def check_statuses(
        self,
        login: HTTP2Response = None,
        get_api_v1: HTTP2Response = None,
        get_api_v2: HTTP2Response = None,
    ) -> Exception | None:
        assert (
            login.status >= 200 and login.status < 300
        ), f"Task One failed status check, got - {login.status}"
        assert (
            get_api_v1.status >= 200 and get_api_v1.status < 300
        ), f"Task Two failed status check, got - {get_api_v1.status}"
        assert (
            get_api_v2.status >= 200 and get_api_v2.status < 300
        ), f"Task Three failed status check, got - {get_api_v2.status}"

    @step("check_statuses")
    async def get_api_v1_failed(
        self,
        get_api_v2: HTTP2Response = None,
    ) -> Metric[COUNT]:
        return 1 if get_api_v2.status >= 400 else 0


@depends("Test")
class TestTwo(Workflow):
    @step()
    async def hello_world_message(self):
        return "Hello world!"

    @state("TestThree", "TestFour")
    async def session_headers(
        self,
        authed_headers: Headers | None = None,
    ) -> Provide[Headers]:
        if authed_headers is None:
            return Headers({"api_key": "123456"})

        return authed_headers


@depends("Test")
class TestThree(Workflow):
    @step()
    async def non_test_step(self):
        return "Hello world!"


@depends("TestTwo", "TestThree")
class TestFour(Workflow):
    @step()
    async def non_test_step(self):
        return "Hello world!"


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
