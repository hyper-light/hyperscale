import asyncio

from hyperscale.core_rewrite import Graph, Workflow, step
from hyperscale.core_rewrite.engines.client.http2 import HTTP2Response
from hyperscale.core_rewrite.testing import COUNT, URL, Metric


class Test(Workflow):
    vus = 400
    threads = 4
    duration = "1m"

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


async def run():
    w = Test()

    g = Graph([w])

    await g.setup()

    await g.run()


loop = asyncio.new_event_loop()

loop.set_task_factory(asyncio.eager_task_factory)
asyncio.set_event_loop(loop)

loop.run_until_complete(run())
