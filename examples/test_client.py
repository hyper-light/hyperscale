import asyncio

from hyperscale.core.jobs import Env, RemoteGraphManager
from hyperscale.graph import Provide, Use, Workflow, depends, state, step
from hyperscale.testing import URL, HTTP2Response


class Test(Workflow):
    vus = 400
    threads = 4
    duration = "1m"

    @step()
    async def login(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)


@depends("Test")
class NonTestWorkflow(Workflow):
    @state()
    async def greeting(self) -> Provide[str]:
        return "Hello again!"


@depends("NonTestWorkflow")
class NonTestUse(Workflow):
    @state("NonTestWorkflow")
    async def custom_greeting(
        self,
        greeting: str | None = None,
    ) -> Use[str]:
        return f"{greeting} Welcome back!"

    @step()
    async def print_greeting(
        self,
        custom_greeting: str | None = None,
    ):
        return custom_greeting


async def run():
    client = RemoteGraphManager([("0.0.0.0", 12399)])

    await client.start(
        "0.0.0.0",
        12446,
        Env(MERCURY_SYNC_AUTH_SECRET="testthissecret"),
    )

    await client.connect_to_workers()

    results = await client.execute_graph(
        "Test",
        [
            Test(),
            NonTestWorkflow(),
            NonTestUse(),
        ],
    )

    print(results)

    await client.close()


asyncio.run(run())
