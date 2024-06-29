import asyncio

from hyperscale.core_rewrite.jobs import Env, RemoteGraphManager
from hyperscale.graph import Use, Workflow, state, step
from hyperscale.testing import URL, HTTP2Response


class Test(Workflow):
    vus = 400
    threads = 4
    duration = "1m"

    @state()
    async def greeting(self) -> Use[str]:
        return "Hello world!"

    @step()
    async def login(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)


async def run():
    client = RemoteGraphManager([("0.0.0.0", 12399)])

    await client.start(
        "0.0.0.0",
        12446,
        Env(MERCURY_SYNC_AUTH_SECRET="testthissecret"),
    )

    await client.connect_to_workers()

    await client.execute_graph("Test", [Test()])

    await client.close()


asyncio.run(run())
