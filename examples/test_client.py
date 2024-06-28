import asyncio

from hyperscale.core_rewrite.jobs import Env, JobServer
from hyperscale.core_rewrite.state import Context
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTP2Response


class Test(Workflow):
    vus = 400
    threads = 4
    duration = "1m"

    @step()
    async def login(self, url: URL = "https://http2.github.io/") -> HTTP2Response:
        return await self.client.http2.get(url)


async def run():
    client = JobServer(
        "0.0.0.0",
        12446,
        Env(MERCURY_SYNC_AUTH_SECRET="testthissecret"),
    )

    await client.start_server()
    await client.connect_client(("0.0.0.0", 12399))

    context = Context()

    await client.submit(Test(), context)

    await client.run_forever()

    await client.close()


asyncio.run(run())
