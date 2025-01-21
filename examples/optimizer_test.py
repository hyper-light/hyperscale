import asyncio

from hyperscale.core.graph import Workflow
from hyperscale.core.jobs import Env, RemoteGraphManager


async def return_response(_: int, workflow: Workflow):
    return workflow


async def run():
    server = RemoteGraphManager()

    await server.start(
        "0.0.0.0",
        12399,
        Env(MERCURY_SYNC_AUTH_SECRET="testthissecret"),
    )

    await server.run_forever()

    await server.close()


loop = asyncio.new_event_loop()

loop.set_task_factory(asyncio.eager_task_factory)
asyncio.set_event_loop(loop)

loop.run_until_complete(run())
