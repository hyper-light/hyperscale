import asyncio

from hyperscale.core_rewrite.graph import Workflow
from hyperscale.core_rewrite.jobs import Env
from hyperscale.core_rewrite.jobs.job_server import JobServer


async def return_response(_: int, workflow: Workflow):
    print("HERE!", workflow.id)
    return workflow


async def run():
    server = JobServer(
        "0.0.0.0",
        12399,
        Env(MERCURY_SYNC_AUTH_SECRET="testthissecret"),
    )

    await server.start_server()
    await server.run_forever()

    await server.close()


loop = asyncio.new_event_loop()

loop.set_task_factory(asyncio.eager_task_factory)
asyncio.set_event_loop(loop)

loop.run_until_complete(run())
