import asyncio
from typing import Dict, List, Literal

from hyperscale.core_rewrite.local.workers import Provisioner, StagePriority
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
    # g = Graph(
    #     [
    #         Test(),
    #     ]
    # )

    # await g.run()

    provisioner = Provisioner()
    configs: List[
        Dict[
            Literal[
                "workflow_name",
                "priority",
                "is_test",
                "threads",
            ],
            str | int,
        ]
    ] = [
        {
            "workflow_name": "test1",
            "priority": StagePriority.LOW,
            "is_test": True,
        },
        {
            "workflow_name": "test2",
            "priority": StagePriority.AUTO,
            "is_test": True,
        },
        {
            "workflow_name": "test3",
            "priority": StagePriority.NORMAL,
            "is_test": True,
        },
    ]
    provisioner.setup()

    print(provisioner.partion_by_priority(configs))


loop = asyncio.new_event_loop()

loop.set_task_factory(asyncio.eager_task_factory)
asyncio.set_event_loop(loop)

loop.run_until_complete(run())
