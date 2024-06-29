import asyncio
import pprint

from hyperscale.core_rewrite.graph import Graph
from hyperscale.core_rewrite.results.workflow_results import WorkflowResults
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
    graph = Graph([Test()])

    results = (await graph.run()).pop()

    print(pprint.pprint(results, compact=True))
    print("\n")

    workflow_results = WorkflowResults()

    merged_results = workflow_results.merge_results([dict(results) for _ in range(4)])

    print(pprint.pprint(merged_results, compact=True))
    print("\n")


asyncio.run(run())
