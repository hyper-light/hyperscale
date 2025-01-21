import asyncio
import pprint

from hyperscale.core.graph import Graph
from hyperscale.core.results.workflow_results import WorkflowResults
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class Test(Workflow):
    vus = 1000
    threads = 12
    duration = "1m"

    @step()
    async def login(self, url: URL = "https://httpbin.org/get") -> HTTPResponse:
        return await self.client.http.get(url)


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
