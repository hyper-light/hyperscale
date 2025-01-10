import asyncio
import json
import uvloop

uvloop.install()

from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class Test(Workflow):
    vus = 4000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)
