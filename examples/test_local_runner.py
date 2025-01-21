import uvloop

uvloop.install()

from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse
from hyperscale.reporting import JSONConfig, CSVConfig


@staticmethod
async def reporting_options():
    return [
        JSONConfig(),
        CSVConfig(),
    ]


class Test(Workflow):
    vus = 4000
    duration = "1m"
    reporting=reporting_options

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)
