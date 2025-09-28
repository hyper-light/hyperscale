from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class Test(Workflow):
    vus = 72000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = 'https://hey.com',
    ) -> HTTPResponse:
        return await self.client.http.get(url)
