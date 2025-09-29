from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class Test(Workflow):
    vus = 120000
    duration = "1m"

    @step()
    async def get_hey(
        self,
        url: URL = 'https://hey.com',
    ) -> HTTPResponse:
        return await self.client.http.get(url)
