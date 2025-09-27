from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTP2Response


class Test(Workflow):
    vus = 18000
    duration = "1m"

    @step()
    async def get_hey(
        self,
        url: URL = 'https://www.hey.com/',
    ) -> HTTP2Response:
        return await self.client.http.get(url)
