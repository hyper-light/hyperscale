from hyperscale.graph import Workflow, step, depends
from hyperscale.testing import URL, HTTPResponse



class Test(Workflow):
    vus = 4000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(
            url,
        )
    

class TestSecond(Workflow):
    vus = 4000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(
            url,
        )
    