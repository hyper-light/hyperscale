from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class Test(Workflow):
    vus = 1000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        response = await self.client.http.get(url)
        assert response.status is not None, "Err. - missing status"
        assert response.status >= 200 and response.status < 300

        return response
