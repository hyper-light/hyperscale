from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, TCPResponse


class Test(Workflow):
    vus = 1000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> TCPResponse:
        return await self.client.tcp.bidirectional(
            url,
            'HELLO!',
        )
