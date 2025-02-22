from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse, TCPResponse


class Test(Workflow):
    vus = 1000
    duration = "1m"

    @step()
    async def get_httpbin(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)

    @step()
    async def tcp_get_httpbin(
        self,
        tcp_url: URL = "https://httpbin.org/get",
    ) -> TCPResponse:
        return await self.client.tcp.send(tcp_url, b"hello")
