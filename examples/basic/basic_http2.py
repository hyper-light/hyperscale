from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTP2Response


class Test(Workflow):
    '''
    Both vus and duration are set here for clarity, but
    both have sane defaults (1000 VUS, 30s).
    '''
    vus = 1000
    duration = "1m"
    cpus=18

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://http2.github.io/',
    ) -> HTTP2Response:
        return await self.client.http2.get(url)