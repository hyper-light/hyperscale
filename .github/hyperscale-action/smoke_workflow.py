from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class Smoke(Workflow):
    """
    The action's smoke test: a few VUs GET a local static file server
    (python -m http.server 8000, started by the job that runs it).
    """

    vus = 8
    duration = "5s"

    @step()
    async def get_index(
        self,
        url: URL = "http://127.0.0.1:8000/",
    ) -> HTTPResponse:
        return await self.client.http.get(url)
