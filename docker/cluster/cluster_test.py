from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class ClusterTest(Workflow):
    """A short HTTP test against the cluster's local nginx target."""

    vus: int = 16
    duration: str = "15s"

    @step()
    async def get_target(
        self,
        url: URL = "http://target/",
    ) -> HTTPResponse:
        return await self.client.http.get(url)
