from hyperscale.core.graphs.stages import (
    Execute,
)
from hyperscale.core.hooks import action


class ExecuteHTTPStage(Execute):

    @action()
    async def http_get(self):
        return await self.client.http.get('https://<url_here>')