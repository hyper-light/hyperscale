from hyperscale.core.graphs.stages import (
    Execute,
)
from hyperscale.core.hooks import action


class ExecuteHTTP2Stage(Execute):
    @action()
    async def http2_get(self):
        return await self.client.http2.get("https://<url_here>")
