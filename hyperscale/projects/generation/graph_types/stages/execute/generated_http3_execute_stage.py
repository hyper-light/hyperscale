from hyperscale.core.graphs.stages import (
    Execute,
)
from hyperscale.core.hooks import action


class ExecuteHTTP3Stage(Execute):
    @action()
    async def http3_get(self):
        return await self.client.http3.get("https://<url_here>")
