from hyperscale.core.graphs.stages import (
    Execute,
)
from hyperscale.core.hooks import action


class ExecuteUDPStage(Execute):
    @action()
    async def udp_send(self):
        return await self.client.udp.send("https://<url_here>", data="PING")

    @action()
    async def udp_receive(self):
        return await self.client.udp.receive("https://<url_here>")
