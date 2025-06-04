from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTP2Response


class Test(Workflow):
    vus = 18000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = 'https://qandle.store',
    ) -> HTTP2Response:
        return await self.client.smtp.send(
            url,
            'test@test.com',
            'info@qandle.store',
            'Mowing the Lawn',
            'Are we touching grass yet?',
        )
