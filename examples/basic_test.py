from hyperscale.graph import Workflow, step, depends, state, Use, Provide
from hyperscale.testing import URL, HTTPResponse


class Test(Workflow):
    vus = 1000
    duration = "5m"

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        return await self.client.http.get(url)
    
    @state('TestTwo')
    def value(self) -> Provide[str]:
        return 'test'


class TestTwo(Workflow):
    vus = 3000
    duration = "53m"

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        return await self.client.http.get(url)
        

@depends('Test', 'TestTwo')
class TestThree(Workflow):

    @state('Test')
    def consume(self, value: str | None = None) -> Use[str]:
        return value

    @step()
    async def return_string(self, value: str | None = None) -> str:
        return f'hello {value}'
        