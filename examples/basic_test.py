from hyperscale.graph import Workflow, step, depends, state, Use, Provide
from hyperscale.testing import URL, HTTPResponse, Headers



# curl 'https://hardware.hellohelium.com/en/search?q=gdskl' \
#   -H 'accept: */*' \
#   -H 'accept-language: en-US,en;q=0.9,ru;q=0.8' \
#   -H 'cookie: intercom-id-i4gsbx08=a56be7ce-00cf-4bb3-b7f4-bcb54c62aa06; intercom-session-i4gsbx08=; intercom-device-id-i4gsbx08=3ec99f5a-54c7-4663-a094-1f367f464822' \
#   -H 'priority: u=1, i' \
#   -H 'referer: https://hardware.hellohelium.com/en/?q=gdskl' \
#   -H 'sec-ch-ua: "Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"' \
#   -H 'sec-ch-ua-mobile: ?0' \
#   -H 'sec-ch-ua-platform: "Linux"' \
#   -H 'sec-fetch-dest: empty' \
#   -H 'sec-fetch-mode: cors' \
#   -H 'sec-fetch-site: same-origin' \
#   -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'

class Test(Workflow):
    vus = 2000
    duration = "15s"

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        return await self.client.http.get(url)
    
    @state('TestTwo')
    def value(self) -> Provide[str]:
        return 'test'


@depends('Test')
class TestTwo(Workflow):
    vus = 2000
    duration = "15s"

    @state('Test')
    def consume(self, value: str | None = None) -> Use[str]:
        return value

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        return await self.client.http.get(url)
        