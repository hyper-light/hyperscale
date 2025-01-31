from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse, Data, Headers


class Test(Workflow):
    vus = 1000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        response = await self.client.http.get(url)
        assert response.status is not None, "Err. - incomplete request"
        assert response.status >= 200 and response.status < 300, "Err. - requested failed"

        return response

    @step('login')
    async def new_user(
        self,
        url: URL = "https://httpbin.org/post",
        headers: Headers = {
            "content-type": "application/json"
        },
        login: HTTPResponse = None,
    ) -> HTTPResponse:
        response = await self.client.http.post(
            url,
            headers=headers,
            data={
                "name": "ada",
                "status": login.status,
            },
        )
        assert response.status is not None, "Err. - incomplete request"
        assert response.status >= 200 and response.status < 300, "Err. - requested failed"
        
        data = response.json()

        assert isinstance(data, dict), "Err. - no data found"
        
        return response
