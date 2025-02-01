from hyperscale.graph import Workflow, step, depends
from hyperscale.testing import URL, HTTPResponse



class Test(Workflow):
    vus = 4000
    duration = "1m"

    @step()
    async def login(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(
            url,
        )
    
    @step('login')
    async def test_again(
        self,
        url: URL = "https://httpbin.org/get",
        login: HTTPResponse | None = None
    ) -> HTTPResponse:
        
        assert login is not None, "Err. - login required"
        data = login.json()

        assert data.get('origin') is not None, "Err. - missing origin on login"

        response = await self.client.http.get(
            url,
            params={
                'origin': data.get('origin')
            }
        )

        assert isinstance(response, HTTPResponse), "Err. - request returned invalid response"
        assert response.status >= 200 and response.status < 300, "Err. - request failed"
        
        data = response.json()

        assert isinstance(data, dict), "Err. - no data"
        assert isinstance(data.get('args'), dict), "Err. - data missing args"
        
        args: dict = data.get('args')

        assert isinstance(args.get('origin'), str), "Err. - origin missing"
        assert args.get('origin') == data.get('origin'), 'Err. - origin mismatch'

        return response