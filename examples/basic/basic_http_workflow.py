from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


'''
Example: Basic HTTP Workflow

This test is a quick introduction to workflow part of Hyperscale's
tesing Workflows. Note now that each:

    @step()

contains a string matching the name of a preceding step method for
that Workflow. You may pass none or as many names as you want so
long as those names correspond to steps in that given workflow.

Note that we *also* 
'''

class Test(Workflow):
    '''
    Both vus and duration are set here for clarity, but
    both have sane defaults (1000 VUS, 30s).
    '''
    vus = 1000
    duration = "1m"

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get?param=one',
    ) -> HTTPResponse:
        return await self.client.http.get(url)

    
    @step("get_httpbin")
    async def post_httpbin(
        self,
        url: URL = 'https://httpbin.org/post',
        get_httpbin: HTTPResponse | None = None,
    ) -> HTTPResponse:
        return await self.client.http.post(
            url,
            data={
                "my_ip": get_httpbin.json().get("origin")
            } if get_httpbin else {
                "my_ip": None
            }
        )

    @step("post_httpbin")
    async def get_httpbin_with_post_response_one(
        self,
        url: URL = 'https://httpbin.org/get',
        post_httpbin: HTTPResponse | None = None,
    ) -> HTTPResponse:
        return await self.client.http.get(
            url,
            params={
                "my_ip": post_httpbin.json().get("origin")
            } if post_httpbin else {
                "my_ip": None,
            }
        )

    
    @step("post_httpbin")
    async def get_httpbin_with_post_response_two(
        self,
        url: URL = 'https://httpbin.org/get',
        post_httpbin: HTTPResponse | None = None,
    ) -> HTTPResponse:
        return await self.client.http.get(
            url,
            params={
                "my_ip": post_httpbin.json().get("origin")
            } if post_httpbin else {
                "my_ip": None,
            }
        )