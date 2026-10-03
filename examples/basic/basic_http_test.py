from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse

'''
Example: Basic HTTP/1.1 Test

This test represents the bare minimum for a valid HTTP/1.1 API
test - one workflow, one step, one client call. This is also
how we benchmark the HTTP client for Hyperscale!
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
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        '''
        Hyperscale uses type annotations for both parameters
        and return types to determine what to do with a step.
        In this case, the:
        
            HTTPResponse
            
        return type annotation indicates that this step is 
        a *test* step. Hyperscale is smart and knows that if 
        a Response return-type annotated function exists in
        a workflow, then that workflow needs to be executed
        as a test.

        Note that this step *also* contains a url parameter with the:
        
            URL
        
        type and a hardcoded string. This is entirely optional
        (you could put the url string anywhere in the function,
        parameterize it inside the function, etc.) but is done
        here to show how to use Hyperscale's *optimized args*.

        Optimized args are static data (like a hardcoded endpoint
        url to test) which Hyperscale detects before running the
        test and recording metrics, and then performs things like
        IP lookup, header serialization, data encoding upon
        ahead of testing/metrics recording. This ensures that
        your tests truly measure the response time/etc. as opposed
        to arbitrary parsing/encoding/lookup overhead!
        '''
        return await self.client.http.get(url)