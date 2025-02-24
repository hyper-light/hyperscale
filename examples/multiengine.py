from hyperscale.graph import Workflow, step
from hyperscale.testing import (
    URL, 
    Email,
    HTTPResponse, 
    HTTP2Response, 
    HTTP3Response,
    
)


class Test(Workflow):
    vus = 1000
    duration = "1m"

    @step()
    async def get_httpbin(
        self,
        url: URL = "https://google.com",
    ) -> HTTPResponse:
        return await self.client.http.get(url)

    @step()
    async def get_httpbin_http2(
        self,
        url: URL = "https://google.com",
    ) -> HTTP2Response:
        return await self.client.http2.get(url)
    
    @step()
    async def get_httpbin_http3(
        self,
        url: URL = "https://google.com",
    ) -> HTTP3Response:
        return await self.client.http3.get(url)
    
    @step()
    async def send_email(
        self,
        server: URL = 'smtp://smtp.gmail.com',
        email: Email = {
            'sender': '<SENDER>@gmail.com',
            'recipients': '<RECIPIENT>@gmail.com',
            'subject': 'A Test Message',
            'body': 'This is a test!',
        }
    ):
        return await self.client.smtp.send(
            server,
            '<SENDER>@gmail.com',
            '<RECIPIENT>@gmail.com',
            'A Test Message',
            email,
            auth=(
                '<GMAIL_USERNAME>',
                '<GMAIL_APP_PASSWORD>',
            )
        )
