from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, PlaywrightResult


class Test(Workflow):
    """
    Each VU drives a real browser page: it loads the page, then reads its
    title. A browser page costs far more than an HTTP connection, so
    browser workflows run with a handful of VUs rather than thousands.

    Needs the Playwright extra and a browser:
    uv add playwright && uv run playwright install chromium
    """

    vus = 4
    duration = "15s"

    @step()
    async def load_page(
        self,
        url: URL = "https://example.com/",
    ) -> PlaywrightResult:
        async with self.client.playwright as page:
            return await page.goto(url)
