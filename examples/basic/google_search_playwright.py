import os

from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, PlaywrightResult

QUERY = "hyperscale load testing"
SEARCH_BOX = 'textarea[name="q"]'
# The container Google's result list renders in: present only on a real
# results page, not on a consent or bot-check page.
RESULTS = "#search"
RESULTS_TIMEOUT_MS = 5000

# One screenshot per page per stage, overwritten each iteration, so the
# folder holds each VU's latest view however long the run is. A string, not
# a pathlib.Path: the workflow travels to its workers pickled, and workers
# refuse to unpickle pathlib objects.
SCREENSHOTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "screenshots")
os.makedirs(SCREENSHOTS, exist_ok=True)


class GoogleSearch(Workflow):
    """
    A user searching Google: each VU opens the home page, types a query,
    submits it and waits for the results list, screenshotting the home page
    and the page the search lands on. An iteration succeeds only when real
    results render: a consent or bot-check page in their place fails it.

    Google answers automated searches with an "unusual traffic" check, so
    expect search failures from a headless browser; the home page and the
    screenshots show where each iteration ended up.
    """

    vus = 2
    duration = "20s"

    @step()
    async def search_google(
        self,
        url: URL = "https://www.google.com/",
    ) -> PlaywrightResult:
        async with self.client.playwright as page:
            loaded = await page.goto(url)
            if loaded.error:
                return loaded

            await page.screenshot(f"{SCREENSHOTS}/home-{id(page)}.png")

            typed = await page.fill(SEARCH_BOX, QUERY)
            if typed.error:
                return typed

            submitted = await page.press(SEARCH_BOX, "Enter")
            if submitted.error:
                return submitted

            await page.wait_for_load_state("load")
            await page.screenshot(f"{SCREENSHOTS}/search-{id(page)}.png")

            return await page.wait_for_selector(RESULTS, timeout=RESULTS_TIMEOUT_MS)
