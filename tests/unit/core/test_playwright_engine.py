"""
The Playwright engine driving a real headless Chromium against a local
HTTP server.

* The engine starts itself on the first page a step asks for: one start,
  shared by every step that arrives meanwhile, and one browser for all of
  its sessions, each in a context of its own.
* A step's URL argument (a ``URL``) is loaded as its address.
* Each step's page goes back with the step that took it, whatever order
  steps finish in: no two steps ever hold the same page.
* close() closes the browser and stops the driver: no browser process
  outlives the engine.

Needs the Playwright extra and its Chromium (uv run playwright install
chromium); skipped without them.
"""

import asyncio
import os
import subprocess

import pytest

sync_api = pytest.importorskip("playwright.sync_api")

with sync_api.sync_playwright() as playwright:
    CHROMIUM_INSTALLED = os.path.exists(playwright.chromium.executable_path)

pytestmark = pytest.mark.skipif(
    not CHROMIUM_INSTALLED,
    reason="Playwright's Chromium is not installed (uv run playwright install chromium)",
)

import hyperscale.testing  # noqa: F401  (the engines' import order)
from hyperscale.core.engines.client.playwright import MercurySyncPlaywrightConnection
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.testing.models import URL

VUS = 3
STEPS = 9
PAGE = b"<html><head><title>hyperscale</title></head><body>ok</body></html>"
# Long enough that steps overlap, and finish in another order than they began.
HOLD_SECONDS = (0.15, 0.05, 0.1)
# Far past a browser start or a local page load: a wait that reaches it is a hang.
HANG_SECONDS = 30.0


async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await reader.readuntil(b"\r\n\r\n")
    writer.write(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nCache-Control: no-store\r\n"
        + f"Content-Length: {len(PAGE)}\r\nConnection: close\r\n\r\n".encode()
        + PAGE
    )
    await writer.drain()
    writer.close()


def browser_processes() -> int:
    """Main headless browser processes on this machine (helpers excluded)."""
    commands = subprocess.run(["ps", "-axo", "command="], capture_output=True, text=True).stdout
    return sum(
        1 for command in commands.splitlines() if "chrome-headless-shell" in command and "--type=" not in command
    )


async def test_steps_share_one_browser_hold_their_own_pages_and_close_leaves_nothing() -> None:
    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    address = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/"
    browsers_before = browser_processes()
    client = setup_client(MercurySyncPlaywrightConnection(), VUS)
    held: set[int] = set()
    overlapped = False

    async def step(index: int):
        nonlocal overlapped
        async with client as page:
            assert id(page) not in held, "a page was handed to two steps at once"
            held.add(id(page))
            overlapped = overlapped or len(held) > 1
            result = await page.goto(URL(address))
            await asyncio.sleep(HOLD_SECONDS[index % len(HOLD_SECONDS)])
            held.discard(id(page))
            return result

    try:
        async with asyncio.timeout(HANG_SECONDS):
            results = await asyncio.gather(*[step(index) for index in range(STEPS)])

        assert [result.error for result in results] == [None] * STEPS
        assert {result.result.status for result in results} == {200}
        assert overlapped
        assert client._active == {}
        assert len(client.sessions) == VUS
        assert len({id(session.context) for session in client.sessions}) == VUS
        assert len({id(session.browser) for session in client.sessions}) == 1
        assert browser_processes() == browsers_before + 1

    finally:
        client.close()
        async with asyncio.timeout(HANG_SECONDS):
            await asyncio.gather(*client._closing_sessions, return_exceptions=True)
        server.close()
        await server.wait_closed()

    assert client._playwright is None
    async with asyncio.timeout(HANG_SECONDS):
        while browser_processes() != browsers_before:
            await asyncio.sleep(0.05)
