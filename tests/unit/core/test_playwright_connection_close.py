"""Closing a Playwright connection closes every browser session.

``close()`` is synchronous (the runner releases engine clients
synchronously), so it schedules each session's close and must own those
tasks until they finish. It used to call ``set_result`` on them, which a
task never accepts: the first call raised and the closes were orphaned.
"""

import asyncio

import pytest

from hyperscale.core.engines.client.playwright.mercury_sync_playwright_connection import (
    MercurySyncPlaywrightConnection,
)


class RecordingSession:
    """A browser session whose close is a real coroutine that yields once."""

    def __init__(self) -> None:
        self.closed_with: dict[str, object] | None = None

    async def close(self, run_before_unload: bool | None, reason: str | None, timeout: float) -> None:
        await asyncio.sleep(0)
        self.closed_with = {"run_before_unload": run_before_unload, "reason": reason, "timeout": timeout}


class FailingSession:
    async def close(self, run_before_unload: bool | None, reason: str | None, timeout: float) -> None:
        await asyncio.sleep(0)
        raise ConnectionResetError("browser already gone")


@pytest.mark.asyncio
async def test_close_closes_every_session_and_releases_its_tasks() -> None:
    connection = MercurySyncPlaywrightConnection()
    sessions = [RecordingSession() for _ in range(3)]
    connection.sessions.extend(sessions)

    connection.close(reason="run ended")
    assert len(connection._closing_sessions) == len(sessions)
    while connection._closing_sessions:
        await asyncio.sleep(0)

    assert all(session.closed_with is not None for session in sessions)
    assert {session.closed_with["reason"] for session in sessions} == {"run ended"}


@pytest.mark.asyncio
async def test_a_failing_session_close_is_retrieved_not_left_unobserved() -> None:
    connection = MercurySyncPlaywrightConnection()
    connection.sessions.extend([FailingSession(), RecordingSession()])
    unretrieved: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _loop, context: unretrieved.append(context))
    try:
        connection.close()
        while connection._closing_sessions:
            await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(None)

    assert not unretrieved
