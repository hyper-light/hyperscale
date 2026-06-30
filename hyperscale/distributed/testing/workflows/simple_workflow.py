"""
Minimal workflow with no external dependencies.

Runs a single step that sleeps briefly and returns a static value, so
harness smoke scenarios can verify the dispatch / execution / result-
push pipeline without depending on httpbin or any other external
service. Default vus=1 / duration=1s keeps the workload short.
"""

import asyncio

from hyperscale.graph import Workflow, step

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()


class SimpleWorkflow(Workflow):
    """One-step workflow: sleep 50 ms, return ``{"ok": True}``."""

    vus: int = 1
    duration: str = "1s"

    @step()
    async def noop(self) -> dict:
        await _DEFAULT_CLOCK.sleep(0.05)
        return {"ok": True}
