"""
Long-running workflow for cancellation / mid-flight fault scenarios.

Sleeps for a fixed duration so cancel / partition tests can land
their fault while the workflow is genuinely in flight rather than
racing a fast-completing workflow.

Defined under ``hyperscale.distributed.testing.workflows`` so the
security allowlist (which restricts workflow imports to
``hyperscale.*`` and the standard library) accepts it. Test-side
inline workflow classes are rejected by the allowlist and would
break submission with ``SecurityError``.
"""

import asyncio

from hyperscale.graph import Workflow, step


class LongRunningWorkflow(Workflow):
    """One-step workflow: sleep 30 s, return ``{"ok": True}``.

    Used by cancellation and mid-workload fault scenarios so the
    fault arrives while execution is in flight. Default ``vus=1``
    keeps the workload single-worker for predictable scheduling.
    """

    vus: int = 1
    duration: str = "30s"

    @step()
    async def long_step(self) -> dict:
        await asyncio.sleep(30.0)
        return {"ok": True}
