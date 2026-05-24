"""
Long-running test workflow for stats aggregation scenarios.

Sleeps briefly before issuing an HTTP request so gate-tier tests can
exercise the real test-hook path and verify that L3 result aggregation
preserves metric payloads.

Defined under ``hyperscale.distributed.testing.workflows`` so the
security allowlist (which restricts workflow imports to
``hyperscale.*`` and the standard library) accepts it. Test-side
inline workflow classes are rejected by the allowlist and would
break submission with ``SecurityError``.
"""

import asyncio

from hyperscale.graph import Workflow, step

from hyperscale.testing import URL, HTTPResponse


class LongRunningTestWorkflow(Workflow):
    """One-step test workflow: sleep briefly, then return ``HTTPResponse``.

    The ``HTTPResponse`` return annotation marks the step as a test
    hook, which causes Hyperscale to collect ``WorkflowStats`` rather
    than treating the step as an action-only state update.
    """

    vus: int = 1
    duration: str = "30s"

    @step()
    async def get_httpbin(
        self,
        url: URL = 'https://httpbin.org/get',
    ) -> HTTPResponse:
        await asyncio.sleep(1)
        return await self.client.http.get(url)
