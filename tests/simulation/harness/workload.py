"""
WorkloadDriver — submits a `WorkloadSpec` against the harness's cluster
and aggregates observations for `Expectation` evaluation.

Phase 2 deliverable: minimal but real. Owns a `HyperscaleClient` for the
lifetime of an ``async with`` block, dispatches submissions per the
spec's pattern, captures status / progress / result callbacks into
`WorkloadObservations`, and evaluates all registered expectations on
exit. A failed expectation triggers the harness's diagnostic dump
before raising — same contract as condition timeouts and invariant
violations.

Patterns supported in Phase 2: ``SINGLE`` and ``PARALLEL``. Other
patterns (STAGGERED, SUSTAINED, BURST) land alongside the fault-
injection scenarios in Phase 3.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient

from tests.simulation.harness.conditions import dc_has_leader, wait_until
from tests.simulation.harness.errors import HarnessError
from tests.simulation.harness.expectations import (
    Expectation,
    ExpectationResult,
    WorkloadObservations,
)
from tests.simulation.harness.submission import (
    Submission,
    SubmissionPattern,
    WorkloadSpec,
)
from tests.simulation.harness.server_handle import ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


class WorkloadFailure(HarnessError):
    """One or more expectations failed at workload exit."""


@dataclass(slots=True)
class WorkloadDriver:
    """Owns the client lifetime and observation collection for one workload.

    Used as ``async with cluster.workload(spec) as workload:`` — the
    enter starts the client, the exit stops it AND evaluates all
    expectations, raising :class:`WorkloadFailure` if any fail.
    """

    harness: "ClusterHarness"
    spec: WorkloadSpec
    client_port: int

    _client: HyperscaleClient | None = field(init=False, default=None)
    _observations: WorkloadObservations = field(
        init=False, default_factory=WorkloadObservations
    )
    _started_at: float = field(init=False, default=0.0)
    _all_complete_event: asyncio.Event = field(init=False)
    _expected_workflow_names: set[str] = field(init=False, default_factory=set)

    @property
    def observations(self) -> WorkloadObservations:
        return self._observations

    async def __aenter__(self) -> "WorkloadDriver":
        self._all_complete_event = asyncio.Event()
        targets = self._select_routing_targets()
        if not targets:
            raise HarnessError(
                "workload requires at least one routing target "
                "(gates for L3, managers for L1/L2); cluster has none"
            )
        # Submit_job requires a known DC leader. The harness's general
        # _stabilize wait does NOT gate on leader election because for
        # multi-manager DCs leadership emerges only after SWIM has fully
        # converged peer membership — coupling the two would force every
        # lifecycle test to also wait for election, even those that never
        # touch the submit path. Instead, gate it here per DC, where
        # submission actually depends on a leader being known.
        for dc_id, managers in self.harness._managers_by_dc.items():
            if not managers:
                continue
            await wait_until(
                dc_has_leader(managers),
                timeout=30.0,
                poll=0.5,
                description=f"DC {dc_id} elects a leader",
                on_fail=lambda dc=dc_id: self.harness.dump_diagnostics(
                    reason=f"workload waited for DC {dc} leader; never arrived"
                ),
            )
        self._client = HyperscaleClient(
            host=self.harness.spec.host,
            port=self.client_port,
            env=Env(MERCURY_SYNC_LOG_LEVEL="error"),
            **targets,
        )
        await self._client.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client is not None:
            try:
                await asyncio.wait_for(self._client.stop(), timeout=10.0)
            except (asyncio.TimeoutError, Exception):
                pass
        if exc_type is not None:
            return
        results = self.evaluate_expectations()
        failures = [r for r in results if not r.holds]
        if failures:
            await self.harness.dump_diagnostics(
                reason=f"workload expectations failed: {[r.name for r in failures]}"
            )
            joined = "\n  - ".join(f"{r.name}: {r.detail}" for r in failures)
            raise WorkloadFailure(f"workload expectations failed:\n  - {joined}")

    async def submit_and_wait(self) -> None:
        """Submit per the spec's pattern, then wait for all workflows to complete.

        The wait is bounded by the largest ``Submission.timeout_seconds``
        across the spec — workload timeout enforcement is the spec's
        responsibility, not the harness's. ``ExpectCompletionWithin``
        evaluates against the actual elapsed time.
        """
        if self._client is None:
            raise RuntimeError("call submit_and_wait inside the async-with block")
        if not self.spec.submissions:
            raise HarnessError("WorkloadSpec has no submissions")

        self._expected_workflow_names = {
            workflow_factory.__name__
            if hasattr(workflow_factory, "__name__")
            else type(workflow_factory()).__name__
            for submission in self.spec.submissions
            for _deps, workflow_factory in submission.workflows
        }

        self._started_at = time.monotonic()
        if self.spec.pattern is SubmissionPattern.SINGLE:
            if len(self.spec.submissions) != 1:
                raise HarnessError(
                    "SubmissionPattern.SINGLE expects exactly one submission; "
                    f"got {len(self.spec.submissions)}"
                )
            await self._submit_one(self.spec.submissions[0])
        elif self.spec.pattern is SubmissionPattern.PARALLEL:
            await asyncio.gather(
                *(self._submit_one(s) for s in self.spec.submissions)
            )
        else:
            raise HarnessError(
                f"SubmissionPattern {self.spec.pattern} not yet supported in Phase 2"
            )

        await self._wait_for_completion()

    def evaluate_expectations(self) -> list[ExpectationResult]:
        """Run every registered expectation against the observations."""
        return [
            expectation.evaluate(self._observations)
            for expectation in self.spec.expectations
        ]

    async def _submit_one(self, submission: Submission) -> None:
        if self._client is None:
            return
        try:
            workflows = [
                (deps, factory()) for deps, factory in submission.workflows
            ]
            job_id = await self._client.submit_job(
                workflows=workflows,
                vus=submission.vus,
                timeout_seconds=submission.timeout_seconds,
                datacenter_count=submission.dc_count,
                on_status_update=self._on_status_update,
                on_progress_update=self._on_progress_update,
                on_workflow_result=self._on_workflow_result,
            )
            self._observations.submitted_job_ids.append(job_id)
        except Exception as submit_error:
            self._observations.submit_errors.append(
                f"{type(submit_error).__name__}: {submit_error}"
            )

    async def _wait_for_completion(self) -> None:
        budget = max(s.timeout_seconds for s in self.spec.submissions)
        try:
            await asyncio.wait_for(self._all_complete_event.wait(), timeout=budget)
        except asyncio.TimeoutError:
            # Leave completion_seconds None — ExpectCompletionWithin will fail
            # with a clear "did not complete" message rather than a flaky pass.
            return
        self._observations.completion_seconds = time.monotonic() - self._started_at

    def _on_status_update(self, push: object) -> None:
        self._observations.status_update_count += 1

    def _on_progress_update(self, push: object) -> None:
        self._observations.progress_update_count += 1

    def _on_workflow_result(self, push: object) -> None:
        workflow_name = getattr(push, "workflow_name", None)
        status = getattr(push, "status", None)
        if not workflow_name or status is None:
            return
        self._observations.workflow_results[workflow_name] = status
        if (
            self._expected_workflow_names
            and self._expected_workflow_names <= set(self._observations.workflow_results)
        ):
            self._all_complete_event.set()

    def _select_routing_targets(self) -> dict[str, list[tuple[str, int]]]:
        """Return either ``{"gates": [...]}`` (L3) or ``{"managers": [...]}``."""
        gates = [
            (h.host, h.tcp_port)
            for h in self.harness.all_handles()
            if h.kind is ServerKind.GATE
        ]
        if gates:
            return {"gates": gates}
        managers = [
            (h.host, h.tcp_port)
            for h in self.harness.all_handles()
            if h.kind is ServerKind.MANAGER
        ]
        if managers:
            return {"managers": managers}
        return {}
