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

    Three-step submission API for fault-injection scenarios:

    1. ``submit()`` — queue the spec's submissions. Returns when the
       job_ids have been registered; does NOT wait for execution.
    2. ``wait_until_running(timeout)`` — block until at least one
       submitted job's ``JobStatusPush`` reports ``JobStatus.RUNNING``
       (or a downstream state). The kill / partition / pause must
       happen between this and ``wait_for_completion`` to land
       mid-workload.
    3. ``wait_for_completion()`` — block until every expected
       workflow has reported a result, bounded by the spec's
       ``timeout_seconds``.

    ``submit_and_wait()`` is a convenience that does ``submit`` +
    ``wait_for_completion`` back-to-back (no ``wait_until_running``)
    for callers that don't need to inject faults during execution.
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
    _running_event: asyncio.Event = field(init=False)
    _expected_workflow_names: set[str] = field(init=False, default_factory=set)

    @property
    def observations(self) -> WorkloadObservations:
        return self._observations

    async def __aenter__(self) -> "WorkloadDriver":
        self._all_complete_event = asyncio.Event()
        self._running_event = asyncio.Event()
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
        await self._wait_for_l3_routing_ready()
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

    async def submit(self) -> None:
        """Submit per the spec's pattern. Returns once submissions are
        queued; does NOT wait for completion or even dispatch.

        Pair with ``wait_until_running`` and ``wait_for_completion``
        when fault injection needs to land mid-workload.
        """
        if self._client is None:
            raise RuntimeError("call submit inside the async-with block")
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
        await self._raise_submit_errors_if_any()

    async def wait_until_running(self, timeout: float = 30.0) -> None:
        """Block until at least one submitted job reaches a dispatched
        state.

        Detection: the gate/manager pushes ``JobStatusPush.status`` =
        ``JobStatus.RUNNING`` once the first workflow on the job has
        actually started executing on a worker. Earlier states
        (SUBMITTED, QUEUED, DISPATCHING) are pre-dispatch — killing
        a worker now would land before the workflow ever reached it.

        Raises ``HarnessError`` on timeout. The harness's diagnostic
        dumper is invoked so the failure surface includes the cluster
        snapshot (which workers exist, which managers are leader,
        in-flight RPCs).
        """
        if self._client is None:
            raise RuntimeError(
                "call wait_until_running inside the async-with block"
            )
        try:
            await asyncio.wait_for(self._running_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            # Print state directly so pytest captures it even when its
            # diagnostic-dump pipeline elides the substituted f-string.
            # Critical for L3 path debugging where the submission may
            # have succeeded (job_id captured) but the reverse status
            # push never reached the client.
            print(
                f"[WAIT-RUNNING-TIMEOUT] "
                f"submitted_jobs={self._observations.submitted_job_ids} "
                f"submit_errors={self._observations.submit_errors} "
                f"status_update_count={self._observations.status_update_count} "
                f"progress_update_count={self._observations.progress_update_count}",
                flush=True,
            )
            await self.harness.dump_diagnostics(
                reason=(
                    f"workload waited {timeout:.1f}s for first workflow "
                    f"to reach RUNNING; never observed. "
                    f"submitted_jobs={self._observations.submitted_job_ids} "
                    f"submit_errors={self._observations.submit_errors}"
                )
            )
            raise HarnessError(
                f"workload did not reach RUNNING within {timeout:.1f}s"
            ) from None

    async def wait_for_completion(self) -> None:
        """Block until every expected workflow reports a result, bounded
        by the largest ``Submission.timeout_seconds`` in the spec.

        On timeout, ``observations.completion_seconds`` is left
        ``None`` so ``ExpectCompletionWithin`` fails with a clear
        "did not complete" message rather than a flaky pass.
        """
        await self._wait_for_completion()

    async def submit_and_wait(self) -> None:
        """Convenience: ``submit`` + ``wait_for_completion``.

        Backward-compat for callers that don't need to inject faults
        mid-workload. ``wait_until_running`` is intentionally
        skipped — the all-complete event is the only completion
        signal needed for the no-fault path.
        """
        await self.submit()
        await self._wait_for_completion()

    async def cancel(
        self,
        job_id: str | None = None,
        reason: str = "harness-injected",
        timeout: float = 30.0,
    ) -> tuple[bool, list[str]]:
        """Cancel a submitted job and await cancellation completion.

        Returns ``(success, errors)`` from ``await_job_cancellation``.
        When ``job_id`` is omitted, cancels the most-recently-
        submitted job — the common case for single-submission
        scenarios.

        The cancellation flow is:

        1. ``client.cancel_job(job_id)`` posts a cancellation request
           to the manager / gate that owns the job. Handles
           leadership redirection via ``max_redirects`` and retries
           transient errors via ``max_retries``.
        2. ``client.await_job_cancellation(job_id, timeout)`` blocks
           on the cancellation-complete callback that propagates
           through the manager → gate → client chain.

        Raises ``HarnessError`` if no job has been submitted yet, or
        if the cancellation does not complete within ``timeout``.
        """
        if self._client is None:
            raise RuntimeError("call cancel inside the async-with block")
        target_job = job_id
        if target_job is None:
            if not self._observations.submitted_job_ids:
                raise HarnessError(
                    "cancel(): no job_id provided and no submissions have "
                    "registered a job_id yet. Call submit() first or pass "
                    "an explicit job_id."
                )
            target_job = self._observations.submitted_job_ids[-1]
        await self._client.cancel_job(
            job_id=target_job, reason=reason, timeout=timeout
        )
        try:
            success, errors = await asyncio.wait_for(
                self._client.await_job_cancellation(target_job, timeout=timeout),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            await self.harness.dump_diagnostics(
                reason=(
                    f"cancellation of job {target_job} did not complete "
                    f"within {timeout:.1f}s"
                )
            )
            raise HarnessError(
                f"cancellation of job {target_job} timed out after "
                f"{timeout:.1f}s"
            ) from None
        return success, errors

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

    async def _raise_submit_errors_if_any(self) -> None:
        if self._observations.submit_errors:
            await self.harness.dump_diagnostics(
                reason=(
                    "workload submission failed: "
                    f"{self._observations.submit_errors}"
                )
            )
            raise HarnessError(
                "workload submission failed: "
                f"{self._observations.submit_errors}"
            )

        if not self._observations.submitted_job_ids:
            await self.harness.dump_diagnostics(
                reason="workload submission produced no job ids"
            )
            raise HarnessError("workload submission produced no job ids")

    async def _wait_for_l3_routing_ready(self) -> None:
        gates = self.harness.gates
        if not gates:
            return

        await wait_until(
            self._l3_routing_ready,
            timeout=30.0,
            poll=0.5,
            description="L3 gate routing ready",
            on_fail=lambda: self.harness.dump_diagnostics(
                reason="workload waited for L3 gate routing readiness"
            ),
            failure_detail=self._l3_routing_snapshot,
        )

    def _l3_routing_ready(self) -> bool:
        for gate in self.harness.gates:
            if not gate.started:
                return False
            if not self._gate_has_usable_datacenter(gate):
                return False
        return True

    def _gate_has_usable_datacenter(self, gate: "ServerHandle") -> bool:
        candidates = gate.instance._get_datacenter_candidates_for_router()
        return any(
            candidate.health_bucket in {"HEALTHY", "BUSY"}
            and candidate.total_managers > 0
            and candidate.healthy_managers > 0
            for candidate in candidates
        )

    def _l3_routing_snapshot(self) -> str:
        snapshots: list[str] = []
        for gate in self.harness.gates:
            try:
                candidates = gate.instance._get_datacenter_candidates_for_router()
                candidate_summary = [
                    {
                        "dc": candidate.datacenter_id,
                        "health": candidate.health_bucket,
                        "total_mgrs": candidate.total_managers,
                        "healthy_mgrs": candidate.healthy_managers,
                        "cores": candidate.available_cores,
                        "total_cores": candidate.total_cores,
                    }
                    for candidate in candidates
                ]
            except Exception as error:
                candidate_summary = [{"error": f"{type(error).__name__}: {error}"}]
            snapshots.append(f"{gate.node_id}={candidate_summary}")
        return "L3 routing candidates: " + "; ".join(snapshots)

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
        # Fire the dispatch-detected event the first time we see any
        # post-dispatch state. RUNNING is the canonical "first workflow
        # is executing on a worker" signal; downstream states
        # (COMPLETING, COMPLETED, FAILED, etc.) imply it transited
        # RUNNING earlier and the push for that transition was
        # delivered out of order or coalesced.
        status = getattr(push, "status", None)
        if status in {
            "running",
            "completing",
            "completed",
            "failed",
            "cancelled",
            "timeout",
        }:
            if not self._running_event.is_set():
                self._running_event.set()

    def _mark_running_if_dispatched(self) -> None:
        """Idempotent helper for the running-event trip wire.

        Called from progress-update and workflow-result callbacks to
        cover the path where the gate / manager coalesces multiple
        state pushes (a fast workflow may produce a progress update
        or final result before any RUNNING status push reaches the
        client). Either signal definitionally implies the workflow
        was dispatched and at least started.
        """
        if not self._running_event.is_set():
            self._running_event.set()

    def _on_progress_update(self, push: object) -> None:
        self._observations.progress_update_count += 1
        # Progress updates only flow once a workflow has started
        # producing telemetry, so the dispatch trip wire fires here
        # too — a gate that coalesces JobStatusPush.RUNNING into a
        # downstream state still emits per-workflow progress updates
        # while execution runs.
        self._mark_running_if_dispatched()

    def _on_workflow_result(self, push: object) -> None:
        workflow_name = getattr(push, "workflow_name", None)
        status = getattr(push, "status", None)
        if not workflow_name or status is None:
            return
        results = getattr(push, "results", [])
        if isinstance(results, list):
            self._observations.workflow_result_stats_counts[workflow_name] = len(
                results
            )
        else:
            self._observations.workflow_result_stats_counts[workflow_name] = 0

        per_dc_results = getattr(push, "per_dc_results", [])
        if isinstance(per_dc_results, list):
            self._observations.workflow_result_per_dc_stats_counts[workflow_name] = sum(
                1
                for dc_result in per_dc_results
                if getattr(dc_result, "stats", None) is not None
            )

        # A final result implies the workflow ran to terminal state.
        self._mark_running_if_dispatched()
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
