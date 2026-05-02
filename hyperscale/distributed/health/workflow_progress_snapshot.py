"""
WorkflowProgressSnapshot — multi-dimensional progress signal for AD-26
extension decisions (Phase H3).

A worker reports progress along three independent integer dimensions
plus a monotonic timestamp. The manager evaluates extension requests
against the strictest possible monotonic-progress criterion: every
dimension must be non-regressing and at least one must strictly
advance. This is tamper-resistant by construction — defeating the
check requires the worker to advance counters across multiple code
paths simultaneously, not just bump a single number.

Combined with the manager-side throughput witness (H6 BOCPD detector
on AD-19 ``health_throughput`` heartbeat field) the multi-witness
decision in H5 catches:

* Wedged workflows that fake progress on a single dimension (other
  dimensions regress or stagnate -> denied)
* Honestly-slow workflows whose throughput sketches still report
  forward motion (extensions granted)
* Truly stuck workflows (every dimension stalls -> denied,
  AD-26 max_extensions cap kicks in)

Universal across workflow shapes:

* **Load tests** — `cores_completed` rises as VUs finish their
  iterations, `actions_completed` rises with HTTP requests,
  `step_transitions` rises with stage advances.
* **ETL/batch** — `cores_completed` rises with chunks, `actions_
  completed` rises with row writes, `step_transitions` rises with
  pipeline-stage advances.
* **Single-action probes** — `step_transitions` and `actions_
  completed` rise as the probe runs even before the (single) core
  finishes.

All counters are unbounded ``int`` — Python integers don't overflow,
so monotonic increase is well-defined even for very long-running
workflows.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class WorkflowProgressSnapshot:
    """
    Tamper-resistant progress snapshot used for AD-26 extension
    decisions.

    All counters are integer monotonic on the worker side. The
    manager cross-validates with a separately-reported throughput
    witness (AD-19 ``health_throughput``) before granting an
    extension; see H5/H6.

    Frozen so snapshots can be safely compared, hashed, and
    persisted in ``TimeoutTrackingState`` without callers
    accidentally mutating the manager's last-known reference.

    Attributes:
        workflow_id: Stable identifier for the workflow this
            snapshot describes. Bound to the dispatcher-issued
            ``WorkflowDispatch.workflow_id`` (which already
            includes the leader fence token for AD-10 / AD-34
            staleness detection).
        cores_completed: PRIMARY counter. Number of cores that have
            finished their assigned VU iterations. Monotonically
            non-decreasing on the worker.
        cores_total: Workflow-level ``vus`` allocation for this
            sub-workflow. Static for the lifetime of one
            ``WorkflowDispatch`` (re-dispatch produces a new
            ``workflow_id`` and resets the counter origin).
        step_transitions: SECONDARY counter. Number of AD-33 step
            state-machine transitions observed since dispatch
            (e.g. PENDING→RUNNING, RUNNING→COMPLETED). Catches
            in-flight cores that are doing meaningful work even
            though no core has finished yet.
        actions_completed: TERTIARY counter. Sum of
            ``StepStats.completed_count`` across in-flight steps —
            captures action-level progress for workflows where each
            core runs many actions.
        snapshot_time: ``time.monotonic()`` on the worker when the
            snapshot was constructed. Used by the throughput witness
            for time-windowed velocity calculations and by the
            rate-limiter ``min_between_extensions_seconds`` check.
    """

    workflow_id: str
    cores_completed: int
    cores_total: int
    step_transitions: int
    actions_completed: int
    snapshot_time: float

    def all_non_regressed(self, other: "WorkflowProgressSnapshot") -> bool:
        """Every dimension is at least as large as ``other``.

        Returns True iff no counter regressed. A False result is a
        red flag — counters in honest worker code never decrease, so
        regression is either a clock skew (different worker
        instance after restart) or an attempt to game the extension
        system.
        """
        return (
            self.cores_completed >= other.cores_completed
            and self.step_transitions >= other.step_transitions
            and self.actions_completed >= other.actions_completed
        )

    def any_advanced(self, other: "WorkflowProgressSnapshot") -> bool:
        """At least one dimension strictly increased relative to ``other``.

        Required (in addition to ``all_non_regressed``) for the
        manager to consider this a meaningful step forward worth
        granting an extension for.
        """
        return (
            self.cores_completed > other.cores_completed
            or self.step_transitions > other.step_transitions
            or self.actions_completed > other.actions_completed
        )

    def is_meaningful_progress(
        self, other: "WorkflowProgressSnapshot"
    ) -> bool:
        """All non-regressed AND at least one advanced.

        The strict-monotonic-progress criterion the AD-26 extension
        decision uses. Both halves are required:

        * ``all_non_regressed`` alone would let a stuck workflow
          (everything equal) get a free extension.
        * ``any_advanced`` alone would accept partial progress that
          masks regression on another dimension — gameable.
        """
        return self.all_non_regressed(other) and self.any_advanced(other)

    @classmethod
    def initial(cls, workflow_id: str, cores_total: int) -> "WorkflowProgressSnapshot":
        """Return the zero-progress baseline for ``workflow_id``.

        Used as the starting point for the manager's per-workflow
        last-snapshot before any extension request has been
        observed. Snapshot-time is left at 0.0 so the rate-limit
        check correctly treats the baseline as "infinitely long
        ago" and does not reject the first request.
        """
        return cls(
            workflow_id=workflow_id,
            cores_completed=0,
            cores_total=cores_total,
            step_transitions=0,
            actions_completed=0,
            snapshot_time=0.0,
        )
