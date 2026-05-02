"""
Expectations — declarative assertions about a workload's outcome.

Each ``Expectation`` is a pure function over a ``WorkloadObservations``
snapshot. The driver collects all expectation results so a single
failure does not mask the rest, and surfaces them together at workload
exit. Phase 2 ships the two simplest ones; the catalog grows as
fault-injection scenarios introduce richer assertions
(ExpectStatusCallbacks, ExpectProgressUpdates, ExpectNoFailures).
"""

from dataclasses import dataclass, field
from typing import Protocol


@dataclass(slots=True)
class WorkloadObservations:
    """What the driver actually observed during the workload's lifetime.

    Populated incrementally by callback handlers on the client. Pure
    data — no async, no IO — so expectations can evaluate it cheaply.
    """

    submitted_job_ids: list[str] = field(default_factory=list)
    """Job IDs returned from successful submission."""

    workflow_results: dict[str, str] = field(default_factory=dict)
    """workflow_name -> final status (e.g. "completed", "failed")."""

    status_update_count: int = 0
    """Total ``JobStatusPush`` callbacks received."""

    progress_update_count: int = 0
    """Total ``WindowedStatsPush`` callbacks received."""

    completion_seconds: float | None = None
    """Wall-clock seconds from first submit to last completion. ``None`` if
    the workload did not finish before evaluation."""

    submit_errors: list[str] = field(default_factory=list)
    """Errors encountered during submission itself."""


@dataclass(slots=True, frozen=True)
class ExpectationResult:
    """Outcome of evaluating one expectation. Aggregated by the driver."""

    name: str
    holds: bool
    detail: str = ""


class Expectation(Protocol):
    """Sentinel protocol every Expectation implements.

    ``evaluate`` is sync because expectations are pure functions over the
    already-collected observations.
    """

    name: str

    def evaluate(self, observations: WorkloadObservations) -> ExpectationResult:
        ...


@dataclass(slots=True, frozen=True)
class ExpectAllWorkflowsComplete:
    """Every workflow named in the submitted set must reach 'completed'.

    The driver populates ``workflow_results`` from
    ``on_workflow_result`` callbacks. Anything not in 'completed' state
    (failed, cancelled, missing entirely) fails the expectation with a
    detail line listing the offending workflow names.
    """

    expected_workflow_names: list[str]
    name: str = "ExpectAllWorkflowsComplete"

    def evaluate(self, observations: WorkloadObservations) -> ExpectationResult:
        expected = set(self.expected_workflow_names)
        actual = observations.workflow_results
        missing = sorted(expected - set(actual.keys()))
        wrong_status = sorted(
            name for name in expected & set(actual.keys())
            if actual[name] != "completed"
        )
        if not missing and not wrong_status:
            return ExpectationResult(name=self.name, holds=True)
        parts: list[str] = []
        if missing:
            parts.append(f"missing={missing}")
        if wrong_status:
            parts.append(
                f"wrong_status={[(name, actual[name]) for name in wrong_status]}"
            )
        return ExpectationResult(
            name=self.name,
            holds=False,
            detail="; ".join(parts),
        )


@dataclass(slots=True, frozen=True)
class ExpectCompletionWithin:
    """The workload must finish within the given wall-clock budget."""

    seconds: float
    name: str = "ExpectCompletionWithin"

    def evaluate(self, observations: WorkloadObservations) -> ExpectationResult:
        if observations.completion_seconds is None:
            return ExpectationResult(
                name=self.name,
                holds=False,
                detail=f"workload did not complete (budget {self.seconds:.1f}s)",
            )
        if observations.completion_seconds > self.seconds:
            return ExpectationResult(
                name=self.name,
                holds=False,
                detail=(
                    f"completion took {observations.completion_seconds:.2f}s, "
                    f"budget {self.seconds:.1f}s"
                ),
            )
        return ExpectationResult(name=self.name, holds=True)
