"""
Worker-deadline resolver (AD-26 / AD-34 Phase H2).

Resolves the deadline a worker should observe for a given workflow
under the explicit override hierarchy:

    1. ``JobSubmission.timeout_seconds_explicit == True``
        → use ``submission.timeout_seconds`` verbatim.
    2. ``Workflow.timeout`` overridden in the workflow class
       (i.e. != base ``Workflow.timeout`` default ``"30s"``)
        → use ``parse(workflow.duration) + parse(workflow.timeout)``.
       This preserves the legacy additive semantics for users who
       were already setting class-level timeouts before Phase H.
    3. Otherwise (framework default)
        → use ``parse(workflow.duration) ×
          HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER`` (default 1.5).

The single source of truth lives here so the gate, manager dispatcher,
and timeout-strategy code all derive deadlines identically. No
duplicate parsing, no drift between layers.
"""

from __future__ import annotations

from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.taskex.util.time_parser import TimeParser


# Default deadline multiplier when neither the submission nor the
# workflow class supplies an explicit timeout. Mirrors
# ``Env.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER`` so callers
# without a populated ``Env`` (tests, embedded uses) still get a
# sane fallback.
DEFAULT_TIMEOUT_MULTIPLIER: float = 1.5


def _workflow_class_timeout_overridden(workflow: Workflow) -> bool:
    """Return True if the workflow class overrode the default ``timeout``.

    Compares the instance's ``timeout`` against the base ``Workflow``
    class default. A subclass that sets ``timeout = "1m"`` (or any
    non-default string) is detected as having opted in to per-class
    timeout semantics.

    Identity comparison would miss subclasses that re-set the field
    to the same default; we use string equality for robustness.
    """
    base_default = Workflow.timeout
    instance_value = getattr(workflow, "timeout", base_default)
    return instance_value != base_default


def resolve_worker_deadline_seconds(
    workflow: Workflow,
    submission_timeout_seconds: float,
    submission_timeout_explicit: bool,
    default_multiplier: float = DEFAULT_TIMEOUT_MULTIPLIER,
) -> float:
    """Compute the worker-observed deadline for ``workflow``.

    Implements the AD-26 / AD-34 Phase H2 override hierarchy.

    Args:
        workflow: The workflow instance about to be dispatched.
            Used to read ``workflow.duration`` and ``workflow.timeout``.
        submission_timeout_seconds: The ``timeout_seconds`` field from
            ``JobSubmission``.
        submission_timeout_explicit: ``JobSubmission.timeout_seconds_explicit``.
            When True, ``submission_timeout_seconds`` wins unconditionally.
        default_multiplier: Multiplier applied to the parsed
            ``workflow.duration`` when neither override is present.
            Defaults to 1.5 (per AD-26/AD-34 design).

    Returns:
        The deadline in seconds that the worker's
        ``WorkflowExecutor.set_workflow_timeout`` should record.
    """
    if submission_timeout_explicit and submission_timeout_seconds > 0.0:
        return submission_timeout_seconds

    duration_seconds = TimeParser(workflow.duration).time

    if _workflow_class_timeout_overridden(workflow):
        timeout_buffer_seconds = TimeParser(workflow.timeout).time
        return duration_seconds + timeout_buffer_seconds

    return duration_seconds * default_multiplier
