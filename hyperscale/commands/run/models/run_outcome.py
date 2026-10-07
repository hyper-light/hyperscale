from dataclasses import dataclass, field

from hyperscale.reporting.common.results_types import WorkflowStats


@dataclass(slots=True, frozen=True)
class RunOutcome:
    """How a ``hyperscale run workflow`` run ended: the line that reports
    it, the exit status the command ends with (0: it completed), and each
    workflow's final results by name -- the ones its reporters were given
    (none for a workflow that has no final results)."""

    line: str
    exit_status: int
    workflow_stats: dict[str, WorkflowStats] = field(default_factory=dict)
