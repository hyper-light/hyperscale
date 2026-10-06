"""Wire model ``WorkflowInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.jobs`` (see that module)."""

import asyncio
from dataclasses import dataclass, field
from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.models.distributed import WorkflowStatus

from .tracking_token import TrackingToken


@dataclass(slots=True)
class WorkflowInfo:
    """Information about a workflow within a job."""

    token: TrackingToken  # Full tracking token (DC:manager:job:workflow)
    name: str
    workflow: Workflow | None = None
    status: WorkflowStatus = WorkflowStatus.PENDING
    sub_workflow_tokens: list[str] = field(
        default_factory=list
    )  # Sub-workflow token strings
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)
    error: str | None = None
    aggregation_error: str | None = None  # Separate from workflow error
    terminal_pushed: bool = False
    terminal_status: str | None = None
    # The workflow ids this one waits on -- its edges in the job's
    # dependency graph, linked at registration -- along which a failure
    # cascades.
    dependency_workflow_ids: frozenset[str] = frozenset()
    # A test workflow drives load: one of its hooks is a TEST hook (core's
    # rule). Its results are merged as load-test stats; any other
    # workflow's results travel as they are.
    is_test: bool = False

    @property
    def token_str(self) -> str:
        """Get token as string."""
        return str(self.token)
