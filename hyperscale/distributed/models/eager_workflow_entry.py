"""Wire model ``EagerWorkflowEntry`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.jobs.workers.stage_priority import StagePriority


@dataclass(slots=True)
class EagerWorkflowEntry:
    """
    Tracking entry for a workflow pending eager dispatch.

    Contains all information needed to dispatch the workflow once
    its dependencies are met and cores are available.
    """

    job_id: str  # Parent job ID
    workflow_name: str  # Workflow name (graph node)
    workflow_idx: int  # Index in job's workflow list
    workflow: Workflow  # The workflow instance
    vus: int  # Virtual users for this workflow
    priority: StagePriority  # Workflow priority
    is_test: bool  # Whether this is a test workflow
    dependencies: set[str]  # Set of workflow names this depends on
    completed_dependencies: set[str] = field(
        default_factory=set
    )  # Dependencies that have completed
    dispatched: bool = False  # Whether this workflow has been dispatched
