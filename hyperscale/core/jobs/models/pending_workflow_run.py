import asyncio
from dataclasses import dataclass, field
from typing import List

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.jobs.workers.stage_priority import StagePriority


@dataclass(slots=True)
class PendingWorkflowRun:
    """Tracks a workflow pending dispatch or in-flight execution."""
    workflow_name: str
    workflow: Workflow
    dependencies: set[str]
    completed_dependencies: set[str]
    vus: int
    priority: StagePriority
    is_test: bool
    ready_event: asyncio.Event
    dispatched: bool
    completed: bool
    failed: bool
    # Allocated at dispatch time (not upfront)
    allocated_cores: int = 0
    allocated_vus: List[int] = field(default_factory=list)

    def is_ready(self) -> bool:
        """Check if all dependencies are satisfied and not yet dispatched."""
        return (
            self.dependencies <= self.completed_dependencies
            and not self.dispatched
            and not self.failed
        )

    def check_and_signal_ready(self) -> bool:
        """If ready for dispatch, set the event and return True."""
        if self.is_ready():
            self.ready_event.set()
            return True
        return False
