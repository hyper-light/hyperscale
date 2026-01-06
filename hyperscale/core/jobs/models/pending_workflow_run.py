import asyncio
from dataclasses import dataclass
from typing import List

from hyperscale.core.graph.workflow import Workflow


@dataclass(slots=True)
class PendingWorkflowRun:
    """Tracks a workflow pending dispatch or in-flight execution."""
    workflow_name: str
    workflow: Workflow
    dependencies: set[str]
    completed_dependencies: set[str]
    threads: int
    workflow_vus: List[int]
    ready_event: asyncio.Event
    dispatched: bool
    completed: bool
    failed: bool

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
