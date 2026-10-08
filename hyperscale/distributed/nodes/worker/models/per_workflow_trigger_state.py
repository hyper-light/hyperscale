"""``_PerWorkflowTriggerState`` -- pickled under the namespace
``hyperscale.distributed.nodes.worker.extension_trigger`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass
from hyperscale.distributed.health.workflow_progress_snapshot import WorkflowProgressSnapshot


@dataclass(slots=True)
class _PerWorkflowTriggerState:
    """Per-workflow trigger bookkeeping kept on the worker side."""

    last_request_snapshot: WorkflowProgressSnapshot | None = None
    last_request_time: float = 0.0
    last_request_count: int = 0
