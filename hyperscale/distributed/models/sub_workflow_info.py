"""Wire model ``SubWorkflowInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.jobs`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models.distributed import WorkflowProgress, WorkflowFinalResult

from .tracking_token import TrackingToken


@dataclass(slots=True)
class SubWorkflowInfo:
    token: TrackingToken
    parent_token: TrackingToken
    cores_allocated: int
    fence_token: int = 0
    progress: WorkflowProgress | None = None
    result: WorkflowFinalResult | None = None
    dispatched_context: bytes = b""
    dispatched_version: int = 0
    superseded: bool = False
    # _DEFAULT_CLOCK.monotonic() when this sub-workflow began executing on
    # its current worker (dispatch or reassignment) — the start AD-43's
    # remaining-time estimate measures from. Local-only; never compared
    # across nodes.
    dispatched_at: float = 0.0

    @property
    def token_str(self) -> str:
        return str(self.token)

    @property
    def worker_id(self) -> str:
        """Get worker ID from token."""
        return self.token.worker_id or ""
