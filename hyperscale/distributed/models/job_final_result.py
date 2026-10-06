"""Wire model ``JobFinalResult`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .workflow_result import WorkflowResult


@dataclass(slots=True)
class JobFinalResult(Message):
    """
    Final result for a job from one datacenter.

    Sent from Manager to Gate (or directly to Client if no gates).
    Contains per-workflow results and aggregated stats.
    """

    job_id: str  # Job identifier
    datacenter: str  # Reporting datacenter
    status: str  # COMPLETED | FAILED | PARTIAL
    workflow_results: list["WorkflowResult"] = field(default_factory=list)
    total_completed: int = 0  # Total successful actions
    total_failed: int = 0  # Total failed actions
    errors: list[str] = field(default_factory=list)  # All error messages
    elapsed_seconds: float = 0.0  # Max elapsed across workflows
    # Legacy wire field — see ``WorkflowResultPush.fence_token`` for the
    # split-domain rationale. Receivers must use
    # ``manager_fence_token`` for manager-leadership gating and never
    # apply the legacy ``fence_token`` to gate-leadership checks.
    fence_token: int = 0
    # Producer identity / split-fence domains / data-plane sequence.
    # See ``WorkflowResultPush`` for full semantics; the contract is
    # identical for terminal per-DC final results.
    producer_id: str = ""
    producer_addr: tuple[str, int] | None = None
    producer_role: str = ""
    manager_fence_token: int = 0
    gate_fence_token: int = 0
    result_sequence: int = 0
