"""Wire model ``WorkflowResultPush`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.reporting.common.results_types import WorkflowStats
from .message import Message
from .workflow_dc_result import WorkflowDCResult


@dataclass(slots=True)
class WorkflowResultPush(Message):
    """
    Push notification for a completed workflow's results.

    Sent from Manager to Client (aggregated) or Manager to Gate (raw) as soon
    as each workflow completes, without waiting for the entire job to finish.

    For client-bound from manager: results contains single aggregated WorkflowStats, per_dc_results empty
    For client-bound from gate: results contains cross-DC aggregated, per_dc_results has per-DC breakdown
    For gate-bound: results contains raw per-core WorkflowStats list for cross-DC aggregation
    """

    job_id: str  # Parent job
    workflow_id: str  # Workflow instance ID
    workflow_name: str  # Workflow class name
    datacenter: str  # Source datacenter (or "aggregated" for cross-DC)
    status: str  # COMPLETED | FAILED
    # Legacy field kept for wire-compat with older nodes. New code uses
    # ``manager_fence_token`` / ``gate_fence_token`` (the two distinct
    # fence domains) and ``result_sequence`` (data-plane ordering).
    # Receivers must NOT apply this to gate-leader fencing — gate-leader
    # claims live on dedicated messages (``GateJobReplica``,
    # ``JobLeaderGateTransfer``). See ``WorkflowResultPush`` header for
    # the producer/fence split rationale.
    fence_token: int = 0
    results: list[WorkflowStats] = field(default_factory=list)
    error: str | None = None  # Error message if failed
    elapsed_seconds: float = 0.0
    # Per-DC breakdown (populated when gate aggregates cross-DC results)
    per_dc_results: list[WorkflowDCResult] = field(default_factory=list)
    # Completion timestamp for ordering
    completed_at: float = 0.0  # Unix timestamp when workflow completed
    # Whether this workflow contains test hooks (determines aggregation behavior)
    # True: aggregate results using merge_results()
    # False: return raw list of WorkflowStats per DC
    is_test: bool = True
    # Client callback for gate-owned L3 delivery. Managers include this so any
    # surviving gate can deliver or aggregate the result without relying on
    # callback state replicated from the original accepting gate.
    callback_addr: tuple[str, int] | None = None
    # Expected datacenters for cross-DC aggregation. ``target_dcs`` is preferred
    # when known; ``target_dc_count`` is a fallback when only cardinality is
    # known by the sender.
    target_dcs: list[str] = field(default_factory=list)
    target_dc_count: int = 0
    # True when this payload is already client-ready and should not be treated
    # as raw per-DC input for gate aggregation.
    is_client_ready: bool = False
    # ----- Producer identity (data-plane provenance) -----
    # Stamped by the originating node. The receiver gates on the
    # producer's leadership domain — manager pushes are gated against
    # per-DC manager leadership; gate pushes (cross-gate aggregation
    # forwards) are gated against per-job gate leadership.
    producer_id: str = ""
    producer_addr: tuple[str, int] | None = None
    producer_role: str = ""  # "manager" | "gate" | "" (legacy/unset)
    # ----- Split-fence domains -----
    # ``manager_fence_token`` is the manager's per-DC leadership
    # generation at the moment the push was built. The receiver
    # rejects only when this is strictly older than the per-DC
    # manager-leadership generation it has on record (i.e. the
    # producing manager is no longer the DC leader). Default 0 is
    # treated as "unknown / wire-compat fallback" and is accepted.
    manager_fence_token: int = 0
    # ``gate_fence_token`` is the gate's per-job leadership fence as
    # known to the producer. Managers set this to 0 (they don't claim
    # gate leadership). Cross-gate forwards stamp the producing
    # gate's current fence. The receiver only applies this to other
    # gate's pushes, not to manager pushes.
    gate_fence_token: int = 0
    # ----- Data-plane idempotency -----
    # Per-``(job_id, workflow_id, datacenter)`` monotonic sequence
    # number stamped by the producer, independent of either fence
    # domain: the order of one producer's pushes for the triple. A gate
    # aggregates each workflow once (``_finalized_workflow_results``)
    # and a client applies each workflow's result once, so neither
    # orders by it.
    result_sequence: int = 0
