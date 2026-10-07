"""Wire model ``WorkflowProgressAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message
from .manager_info import ManagerInfo


@dataclass(slots=True, kw_only=True)
class WorkflowProgressAck(Message):
    """
    Acknowledgment for workflow progress updates.

    Includes updated manager list so workers can maintain
    accurate view of cluster topology and leadership.

    Also includes job_leader_addr for the specific job, enabling workers
    to route progress updates to the correct manager even after failover.

    Backpressure fields (AD-23):
    When the manager's stats buffer fill level reaches thresholds, it signals
    backpressure to workers via these fields. Workers should adjust their
    update behavior accordingly (throttle, batch-only, or drop non-critical).
    """

    manager_id: str  # Responding manager's node_id
    is_leader: bool  # Whether this manager is cluster leader
    healthy_managers: list[ManagerInfo]  # Current healthy managers
    # Job leader address - the manager currently responsible for this job.
    # None if the job is unknown or this manager doesn't track it.
    # Workers should update their routing to send progress to this address.
    job_leader_addr: tuple[str, int] | None = None
    # AD-23: Backpressure fields for stats update throttling
    backpressure_level: int = (
        0  # BackpressureLevel enum value (0=NONE, 1=THROTTLE, 2=BATCH, 3=REJECT)
    )
    backpressure_delay_ms: int = 0  # Suggested delay before next update (milliseconds)
    backpressure_batch_only: bool = False  # Should sender switch to batch mode?

    def __getstate__(self) -> dict[str, object]:
        return {
            "manager_id": self.manager_id,
            "is_leader": self.is_leader,
            "healthy_managers": self.healthy_managers,
            "job_leader_addr": self.job_leader_addr,
            "backpressure_level": self.backpressure_level,
            "backpressure_delay_ms": self.backpressure_delay_ms,
            "backpressure_batch_only": self.backpressure_batch_only,
            "message_id": self._message_id,
            "sender_incarnation": self._sender_incarnation,
        }

    def __setstate__(self, state: object) -> None:
        if isinstance(state, dict):
            manager_id = state.get("manager_id", "")
            is_leader = state.get("is_leader", False)
            healthy_managers = state.get("healthy_managers", [])
            job_leader_addr = state.get("job_leader_addr")
            backpressure_level = state.get("backpressure_level", 0)
            backpressure_delay_ms = state.get("backpressure_delay_ms", 0)
            backpressure_batch_only = state.get("backpressure_batch_only", False)
            message_id = state.get("message_id")
            sender_incarnation = state.get("sender_incarnation")
        elif isinstance(state, (list, tuple)):
            values = list(state)
            # Positional states: the 7+ field layout carries job_leader_addr at
            # index 3 and the optional message_id/sender_incarnation at 7/8; the
            # legacy <=6 field layout has no job_leader_addr. Missing trailing
            # fields take their defaults by padding (inline: per-ack path).
            if len(values) > 6:
                (
                    manager_id,
                    is_leader,
                    healthy_managers,
                    job_leader_addr,
                    backpressure_level,
                    backpressure_delay_ms,
                    backpressure_batch_only,
                    message_id,
                    sender_incarnation,
                ) = (values + [None, None])[:9]
            else:
                (
                    manager_id,
                    is_leader,
                    healthy_managers,
                    backpressure_level,
                    backpressure_delay_ms,
                    backpressure_batch_only,
                ) = values + ["", False, [], 0, 0, False][len(values):]
                job_leader_addr = None
                message_id = None
                sender_incarnation = None
        else:
            raise TypeError("Unsupported WorkflowProgressAck state")

        if healthy_managers is None:
            healthy_managers = []
        elif isinstance(healthy_managers, tuple):
            healthy_managers = list(healthy_managers)

        if isinstance(job_leader_addr, list):
            job_leader_addr = tuple(job_leader_addr)

        if message_id is not None:
            self.message_id = message_id
        if sender_incarnation is not None:
            self.sender_incarnation = sender_incarnation

        self.manager_id = manager_id
        self.is_leader = is_leader
        self.healthy_managers = healthy_managers
        self.job_leader_addr = job_leader_addr
        self.backpressure_level = backpressure_level
        self.backpressure_delay_ms = backpressure_delay_ms
        self.backpressure_batch_only = backpressure_batch_only
