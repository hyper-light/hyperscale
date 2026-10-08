"""``IdempotencyCommittedEvent`` -- pickled under the namespace
``hyperscale.distributed.idempotency.idempotency_events`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class IdempotencyCommittedEvent:
    """Event emitted when an idempotency key is committed."""

    idempotency_key: str
    job_id: str
    committed_at: float
    result_serialized: bytes
