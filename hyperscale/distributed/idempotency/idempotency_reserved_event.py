"""``IdempotencyReservedEvent`` -- pickled under the namespace
``hyperscale.distributed.idempotency.idempotency_events`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class IdempotencyReservedEvent:
    """Event emitted when an idempotency key is reserved."""

    idempotency_key: str
    job_id: str
    reserved_at: float
    source_dc: str
