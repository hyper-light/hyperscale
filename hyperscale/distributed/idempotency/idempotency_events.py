from dataclasses import dataclass


@dataclass(slots=True)
class IdempotencyReservedEvent:
    """Event emitted when an idempotency key is reserved."""

    idempotency_key: str
    job_id: str
    reserved_at: float
    source_dc: str


@dataclass(slots=True)
class IdempotencyCommittedEvent:
    """Event emitted when an idempotency key is committed."""

    idempotency_key: str
    job_id: str
    committed_at: float
    result_serialized: bytes
