"""``IdempotencyKey`` -- pickled under the namespace
``hyperscale.distributed.idempotency.idempotency_key`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class IdempotencyKey:
    """Client-generated idempotency key for job submissions."""

    client_id: str
    sequence: int
    nonce: str

    def __str__(self) -> str:
        return f"{self.client_id}:{self.sequence}:{self.nonce}"

    @classmethod
    def parse(cls, key_str: str) -> "IdempotencyKey":
        """Parse an idempotency key from its string representation."""
        parts = key_str.split(":", 2)
        if len(parts) != 3:
            raise ValueError(f"Invalid idempotency key format: {key_str}")

        return cls(
            client_id=parts[0],
            sequence=int(parts[1]),
            nonce=parts[2],
        )
