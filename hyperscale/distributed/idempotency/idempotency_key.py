from __future__ import annotations

from dataclasses import dataclass
from itertools import count
import secrets


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


class IdempotencyKeyGenerator:
    """Generates idempotency keys for a client."""

    def __init__(
        self, client_id: str, start_sequence: int = 0, nonce: str | None = None
    ) -> None:
        self._client_id = client_id
        self._sequence = count(start_sequence)
        self._nonce = nonce or secrets.token_hex(8)

    def generate(self) -> IdempotencyKey:
        """Generate the next idempotency key."""
        sequence = next(self._sequence)
        return IdempotencyKey(
            client_id=self._client_id,
            sequence=sequence,
            nonce=self._nonce,
        )
