"""``IdempotencyKeyGenerator`` -- pickled under the namespace
``hyperscale.distributed.idempotency.idempotency_key`` (see that module)."""

from __future__ import annotations

from itertools import count
import secrets

from .idempotency_key_model import IdempotencyKey


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
