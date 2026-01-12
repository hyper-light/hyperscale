from __future__ import annotations

from dataclasses import dataclass
import struct

from .idempotency_key import IdempotencyKey
from .idempotency_status import IdempotencyStatus


@dataclass(slots=True)
class IdempotencyLedgerEntry:
    """Persistent idempotency entry stored in the manager WAL."""

    idempotency_key: IdempotencyKey
    job_id: str
    status: IdempotencyStatus
    result_serialized: bytes | None
    created_at: float
    committed_at: float | None

    def to_bytes(self) -> bytes:
        """Serialize the entry for WAL persistence."""
        key_bytes = str(self.idempotency_key).encode("utf-8")
        job_id_bytes = self.job_id.encode("utf-8")
        result_bytes = self.result_serialized or b""
        committed_at = self.committed_at or 0.0

        return struct.pack(
            f">I{len(key_bytes)}sI{len(job_id_bytes)}sBddI{len(result_bytes)}s",
            len(key_bytes),
            key_bytes,
            len(job_id_bytes),
            job_id_bytes,
            self.status.value,
            self.created_at,
            committed_at,
            len(result_bytes),
            result_bytes,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "IdempotencyLedgerEntry":
        """Deserialize the entry from WAL bytes."""
        offset = 0
        key_len = struct.unpack_from(">I", data, offset)[0]
        offset += 4
        key_str = data[offset : offset + key_len].decode("utf-8")
        offset += key_len

        job_id_len = struct.unpack_from(">I", data, offset)[0]
        offset += 4
        job_id = data[offset : offset + job_id_len].decode("utf-8")
        offset += job_id_len

        status_value = struct.unpack_from(">B", data, offset)[0]
        offset += 1

        created_at, committed_at = struct.unpack_from(">dd", data, offset)
        offset += 16

        result_len = struct.unpack_from(">I", data, offset)[0]
        offset += 4
        result_bytes = data[offset : offset + result_len] if result_len else None

        return cls(
            idempotency_key=IdempotencyKey.parse(key_str),
            job_id=job_id,
            status=IdempotencyStatus(status_value),
            result_serialized=result_bytes,
            created_at=created_at,
            committed_at=committed_at if committed_at > 0 else None,
        )
