"""``WriteBatch`` -- pickled under the namespace
``hyperscale.distributed.ledger.wal.wal_writer`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass, field

from .write_request import WriteRequest


@dataclass(slots=True)
class WriteBatch:
    requests: list[WriteRequest] = field(default_factory=list)
    total_bytes: int = 0

    def add(self, request: WriteRequest) -> None:
        self.requests.append(request)
        self.total_bytes += len(request.data)

    def clear(self) -> None:
        self.requests.clear()
        self.total_bytes = 0

    def __len__(self) -> int:
        return len(self.requests)
