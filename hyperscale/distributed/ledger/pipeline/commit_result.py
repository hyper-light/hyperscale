"""``CommitResult`` -- pickled under the namespace
``hyperscale.distributed.ledger.pipeline.commit_pipeline`` (see that module)."""

from __future__ import annotations

from hyperscale.distributed.reliability.backpressure import BackpressureLevel, BackpressureSignal
from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.wal.wal_entry import WALEntry


class CommitResult:
    __slots__ = ("_entry", "_level_achieved", "_error", "_backpressure")

    def __init__(
        self,
        entry: WALEntry,
        level_achieved: DurabilityLevel,
        error: Exception | None = None,
        backpressure: BackpressureSignal | None = None,
    ) -> None:
        self._entry = entry
        self._level_achieved = level_achieved
        self._error = error
        self._backpressure = backpressure or BackpressureSignal.from_level(
            BackpressureLevel.NONE
        )

    @property
    def entry(self) -> WALEntry:
        return self._entry

    @property
    def level_achieved(self) -> DurabilityLevel:
        return self._level_achieved

    @property
    def error(self) -> Exception | None:
        return self._error

    @property
    def backpressure(self) -> BackpressureSignal:
        return self._backpressure

    @property
    def success(self) -> bool:
        return self._error is None

    @property
    def lsn(self) -> int:
        return self._entry.lsn
