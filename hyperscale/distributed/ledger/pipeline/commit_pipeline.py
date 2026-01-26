from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Callable, Awaitable

from hyperscale.distributed.reliability.backpressure import (
    BackpressureLevel,
    BackpressureSignal,
)

from ..durability_level import DurabilityLevel
from ..wal.entry_state import TransitionResult
from ..wal.wal_entry import WALEntry

if TYPE_CHECKING:
    from hyperscale.logging import Logger

    from ..wal.node_wal import NodeWAL


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


class CommitPipeline:
    """
    Three-stage commit pipeline with progressive durability.

    Stages:
    1. LOCAL: Write to node WAL with fsync (<1ms)
    2. REGIONAL: Replicate within datacenter (2-10ms)
    3. GLOBAL: Commit to global ledger (50-300ms)
    """

    __slots__ = (
        "_wal",
        "_regional_replicator",
        "_global_replicator",
        "_regional_timeout",
        "_global_timeout",
        "_logger",
    )

    def __init__(
        self,
        wal: NodeWAL,
        regional_replicator: Callable[[WALEntry], Awaitable[bool]] | None = None,
        global_replicator: Callable[[WALEntry], Awaitable[bool]] | None = None,
        regional_timeout: float = 10.0,
        global_timeout: float = 300.0,
        logger: Logger | None = None,
    ) -> None:
        self._wal = wal
        self._regional_replicator = regional_replicator
        self._global_replicator = global_replicator
        self._regional_timeout = regional_timeout
        self._global_timeout = global_timeout
        self._logger = logger

    async def commit(
        self,
        entry: WALEntry,
        required_level: DurabilityLevel,
        backpressure: BackpressureSignal | None = None,
    ) -> CommitResult:
        level_achieved = DurabilityLevel.LOCAL

        if required_level == DurabilityLevel.LOCAL:
            return CommitResult(
                entry=entry,
                level_achieved=level_achieved,
                backpressure=backpressure,
            )

        if required_level >= DurabilityLevel.REGIONAL:
            try:
                regional_success = await self._replicate_regional(entry)
                if regional_success:
                    transition_result = await self._wal.mark_regional(entry.lsn)
                    if not transition_result.is_ok:
                        return CommitResult(
                            entry=entry,
                            level_achieved=level_achieved,
                            error=RuntimeError(
                                f"WAL state transition failed: {transition_result.value}"
                            ),
                            backpressure=backpressure,
                        )
                    level_achieved = DurabilityLevel.REGIONAL
                else:
                    return CommitResult(
                        entry=entry,
                        level_achieved=level_achieved,
                        error=RuntimeError("Regional replication failed"),
                        backpressure=backpressure,
                    )
            except asyncio.TimeoutError:
                return CommitResult(
                    entry=entry,
                    level_achieved=level_achieved,
                    error=asyncio.TimeoutError("Regional replication timed out"),
                    backpressure=backpressure,
                )
            except Exception as exc:
                return CommitResult(
                    entry=entry,
                    level_achieved=level_achieved,
                    error=exc,
                    backpressure=backpressure,
                )

        if required_level >= DurabilityLevel.GLOBAL:
            try:
                global_success = await self._replicate_global(entry)
                if global_success:
                    transition_result = await self._wal.mark_global(entry.lsn)
                    if not transition_result.is_ok:
                        return CommitResult(
                            entry=entry,
                            level_achieved=level_achieved,
                            error=RuntimeError(
                                f"WAL state transition failed: {transition_result.value}"
                            ),
                            backpressure=backpressure,
                        )
                    level_achieved = DurabilityLevel.GLOBAL
                else:
                    return CommitResult(
                        entry=entry,
                        level_achieved=level_achieved,
                        error=RuntimeError("Global replication failed"),
                        backpressure=backpressure,
                    )
            except asyncio.TimeoutError:
                return CommitResult(
                    entry=entry,
                    level_achieved=level_achieved,
                    error=asyncio.TimeoutError("Global replication timed out"),
                    backpressure=backpressure,
                )
            except Exception as exc:
                return CommitResult(
                    entry=entry,
                    level_achieved=level_achieved,
                    error=exc,
                    backpressure=backpressure,
                )

        return CommitResult(
            entry=entry,
            level_achieved=level_achieved,
            backpressure=backpressure,
        )

    async def _replicate_regional(self, entry: WALEntry) -> bool:
        if self._regional_replicator is None:
            return True

        return await asyncio.wait_for(
            self._regional_replicator(entry),
            timeout=self._regional_timeout,
        )

    async def _replicate_global(self, entry: WALEntry) -> bool:
        if self._global_replicator is None:
            return True

        return await asyncio.wait_for(
            self._global_replicator(entry),
            timeout=self._global_timeout,
        )
