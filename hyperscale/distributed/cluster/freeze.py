"""
Membership freeze (AD-52 §13).

POST /admin/freeze sets a cluster-wide flag (via an UpdateClusterMetadata
Raft entry — modeled here as an UpdateMetadata against a synthetic
"cluster" node_id, since the apply layer treats node_id="" as the
cluster-level metadata anchor).

While frozen, all membership change proposals (AddLearner, Promote,
Remove) are rejected with FROZEN. Useful during maintenance windows.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hyperscale.logging import Logger


class FreezeController:
    """
    Wraps the freeze flag toggling. The actual flag lives in
    ClusterMetadata.freeze_active and is set by a dedicated entry type;
    for now we model it as a metadata convention so we can ship without
    expanding the membership_log entry set. A follow-on can promote
    this to a first-class log entry.
    """

    __slots__ = (
        "_freeze_active_provider",
        "_set_freeze_active",
        "_logger",
    )

    def __init__(
        self,
        freeze_active_provider: Callable[[], bool],
        set_freeze_active: Callable[[bool], Awaitable[int]],
        logger: "Logger | None" = None,
    ) -> None:
        self._freeze_active_provider = freeze_active_provider
        self._set_freeze_active = set_freeze_active
        self._logger = logger

    @property
    def is_frozen(self) -> bool:
        return self._freeze_active_provider()

    async def freeze(self) -> int:
        committed_lsn = await self._set_freeze_active(True)
        if self._logger is not None:
            await self._logger.log({
                "event": "ClusterFreeze",
                "committed_lsn": committed_lsn,
            })
        return committed_lsn

    async def unfreeze(self) -> int:
        committed_lsn = await self._set_freeze_active(False)
        if self._logger is not None:
            await self._logger.log({
                "event": "ClusterUnfreeze",
                "committed_lsn": committed_lsn,
            })
        return committed_lsn
