"""
Force-remove (AD-52 §13).

Operator escape for unresponsive members. Skips the tombstone retention
window and proposes Remove immediately. Gated by the current
membership_epoch — operator must pass --epoch matching the leader's
view, preventing the "removed the wrong node after churn" failure mode.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from .membership_log import Remove, RemoveReason
from .membership_log.base import EntryMetadata, CURRENT_SCHEMA_VERSION

if TYPE_CHECKING:
    from hyperscale.logging import Logger


class ForceRemoveEpochMismatch(Exception):
    """Raised when the operator-supplied current_epoch does not match
    the leader's view. The exception carries the leader's current
    epoch so the operator can retry."""

    __slots__ = ("operator_epoch", "leader_epoch")

    def __init__(self, operator_epoch: int, leader_epoch: int) -> None:
        self.operator_epoch = operator_epoch
        self.leader_epoch = leader_epoch
        super().__init__(
            f"force-remove epoch mismatch: operator={operator_epoch}, "
            f"leader={leader_epoch} — refresh and retry"
        )


class ForceRemover:
    """
    Leader-side handler for POST /admin/force-remove.
    """

    __slots__ = (
        "_current_epoch_provider",
        "_propose_remove",
        "_logger",
    )

    def __init__(
        self,
        current_epoch_provider: Callable[[], int],
        propose_remove: Callable[[Remove], Awaitable[int]],
        logger: "Logger | None" = None,
    ) -> None:
        self._current_epoch_provider = current_epoch_provider
        self._propose_remove = propose_remove
        self._logger = logger

    async def force_remove(
        self,
        node_id: str,
        operator_supplied_epoch: int,
    ) -> int:
        """
        Validates the epoch, proposes Remove(force). Returns the
        committed LSN on success; raises ForceRemoveEpochMismatch on
        stale epoch.
        """
        leader_epoch = self._current_epoch_provider()
        if leader_epoch != operator_supplied_epoch:
            raise ForceRemoveEpochMismatch(
                operator_epoch=operator_supplied_epoch,
                leader_epoch=leader_epoch,
            )
        committed_lsn = await self._propose_remove(
            Remove(
                node_id=node_id,
                reason=RemoveReason.FORCE,
                metadata=EntryMetadata(schema_version=CURRENT_SCHEMA_VERSION),
            )
        )
        if self._logger is not None:
            await self._logger.log({
                "event": "ClusterForceRemove",
                "removed_node_id": node_id,
                "operator_epoch": operator_supplied_epoch,
                "committed_lsn": committed_lsn,
            })
        return committed_lsn
