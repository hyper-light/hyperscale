"""
AD-38 GLOBAL durability for the gate job ledger: copies in more than one region.
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import msgspec

from hyperscale.distributed.raft.logging_models import RaftDebug
from hyperscale.distributed.raft.models import (
    LedgerPlacementQuery,
    LedgerPlacementResult,
    RaftLogEntry,
)
from hyperscale.distributed.raft.models.ledger_append_command import LEDGER_APPEND_COMMAND, LedgerAppendCommand
from hyperscale.distributed.raft.raft_node import HEARTBEAT_INTERVAL, RaftNode

if TYPE_CHECKING:
    from hyperscale.distributed.ledger.wal.wal_entry import WALEntry
    from hyperscale.distributed.raft import GateRaftConsensus
    from hyperscale.distributed.runtime import Clock
    from hyperscale.logging import Logger


class LedgerRegionSpan:
    """Global replicator for the gate ledger: GLOBAL once the entry's
    holders span at least two regions.

    AD-38 defines GLOBAL as surviving a region failure. The REGIONAL step
    already committed the entry in the job's gate group (a majority of
    gates hold it); losing a whole region still leaves a copy only if the
    holders are not all in that region. The group's Raft leader alone
    knows which members acknowledged the entry, so a non-leader asks it
    (``query_method``); holders map to regions through each gate's
    datacenter identity.

    A gate tier that spans a single region can never meet this, and says
    so at once (``tier_spans_regions``) instead of waiting out a timeout;
    callers then request REGIONAL, the highest level that tier provides.
    """

    __slots__ = (
        "_consensus",
        "_node_id",
        "_region_of",
        "_tier_regions",
        "_send_tcp",
        "_query_method",
        "_query_timeout_seconds",
        "_clock",
        "_logger",
    )

    def __init__(
        self,
        consensus: "GateRaftConsensus",
        node_id: str,
        region_of: Callable[[str], str | None],
        tier_regions: Callable[[], set[str]],
        send_tcp: Callable[..., Awaitable[bytes | Exception | None]],
        query_method: str,
        query_timeout_seconds: float,
        clock: "Clock",
        logger: "Logger",
    ) -> None:
        self._consensus = consensus
        self._node_id = node_id
        self._region_of = region_of
        self._tier_regions = tier_regions
        self._send_tcp = send_tcp
        self._query_method = query_method
        self._query_timeout_seconds = query_timeout_seconds
        self._clock = clock
        self._logger = logger

    def tier_spans_regions(self) -> bool:
        """Whether GLOBAL is achievable at all with the known gate tier."""
        return len(self._tier_regions()) >= 2

    async def replicate(self, entry: "WALEntry") -> bool:
        """``CommitPipeline`` global replicator: True once the entry's
        holders span two regions."""
        if not self.tier_spans_regions():
            return False

        return await self._await_holders_spanning_regions(entry)

    async def _await_holders_spanning_regions(self, entry: "WALEntry") -> bool:
        """Poll the job group's leader each heartbeat until the entry's holders
        span two regions (AD-38 GLOBAL), or the group retires."""
        job_id = msgspec.msgpack.decode(entry.payload)[0]
        while (node := self._consensus.get_node(job_id)) is not None:
            if await self._leader_holders_span_regions(node, job_id, entry.payload):
                return True
            await self._clock.sleep(HEARTBEAT_INTERVAL)

        # The group retired (job cleaned up) before a second region held it.
        return False

    async def _leader_holders_span_regions(self, node: RaftNode, job_id: str, payload: bytes) -> bool:
        """Whether the group has a leader and the holders it reports span two regions."""
        leader_id = node.current_leader
        return leader_id is not None and self._spans_regions(
            await self._holders_via(leader_id, job_id, payload)
        )

    def holders(self, job_id: str, payload: bytes) -> list[str]:
        """Members holding the entry, answered by the group's leader."""
        node = self._consensus.get_node(job_id)
        if node is None or not node.is_leader():
            return []
        return self._leader_holders(node, payload)

    @staticmethod
    def _leader_holders(node: RaftNode, payload: bytes) -> list[str]:
        """Members the leader ``node`` knows hold the latest entry carrying ``payload``."""
        index = node.last_index_where(lambda log_entry: _carries_payload(log_entry, payload))
        if index is None:
            return []
        return sorted(node.members_holding(index) or ())

    async def handle_query(self, query: LedgerPlacementQuery) -> LedgerPlacementResult:
        return LedgerPlacementResult(
            job_id=query.job_id, holders=self.holders(query.job_id, query.payload)
        )

    async def _holders_via(self, leader_id: str, job_id: str, payload: bytes) -> list[str]:
        if leader_id == self._node_id:
            return self.holders(job_id, payload)
        if (leader_addr := self._consensus.member_address(leader_id)) is None:
            return []

        return await self._query_leader_holders(leader_id, leader_addr, job_id, payload)

    async def _query_leader_holders(
        self,
        leader_id: str,
        leader_addr: tuple[str, int],
        job_id: str,
        payload: bytes,
    ) -> list[str]:
        """Ask the remote group leader which members hold the entry; none on no answer."""
        response = await self._send_tcp(
            leader_addr,
            self._query_method,
            LedgerPlacementQuery(job_id=job_id, payload=payload).dump(),
            timeout=self._query_timeout_seconds,
        )
        if isinstance(response, Exception) or not response:
            await self._logger.log(
                RaftDebug(
                    message=f"Ledger placement query to {leader_id} got no answer ({response!r}); retrying",
                    node_id=self._node_id,
                    job_id=job_id,
                )
            )
            return []
        return LedgerPlacementResult.load(response).holders

    def _spans_regions(self, holders: list[str]) -> bool:
        regions = {region for holder in holders if (region := self._region_of(holder)) is not None}
        return len(regions) >= 2


def _carries_payload(log_entry: RaftLogEntry, payload: bytes) -> bool:
    if log_entry.command_type != LEDGER_APPEND_COMMAND:
        return False
    return msgspec.msgpack.decode(log_entry.command, type=LedgerAppendCommand).ledger_payload == payload
