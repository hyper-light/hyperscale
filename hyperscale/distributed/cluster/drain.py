"""
Drain mode controller (AD-52 §13).

POST /admin/drain marks a node as draining via an UpdateMetadata Raft
entry. The leader stops routing new work to the draining node;
in-flight work continues until completion. When in-flight count
reaches zero, the node sends LeaveRequest and exits.

This module owns the controller. The HTTP surface (POST /admin/drain)
is wired by the server layer; the K8s preStop hook also calls it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TYPE_CHECKING

from hyperscale.logging.hyperscale_logging_models import ServerInfo

from .membership_log import UpdateMetadata
from .membership_log.base import EntryMetadata, CURRENT_SCHEMA_VERSION

if TYPE_CHECKING:
    from hyperscale.logging import Logger


class DrainState(str, Enum):
    NOT_DRAINING = "not_draining"
    DRAINING = "draining"
    DRAINED = "drained"
    LEFT = "left"


class DrainController:
    """
    One instance per ClusterNode. The serve command's preStop hook calls
    start_drain(); the leader's apply layer observes the resulting
    UpdateMetadata entry and stops routing.

    The controller waits for in_flight_provider() to drop to zero (with
    a configurable timeout = terminationGracePeriodSeconds - 5s) before
    proposing the LeaveRequest.
    """

    __slots__ = (
        "_node_id",
        "_propose_update_metadata",
        "_propose_remove",
        "_in_flight_provider",
        "_logger",
        "_state",
        "_drain_complete_event",
    )

    DRAIN_METADATA_KEY: str = "drain_status"
    DRAIN_METADATA_VALUE: str = "draining"

    def __init__(
        self,
        node_id: str,
        propose_update_metadata: Callable[[UpdateMetadata], Awaitable[int]],
        propose_remove: Callable[[str], Awaitable[int]],
        in_flight_provider: Callable[[], int],
        logger: "Logger | None" = None,
    ) -> None:
        self._node_id = node_id
        self._propose_update_metadata = propose_update_metadata
        self._propose_remove = propose_remove
        self._in_flight_provider = in_flight_provider
        self._logger = logger
        self._state = DrainState.NOT_DRAINING
        self._drain_complete_event = asyncio.Event()

    @property
    def state(self) -> DrainState:
        return self._state

    async def start_drain(self, drain_timeout_seconds: float = 55.0) -> None:
        """
        Mark this node as draining. Waits up to drain_timeout_seconds
        for in-flight count to reach zero, then proposes Remove(leave).
        Idempotent: a second call is a no-op while DRAINING.
        """
        if self._state != DrainState.NOT_DRAINING:
            return
        self._state = DrainState.DRAINING

        await self._propose_update_metadata(
            UpdateMetadata(
                node_id=self._node_id,
                metadata_delta=((self.DRAIN_METADATA_KEY, self.DRAIN_METADATA_VALUE),),
                metadata=EntryMetadata(schema_version=CURRENT_SCHEMA_VERSION),
            )
        )
        if self._logger is not None:
            await self._logger.log(
                ServerInfo(
                    message="Cluster drain started",
                    node_id=self._node_id,
                    node_host="",
                    node_port=0,
                )
            )

        # Wait for in-flight to drain.
        deadline_seconds = drain_timeout_seconds
        poll_interval_seconds = 0.5
        while deadline_seconds > 0:
            in_flight = self._in_flight_provider()
            if in_flight <= 0:
                break
            await asyncio.sleep(poll_interval_seconds)
            deadline_seconds -= poll_interval_seconds

        self._state = DrainState.DRAINED
        if self._logger is not None:
            await self._logger.log(
                ServerInfo(
                    message=(
                        "Cluster drain complete "
                        f"(remaining_in_flight={self._in_flight_provider()})"
                    ),
                    node_id=self._node_id,
                    node_host="",
                    node_port=0,
                )
            )

        # Propose Remove(leave) — voluntary. The apply layer admits
        # this because we are draining.
        await self._propose_remove(self._node_id)
        self._state = DrainState.LEFT
        self._drain_complete_event.set()

    async def wait_for_drain_complete(self) -> None:
        """Wait until the drain controller reaches LEFT."""
        await self._drain_complete_event.wait()
