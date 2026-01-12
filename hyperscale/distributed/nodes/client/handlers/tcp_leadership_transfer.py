"""
TCP handlers for leadership transfer notifications.

Handles GateJobLeaderTransfer and ManagerJobLeaderTransfer messages.
"""

from hyperscale.distributed.models import (
    GateJobLeaderTransfer,
    GateJobLeaderTransferAck,
    ManagerJobLeaderTransfer,
    ManagerJobLeaderTransferAck,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError


def _addr_str(addr: tuple[str, int] | None) -> str:
    """Format address as string, or 'unknown' if None."""
    return f"{addr}" if addr else "unknown"


class GateLeaderTransferHandler:
    """
    Handle gate job leadership transfer notification.

    Received from the new gate job leader when taking over from a failed gate.
    """

    def __init__(
        self,
        state: ClientState,
        logger: Logger,
        leadership_manager=None,
        node_id=None,
    ) -> None:
        self._state = state
        self._logger = logger
        self._leadership_manager = leadership_manager
        self._node_id = node_id

    def _client_id(self) -> str:
        return self._node_id.full if self._node_id else "client"

    def _short_id(self) -> str:
        return self._node_id.short if self._node_id else "client"

    async def _apply_transfer(
        self,
        transfer: GateJobLeaderTransfer,
    ) -> GateJobLeaderTransferAck:
        """Apply the transfer, validating fence token. Returns ack."""
        job_id = transfer.job_id

        if not self._leadership_manager:
            return GateJobLeaderTransferAck(
                job_id=job_id, client_id=self._client_id(), accepted=True
            )

        fence_valid, fence_reason = self._leadership_manager.validate_gate_fence_token(
            job_id, transfer.fence_token
        )
        if not fence_valid:
            await self._logger.log(
                ServerInfo(
                    message=f"Rejected gate transfer for job {job_id[:8]}...: {fence_reason}",
                    node_host="client",
                    node_port=0,
                    node_id=self._short_id(),
                )
            )
            return GateJobLeaderTransferAck(
                job_id=job_id,
                client_id=self._client_id(),
                accepted=False,
                rejection_reason=fence_reason,
            )

        self._leadership_manager.update_gate_leader(
            job_id=job_id,
            gate_addr=transfer.new_gate_addr,
            fence_token=transfer.fence_token,
        )
        self._state.mark_job_target(job_id, transfer.new_gate_addr)

        await self._logger.log(
            ServerInfo(
                message=f"Gate job leader transfer: job={job_id[:8]}..., "
                f"old={_addr_str(transfer.old_gate_addr)}, new={transfer.new_gate_addr}, "
                f"fence_token={transfer.fence_token}",
                node_host="client",
                node_port=0,
                node_id=self._short_id(),
            )
        )
        return GateJobLeaderTransferAck(
            job_id=job_id, client_id=self._client_id(), accepted=True
        )

    async def handle(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        await self._state.increment_gate_transfers()

        try:
            transfer = GateJobLeaderTransfer.load(data)
            routing_lock = self._state.get_or_create_routing_lock(transfer.job_id)
            async with routing_lock:
                ack = await self._apply_transfer(transfer)
            return ack.dump()

        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Error processing gate transfer: {error}",
                    node_host="client",
                    node_port=0,
                    node_id=self._short_id(),
                )
            )
            return GateJobLeaderTransferAck(
                job_id="unknown",
                client_id=self._client_id(),
                accepted=False,
                rejection_reason=str(error),
            ).dump()


class ManagerLeaderTransferHandler:
    """
    Handle manager job leadership transfer notification.

    Typically forwarded by gate to client when a manager job leader changes.
    """

    def __init__(
        self,
        state: ClientState,
        logger: Logger,
        leadership_manager=None,
        node_id=None,
    ) -> None:
        self._state = state
        self._logger = logger
        self._leadership_manager = leadership_manager
        self._node_id = node_id

    def _client_id(self) -> str:
        return self._node_id.full if self._node_id else "client"

    def _short_id(self) -> str:
        return self._node_id.short if self._node_id else "client"

    async def _apply_transfer(
        self,
        transfer: ManagerJobLeaderTransfer,
    ) -> ManagerJobLeaderTransferAck:
        """Apply the transfer, validating fence token. Returns ack."""
        job_id = transfer.job_id
        datacenter_id = transfer.datacenter_id

        if not self._leadership_manager:
            return ManagerJobLeaderTransferAck(
                job_id=job_id,
                client_id=self._client_id(),
                datacenter_id=datacenter_id,
                accepted=True,
            )

        fence_valid, fence_reason = (
            self._leadership_manager.validate_manager_fence_token(
                job_id, datacenter_id, transfer.fence_token
            )
        )
        if not fence_valid:
            await self._logger.log(
                ServerInfo(
                    message=f"Rejected manager transfer for job {job_id[:8]}...: {fence_reason}",
                    node_host="client",
                    node_port=0,
                    node_id=self._short_id(),
                )
            )
            return ManagerJobLeaderTransferAck(
                job_id=job_id,
                client_id=self._client_id(),
                datacenter_id=datacenter_id,
                accepted=False,
                rejection_reason=fence_reason,
            )

        self._leadership_manager.update_manager_leader(
            job_id=job_id,
            datacenter_id=datacenter_id,
            manager_addr=transfer.new_manager_addr,
            fence_token=transfer.fence_token,
        )

        await self._logger.log(
            ServerInfo(
                message=f"Manager job leader transfer: job={job_id[:8]}..., dc={datacenter_id}, "
                f"old={_addr_str(transfer.old_manager_addr)}, new={transfer.new_manager_addr}, "
                f"fence_token={transfer.fence_token}",
                node_host="client",
                node_port=0,
                node_id=self._short_id(),
            )
        )
        return ManagerJobLeaderTransferAck(
            job_id=job_id,
            client_id=self._client_id(),
            datacenter_id=datacenter_id,
            accepted=True,
        )

    async def handle(
        self, addr: tuple[str, int], data: bytes, clock_time: int
    ) -> bytes:
        await self._state.increment_manager_transfers()

        try:
            transfer = ManagerJobLeaderTransfer.load(data)
            routing_lock = self._state.get_or_create_routing_lock(transfer.job_id)
            async with routing_lock:
                ack = await self._apply_transfer(transfer)
            return ack.dump()

        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Error processing manager transfer: {error}",
                    node_host="client",
                    node_port=0,
                    node_id=self._short_id(),
                )
            )
            return ManagerJobLeaderTransferAck(
                job_id="unknown",
                client_id=self._client_id(),
                datacenter_id="",
                accepted=False,
                rejection_reason=str(error),
            ).dump()
