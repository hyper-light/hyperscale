"""
TCP handlers for leadership transfer notifications.

Handles GateJobLeaderTransfer and ManagerJobLeaderTransfer messages.
"""

from hyperscale.distributed_rewrite.models import (
    GateJobLeaderTransfer,
    GateJobLeaderTransferAck,
    ManagerJobLeaderTransfer,
    ManagerJobLeaderTransferAck,
)
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.logging.hyperscale_logger import Logger
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerError


class GateLeaderTransferHandler:
    """
    Handle gate job leadership transfer notification.

    Received from the new gate job leader when taking over from a failed gate.
    """

    def __init__(
        self,
        state: ClientState,
        logger: Logger,
        leadership_manager=None,  # Will be injected
        node_id=None,  # Will be injected
    ) -> None:
        self._state = state
        self._logger = logger
        self._leadership_manager = leadership_manager
        self._node_id = node_id

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Process gate leadership transfer.

        Args:
            addr: Source address (new gate leader)
            data: Serialized GateJobLeaderTransfer message
            clock_time: Logical clock time

        Returns:
            Serialized GateJobLeaderTransferAck
        """
        self._state.increment_gate_transfers()

        try:
            transfer = GateJobLeaderTransfer.load(data)
            job_id = transfer.job_id

            # Acquire routing lock to prevent race with in-flight requests
            routing_lock = self._state.get_or_create_routing_lock(job_id)
            async with routing_lock:

                # Validate fence token via leadership manager
                if self._leadership_manager:
                    fence_valid, fence_reason = (
                        self._leadership_manager.validate_gate_fence_token(
                            job_id, transfer.fence_token
                        )
                    )
                    if not fence_valid:
                        await self._logger.log(
                            ServerInfo(
                                message=f"Rejected gate transfer for job {job_id[:8]}...: {fence_reason}",
                                node_host="client",
                                node_port=0,
                                node_id=self._node_id.short if self._node_id else "client",
                            )
                        )
                        return GateJobLeaderTransferAck(
                            job_id=job_id,
                            client_id=self._node_id.full if self._node_id else "client",
                            accepted=False,
                            rejection_reason=fence_reason,
                        ).dump()

                    # Update gate leader
                    old_gate_str = (
                        f"{transfer.old_gate_addr}"
                        if transfer.old_gate_addr
                        else "unknown"
                    )
                    self._leadership_manager.update_gate_leader(
                        job_id=job_id,
                        gate_addr=transfer.new_gate_addr,
                        fence_token=transfer.fence_token,
                    )

                    # Update job target for future requests
                    self._state.mark_job_target(job_id, transfer.new_gate_addr)

                    await self._logger.log(
                        ServerInfo(
                            message=f"Gate job leader transfer: job={job_id[:8]}..., "
                            f"old={old_gate_str}, new={transfer.new_gate_addr}, "
                            f"fence_token={transfer.fence_token}",
                            node_host="client",
                            node_port=0,
                            node_id=self._node_id.short if self._node_id else "client",
                        )
                    )

                return GateJobLeaderTransferAck(
                    job_id=job_id,
                    client_id=self._node_id.full if self._node_id else "client",
                    accepted=True,
                ).dump()

        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Error processing gate transfer: {error}",
                    node_host="client",
                    node_port=0,
                    node_id=self._node_id.short if self._node_id else "client",
                )
            )
            return GateJobLeaderTransferAck(
                job_id="unknown",
                client_id=self._node_id.full if self._node_id else "client",
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
        leadership_manager=None,  # Will be injected
        node_id=None,  # Will be injected
    ) -> None:
        self._state = state
        self._logger = logger
        self._leadership_manager = leadership_manager
        self._node_id = node_id

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Process manager leadership transfer.

        Args:
            addr: Source address (gate or manager)
            data: Serialized ManagerJobLeaderTransfer message
            clock_time: Logical clock time

        Returns:
            Serialized ManagerJobLeaderTransferAck
        """
        self._state.increment_manager_transfers()

        try:
            transfer = ManagerJobLeaderTransfer.load(data)
            job_id = transfer.job_id
            datacenter_id = transfer.datacenter_id

            # Acquire routing lock
            routing_lock = self._state.get_or_create_routing_lock(job_id)
            async with routing_lock:

                # Validate fence token via leadership manager
                if self._leadership_manager:
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
                                node_id=self._node_id.short if self._node_id else "client",
                            )
                        )
                        return ManagerJobLeaderTransferAck(
                            job_id=job_id,
                            client_id=self._node_id.full if self._node_id else "client",
                            datacenter_id=datacenter_id,
                            accepted=False,
                            rejection_reason=fence_reason,
                        ).dump()

                    # Update manager leader
                    old_manager_str = (
                        f"{transfer.old_manager_addr}"
                        if transfer.old_manager_addr
                        else "unknown"
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
                            f"old={old_manager_str}, new={transfer.new_manager_addr}, "
                            f"fence_token={transfer.fence_token}",
                            node_host="client",
                            node_port=0,
                            node_id=self._node_id.short if self._node_id else "client",
                        )
                    )

                return ManagerJobLeaderTransferAck(
                    job_id=job_id,
                    client_id=self._node_id.full if self._node_id else "client",
                    datacenter_id=datacenter_id,
                    accepted=True,
                ).dump()

        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=f"Error processing manager transfer: {error}",
                    node_host="client",
                    node_port=0,
                    node_id=self._node_id.short if self._node_id else "client",
                )
            )
            return ManagerJobLeaderTransferAck(
                job_id="unknown",
                client_id=self._node_id.full if self._node_id else "client",
                datacenter_id="",
                accepted=False,
                rejection_reason=str(error),
            ).dump()
