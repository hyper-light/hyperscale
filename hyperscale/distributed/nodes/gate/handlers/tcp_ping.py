"""
TCP handler for ping/health check requests.

Handles PingRequest messages from clients and returns gate status.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    PingRequest,
    GatePingResponse,
    DatacenterInfo,
)

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.logging import Logger


class GatePingHandler:
    """
    Handle ping requests from clients.

    Returns comprehensive gate status including:
    - Gate identity and leadership status
    - Per-datacenter health and leader info
    - Active jobs and peer gates
    """

    def __init__(
        self,
        state: "GateRuntimeState",
        logger: "Logger",
        get_node_id: callable,
        get_host: callable,
        get_tcp_port: callable,
        is_leader: callable,
        get_current_term: callable,
        classify_dc_health: callable,
        count_active_dcs: callable,
        get_all_job_ids: callable,
        get_datacenter_managers: callable,
    ) -> None:
        self._state = state
        self._logger = logger
        self._get_node_id = get_node_id
        self._get_host = get_host
        self._get_tcp_port = get_tcp_port
        self._is_leader = is_leader
        self._get_current_term = get_current_term
        self._classify_dc_health = classify_dc_health
        self._count_active_dcs = count_active_dcs
        self._get_all_job_ids = get_all_job_ids
        self._get_datacenter_managers = get_datacenter_managers

    async def handle_ping(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: callable,
    ) -> bytes:
        """
        Process ping request.

        Args:
            addr: Source address (client)
            data: Serialized PingRequest message
            handle_exception: Callback for exception handling

        Returns:
            Serialized GatePingResponse
        """
        try:
            request = PingRequest.load(data)

            # Build per-datacenter info
            datacenters: list[DatacenterInfo] = []
            datacenter_managers = self._get_datacenter_managers()

            for dc_id in datacenter_managers.keys():
                status = self._classify_dc_health(dc_id)

                # Find the DC leader address
                leader_addr: tuple[str, int] | None = None
                manager_statuses = self._state._datacenter_manager_status.get(dc_id, {})
                for manager_addr, heartbeat in manager_statuses.items():
                    if heartbeat.is_leader:
                        leader_addr = (heartbeat.tcp_host, heartbeat.tcp_port)
                        break

                datacenters.append(
                    DatacenterInfo(
                        dc_id=dc_id,
                        health=status.health,
                        leader_addr=leader_addr,
                        available_cores=status.available_capacity,
                        manager_count=status.manager_count,
                        worker_count=status.worker_count,
                    )
                )

            # Get active job IDs
            active_job_ids = self._get_all_job_ids()

            # Get peer gate addresses
            peer_gates = list(self._state._active_gate_peers)

            node_id = self._get_node_id()
            response = GatePingResponse(
                request_id=request.request_id,
                gate_id=node_id.full,
                datacenter=node_id.datacenter,
                host=self._get_host(),
                port=self._get_tcp_port(),
                is_leader=self._is_leader(),
                state=self._state._gate_state.value,
                term=self._get_current_term(),
                datacenters=datacenters,
                active_datacenter_count=self._count_active_dcs(),
                active_job_ids=active_job_ids,
                active_job_count=len(active_job_ids),
                peer_gates=peer_gates,
            )

            return response.dump()

        except Exception:
            return b"error"


__all__ = ["GatePingHandler"]
