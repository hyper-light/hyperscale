"""
Discovery and query operations for HyperscaleClient.

Handles ping, workflow query, and datacenter discovery operations.
"""

import asyncio
import secrets
from typing import Callable

from hyperscale.distributed.models import (
    PingRequest,
    ManagerPingResponse,
    GatePingResponse,
    WorkflowQueryRequest,
    WorkflowStatusInfo,
    WorkflowQueryResponse,
    GateWorkflowQueryResponse,
    DatacenterListRequest,
    DatacenterListResponse,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.config import ClientConfig
from hyperscale.logging import Logger


class ClientDiscovery:
    """
    Manages discovery and query operations.

    Provides methods for:
    - Pinging managers and gates to check status
    - Querying workflow status from managers or gates
    - Discovering datacenter information from gates
    """

    def __init__(
        self,
        state: ClientState,
        config: ClientConfig,
        logger: Logger,
        targets,  # ClientTargetSelector
        send_tcp_func,  # Callable for sending TCP messages
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._targets = targets
        self._send_tcp = send_tcp_func

    # =========================================================================
    # Ping Methods
    # =========================================================================

    async def ping_manager(
        self,
        addr: tuple[str, int] | None = None,
        timeout: float = 5.0,
    ) -> ManagerPingResponse:
        """
        Ping a manager to get its current status.

        Args:
            addr: Manager (host, port) to ping. If None, uses next manager in rotation.
            timeout: Request timeout in seconds.

        Returns:
            ManagerPingResponse with manager status, worker health, and active jobs.

        Raises:
            RuntimeError: If no managers configured or ping fails.
        """
        target = self._resolve_target(addr, self._targets.get_next_manager, "No managers configured")

        request = PingRequest(request_id=secrets.token_hex(8))

        response = await self._send_checked(
            target,
            "ping",
            request.dump(),
            timeout,
            "Ping failed",
            "Ping failed: server returned error",
        )

        return ManagerPingResponse.load(response)

    async def ping_gate(
        self,
        addr: tuple[str, int] | None = None,
        timeout: float = 5.0,
    ) -> GatePingResponse:
        """
        Ping a gate to get its current status.

        Args:
            addr: Gate (host, port) to ping. If None, uses next gate in rotation.
            timeout: Request timeout in seconds.

        Returns:
            GatePingResponse with gate status, datacenter health, and active jobs.

        Raises:
            RuntimeError: If no gates configured or ping fails.
        """
        target = self._resolve_target(addr, self._targets.get_next_gate, "No gates configured")

        request = PingRequest(request_id=secrets.token_hex(8))

        response = await self._send_checked(
            target,
            "ping",
            request.dump(),
            timeout,
            "Ping failed",
            "Ping failed: server returned error",
        )

        return GatePingResponse.load(response)

    @staticmethod
    def _resolve_target(
        addr: tuple[str, int] | None,
        next_target: Callable[[], tuple[str, int] | None],
        missing_message: str,
    ) -> tuple[str, int]:
        """The given address, else the next one in rotation; raises when none is configured."""
        target = addr or next_target()
        if not target:
            raise RuntimeError(missing_message)
        return target

    async def _send_checked(
        self,
        target: tuple[str, int],
        action: str,
        payload: bytes,
        timeout: float,
        failure_prefix: str,
        error_reply_message: str,
    ) -> bytes:
        """Send a request, raising RuntimeError on a transport error or an error reply."""
        response, _ = await self._send_tcp(
            target,
            action,
            payload,
            timeout=timeout,
        )

        if isinstance(response, Exception):
            raise RuntimeError(f"{failure_prefix}: {response}")

        if response == b'error':
            raise RuntimeError(error_reply_message)

        return response

    async def ping_all_managers(
        self,
        timeout: float = 5.0,
    ) -> dict[tuple[str, int], ManagerPingResponse | Exception]:
        """
        Ping all configured managers concurrently.

        Args:
            timeout: Request timeout in seconds per manager.

        Returns:
            Dict mapping manager address to response or exception.
        """
        if not self._config.managers:
            return {}

        async def ping_one(addr: tuple[str, int]) -> tuple[tuple[str, int], ManagerPingResponse | Exception]:
            try:
                response = await self.ping_manager(addr, timeout=timeout)
                return (addr, response)
            except Exception as e:
                return (addr, e)

        results = await asyncio.gather(
            *[ping_one(addr) for addr in self._config.managers],
            return_exceptions=False,
        )

        return dict(results)

    async def ping_all_gates(
        self,
        timeout: float = 5.0,
    ) -> dict[tuple[str, int], GatePingResponse | Exception]:
        """
        Ping all configured gates concurrently.

        Args:
            timeout: Request timeout in seconds per gate.

        Returns:
            Dict mapping gate address to response or exception.
        """
        if not self._config.gates:
            return {}

        async def ping_one(addr: tuple[str, int]) -> tuple[tuple[str, int], GatePingResponse | Exception]:
            try:
                response = await self.ping_gate(addr, timeout=timeout)
                return (addr, response)
            except Exception as e:
                return (addr, e)

        results = await asyncio.gather(
            *[ping_one(addr) for addr in self._config.gates],
            return_exceptions=False,
        )

        return dict(results)

    # =========================================================================
    # Workflow Query Methods
    # =========================================================================

    async def query_workflows(
        self,
        workflow_names: list[str],
        job_id: str | None = None,
        timeout: float = 5.0,
    ) -> dict[str, list[WorkflowStatusInfo]]:
        """
        Query workflow status from managers.

        If job_id is specified and we know which manager accepted that job,
        queries that manager first. Otherwise queries all configured managers.

        Args:
            workflow_names: List of workflow class names to query.
            job_id: Optional job ID to filter results.
            timeout: Request timeout in seconds.

        Returns:
            Dict mapping datacenter ID to list of WorkflowStatusInfo.
            If querying managers directly, uses the manager's datacenter.

        Raises:
            RuntimeError: If no managers configured.
        """
        if not self._config.managers:
            raise RuntimeError("No managers configured")

        request = WorkflowQueryRequest(
            request_id=secrets.token_hex(8),
            workflow_names=workflow_names,
            job_id=job_id,
        )

        results: dict[str, list[WorkflowStatusInfo]] = {}
        query_errors: list[Exception] = []

        # If we know which manager accepted this job, query it first
        # This ensures we get results from the job leader
        if await self._query_job_target_first(job_id, request, timeout, results, query_errors):
            # If we got results, return them (job leader has authoritative state)
            return results

        # Query all managers (either no job_id, or job target query failed)
        await self._query_all_managers(request, timeout, results, query_errors)
        return results

    async def _query_job_target_first(
        self,
        job_id: str | None,
        request: WorkflowQueryRequest,
        timeout: float,
        results: dict[str, list[WorkflowStatusInfo]],
        query_errors: list[Exception],
    ) -> bool:
        """Query the manager that accepted the job, when known; True when it returned results."""
        if job_id:
            return await self._query_known_job_target(job_id, request, timeout, results, query_errors)
        return False

    async def _query_known_job_target(
        self,
        job_id: str,
        request: WorkflowQueryRequest,
        timeout: float,
        results: dict[str, list[WorkflowStatusInfo]],
        query_errors: list[Exception],
    ) -> bool:
        """Query the job's accepting manager if this client recorded one; True when it returned results."""
        job_target = self._state.get_job_target(job_id)
        if not job_target:
            return False
        await self._query_manager_workflows(job_target, request, timeout, results, query_errors)
        return bool(results)

    async def _query_all_managers(
        self,
        request: WorkflowQueryRequest,
        timeout: float,
        results: dict[str, list[WorkflowStatusInfo]],
        query_errors: list[Exception],
    ) -> None:
        """Query every configured manager concurrently; raises when every one of them failed."""
        query_errors.clear()
        await asyncio.gather(
            *[
                self._query_manager_workflows(addr, request, timeout, results, query_errors)
                for addr in self._config.managers
            ],
            return_exceptions=False,
        )

        # Every manager failing is not "no workflows".
        if len(query_errors) == len(self._config.managers):
            raise ExceptionGroup("every manager failed the workflow query", list(query_errors))

    async def _query_manager_workflows(
        self,
        addr: tuple[str, int],
        request: WorkflowQueryRequest,
        timeout: float,
        results: dict[str, list[WorkflowStatusInfo]],
        query_errors: list[Exception],
    ) -> None:
        """Query one manager, merging its workflows into the results or recording its error."""
        try:
            await self._collect_manager_workflows(addr, request, timeout, results)

        except Exception as query_error:
            # Another manager may answer; if none does, these raise.
            query_errors.append(query_error)

    async def _collect_manager_workflows(
        self,
        addr: tuple[str, int],
        request: WorkflowQueryRequest,
        timeout: float,
        results: dict[str, list[WorkflowStatusInfo]],
    ) -> None:
        """Send the workflow query to one manager and add its workflows under its datacenter."""
        response_data, _ = await self._send_tcp(
            addr,
            "workflow_query",
            request.dump(),
            timeout=timeout,
        )

        if isinstance(response_data, Exception):
            raise response_data
        if response_data == b'error':
            raise RuntimeError(f"manager {addr[0]}:{addr[1]} answered the workflow query with an error")

        response = WorkflowQueryResponse.load(response_data)
        dc_id = response.datacenter

        results.setdefault(dc_id, []).extend(response.workflows)

    async def query_workflows_via_gate(
        self,
        workflow_names: list[str],
        job_id: str | None = None,
        addr: tuple[str, int] | None = None,
        timeout: float = 10.0,
    ) -> dict[str, list[WorkflowStatusInfo]]:
        """
        Query workflow status via a gate.

        Gates query all datacenter managers and return aggregated results
        grouped by datacenter.

        Args:
            workflow_names: List of workflow class names to query.
            job_id: Optional job ID to filter results.
            addr: Gate (host, port) to query. If None, uses next gate in rotation.
            timeout: Request timeout in seconds (higher for gate aggregation).

        Returns:
            Dict mapping datacenter ID to list of WorkflowStatusInfo.

        Raises:
            RuntimeError: If no gates configured or query fails.
        """
        target = self._resolve_target(addr, self._targets.get_next_gate, "No gates configured")

        request = WorkflowQueryRequest(
            request_id=secrets.token_hex(8),
            workflow_names=workflow_names,
            job_id=job_id,
        )

        response_data = await self._send_checked(
            target,
            "workflow_query",
            request.dump(),
            timeout,
            "Workflow query failed",
            "Workflow query failed: gate returned error",
        )

        response = GateWorkflowQueryResponse.load(response_data)

        # Convert to dict format
        results: dict[str, list[WorkflowStatusInfo]] = {
            dc_status.dc_id: dc_status.workflows for dc_status in response.datacenters
        }

        return results

    async def query_all_gates_workflows(
        self,
        workflow_names: list[str],
        job_id: str | None = None,
        timeout: float = 10.0,
    ) -> dict[tuple[str, int], dict[str, list[WorkflowStatusInfo]] | Exception]:
        """
        Query workflow status from all configured gates concurrently.

        Each gate returns results aggregated by datacenter.

        Args:
            workflow_names: List of workflow class names to query.
            job_id: Optional job ID to filter results.
            timeout: Request timeout in seconds per gate.

        Returns:
            Dict mapping gate address to either:
            - Dict of datacenter -> workflow status list
            - Exception if query failed
        """
        if not self._config.gates:
            return {}

        async def query_one(
            addr: tuple[str, int],
        ) -> tuple[tuple[str, int], dict[str, list[WorkflowStatusInfo]] | Exception]:
            try:
                result = await self.query_workflows_via_gate(
                    workflow_names,
                    job_id=job_id,
                    addr=addr,
                    timeout=timeout,
                )
                return (addr, result)
            except Exception as e:
                return (addr, e)

        results = await asyncio.gather(
            *[query_one(addr) for addr in self._config.gates],
            return_exceptions=False,
        )

        return dict(results)

    # =========================================================================
    # Datacenter Discovery
    # =========================================================================

    async def get_datacenters(
        self,
        addr: tuple[str, int] | None = None,
        timeout: float = 5.0,
    ) -> DatacenterListResponse:
        """
        Get list of registered datacenters from a gate.

        Returns datacenter information including health status, capacity,
        and leader addresses. Use this to discover available datacenters
        before submitting jobs or to check cluster health.

        Args:
            addr: Gate (host, port) to query. If None, uses next gate in rotation.
            timeout: Request timeout in seconds.

        Returns:
            DatacenterListResponse containing:
            - gate_id: Responding gate's node ID
            - datacenters: List of DatacenterInfo with health/capacity details
            - total_available_cores: Sum of available cores across all DCs
            - healthy_datacenter_count: Count of healthy datacenters

        Raises:
            RuntimeError: If no gates configured or query fails.
        """
        target = self._resolve_target(addr, self._targets.get_next_gate, "No gates configured")

        request = DatacenterListRequest(
            request_id=secrets.token_hex(8),
        )

        response_data = await self._send_checked(
            target,
            "datacenter_list",
            request.dump(),
            timeout,
            "Datacenter list query failed",
            "Datacenter list query failed: gate returned error",
        )

        return DatacenterListResponse.load(response_data)

    async def get_datacenters_from_all_gates(
        self,
        timeout: float = 5.0,
    ) -> dict[tuple[str, int], DatacenterListResponse | Exception]:
        """
        Query datacenter list from all configured gates concurrently.

        Each gate returns its view of registered datacenters. In a healthy
        cluster, all gates should return the same information.

        Args:
            timeout: Request timeout in seconds per gate.

        Returns:
            Dict mapping gate address to either:
            - DatacenterListResponse on success
            - Exception if query failed
        """
        if not self._config.gates:
            return {}

        async def query_one(
            gate_addr: tuple[str, int],
        ) -> tuple[tuple[str, int], DatacenterListResponse | Exception]:
            try:
                result = await self.get_datacenters(addr=gate_addr, timeout=timeout)
                return (gate_addr, result)
            except Exception as e:
                return (gate_addr, e)

        results = await asyncio.gather(
            *[query_one(gate_addr) for gate_addr in self._config.gates],
            return_exceptions=False,
        )

        return dict(results)
