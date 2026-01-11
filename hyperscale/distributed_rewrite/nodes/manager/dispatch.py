"""
Manager dispatch module for workflow dispatch orchestration.

Handles worker allocation, quorum coordination, and dispatch tracking.
"""

import asyncio
from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.models import (
    WorkflowDispatch,
    WorkflowDispatchAck,
    ProvisionRequest,
    ProvisionConfirm,
)
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.state import ManagerState
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.distributed_rewrite.nodes.manager.registry import ManagerRegistry
    from hyperscale.distributed_rewrite.nodes.manager.leases import ManagerLeaseCoordinator
    from hyperscale.logging.hyperscale_logger import Logger


class ManagerDispatchCoordinator:
    """
    Coordinates workflow dispatch to workers.

    Handles:
    - Worker selection based on capacity and health
    - Quorum coordination for workflow provisioning
    - Dispatch tracking and retry logic
    - Core allocation management
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        registry: "ManagerRegistry",
        leases: "ManagerLeaseCoordinator",
        logger: "Logger",
        node_id: str,
        task_runner,
        send_to_worker,  # Callable to send TCP to worker
        send_to_peer,  # Callable to send TCP to peer manager
    ) -> None:
        self._state = state
        self._config = config
        self._registry = registry
        self._leases = leases
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner
        self._send_to_worker = send_to_worker
        self._send_to_peer = send_to_peer

    async def dispatch_workflow(
        self,
        job_id: str,
        workflow_id: str,
        workflow_data: bytes,
        cores_required: int = 1,
    ) -> WorkflowDispatchAck | None:
        """
        Dispatch a workflow to a worker.

        Args:
            job_id: Job ID
            workflow_id: Workflow ID
            workflow_data: Serialized workflow data
            cores_required: Number of cores required

        Returns:
            WorkflowDispatchAck on success, None on failure
        """
        # Select worker with capacity
        worker = await self._select_worker(cores_required)
        if not worker:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"No worker available for workflow {workflow_id[:8]}... requiring {cores_required} cores",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )
            return None

        worker_id = worker.node.node_id

        # Get dispatch semaphore for worker
        semaphore = self._state.get_dispatch_semaphore(
            worker_id,
            self._config.dispatch_max_concurrent_per_worker,
        )

        async with semaphore:
            # Increment fence token
            fence_token = self._leases.increment_fence_token(job_id)

            # Build dispatch message
            dispatch = WorkflowDispatch(
                job_id=job_id,
                workflow_id=workflow_id,
                workflow_data=workflow_data,
                fence_token=fence_token,
                manager_id=self._node_id,
                cores_required=cores_required,
            )

            # Send to worker
            worker_addr = (worker.node.host, worker.node.tcp_port)
            try:
                response = await self._send_to_worker(
                    worker_addr,
                    "workflow_dispatch",
                    dispatch.dump(),
                    timeout=self._config.tcp_timeout_standard_seconds,
                )

                if response and not isinstance(response, Exception):
                    ack = WorkflowDispatchAck.load(response)
                    if ack.accepted:
                        self._task_runner.run(
                            self._logger.log,
                            ServerDebug(
                                message=f"Workflow {workflow_id[:8]}... dispatched to worker {worker_id[:8]}...",
                                node_host=self._config.host,
                                node_port=self._config.tcp_port,
                                node_id=self._node_id,
                            )
                        )
                        # Update throughput counter
                        self._state._dispatch_throughput_count += 1
                    return ack

            except Exception as e:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Dispatch to worker {worker_id[:8]}... failed: {e}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    )
                )
                # Record failure in circuit breaker
                if circuit := self._state._worker_circuits.get(worker_id):
                    circuit.record_error()

        return None

    async def _select_worker(self, cores_required: int):
        """
        Select a worker with sufficient capacity.

        Args:
            cores_required: Number of cores required

        Returns:
            WorkerRegistration or None if no worker available
        """
        healthy_ids = self._registry.get_healthy_worker_ids()

        for worker_id in healthy_ids:
            worker = self._registry.get_worker(worker_id)
            if not worker:
                continue

            # Check circuit breaker
            if circuit := self._state._worker_circuits.get(worker_id):
                if circuit.is_open():
                    continue

            # Check capacity (simplified - full impl uses WorkerPool)
            if worker.node.total_cores >= cores_required:
                return worker

        return None

    async def request_quorum_provision(
        self,
        job_id: str,
        workflow_id: str,
        worker_id: str,
        cores_required: int,
    ) -> bool:
        """
        Request quorum confirmation for workflow provisioning.

        Args:
            job_id: Job ID
            workflow_id: Workflow ID
            worker_id: Target worker ID
            cores_required: Cores being allocated

        Returns:
            True if quorum achieved
        """
        request = ProvisionRequest(
            job_id=job_id,
            workflow_id=workflow_id,
            worker_id=worker_id,
            cores_requested=cores_required,
            requesting_manager=self._node_id,
        )

        # Track pending provision
        self._state._pending_provisions[workflow_id] = request
        self._state._provision_confirmations[workflow_id] = {self._node_id}

        # Send to all active peers
        peers = list(self._state._active_manager_peers)
        quorum_size = (len(peers) + 1) // 2 + 1

        for peer_addr in peers:
            try:
                response = await self._send_to_peer(
                    peer_addr,
                    "provision_request",
                    request.dump(),
                    timeout=self._config.quorum_timeout_seconds,
                )

                if response and not isinstance(response, Exception):
                    # Parse confirmation and track
                    pass  # Full impl parses ProvisionConfirm

            except Exception:
                pass  # Continue with other peers

        # Check quorum
        confirmed = self._state._provision_confirmations.get(workflow_id, set())
        quorum_achieved = len(confirmed) >= quorum_size

        # Cleanup
        self._state._pending_provisions.pop(workflow_id, None)
        self._state._provision_confirmations.pop(workflow_id, None)

        return quorum_achieved

    def get_dispatch_metrics(self) -> dict:
        """Get dispatch-related metrics."""
        return {
            "throughput_count": self._state._dispatch_throughput_count,
            "pending_provisions": len(self._state._pending_provisions),
            "active_semaphores": len(self._state._dispatch_semaphores),
        }
