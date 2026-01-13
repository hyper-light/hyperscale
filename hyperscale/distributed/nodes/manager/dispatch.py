"""
Manager dispatch module for workflow dispatch orchestration.

Handles worker allocation, quorum coordination, and dispatch tracking.
Implements AD-17 smart dispatch with health bucket selection.
"""

import asyncio
from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    WorkflowDispatch,
    WorkflowDispatchAck,
    ProvisionRequest,
    ProvisionConfirm,
    WorkerRegistration,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerDebug,
    ServerWarning,
)

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.nodes.manager.registry import ManagerRegistry
    from hyperscale.distributed.nodes.manager.leases import ManagerLeaseCoordinator
    from hyperscale.logging import Logger


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
                ),
            )
            return None

        worker_id = worker.node.node_id

        # Get dispatch semaphore for worker
        semaphore = await self._state.get_dispatch_semaphore(
            worker_id,
            self._config.dispatch_max_concurrent_per_worker,
        )

        async with semaphore:
            fence_token = await self._leases.increment_fence_token(job_id)

            dispatch = WorkflowDispatch(
                job_id=job_id,
                workflow_id=workflow_id,
                workflow=workflow_data,
                fence_token=fence_token,
                cores=cores_required,
            )

            worker_addr = (worker.node.host, worker.node.port)
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
                            ),
                        )
                        # Update throughput counter
                        self._state._dispatch_throughput_count += 1
                    else:
                        # Worker rejected dispatch - record failure
                        self._task_runner.run(
                            self._logger.log,
                            ServerWarning(
                                message=f"Worker {worker_id[:8]}... rejected dispatch for workflow {workflow_id[:8]}...: {ack.rejection_reason}",
                                node_host=self._config.host,
                                node_port=self._config.tcp_port,
                                node_id=self._node_id,
                            ),
                        )
                        self._state._dispatch_failure_count += 1
                    return ack

                # Response was None or Exception - worker unreachable or timeout
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Dispatch to worker {worker_id[:8]}... got no response for workflow {workflow_id[:8]}...",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )
                self._state._dispatch_failure_count += 1
                if circuit := self._state._worker_circuits.get(worker_id):
                    circuit.record_error()

            except Exception as e:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Dispatch to worker {worker_id[:8]}... failed: {e}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )
                self._state._dispatch_failure_count += 1
                # Record failure in circuit breaker
                if circuit := self._state._worker_circuits.get(worker_id):
                    circuit.record_error()

        return None

    async def _select_worker(
        self,
        cores_required: int,
    ) -> WorkerRegistration | None:
        """
        Select a worker using AD-17 health bucket selection.

        Selection priority: HEALTHY > BUSY > DEGRADED (overloaded excluded).
        Within each bucket, workers are sorted by capacity (descending).

        Args:
            cores_required: Number of cores required

        Returns:
            WorkerRegistration or None if no worker available
        """
        worker, worst_health = self._select_worker_with_fallback(cores_required)

        # Log if we had to fall back to degraded workers
        if worker and worst_health == "degraded":
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Dispatching to degraded worker {worker.node.node_id[:8]}..., no healthy workers available",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
        elif worker and worst_health == "busy":
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Dispatching to busy worker {worker.node.node_id[:8]}..., no healthy workers available",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

        return worker

    def _select_worker_with_fallback(
        self,
        cores_required: int,
    ) -> tuple[WorkerRegistration | None, str]:
        """
        Select worker with AD-17 fallback chain.

        Args:
            cores_required: Number of cores required

        Returns:
            Tuple of (selected worker or None, worst health used)
        """
        # Get workers bucketed by health state
        buckets = self._registry.get_workers_by_health_bucket(cores_required)

        # Selection priority: HEALTHY > BUSY > DEGRADED
        for health_level in ("healthy", "busy", "degraded"):
            workers = buckets.get(health_level, [])
            if workers:
                # Workers are already sorted by capacity (descending)
                return workers[0], health_level

        return None, "unhealthy"

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
                    confirmation = ProvisionConfirm.load(response)
                    if (
                        confirmation.confirmed
                        and confirmation.workflow_id == workflow_id
                    ):
                        self._state._provision_confirmations[workflow_id].add(
                            confirmation.confirming_node
                        )
                        self._task_runner.run(
                            self._logger.log,
                            ServerDebug(
                                message=f"Provision confirmed by {confirmation.confirming_node[:8]}... for workflow {workflow_id[:8]}...",
                                node_host=self._config.host,
                                node_port=self._config.tcp_port,
                                node_id=self._node_id,
                            ),
                        )

            except Exception as provision_error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Provision request to peer {peer_addr} failed: {provision_error}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

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
