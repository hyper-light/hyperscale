"""
Workflow progress TCP handler for worker.

Handles workflow progress acks from managers (AD-23 backpressure).
"""

from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.models import WorkflowProgressAck
from hyperscale.distributed_rewrite.reliability import BackpressureLevel, BackpressureSignal

if TYPE_CHECKING:
    from ..server import WorkerServer


class WorkflowProgressHandler:
    """
    Handler for workflow progress acknowledgments from managers.

    Processes progress acks to update manager topology and handle
    backpressure signals (AD-23).
    """

    def __init__(self, server: "WorkerServer") -> None:
        """
        Initialize handler with server reference.

        Args:
            server: WorkerServer instance for state access
        """
        self._server = server

    def process_ack(self, data: bytes, workflow_id: str | None = None) -> None:
        """
        Process WorkflowProgressAck to update manager topology.

        Args:
            data: Serialized WorkflowProgressAck bytes
            workflow_id: If provided, updates job leader routing for this workflow
        """
        try:
            ack = WorkflowProgressAck.load(data)

            # Update known managers from ack
            self._update_known_managers(ack)

            # Update primary manager if cluster leadership changed
            if ack.is_leader and self._server._primary_manager_id != ack.manager_id:
                self._server._primary_manager_id = ack.manager_id

            # Update job leader routing if provided and changed
            if workflow_id and ack.job_leader_addr:
                current_leader = self._server._workflow_job_leader.get(workflow_id)
                if current_leader != ack.job_leader_addr:
                    self._server._workflow_job_leader[workflow_id] = ack.job_leader_addr

            # AD-23: Extract and apply backpressure signal
            if ack.backpressure_level > 0:
                self._handle_backpressure(ack)

        except Exception:
            # Backwards compatibility: ignore parse errors for old b'ok' responses
            pass

    def _update_known_managers(self, ack: WorkflowProgressAck) -> None:
        """Update known managers from ack response."""
        for manager in ack.healthy_managers:
            self._server._registry.add_manager(manager.node_id, manager)

    def _handle_backpressure(self, ack: WorkflowProgressAck) -> None:
        """Handle backpressure signal from manager."""
        signal = BackpressureSignal(
            level=BackpressureLevel(ack.backpressure_level),
            suggested_delay_ms=ack.backpressure_delay_ms,
            batch_only=ack.backpressure_batch_only,
        )
        self._server._backpressure_manager.set_manager_backpressure(
            ack.manager_id, signal.level
        )
        self._server._backpressure_manager.set_backpressure_delay_ms(
            max(
                self._server._backpressure_manager.get_backpressure_delay_ms(),
                signal.suggested_delay_ms,
            )
        )
