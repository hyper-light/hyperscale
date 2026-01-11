"""
Target selection for HyperscaleClient.

Handles round-robin selection of gates/managers and sticky routing to job targets.
"""

from hyperscale.distributed_rewrite.nodes.client.config import ClientConfig
from hyperscale.distributed_rewrite.nodes.client.state import ClientState


class ClientTargetSelector:
    """
    Manages target selection for job submission and queries.

    Uses round-robin selection for new jobs and sticky routing for
    existing jobs (returns to the server that accepted the job).

    Leadership-aware: when a job's leader is known, routes to that leader first.
    """

    def __init__(self, config: ClientConfig, state: ClientState) -> None:
        self._config = config
        self._state = state

    def get_callback_addr(self) -> tuple[str, int]:
        """
        Get this client's address for push notifications.

        Returns:
            (host, port) tuple for TCP callbacks
        """
        return (self._config.host, self._config.tcp_port)

    def get_next_manager(self) -> tuple[str, int] | None:
        """
        Get next manager address using round-robin selection.

        Returns:
            Manager (host, port) or None if no managers configured
        """
        if not self._config.managers:
            return None

        addr = self._config.managers[self._state._current_manager_idx]
        self._state._current_manager_idx = (self._state._current_manager_idx + 1) % len(
            self._config.managers
        )
        return addr

    def get_next_gate(self) -> tuple[str, int] | None:
        """
        Get next gate address using round-robin selection.

        Returns:
            Gate (host, port) or None if no gates configured
        """
        if not self._config.gates:
            return None

        addr = self._config.gates[self._state._current_gate_idx]
        self._state._current_gate_idx = (self._state._current_gate_idx + 1) % len(
            self._config.gates
        )
        return addr

    def get_all_targets(self) -> list[tuple[str, int]]:
        """
        Get all available gate and manager targets.

        Returns:
            List of all gates + managers
        """
        return list(self._config.gates) + list(self._config.managers)

    def get_targets_for_job(self, job_id: str) -> list[tuple[str, int]]:
        """
        Get targets prioritizing the one that accepted the job.

        Implements sticky routing: if we know which server accepted this job,
        return it first for faster reconnection and consistent routing.

        Args:
            job_id: Job identifier

        Returns:
            List with job target first if known, then all other gates/managers
        """
        all_targets = self.get_all_targets()

        # Check if we have a known target for this job
        job_target = self._state.get_job_target(job_id)
        if not job_target:
            return all_targets

        # Put job target first, then others
        return [job_target] + [t for t in all_targets if t != job_target]

    def get_preferred_gate_for_job(self, job_id: str) -> tuple[str, int] | None:
        """
        Get the gate address from gate leader tracking.

        Args:
            job_id: Job identifier

        Returns:
            Gate (host, port) if leader known, else None
        """
        leader_info = self._state._gate_job_leaders.get(job_id)
        if leader_info:
            return leader_info.gate_addr
        return None

    def get_preferred_manager_for_job(
        self, job_id: str, datacenter_id: str
    ) -> tuple[str, int] | None:
        """
        Get the manager address from manager leader tracking.

        Args:
            job_id: Job identifier
            datacenter_id: Datacenter identifier

        Returns:
            Manager (host, port) if leader known, else None
        """
        leader_info = self._state._manager_job_leaders.get((job_id, datacenter_id))
        if leader_info:
            return leader_info.manager_addr
        return None
