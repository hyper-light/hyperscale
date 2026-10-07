"""
Target selection for HyperscaleClient.

Handles AD-28 ranked selection of submission targets, round-robin selection
of gates/managers for queries, and sticky routing to job targets.
"""

from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.nodes.client.config import ClientConfig
from hyperscale.distributed.nodes.client.state import ClientState


class ClientTargetSelector:
    """
    Manages target selection for job submission and queries.

    Ranks submission targets for new jobs with AD-28 (rendezvous + EWMA),
    uses round-robin for target-agnostic queries, and sticky routing for
    existing jobs (returns to the server that accepted the job).

    Leadership-aware: when a job's leader is known, routes to that leader first.
    """

    def __init__(
        self,
        config: ClientConfig,
        state: ClientState,
        discovery: DiscoveryService,
    ) -> None:
        self._config = config
        self._state = state
        # AD-28: ranks submission targets (weighted rendezvous on the job id
        # + EWMA latency). Every configured gate/manager is a peer keyed by
        # "host:port"; the configured set is fixed for the client's life.
        self._discovery = discovery
        self._target_by_peer_id: dict[str, tuple[str, int]] = {}
        for role, targets in (("gate", config.gates), ("manager", config.managers)):
            for target in targets:
                peer_id = _peer_id(target)
                self._target_by_peer_id[peer_id] = target
                discovery.add_peer(
                    peer_id=peer_id,
                    host=target[0],
                    port=target[1],
                    role=role,
                    datacenter_id="",
                )

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

    def get_submission_targets(self, job_id: str) -> list[tuple[str, int]]:
        """Order targets for submitting ``job_id`` (AD-28).

        Gates stay preferred over managers (managers are the gateless
        fallback); within each tier, rendezvous ranking on the job id
        spreads jobs across targets — instead of every submission from
        every client starting at the first configured gate — and EWMA
        latency steers away from slow or failing targets. Every
        configured target appears exactly once.
        """
        ranked = [
            *self._ranked_tier(job_id, self._config.gates),
            *self._ranked_tier(job_id, self._config.managers),
        ]
        return list(dict.fromkeys(ranked))

    def record_target_success(self, target: tuple[str, int], latency_ms: float) -> None:
        """Feed a successful submission round trip into AD-28's EWMA.

        Only configured targets are tracked: a leader redirect can name an
        address outside the configured set, and recording it would grow
        selector state without bound.
        """
        if (peer_id := _peer_id(target)) in self._target_by_peer_id:
            self._discovery.record_success(peer_id, latency_ms)

    def record_target_failure(self, target: tuple[str, int]) -> None:
        """Count a failed submission attempt against a configured ``target``."""
        if (peer_id := _peer_id(target)) in self._target_by_peer_id:
            self._discovery.record_failure(peer_id)

    def _ranked_tier(
        self,
        job_id: str,
        tier_targets: list[tuple[str, int]],
    ) -> list[tuple[str, int]]:
        if not tier_targets:
            return []

        ranked = self._rank_tier_targets(job_id, tier_targets)
        return list(dict.fromkeys([*ranked, *tier_targets]))

    def _rank_tier_targets(
        self,
        job_id: str,
        tier_targets: list[tuple[str, int]],
    ) -> list[tuple[str, int]]:
        """The tier's targets in AD-28 rendezvous order for the job."""
        tier_peer_ids = set(map(_peer_id, tier_targets))
        selections = self._discovery.select_peers(
            job_id,
            count=len(self._target_by_peer_id),
        )
        return [
            self._target_by_peer_id[selection.peer_id]
            for selection in selections
            if selection.peer_id in tier_peer_ids
        ]

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
        return [job_target] + self._targets_other_than(all_targets, job_target)

    @staticmethod
    def _targets_other_than(
        all_targets: list[tuple[str, int]],
        job_target: tuple[str, int],
    ) -> list[tuple[str, int]]:
        """Every target except the job's, in order."""
        return [target for target in all_targets if target != job_target]

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

    def get_gate_for_job(self, job_id: str) -> tuple[str, int] | None:
        """
        Get the best known gate address for a job.

        Args:
            job_id: Job identifier

        Returns:
            Gate (host, port) if a leader or sticky gate target is known, else None
        """
        preferred_gate = self.get_preferred_gate_for_job(job_id)
        if preferred_gate is not None:
            return preferred_gate

        job_target = self._state.get_job_target(job_id)
        if job_target in self._config.gates:
            return job_target
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


def _peer_id(target: tuple[str, int]) -> str:
    return f"{target[0]}:{target[1]}"
