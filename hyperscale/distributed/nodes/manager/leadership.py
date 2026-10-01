"""
Manager leadership module.

Handles leader election callbacks, split-brain detection, and leadership
state transitions.
"""

from typing import TYPE_CHECKING, Any, Callable

from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


# Consecutive lost-quorum checks before a leader steps down: one missed
# check is a probe hiccup, several in a row are a partition.
MAX_CONSECUTIVE_QUORUM_FAILURES = 3


class ManagerLeadershipCoordinator:
    """
    Coordinates manager leadership and election.

    Handles:
    - Leader election callbacks from LocalLeaderElection
    - Split-brain detection and resolution
    - Leadership state transitions
    - Quorum tracking
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner: "TaskRunner",
        is_leader_fn: Callable[[], bool],
        get_term_fn: Callable[[], int],
        step_down_fn: Callable[[], None],
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner
        self._is_leader: Callable[[], bool] = is_leader_fn
        self._get_term: Callable[[], int] = get_term_fn
        self._step_down: Callable[[], None] = step_down_fn
        self._on_become_leader_callbacks: list[Callable[[], None]] = []
        self._on_lose_leadership_callbacks: list[Callable[[], None]] = []

    def register_on_become_leader(self, callback: Callable[[], None]) -> None:
        """
        Register callback for when this manager becomes leader.

        Args:
            callback: Callback function (no args)
        """
        self._on_become_leader_callbacks.append(callback)

    def register_on_lose_leadership(self, callback: Callable[[], None]) -> None:
        """
        Register callback for when this manager loses leadership.

        Args:
            callback: Callback function (no args)
        """
        self._on_lose_leadership_callbacks.append(callback)

    def on_become_leader(self) -> None:
        """
        Called when this manager becomes the SWIM cluster leader.

        Triggers:
        - State sync from workers
        - State sync from peer managers
        - Orphaned job scanning
        """
        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Manager became leader (term {self._get_term()})",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

        for callback in self._on_become_leader_callbacks:
            try:
                callback()
            except Exception as e:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"On-become-leader callback failed: {e}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

    def on_lose_leadership(self) -> None:
        """
        Called when this manager loses SWIM cluster leadership.
        """
        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message="Manager lost leadership",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

        for callback in self._on_lose_leadership_callbacks:
            try:
                callback()
            except Exception as e:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"On-lose-leadership callback failed: {e}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

    def has_quorum(self) -> bool:
        """Whether the managers this node can reach (itself included) are a
        quorum of the configured cluster.

        ``get_active_peer_count`` already counts this node; adding it again
        let an isolated manager of three believe it held quorum.
        """
        return self._state.get_active_peer_count() >= self.get_quorum_size()

    async def check_quorum_status(self) -> None:
        """One lost-quorum check: reset the failure streak while quorum
        holds; a leader that has lacked quorum for
        ``MAX_CONSECUTIVE_QUORUM_FAILURES`` checks steps down (AD-3: a
        leader must not keep acting on a minority view)."""
        if self.has_quorum():
            self._state.reset_quorum_failures()
            return

        failure_count = self._state.increment_quorum_failures()
        if not self._is_leader() or failure_count < MAX_CONSECUTIVE_QUORUM_FAILURES:
            return

        await self._logger.log(
            ServerWarning(
                message=f"Lost quorum for {failure_count} consecutive checks, stepping down",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )
        self._step_down()

    def get_quorum_size(self) -> int:
        """Quorum size from **configured** cluster size (AD-3).

        Per AD-3, quorum is derived from the static seed list at
        startup, not from the runtime ``_known_manager_peers``
        dictionary. The known dict is dynamically built as managers
        learn about each other via TCP register; using it for the
        quorum threshold collapses to a single-node majority during
        the post-restart window when no peer registrations have
        landed yet, and a manager whose peer-discovery is in-flight
        sees ``known_count = 1`` and self-elects with quorum = 1 —
        the canonical split-brain bug AD-3 was written to prevent.
        """
        configured_managers = len(self._config.manager_udp_peers) + 1
        return configured_managers // 2 + 1

    def detect_split_brain(self) -> bool:
        if not self._is_leader():
            return False

        if not self.has_quorum():
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message="Split-brain suspected: leader without quorum",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            return True

        return False

    def get_cluster_health_level(self) -> str:
        active_count = self._state.get_active_peer_count()
        known_count = len(self._state._known_manager_peers) + 1
        dead_count = len(self._state._dead_managers)

        if known_count <= 1:
            return "standalone"

        healthy_ratio = active_count / known_count

        if healthy_ratio >= 0.8 and dead_count == 0:
            return "healthy"
        elif healthy_ratio >= 0.5:
            return "degraded"
        elif self.has_quorum():
            return "critical"
        else:
            return "no_quorum"

    def get_leadership_metrics(self) -> dict[str, Any]:
        return {
            "is_leader": self._is_leader(),
            "current_term": self._get_term(),
            "has_quorum": self.has_quorum(),
            "quorum_size": self.get_quorum_size(),
            "active_peer_count": self._state.get_active_peer_count(),
            "known_peer_count": len(self._state._known_manager_peers),
            "cluster_health_level": self.get_cluster_health_level(),
            "dead_manager_count": len(self._state._dead_managers),
        }
