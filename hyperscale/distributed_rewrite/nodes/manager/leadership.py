"""
Manager leadership module.

Handles leader election callbacks, split-brain detection, and leadership
state transitions.
"""

from typing import TYPE_CHECKING, Callable

from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.state import ManagerState
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.logging import Logger


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
        task_runner,
        is_leader_fn: Callable[[], bool],
        get_term_fn: Callable[[], int],
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner
        self._is_leader = is_leader_fn
        self._get_term = get_term_fn
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
            )
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
                    )
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
            )
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
                    )
                )

    def has_quorum(self) -> bool:
        """
        Check if manager cluster has quorum.

        Returns:
            True if quorum is available
        """
        active_count = self._state.get_active_peer_count()
        known_count = len(self._state._known_manager_peers) + 1  # Include self
        quorum_size = known_count // 2 + 1
        return active_count >= quorum_size

    def get_quorum_size(self) -> int:
        """
        Get required quorum size.

        Returns:
            Number of managers needed for quorum
        """
        known_count = len(self._state._known_manager_peers) + 1
        return known_count // 2 + 1

    def detect_split_brain(self) -> bool:
        """
        Detect potential split-brain scenario.

        Returns:
            True if split-brain is suspected
        """
        if not self._is_leader():
            return False

        # Check if we have quorum
        if not self.has_quorum():
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message="Split-brain suspected: leader without quorum",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )
            return True

        return False

    def get_leadership_metrics(self) -> dict:
        """Get leadership-related metrics."""
        return {
            "is_leader": self._is_leader(),
            "current_term": self._get_term(),
            "has_quorum": self.has_quorum(),
            "quorum_size": self.get_quorum_size(),
            "active_peer_count": self._state.get_active_peer_count(),
            "known_peer_count": len(self._state._known_manager_peers),
        }
