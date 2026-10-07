"""
Manager leadership module.

Tracks the manager quorum the leader needs, and steps a leader down
that lost it.
"""

from typing import TYPE_CHECKING, Callable

from hyperscale.logging.hyperscale_logging_models import ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.logging import Logger


# Consecutive lost-quorum checks before a leader steps down: one missed
# check is a probe hiccup, several in a row are a partition.
MAX_CONSECUTIVE_QUORUM_FAILURES = 3


class ManagerLeadershipCoordinator:
    """
    Coordinates manager leadership and election.

    Handles quorum tracking: whether the managers this node reaches are a
    quorum of the cohort, and stepping a leader down that lost it.
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        is_leader_fn: Callable[[], bool],
        step_down_fn: Callable[[], None],
        cohort_size_fn: Callable[[], int],
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._is_leader: Callable[[], bool] = is_leader_fn
        self._step_down: Callable[[], None] = step_down_fn
        # The datacenter's manager cohort: the one each manager was
        # launched with, until a committed resize changes it (AD-52).
        self._cohort_size: Callable[[], int] = cohort_size_fn

    def has_quorum(self) -> bool:
        """Whether the managers this node can reach (itself included) are a
        quorum of the configured cluster.

        ``get_active_peer_count`` already counts this node; adding it again
        let an isolated manager of three believe it held quorum.
        """
        return self._state.get_active_peer_count() >= self.get_quorum_size()

    def _should_step_down_for_quorum(self, failure_count: int) -> bool:
        """AD-3: a leader that has lacked quorum for MAX_CONSECUTIVE_QUORUM_FAILURES checks."""
        return self._is_leader() and failure_count >= MAX_CONSECUTIVE_QUORUM_FAILURES

    async def check_quorum_status(self) -> None:
        """One lost-quorum check: reset the failure streak while quorum
        holds; a leader that has lacked quorum for
        ``MAX_CONSECUTIVE_QUORUM_FAILURES`` checks steps down (AD-3: a
        leader must not keep acting on a minority view)."""
        if self.has_quorum():
            self._state.reset_quorum_failures()
            return

        failure_count = self._state.increment_quorum_failures()
        if not self._should_step_down_for_quorum(failure_count):
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
        """Quorum size from the cohort (AD-3): the one this manager was
        configured with, until a committed resize changes it (AD-52).

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
        return self._cohort_size() // 2 + 1
