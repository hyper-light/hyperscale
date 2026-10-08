"""
MonotonicFenceTokens (simulation_framework.md §12, SCENARIOS.md §11):
every node's fence token for a job never goes backwards.

The tracker keeps each (node, token map, job)'s high-water mark for the
life of the node's instance. The mark outlives the job's entry, so a
map that forgets a job and later re-learns it at an older token -- a
stale leader's late message re-arming a superseded fence -- is caught
too. A restarted node is a new instance with new memory, so its marks
start over.
"""

from typing import TYPE_CHECKING

from tests.simulation.harness.invariant_checks.fence_token_sources import (
    FENCE_TOKEN_SOURCES,
    FenceTokenMap,
)
from tests.simulation.harness.invariant_checks.live_nodes import all_live_handles
from tests.simulation.harness.invariant_result import InvariantResult
from tests.simulation.harness.server_handle import ServerHandle

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness

HighWaterKey = tuple[str, str, str]


class MonotonicFenceTokens:
    """Stateful evaluator: the high-water fence token per node, map and job."""

    def __init__(self) -> None:
        self._instances: dict[str, object] = {}
        self._high_water: dict[HighWaterKey, int] = {}

    def evaluate(self, harness: "ClusterHarness") -> InvariantResult:
        """Holds while no live node's fence token for any job went backwards."""
        for handle in all_live_handles(harness):
            if detail := self._observe_node(handle):
                return InvariantResult(holds=False, detail=detail)
        return InvariantResult(holds=True)

    def _observe_node(self, handle: ServerHandle) -> str:
        self._forget_replaced_instance(handle)
        details = [
            self._observe_source(handle.node_id, source_name, read_tokens(handle))
            for source_name, read_tokens in FENCE_TOKEN_SOURCES[handle.kind]
        ]
        return next(filter(None, details), "")

    def _observe_source(self, node_id: str, source_name: str, tokens: FenceTokenMap) -> str:
        regressions = [
            self._regression((node_id, source_name, job_id), token)
            for job_id, token in list(tokens.items())
        ]
        return next(filter(None, regressions), "")

    def _regression(self, key: HighWaterKey, token: int) -> str:
        high_water = self._high_water.get(key, token)
        self._high_water[key] = max(high_water, token)
        if token >= high_water:
            return ""
        node_id, source_name, job_id = key
        return (
            f"{node_id} {source_name} fence token for job {job_id!r} went "
            f"backwards: {high_water} -> {token}"
        )

    def _forget_replaced_instance(self, handle: ServerHandle) -> None:
        if self._instances.get(handle.node_id) is handle.instance:
            return
        self._instances[handle.node_id] = handle.instance
        for key in self._keys_of_node(handle.node_id):
            del self._high_water[key]

    def _keys_of_node(self, node_id: str) -> list[HighWaterKey]:
        return [key for key in self._high_water if key[0] == node_id]
