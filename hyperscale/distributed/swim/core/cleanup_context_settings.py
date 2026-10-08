"""Optional cleanup overrides read by ``create_cleanup_config_from_context``."""

from typing import TypedDict


class CleanupContextSettings(TypedDict, total=False):
    """Server-context keys that override ``CleanupConfig`` defaults."""

    max_node_states: int
    dead_node_retention: float
    max_suspicions: int
    max_gossip_updates: int
    max_pending_probes: int
    cleanup_interval: float
