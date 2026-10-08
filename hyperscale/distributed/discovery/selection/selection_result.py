"""``SelectionResult`` -- pickled under the namespace
``hyperscale.distributed.discovery.selection.adaptive_selector`` (see that module)."""

from dataclasses import dataclass


@dataclass
class SelectionResult:
    """Result of peer selection."""

    peer_id: str
    """Selected peer ID."""

    effective_latency_ms: float
    """Effective latency of selected peer."""

    was_load_balanced: bool
    """True if load-aware selection was used."""

    candidates_considered: int
    """Number of candidates that were considered."""
