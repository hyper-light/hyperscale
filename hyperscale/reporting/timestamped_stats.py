from dataclasses import dataclass

from hyperscale.reporting.common.results_types import WorkflowStats


@dataclass
class TimestampedStats:
    """WorkflowStats with associated collection timestamp."""
    stats: WorkflowStats
    collected_at: float  # Unix timestamp when stats were collected
    source: str = ""     # Identifier for source (worker_id, datacenter, etc.)
