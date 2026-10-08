"""Wire model ``AggregatedJobStats`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message

if TYPE_CHECKING:
    from .global_job_result import GlobalJobResult


@dataclass(slots=True)
class AggregatedJobStats(Message):
    """
    Aggregated statistics across all datacenters.

    Part of GlobalJobResult for cross-DC aggregation.
    """

    total_requests: int = 0  # Total actions across all DCs
    successful_requests: int = 0  # Successful actions
    failed_requests: int = 0  # Failed actions
    overall_rate: float = 0.0  # Combined rate (requests/sec)
    avg_latency_ms: float = 0.0  # Average latency
    p50_latency_ms: float = 0.0  # Median latency
    p95_latency_ms: float = 0.0  # 95th percentile
    p99_latency_ms: float = 0.0  # 99th percentile
