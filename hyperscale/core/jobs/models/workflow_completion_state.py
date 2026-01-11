import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Literal

StepStatsType = Literal[
    "total",
    "ok",
    "err",
]

StepStatsUpdate = Dict[str, Dict[StepStatsType, int]]


@dataclass(slots=True)
class WorkflowCompletionState:
    """Tracks completion state for a workflow across all workers."""
    expected_workers: int
    completion_event: asyncio.Event
    status_update_queue: asyncio.Queue
    cores_update_queue: asyncio.Queue
    completed_count: int
    failed_count: int
    step_stats: StepStatsUpdate
    avg_cpu_usage: float
    avg_memory_usage_mb: float
    workers_completed: int
    workers_assigned: int
