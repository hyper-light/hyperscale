"""
AD-41 per-workflow resource estimates on a worker.
"""

from dataclasses import dataclass, field

from hyperscale.distributed.runtime import Clock

from .adaptive_kalman_filter import AdaptiveKalmanFilter
from .process_resource_monitor import (
    CPU_MEASUREMENT_NOISE,
    CPU_PROCESS_NOISE,
    MEMORY_MEASUREMENT_NOISE,
    MEMORY_PROCESS_NOISE,
)
from .resource_metrics import ResourceMetrics

BYTES_PER_MEGABYTE = 1024 * 1024


@dataclass(slots=True)
class WorkflowResourceTracker:
    """Kalman-filtered CPU and memory per running workflow.

    Fed with each workflow's whole footprint as its executors measure it
    (summed across the workflow's executor processes, which the executor
    pool allocates to one workflow at a time), so an estimate is that
    workflow's use and nothing else's -- what AD-41 enforcement needs to
    act on one workflow without collateral damage. Every estimate carries
    the filter's uncertainty, which the enforcer's 2-sigma kill gate uses.

    A workflow's filters are released with the workflow, so state is
    bounded by the workflows running on this worker.
    """

    clock: Clock
    total_memory_bytes: int
    _cpu_filters: dict[str, AdaptiveKalmanFilter] = field(default_factory=dict, init=False)
    _memory_filters: dict[str, AdaptiveKalmanFilter] = field(default_factory=dict, init=False)
    _latest: dict[str, ResourceMetrics] = field(default_factory=dict, init=False)

    @property
    def tracked_workflow_count(self) -> int:
        return len(self._latest)

    def observe(
        self,
        workflow_id: str,
        cpu_percent: float,
        memory_megabytes: float,
        process_count: int,
    ) -> ResourceMetrics:
        """Fold one measurement of ``workflow_id`` into its estimates."""
        cpu_filter = self._cpu_filters.get(workflow_id)
        if cpu_filter is None:
            cpu_filter = AdaptiveKalmanFilter(
                initial_process_noise=CPU_PROCESS_NOISE,
                initial_measurement_noise=CPU_MEASUREMENT_NOISE,
            )
            self._cpu_filters[workflow_id] = cpu_filter
        memory_filter = self._memory_filters.get(workflow_id)
        if memory_filter is None:
            memory_filter = AdaptiveKalmanFilter(
                initial_process_noise=MEMORY_PROCESS_NOISE,
                initial_measurement_noise=MEMORY_MEASUREMENT_NOISE,
            )
            self._memory_filters[workflow_id] = memory_filter

        cpu_estimate, cpu_uncertainty = cpu_filter.update(cpu_percent)
        memory_estimate, memory_uncertainty = memory_filter.update(
            memory_megabytes * BYTES_PER_MEGABYTE
        )
        previous = self._latest.get(workflow_id)
        metrics = ResourceMetrics(
            cpu_percent=max(cpu_estimate, 0.0),
            cpu_uncertainty=cpu_uncertainty,
            memory_bytes=int(max(memory_estimate, 0.0)),
            memory_uncertainty=memory_uncertainty,
            memory_percent=(
                100.0 * max(memory_estimate, 0.0) / self.total_memory_bytes
                if self.total_memory_bytes > 0
                else 0.0
            ),
            file_descriptor_count=0,
            timestamp_monotonic=self.clock.monotonic(),
            sample_count=(previous.sample_count + 1) if previous is not None else 1,
            process_count=process_count,
        )
        self._latest[workflow_id] = metrics
        return metrics

    def snapshot(self) -> dict[str, ResourceMetrics]:
        """Latest estimate per running workflow (a copy)."""
        return dict(self._latest)

    def release(self, workflow_id: str) -> None:
        """Drop a finished workflow's filters and estimate."""
        self._cpu_filters.pop(workflow_id, None)
        self._memory_filters.pop(workflow_id, None)
        self._latest.pop(workflow_id, None)
