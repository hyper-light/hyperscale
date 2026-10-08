from __future__ import annotations

from collections.abc import Callable

from hyperscale.distributed.runtime import Clock

from hyperscale.distributed.resources.led_workflow_sample import LedWorkflowSample
from hyperscale.distributed.resources.workload_resource_totals import WorkloadResourceTotals


class LedWorkflowResources:
    """AD-41: resource use of the workflows this manager leads.

    Each workflow's progress reaches only its job's leader, so the
    workflows every manager leads partition the datacenter's running
    workload: summing each manager's totals counts every workflow once,
    however many managers know a worker.

    Holds the latest estimate per workflow, released when the workflow
    reaches a terminal status or its job is cleaned up. An estimate is
    dropped rather than counted when no progress came for
    ``staleness_seconds`` (its worker died, its reports were lost) or when
    this manager no longer leads the job (``leads_job``; a takeover moved
    it), since the new leader counts it from then on.
    """

    __slots__ = (
        "_clock",
        "_staleness_seconds",
        "_leads_job",
        "_samples",
        "_workflows_by_job",
    )

    def __init__(
        self,
        clock: Clock,
        staleness_seconds: float,
        leads_job: Callable[[str], bool],
    ) -> None:
        self._clock = clock
        self._staleness_seconds = staleness_seconds
        self._leads_job = leads_job
        self._samples: dict[str, LedWorkflowSample] = {}
        self._workflows_by_job: dict[str, set[str]] = {}

    def record(
        self,
        workflow_id: str,
        job_id: str,
        cpu_percent: float,
        cpu_uncertainty: float,
        memory_bytes: float,
        memory_uncertainty: float,
    ) -> None:
        """Replace ``workflow_id``'s estimate with the newest progress."""
        self._samples[workflow_id] = LedWorkflowSample(
            job_id=job_id,
            cpu_percent=cpu_percent,
            cpu_variance=cpu_uncertainty**2,
            memory_bytes=memory_bytes,
            memory_variance=memory_uncertainty**2,
            observed_at=self._clock.monotonic(),
        )
        self._workflows_by_job.setdefault(job_id, set()).add(workflow_id)

    def release_workflow(self, workflow_id: str) -> None:
        """Forget a workflow that ended."""
        if (sample := self._samples.pop(workflow_id, None)) is None:
            return
        if (job_workflows := self._workflows_by_job.get(sample.job_id)) is not None:
            job_workflows.discard(workflow_id)
            if not job_workflows:
                del self._workflows_by_job[sample.job_id]

    def release_job(self, job_id: str) -> None:
        """Forget every workflow of a job that was cleaned up."""
        for workflow_id in self._workflows_by_job.pop(job_id, set()):
            self._samples.pop(workflow_id, None)

    def totals(self) -> WorkloadResourceTotals:
        """Sum of the live estimates; stale ones, and those of jobs this
        manager no longer leads, are dropped first."""
        cutoff = self._clock.monotonic() - self._staleness_seconds
        for job_id in [job_id for job_id in self._workflows_by_job if not self._leads_job(job_id)]:
            self.release_job(job_id)
        for workflow_id in [
            workflow_id
            for workflow_id, sample in self._samples.items()
            if sample.observed_at < cutoff
        ]:
            self.release_workflow(workflow_id)

        samples = self._samples.values()
        return WorkloadResourceTotals(
            cpu_percent=sum(sample.cpu_percent for sample in samples),
            cpu_variance=sum(sample.cpu_variance for sample in samples),
            memory_bytes=sum(sample.memory_bytes for sample in samples),
            memory_variance=sum(sample.memory_variance for sample in samples),
            workflow_count=len(self._samples),
        )

    @property
    def workflow_count(self) -> int:
        return len(self._samples)

    @property
    def job_count(self) -> int:
        return len(self._workflows_by_job)
