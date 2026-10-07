"""
The shape of a job as admission control sees it (D-65, D-67).

A job's *class* is the set of workflows it runs, named as the job's
workflows depend on each other (``workflow_name``): every submission of
the same test is the same class, whatever its job id. Its *work* is the
core-seconds its workflows hold: each workflow runs on one core per VU
(its own VUs, else the submission's), at least one and at most the
datacenter's registered cores -- the cores the dispatcher gives an AUTO
workflow that has the datacenter to itself -- for its ``duration``.
"""

from collections.abc import Iterable

from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.taskex.util.time_parser import TimeParser

JOB_CLASS_SEPARATOR = "+"


def job_class_name(workflow_names: Iterable[str]) -> str:
    """The job class of a job running workflows with these names: the
    distinct names, sorted, joined by ``JOB_CLASS_SEPARATOR``."""
    return JOB_CLASS_SEPARATOR.join(sorted(set(workflow_names)))


def workflow_cores(instance: Workflow, submission_vus: int, registered_cores: int) -> int:
    """The cores a workflow holds while it runs: one per VU -- its own
    positive VU count, else the submission's -- at least one, at most the
    datacenter's registered cores."""
    vus = instance.vus if instance.vus and instance.vus > 0 else submission_vus
    return min(registered_cores, max(1, vus))


def job_core_seconds(
    instances: Iterable[Workflow],
    submission_vus: int,
    registered_cores: int,
) -> float:
    """The core-seconds a job's workflows hold in all: each workflow's
    cores for its duration."""
    return sum(
        workflow_cores(instance, submission_vus, registered_cores) * TimeParser(instance.duration).time
        for instance in instances
    )
