"""
Workload submission shapes — what the harness submits to the cluster.

Decoupled from `ClusterSpec` (cluster topology) so a workload runs the
same way at L1, L2, or L3 — only the routing tier the
``WorkloadDriver`` connects through changes.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


WorkflowFactory = Callable[[], Any]
"""Zero-arg callable returning a fresh `hyperscale.graph.Workflow` instance.

Workflows hold state (graph, id, client) so each submission must use a
fresh instance — passing a class object directly would let the harness
construct it, but factories keep the door open for parameterized
workflows (e.g. workflow with custom ``vus``).
"""


@dataclass(slots=True, frozen=True)
class Submission:
    """One job submission: a list of (dependencies, workflow) tuples.

    The same shape ``ClientJobSubmitter.submit_job`` accepts:
    ``workflows = [(["dep_name", ...], workflow_instance), ...]``. The
    driver materializes workflows at submission time using the factories.
    """

    workflows: list[tuple[list[str], WorkflowFactory]]
    dc_count: int = 1
    timeout_seconds: float = 120.0
    vus: int = 1


class SubmissionPattern(StrEnum):
    """How submissions in a `WorkloadSpec` are dispatched."""

    SINGLE = "single"
    """Exactly one submission. Errors if `submissions` has more than one."""

    PARALLEL = "parallel"
    """All submissions dispatched concurrently as a single ``asyncio.gather``."""


@dataclass(slots=True, frozen=True)
class WorkloadSpec:
    """Full description of what the harness should run on a cluster."""

    submissions: list[Submission]
    pattern: SubmissionPattern = SubmissionPattern.SINGLE
    expectations: list = field(default_factory=list)
    """Each entry is an ``Expectation`` instance. Imported via the Expectation
    module to avoid an import cycle here."""
