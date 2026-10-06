"""
The SIM oracles — the state checkers every suite's invariants build on.

``JobStatusOracle`` (Phase 7, G1) judges CLIENT-OBSERVED histories —
the ``("status-seen", status, t)`` / ``("job-finished", status, t)``
milestones the SIM client entries record — against the job-status
lifecycle spec: monotone rank order, absorbing terminal states, and
finished/observed coherence.

``ClusterTraceOracle`` (G3) judges the MERGED cross-node milestone
trace of one run — gate-leader exclusivity and convergence, per-job
client/server terminal agreement, workflow execution counts,
single-DC placement, datacenter-health convergence, and
determinism-audit absence — catching silent wrongness that never
reaches the client.

``JobLogSplitter`` (K2) splits a prefixed multi-job client log into
per-job unprefixed streams, each feedable to ``JobStatusOracle``.

``WorkflowLifecycleOracle`` (AD-54) judges a MANAGER's observed workflow
lifecycle histories — table edges only, continuity, absorbing terminals,
FAILED observable only as terminal, status equal to the state's
projection, and every record released with its job.
"""

from .cluster_trace_oracle import ClusterTraceOracle
from .job_log_splitter import JobLogSplitter
from .job_status_oracle import JobStatusOracle
from .workflow_lifecycle_oracle import WorkflowLifecycleOracle

__all__ = (
    "ClusterTraceOracle",
    "JobLogSplitter",
    "JobStatusOracle",
    "WorkflowLifecycleOracle",
)
