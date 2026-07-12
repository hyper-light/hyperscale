"""
The SIM linearizability oracle for the client-facing job API.

``JobStatusOracle`` (Phase 7) judges CLIENT-OBSERVED histories — the
``("status-seen", status, t)`` / ``("job-finished", status, t)``
milestones the SIM client entries record — against the job-status
lifecycle spec: monotone rank order, absorbing terminal states, and
finished/observed coherence.
"""

from .job_status_oracle import JobStatusOracle

__all__ = ("JobStatusOracle",)
