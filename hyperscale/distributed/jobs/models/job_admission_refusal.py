"""``JobAdmissionRefusal`` -- why a job may not be admitted now, and when to retry."""

from dataclasses import dataclass


@dataclass(slots=True)
class JobAdmissionRefusal:
    """A refusal decided by a job-control policy (D-65 caps, D-67 breaker):
    the ``control`` that refused, its ``reason``, and the seconds after
    which the submitter should try again."""

    control: str
    reason: str
    retry_after_seconds: float
