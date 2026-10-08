"""The last progress a ``JobMakesProgress`` watch saw for one job on one leader."""

from dataclasses import dataclass

JobProgressSignature = tuple[object, ...]


@dataclass(slots=True, frozen=True)
class JobProgressMark:
    """The leader instance observed, the job's progress signature then, and
    when (harness monotonic seconds) the signature last changed."""

    leader_instance: object
    signature: JobProgressSignature
    changed_at: float
