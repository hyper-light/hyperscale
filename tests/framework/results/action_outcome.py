from dataclasses import dataclass


@dataclass(slots=True)
class ActionOutcome:
    name: str
    succeeded: bool
    duration_seconds: float
    details: str | None = None
