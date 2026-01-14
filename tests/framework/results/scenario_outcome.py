from dataclasses import dataclass, field

from .action_outcome import ActionOutcome
from .scenario_result import ScenarioResult


@dataclass(slots=True)
class ScenarioOutcome:
    name: str
    result: ScenarioResult
    duration_seconds: float
    actions: list[ActionOutcome] = field(default_factory=list)
    error: str | None = None
