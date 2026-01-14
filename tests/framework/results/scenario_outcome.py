from dataclasses import dataclass, field

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.results.scenario_result import ScenarioResult


@dataclass(slots=True)
class ScenarioOutcome:
    name: str
    result: ScenarioResult
    duration_seconds: float
    actions: list[ActionOutcome] = field(default_factory=list)
    error: str | None = None
