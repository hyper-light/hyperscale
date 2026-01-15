from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.results.scenario_result import ScenarioResult

if TYPE_CHECKING:
    from tests.framework.runtime.scenario_runtime import ScenarioRuntime


@dataclass(slots=True)
class ScenarioOutcome:
    name: str
    result: ScenarioResult
    duration_seconds: float
    actions: list[ActionOutcome] = field(default_factory=list)
    error: str | None = None
    runtime: "ScenarioRuntime | None" = None
