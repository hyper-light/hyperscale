import json
from dataclasses import dataclass
from pathlib import Path

from tests.framework.specs.action_spec import ActionSpec
from tests.framework.specs.cluster_spec import ClusterSpec


@dataclass(slots=True)
class ScenarioSpec:
    name: str
    description: str | None
    cluster: ClusterSpec
    actions: list[ActionSpec]
    timeouts: dict[str, float]

    @classmethod
    def from_dict(cls, data: dict) -> "ScenarioSpec":
        name = data.get("name")
        if not name:
            raise ValueError("Scenario requires name")
        description = data.get("description")
        cluster_data = data.get("cluster")
        if not isinstance(cluster_data, dict):
            raise ValueError("Scenario requires cluster definition")
        cluster = ClusterSpec.from_dict(cluster_data)
        actions_data = data.get("actions", [])
        actions = [ActionSpec.from_dict(action) for action in actions_data]
        timeouts = data.get("timeouts", {})
        normalized_timeouts = {key: float(value) for key, value in timeouts.items()}
        return cls(
            name=name,
            description=description,
            cluster=cluster,
            actions=actions,
            timeouts=normalized_timeouts,
        )

    @classmethod
    def from_json(cls, path: str | Path) -> "ScenarioSpec":
        scenario_path = Path(path)
        payload = json.loads(scenario_path.read_text())
        return cls.from_dict(payload)
