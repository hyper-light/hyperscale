from dataclasses import dataclass


@dataclass(slots=True)
class ActionSpec:
    action_type: str
    params: dict
    timeout_seconds: float | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "ActionSpec":
        action_type = data.get("type")
        if not action_type:
            raise ValueError("Action requires 'type'")
        params = data.get("params", {})
        timeout_seconds = data.get("timeout_seconds")
        if timeout_seconds is not None:
            timeout_seconds = float(timeout_seconds)
        return cls(
            action_type=action_type, params=params, timeout_seconds=timeout_seconds
        )
