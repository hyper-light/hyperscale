import time

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.action_spec import ActionSpec


def _assert_count(
    label: str,
    count: int,
    min_count: int | None,
    max_count: int | None,
    equals_count: int | None,
) -> None:
    if equals_count is not None:
        assert count == equals_count, (
            f"Expected {label} count {equals_count}, got {count}"
        )
    if min_count is not None:
        assert count >= min_count, f"Expected {label} count >= {min_count}, got {count}"
    if max_count is not None:
        assert count <= max_count, f"Expected {label} count <= {max_count}, got {count}"


def _resolve_target(runtime: ScenarioRuntime, target: str) -> object:
    if target == "status_updates":
        return runtime.callbacks.status_updates
    if target == "progress_updates":
        return runtime.callbacks.progress_updates
    if target == "workflow_results":
        return runtime.callbacks.workflow_results
    if target == "reporter_results":
        return runtime.callbacks.reporter_results
    if target == "job_ids":
        return runtime.job_ids
    if target == "last_job_id":
        return runtime.last_job_id
    raise ValueError(f"Unknown assert target '{target}'")


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    target_name = action.params.get("target")
    if not target_name:
        raise ValueError("assert_condition requires target")
    target = _resolve_target(runtime, target_name)
    min_count = action.params.get("min_count")
    max_count = action.params.get("max_count")
    equals_count = action.params.get("equals_count")
    if min_count is not None:
        min_count = int(min_count)
    if max_count is not None:
        max_count = int(max_count)
    if equals_count is not None:
        equals_count = int(equals_count)
    if isinstance(target, list):
        _assert_count("list", len(target), min_count, max_count, equals_count)
        contains = action.params.get("contains")
        if contains is not None:
            assert contains in target, f"Expected list to contain {contains}"
    elif isinstance(target, dict):
        _assert_count("dict", len(target), min_count, max_count, equals_count)
        key = action.params.get("key")
        if key is not None:
            assert key in target, f"Expected dict to include key '{key}'"
            value_equals = action.params.get("value_equals")
            if value_equals is not None:
                assert target[key] == value_equals, (
                    f"Expected dict value for '{key}' to equal {value_equals}"
                )
    else:
        equals_value = action.params.get("equals")
        if equals_value is None:
            raise ValueError("assert_condition requires equals for scalar target")
        assert target == equals_value, f"Expected {target_name} to equal {equals_value}"
    return ActionOutcome(
        name="assert_condition",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
        details=target_name,
    )
