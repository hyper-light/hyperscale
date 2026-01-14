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


def _resolve_path(value: object, path: str) -> object:
    current_value = value
    for segment in path.split("."):
        if isinstance(current_value, dict):
            if segment not in current_value:
                raise KeyError(f"Missing key '{segment}' in path '{path}'")
            current_value = current_value[segment]
            continue
        if isinstance(current_value, (list, tuple)):
            try:
                index = int(segment)
            except ValueError as error:
                raise ValueError(
                    f"List path segment '{segment}' must be an index"
                ) from error
            try:
                current_value = current_value[index]
            except IndexError as error:
                raise IndexError(
                    f"List index {index} out of range for path '{path}'"
                ) from error
            continue
        if not hasattr(current_value, segment):
            raise AttributeError(f"Missing attribute '{segment}' in path '{path}'")
        current_value = getattr(current_value, segment)
    return current_value


def _select_nodes(
    runtime: ScenarioRuntime, role: str, dc_id: str | None
) -> list[object]:
    cluster = runtime.require_cluster()
    if role == "gate":
        return list(cluster.gates)
    if role == "manager":
        if dc_id:
            return list(cluster.managers.get(dc_id, []))
        nodes: list[object] = []
        for managers in cluster.managers.values():
            nodes.extend(managers)
        return nodes
    if role == "worker":
        if dc_id:
            return list(cluster.workers.get(dc_id, []))
        nodes = []
        for workers in cluster.workers.values():
            nodes.extend(workers)
        return nodes
    raise ValueError(f"Unknown role '{role}'")


def _resolve_target(runtime: ScenarioRuntime, action: ActionSpec) -> object:
    target_name = action.params.get("target")
    if not target_name:
        raise ValueError("assert_condition requires target")
    if target_name == "status_updates":
        return runtime.callbacks.status_updates
    if target_name == "progress_updates":
        return runtime.callbacks.progress_updates
    if target_name == "workflow_results":
        return runtime.callbacks.workflow_results
    if target_name == "reporter_results":
        return runtime.callbacks.reporter_results
    if target_name == "job_ids":
        return runtime.job_ids
    if target_name == "last_job_id":
        return runtime.last_job_id
    cluster = runtime.require_cluster()
    if target_name == "cluster_gate_count":
        return len(cluster.gates)
    if target_name == "cluster_manager_count":
        return len(cluster.get_all_managers())
    if target_name == "cluster_worker_count":
        return len(cluster.get_all_workers())
    if target_name == "cluster_datacenters":
        datacenter_ids = set(cluster.managers.keys()) | set(cluster.workers.keys())
        return sorted(datacenter_ids)
    if target_name == "gate_leader":
        return cluster.get_gate_leader()
    if target_name == "manager_leader":
        datacenter_id = action.params.get("dc_id")
        if not datacenter_id:
            raise ValueError("manager_leader requires dc_id")
        return cluster.get_manager_leader(datacenter_id)
    if target_name == "node_attribute":
        role = action.params.get("role")
        if not role:
            raise ValueError("node_attribute requires role")
        path = action.params.get("path")
        if not path:
            raise ValueError("node_attribute requires path")
        dc_id = action.params.get("dc_id")
        nodes = _select_nodes(runtime, role, dc_id)
        if not nodes:
            raise ValueError(f"No nodes found for role '{role}'")
        all_nodes = bool(action.params.get("all_nodes"))
        if all_nodes:
            return [_resolve_path(node, path) for node in nodes]
        index = int(action.params.get("index", 0))
        try:
            node = nodes[index]
        except IndexError as error:
            raise IndexError(
                f"Node index {index} out of range for role '{role}'"
            ) from error
        return _resolve_path(node, path)
    raise ValueError(f"Unknown assert target '{target_name}'")


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    target = _resolve_target(runtime, action)
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
        assert target == equals_value, f"Expected value to equal {equals_value}"
    return ActionOutcome(
        name="assert_condition",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
        details=action.params.get("target"),
    )
