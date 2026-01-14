from tests.framework.actions.action_registry import ActionRegistry
from tests.framework.actions.assert_condition import run as assert_condition
from tests.framework.actions.await_gate_leader import run as await_gate_leader
from tests.framework.actions.await_job import run as await_job
from tests.framework.actions.await_manager_leader import run as await_manager_leader
from tests.framework.actions.restart_nodes import run as restart_nodes
from tests.framework.actions.sleep_action import run as sleep_action
from tests.framework.actions.start_cluster import run as start_cluster
from tests.framework.actions.stop_cluster import run as stop_cluster
from tests.framework.actions.stop_nodes import run as stop_nodes
from tests.framework.actions.submit_job import run as submit_job


def build_default_registry() -> ActionRegistry:
    registry = ActionRegistry()
    registry.register("start_cluster", start_cluster)
    registry.register("stop_cluster", stop_cluster)
    registry.register("await_gate_leader", await_gate_leader)
    registry.register("await_manager_leader", await_manager_leader)
    registry.register("assert_condition", assert_condition)
    registry.register("sleep", sleep_action)
    registry.register("submit_job", submit_job)
    registry.register("await_job", await_job)
    registry.register("stop_nodes", stop_nodes)
    registry.register("restart_nodes", restart_nodes)
    return registry
