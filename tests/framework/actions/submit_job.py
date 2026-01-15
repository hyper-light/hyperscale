import time

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.runtime.workflow_factory import DynamicWorkflowFactory
from tests.framework.specs.action_spec import ActionSpec


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    cluster = runtime.require_cluster()
    workflow_instances = action.params.get("workflow_instances")
    workflows: list[tuple[list[str], object]] = []
    if workflow_instances:
        workflows = DynamicWorkflowFactory(runtime.workflow_registry).build_workflows(
            workflow_instances
        )
    else:
        workflow_names = action.params.get("workflows") or []
        if isinstance(workflow_names, str):
            workflow_names = [workflow_names]
        if not workflow_names:
            workflow_name = action.params.get("workflow")
            if workflow_name:
                workflow_names = [workflow_name]
        for name in workflow_names:
            workflow_class = runtime.resolve_workflow(name)
            workflows.append(([], workflow_class()))

    vus = int(action.params.get("vus", 1))
    timeout_seconds = float(action.params.get("timeout_seconds", 300.0))
    datacenter_count = int(action.params.get("datacenter_count", 1))
    datacenters = action.params.get("datacenters")
    client = cluster.client
    if client is None:
        raise RuntimeError("Cluster client not initialized")
    job_id = await client.submit_job(
        workflows=workflows,
        vus=vus,
        timeout_seconds=timeout_seconds,
        datacenter_count=datacenter_count,
        datacenters=datacenters,
        on_status_update=runtime.callbacks.on_status_update,
        on_progress_update=runtime.callbacks.on_progress_update,
        on_workflow_result=runtime.callbacks.on_workflow_result,
        on_reporter_result=runtime.callbacks.on_reporter_result,
    )
    alias = action.params.get("job_alias")
    if alias:
        runtime.job_ids[alias] = job_id
    runtime.last_job_id = job_id
    return ActionOutcome(
        name="submit_job",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
        details=job_id,
    )
