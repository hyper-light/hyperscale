import time

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.action_spec import ActionSpec


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    cluster = runtime.require_cluster()
    workflow_names = action.params.get("workflows") or []
    if isinstance(workflow_names, str):
        workflow_names = [workflow_names]
    if not workflow_names:
        workflow_name = action.params.get("workflow")
        if workflow_name:
            workflow_names = [workflow_name]
    workflows = [runtime.resolve_workflow(name) for name in workflow_names]
    vus = int(action.params.get("vus", 1))
    timeout_seconds = float(action.params.get("timeout_seconds", 300.0))
    datacenter_count = int(action.params.get("datacenter_count", 1))
    datacenters = action.params.get("datacenters")
    job_id = await cluster.client.submit_job(
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
