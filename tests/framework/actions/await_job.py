import time

from tests.framework.results.action_outcome import ActionOutcome
from tests.framework.runtime.scenario_runtime import ScenarioRuntime
from tests.framework.specs.action_spec import ActionSpec


async def run(runtime: ScenarioRuntime, action: ActionSpec) -> ActionOutcome:
    start = time.monotonic()
    cluster = runtime.require_cluster()
    alias = action.params.get("job_alias")
    job_id = None
    if alias:
        job_id = runtime.job_ids.get(alias)
    if job_id is None:
        job_id = runtime.last_job_id
    assert job_id, "No job id available for await_job"
    timeout = action.params.get("timeout")
    await cluster.client.wait_for_job(job_id, timeout=timeout)
    return ActionOutcome(
        name="await_job",
        succeeded=True,
        duration_seconds=time.monotonic() - start,
        details=job_id,
    )
