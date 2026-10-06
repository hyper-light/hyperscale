"""
A workflow is a test workflow when it drives load, whatever its name.

The dispatcher called a workflow a test when "test" appeared in its name --
so a load workflow named "Checkout" was not one and a plain one named
"LatestTestimonials" was -- and the manager's result pushes never said
which it was (``is_test`` defaulted to True), so every workflow's results
were merged as load-test stats. Core's runners apply one rule: a test
workflow has a TEST hook (a step returning a load-test response). The
classification now follows that rule and is kept on the job's workflow
record, where the result push reads it.
"""

import sys

import cloudpickle
import pytest

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import step
from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.jobs.worker_pool import WorkerPool
from hyperscale.distributed.jobs.workflow_dispatcher import WorkflowDispatcher
from hyperscale.distributed.models import JobSubmission
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.testing import URL, HTTPResponse

cloudpickle.register_pickle_by_value(sys.modules[__name__])


class Checkout(Workflow):
    vus = 1

    @step()
    async def check_out(self, url: URL = "https://example.com/checkout") -> HTTPResponse:
        return await self.client.http.get(url)


class LatestTestimonials(Workflow):
    vus = 1

    @step()
    async def read_testimonials(self) -> dict:
        return {}


async def ignore(*args: object) -> None:
    return None


@pytest.mark.asyncio
async def test_a_workflow_is_a_test_by_its_hooks_not_its_name() -> None:
    job_manager = JobManager(
        datacenter="local",
        manager_id="manager-1",
        clock=RealClock(),
        max_budgeted_retries=Env().RETRY_BUDGET_PER_WORKFLOW_MAX,
    )
    task_runner = TaskRunner()
    dispatcher = WorkflowDispatcher(
        job_manager=job_manager,
        worker_pool=WorkerPool(),
        send_dispatch=ignore,
        datacenter="local",
        manager_id="manager-1",
        task_runner=task_runner,
        on_dispatch_exhausted=ignore,
        stop_dispatched_plans=ignore,
    )
    try:
        submission = JobSubmission(job_id="job-1", workflows=b"", vus=1, timeout_seconds=60)
        await job_manager.create_job(submission)
        assert await dispatcher.register_workflows(
            submission,
            [
                ("wf-checkout", [], Checkout()),
                ("wf-testimonials", [], LatestTestimonials()),
            ],
        )

        job = job_manager.get_job_by_id("job-1")
        classification = {workflow.name: workflow.is_test for workflow in job.workflows.values()}

        assert classification == {"Checkout": True, "LatestTestimonials": False}
    finally:
        await dispatcher.shutdown()
        await task_runner.shutdown()
