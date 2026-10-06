"""
Workflow context (AD-49), from a provider's hook to its consumer's run.

The chain was broken at every hop:

* core: a ``Provide`` hook wrote ``context[target]`` while a ``Use`` hook
  reads the namespaces it names, so the documented pairing -- provider
  ``@state('Consumer')``, consumer ``@state('Provider')`` -- never met; in a
  worker process the written value was ``hook.result``, which nothing
  assigns (always None); and a plain ``def`` hook, the documented form, was
  awaited, storing a TypeError as the value;
* manager: a run's ``Context.dict()`` was stored nested one level too deep
  under the producer's name, and a dependent was handed its dependencies'
  context looked up by workflow id in a store keyed by workflow name -- so
  it always got ``{}``;
* worker: the received context was seeded, then replaced by a fresh empty
  run context before the run began.

Now a provided value lands in the provider's namespace and each target's,
the manager keeps the job's context by namespace and hands a dispatched
workflow all of it, and the worker's run starts from every namespace.
"""

import asyncio
import sys
from collections import defaultdict
from types import SimpleNamespace

import cloudpickle
import pytest

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.jobs.graphs.remote_graph_controller import RemoteGraphController
from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core.jobs.graphs.workflow_runner import WorkflowRunner
from hyperscale.core.state import Context, Provide, Use, state
from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs.dispatch_outcome import DispatchOutcome
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.jobs.worker_pool import WorkerPool
from hyperscale.distributed.jobs.workflow_dispatcher import WorkflowDispatcher
from hyperscale.distributed.models import (
    JobSubmission,
    NodeInfo,
    WorkerRegistration,
    WorkflowDispatch,
)
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.taskex import TaskRunner

TOKEN = "token-12345"

# Workflows travel by value, as the CLI ships a user's workflow module: the
# workers' restricted unpickler admits no other module by reference.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


class ContextProvider(Workflow):
    @state("SourceNamingConsumer", "SelfReadingConsumer")
    def auth_token(self) -> Provide[str]:
        return TOKEN


class SourceNamingConsumer(Workflow):
    @state("ContextProvider")
    def get_auth_token(self, auth_token: str | None = None) -> Use[str]:
        return auth_token


class SelfReadingConsumer(Workflow):
    @state()
    async def get_auth_token(self, auth_token: str | None = None) -> Use[str]:
        return auth_token


async def run_provider_then_consumers(
    provide: object,
    use: object,
    runner: object,
) -> dict[str, dict[str, object]]:
    context = Context()
    await provide(
        runner,
        "ContextProvider",
        runner._setup_state_actions(ContextProvider()),
        context,
        {},
    )
    for consumer in (SourceNamingConsumer(), SelfReadingConsumer()):
        await use(runner, consumer.name, runner._setup_state_actions(consumer), context)
    return context.dict()


class SilentLogContext:
    async def log_prepared(self, *args: object, **kwargs: object) -> None:
        return None

    async def __aenter__(self) -> "SilentLogContext":
        return self

    async def __aexit__(self, *exception_info: object) -> None:
        return None


EXPECTED_CONTEXT = {
    "ContextProvider": {"auth_token": TOKEN},
    "SourceNamingConsumer": {"auth_token": TOKEN, "get_auth_token": TOKEN},
    "SelfReadingConsumer": {"auth_token": TOKEN, "get_auth_token": TOKEN},
}


@pytest.mark.asyncio
async def test_a_worker_process_delivers_provided_values_to_both_consumer_forms() -> None:
    runner = object.__new__(WorkflowRunner)

    context = await run_provider_then_consumers(
        WorkflowRunner._provide_context,
        WorkflowRunner._use_context,
        runner,
    )

    assert context == EXPECTED_CONTEXT


@pytest.mark.asyncio
async def test_a_graph_run_delivers_provided_values_to_both_consumer_forms() -> None:
    graph_manager = object.__new__(RemoteGraphManager)
    graph_manager._logger = SimpleNamespace(context=lambda **kwargs: SilentLogContext())

    context = await run_provider_then_consumers(
        RemoteGraphManager._provide_context,
        RemoteGraphManager._use_context,
        graph_manager,
    )

    assert context == EXPECTED_CONTEXT


@pytest.mark.asyncio
async def test_a_run_started_elsewhere_begins_from_every_namespace() -> None:
    controller = object.__new__(RemoteGraphController)
    controller._node_context = defaultdict(Context)
    controller.create_run_contexts(run_id=7)

    seeded = await controller.seed_run_context(7, EXPECTED_CONTEXT)

    assert seeded is controller._node_context[7]
    assert seeded.dict() == EXPECTED_CONTEXT


@pytest.mark.asyncio
async def test_a_runs_context_is_kept_by_namespace_and_versions_the_job() -> None:
    job_manager = JobManager(
        datacenter="local",
        manager_id="manager-1",
        clock=RealClock(),
        max_budgeted_retries=Env().RETRY_BUDGET_PER_WORKFLOW_MAX,
    )
    await job_manager.create_job(
        JobSubmission(job_id="job-1", workflows=b"", vus=1, timeout_seconds=60)
    )

    applied = await job_manager.apply_workflow_context(
        "job-1",
        cloudpickle.dumps({"ContextProvider": {"auth_token": TOKEN}}),
    )
    await job_manager.apply_workflow_context(
        "job-1",
        cloudpickle.dumps({"SelfReadingConsumer": {"auth_token": TOKEN}}),
    )

    assert applied is True
    assert await job_manager.get_job_context("job-1") == {
        "ContextProvider": {"auth_token": TOKEN},
        "SelfReadingConsumer": {"auth_token": TOKEN},
    }
    assert await job_manager.get_layer_version("job-1") == 2
    assert await job_manager.get_job_context("job-unknown") == {}


@pytest.mark.asyncio
async def test_a_dependent_is_dispatched_with_the_jobs_whole_context() -> None:
    job_manager = JobManager(
        datacenter="local",
        manager_id="manager-1",
        clock=RealClock(),
        max_budgeted_retries=Env().RETRY_BUDGET_PER_WORKFLOW_MAX,
    )
    worker_pool = WorkerPool()
    dispatched: dict[str, WorkflowDispatch] = {}
    dispatches_taken: list[asyncio.Event] = [asyncio.Event(), asyncio.Event()]

    # A dispatch is done once its worker takes it: the worker's results
    # come after that, never before.
    async def take_dispatch(
        worker_id: str,
        dispatch: WorkflowDispatch,
    ) -> tuple[DispatchOutcome, str]:
        dispatched[dispatch.load_workflow().name] = dispatch
        next(event for event in dispatches_taken if not event.is_set()).set()
        return DispatchOutcome.ACCEPTED, ""

    async def ignore(*args: object) -> None:
        return None

    task_runner = TaskRunner()
    dispatcher = WorkflowDispatcher(
        job_manager=job_manager,
        worker_pool=worker_pool,
        send_dispatch=take_dispatch,
        datacenter="local",
        manager_id="manager-1",
        task_runner=task_runner,
        on_dispatch_exhausted=ignore,
        stop_dispatched_plans=ignore,
    )
    try:
        await worker_pool.register_worker(
            WorkerRegistration(
                node=NodeInfo(
                    node_id="worker-1",
                    role="worker",
                    host="127.0.0.1",
                    port=10_001,
                    datacenter="local",
                    udp_port=10_002,
                ),
                total_cores=4,
                available_cores=4,
                memory_mb=1024,
            )
        )
        submission = JobSubmission(job_id="job-1", workflows=b"", vus=1, timeout_seconds=60)
        await job_manager.create_job(submission)
        assert await dispatcher.register_workflows(
            submission,
            [
                ("wf-provider", [], ContextProvider()),
                ("wf-consumer", ["ContextProvider"], SourceNamingConsumer()),
            ],
        )
        await dispatcher.start_job_dispatch("job-1", submission)
        await asyncio.wait_for(dispatches_taken[0].wait(), timeout=5.0)

        # The provider's final result, as the manager handles it: its run's
        # context is applied, the worker's cores come back, and its
        # completion readies the consumer -- dispatched with everything the
        # job now holds.
        await job_manager.apply_workflow_context(
            "job-1",
            cloudpickle.dumps(
                {
                    "ContextProvider": {"auth_token": TOKEN},
                    "SourceNamingConsumer": {"auth_token": TOKEN},
                }
            ),
        )
        # The provider's result: its cores (allocated at core version 1)
        # freed again at 2.
        await worker_pool.update_worker_cores_from_progress(
            "worker-1", 4, dispatched["ContextProvider"].workflow_id, 2
        )
        await dispatcher.mark_workflow_completed("job-1", "wf-provider")
        await asyncio.wait_for(dispatches_taken[1].wait(), timeout=5.0)

        consumer_dispatch = dispatched["SourceNamingConsumer"]
        assert consumer_dispatch.load_context() == {
            "ContextProvider": {"auth_token": TOKEN},
            "SourceNamingConsumer": {"auth_token": TOKEN},
        }
        assert consumer_dispatch.context_version == await job_manager.get_layer_version("job-1")
        assert dispatched["ContextProvider"].load_context() == {}
    finally:
        await dispatcher.shutdown()
        await task_runner.shutdown()


class BindingProvider(Workflow):
    """Bound only by RemoteGraphManager here, so nothing else in this
    process mutates its class-level hook."""

    @state("SourceNamingConsumer")
    def auth_token(self) -> Provide[str]:
        return TOKEN


def test_each_run_of_a_workflow_class_binds_its_own_copy_of_its_hooks() -> None:
    """A state hook is a class attribute every instance shares; binding it
    let runs of one class take each other's instance, and from Python 3.14
    re-binding an already-bound method returns it unchanged."""
    graph_manager = object.__new__(RemoteGraphManager)
    first_run, second_run = BindingProvider(), BindingProvider()

    first_actions = graph_manager._setup_state_actions(first_run)
    second_actions = graph_manager._setup_state_actions(second_run)
    repeated_actions = graph_manager._setup_state_actions(second_run)

    assert first_actions["auth_token"]._call.__self__ is first_run
    assert second_actions["auth_token"]._call.__self__ is second_run
    assert first_actions["auth_token"] is not second_actions["auth_token"]
    # The class's hook is never bound, and a second setup of one instance
    # still finds it.
    assert not hasattr(BindingProvider.__dict__["auth_token"]._call, "__self__")
    assert repeated_actions["auth_token"]._call.__self__ is second_run
