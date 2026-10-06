"""
A manager given a lost datacenter's unfinished share runs that share, and
what it depends on -- on real managers, on virtual time.

AD-36 mid-flight failover: a job's leader gate re-dispatches the workflows
a lost datacenter left unfinished to another datacenter, naming them in
the submission (``rerun_workflow_ids``). The manager runs those and every
workflow they depend on, directly or not -- re-run for the context their
dependents read -- and nothing else of the job. A share naming a workflow
the job does not contain is refused.

Three real ``ManagerServer`` instances form a datacenter on a
``SimulationLoop``; the one stand-in is the worker.
"""

import sys

import cloudpickle

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import step
from hyperscale.distributed.models import JobSubmission
from hyperscale.distributed.nodes.manager.server import ManagerServer

from .leader_to_peer_link import LeaderToPeerLink
from .manager_datacenter import (
    MANAGER_TCP_ADDRESSES,
    form_datacenter,
    run_scenario,
    submit,
)

cloudpickle.register_pickle_by_value(sys.modules[__name__])

JOB_ID = "job-1"
JOB_TIMEOUT_SECONDS = 60.0


class Login(Workflow):
    vus = 1

    @step()
    async def log_in(self) -> dict:
        return {}


class Browse(Workflow):
    vus = 1

    @step()
    async def browse(self) -> dict:
        return {}


class Checkout(Workflow):
    vus = 1

    @step()
    async def check_out(self) -> dict:
        return {}


class Search(Workflow):
    vus = 1

    @step()
    async def search(self) -> dict:
        return {}


def rerun_submission(rerun_workflow_ids: list[str]) -> bytes:
    """Checkout after Browse after Login; Search on its own."""
    return JobSubmission(
        job_id=JOB_ID,
        workflows=cloudpickle.dumps(
            [
                ("wf-login", [], Login()),
                ("wf-browse", ["Login"], Browse()),
                ("wf-checkout", ["Browse"], Checkout()),
                ("wf-search", [], Search()),
            ]
        ),
        vus=1,
        timeout_seconds=JOB_TIMEOUT_SECONDS,
        rerun_workflow_ids=rerun_workflow_ids,
    ).dump()


def held_workflow_names(manager: ManagerServer) -> list[str] | None:
    job = manager._job_manager.get_job_by_id(JOB_ID)
    if job is None:
        return None
    return sorted(workflow.name for workflow in job.workflows.values())


def test_a_rerun_share_runs_with_its_ancestors_and_nothing_else() -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], _build_manager):
        leader = await form_datacenter(managers, link)
        ack = await submit(leader, rerun_submission(["wf-checkout"]))
        return ack, held_workflow_names(leader)

    ack, held_workflows = run_scenario(scenario, link)

    assert ack.accepted
    assert held_workflows == ["Browse", "Checkout", "Login"]


def test_a_rerun_share_naming_a_workflow_the_job_lacks_is_refused() -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], _build_manager):
        leader = await form_datacenter(managers, link)
        ack = await submit(leader, rerun_submission(["wf-checkout", "wf-refund"]))
        return ack, held_workflow_names(leader)

    ack, held_workflows = run_scenario(scenario, link)

    assert not ack.accepted
    assert "wf-refund" in ack.error
    assert held_workflows is None
