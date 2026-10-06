"""
A job submitted without a timeout of its own has the budget its workflows
need -- on real managers, on virtual time.

A client that passes no timeout (the live CLI's default) submits
``timeout_seconds=0``: each workflow's workers then observe their own
deadline (AD-26/AD-34 Phase H2), but the job's AD-34 timeout was the
zero itself, so every such job was timed out at its manager's first
timeout check, whatever its workflows. Its budget is now the longest
chain of its dependent workflows, each taking its workers' deadline.

A manager that restarts resumes the jobs it led from their persisted
submissions -- and restarted their timeout from zero: each restart gave a
job its whole budget again. A resumed job has what is left of its budget.

Real ``ManagerServer`` instances on a ``SimulationLoop``; the one stand-in
is the worker, registered through the leader's own registration handler
(nothing runs at its address, so the job waits for workers throughout).

* a three-manager datacenter times a no-timeout job by the longest chain
  of its workflows: still running past its first timeout checks, timed
  out once its budget ran out;
* a manager alone in its datacenter, restarted mid-job, resumes the job
  with what is left of its budget -- once its cluster's membership has
  formed again (the job's Raft group is founded with its members), the
  budget less everything since the job was first accepted.
"""

import asyncio
import sys
from pathlib import Path

import cloudpickle

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import step
from hyperscale.distributed.env.env import Env
from hyperscale.distributed.ledger.job_event_applier import JOB_TIMED_OUT_STATUS
from hyperscale.distributed.models import JobSubmission
from hyperscale.distributed.nodes.manager.server import ManagerServer
from tests.simulation.harness.sim import SimulationRuntime

from .leader_to_peer_link import LeaderToPeerLink
from .manager_datacenter import (
    DATACENTER,
    FORMATION_SECONDS,
    HOST,
    MANAGER_TCP_ADDRESSES,
    form_datacenter,
    register_worker,
    run_scenario,
    submit,
)

cloudpickle.register_pickle_by_value(sys.modules[__name__])

JOB_ID = "job-1"
SETTINGS = Env()


class Login(Workflow):
    vus = 1
    duration = "20s"

    @step()
    async def log_in(self) -> dict:
        return {}


class Browse(Workflow):
    vus = 1
    duration = "20s"

    @step()
    async def browse(self) -> dict:
        return {}


class Search(Workflow):
    vus = 1
    duration = "30s"

    @step()
    async def search(self) -> dict:
        return {}


def parse_duration_seconds(workflow: type[Workflow]) -> float:
    return float(workflow.duration.removesuffix("s"))


# Browse runs after Login; Search alongside both. Each workflow's workers
# observe its duration times the default multiplier.
CHAIN_BUDGET_SECONDS = (
    parse_duration_seconds(Login) + parse_duration_seconds(Browse)
) * SETTINGS.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER
TIMEOUT_CHECK_SECONDS = SETTINGS.JOB_TIMEOUT_CHECK_INTERVAL


def no_timeout_submission() -> bytes:
    return JobSubmission(
        job_id=JOB_ID,
        workflows=cloudpickle.dumps(
            [
                ("wf-login", [], Login()),
                ("wf-browse", ["Login"], Browse()),
                ("wf-search", [], Search()),
            ]
        ),
        vus=1,
        timeout_seconds=0.0,
        timeout_seconds_explicit=False,
    ).dump()


def ledger_status(manager: ManagerServer) -> str | None:
    record = manager._job_ledger.get_job(JOB_ID)
    return record.status if record is not None else None


def test_a_job_without_a_timeout_has_the_budget_of_its_longest_workflow_chain() -> None:
    link = LeaderToPeerLink(MANAGER_TCP_ADDRESSES)

    async def scenario(managers: list[ManagerServer], _build_manager):
        leader = await form_datacenter(managers, link)
        submitted_at = asyncio.get_running_loop().time()
        ack = await submit(leader, no_timeout_submission())
        held_budget = leader._job_manager.get_job_by_id(JOB_ID).timeout_tracking.timeout_seconds

        # Past the first timeout checks, short of the budget.
        await asyncio.sleep(CHAIN_BUDGET_SECONDS - TIMEOUT_CHECK_SECONDS / 2)
        before_budget = ledger_status(leader)

        # The first check past the budget times the job out.
        await asyncio.sleep(
            submitted_at
            + CHAIN_BUDGET_SECONDS
            + TIMEOUT_CHECK_SECONDS
            - asyncio.get_running_loop().time()
        )
        return ack, held_budget, before_budget, ledger_status(leader)

    ack, held_budget, before_budget, after_budget = run_scenario(scenario, link, keep_ledger=True)

    assert ack.accepted
    assert held_budget == CHAIN_BUDGET_SECONDS
    assert before_budget != JOB_TIMED_OUT_STATUS
    assert after_budget == JOB_TIMED_OUT_STATUS


LONE_MANAGER_PORTS = (9010, 9011)
# The job runs this long before its manager dies, and the manager stays
# down this long.
RUN_BEFORE_RESTART_SECONDS = 20.0
DOWNTIME_SECONDS = 10.0
# Boot reads the ledger, the cluster's membership forms and recovered jobs
# settle within this.
RECOVERY_SECONDS = 5.0


def test_a_resumed_job_has_what_is_left_of_its_budget() -> None:
    runtime = SimulationRuntime(seed=1)

    def build_lone_manager() -> ManagerServer:
        return ManagerServer(
            HOST,
            *LONE_MANAGER_PORTS,
            Env(MERCURY_SYNC_AUTH_SECRET="manager-job-budget-scenario-secret-0123"),
            dc_id=DATACENTER,
            seed_managers=[],
            manager_udp_peers=[],
            wal_data_dir=Path(f"/sim/{HOST}-{LONE_MANAGER_PORTS[0]}/ledger"),
            **runtime.sim_kwargs(),
        )

    async def scenario():
        manager = build_lone_manager()
        await manager.start()
        await asyncio.sleep(FORMATION_SECONDS)
        await register_worker(manager, "worker-1", 9100)
        ack = await submit(manager, no_timeout_submission())
        accepted_at = manager._clock.monotonic()
        await asyncio.sleep(RUN_BEFORE_RESTART_SECONDS)
        manager.abort()

        await asyncio.sleep(DOWNTIME_SECONDS)
        restarted = build_lone_manager()
        try:
            await restarted.start()
            await asyncio.sleep(RECOVERY_SECONDS)
            resumed_job = restarted._job_manager.get_job_by_id(JOB_ID)
            if resumed_job is None or resumed_job.timeout_tracking is None:
                return ack, None, None
            tracking = resumed_job.timeout_tracking
            return ack, tracking.timeout_seconds, tracking.started_at - accepted_at
        finally:
            await restarted.stop()

    try:
        ack, resumed_budget, resumed_after_seconds = runtime.run(scenario())
    finally:
        runtime.close()

    assert ack.accepted
    assert resumed_budget is not None
    # Resumed once the restarted manager's membership formed: after the
    # time the job ran and its manager was down, and within a recovery.
    assert (
        RUN_BEFORE_RESTART_SECONDS + DOWNTIME_SECONDS
        <= resumed_after_seconds
        <= RUN_BEFORE_RESTART_SECONDS + DOWNTIME_SECONDS + RECOVERY_SECONDS
    )
    # The budget less everything since the job was first accepted (the
    # ledger's HLC reads milliseconds).
    assert abs(resumed_budget - (CHAIN_BUDGET_SECONDS - resumed_after_seconds)) <= 0.01
