"""
A job's leader gate moves the job off a datacenter it loses mid-run
(AD-36 Part 13: re-run elsewhere).

Before, a datacenter lost while a job ran there stranded its share: the
job waited for results that never came until its global timeout ended it,
while datacenters with room sat idle. Now the job's leader gate, finding a
datacenter of the job UNHEALTHY:

* moves the lost datacenter's result slots to a replacement -- the best
  eligible datacenter not already holding the job -- except the slots of
  workflows it delivered before it was lost, which stay with it; the new
  placement commits to the gate tier before anything is dispatched;
* dispatches the replacement the workflows the lost datacenter left
  unfinished (its managers add their ancestors, for context), with what
  is left of the job's budget;
* aggregates each workflow over the datacenters holding its slot, marking
  a replacement's result as the lost datacenter's re-run; and drops what
  the lost datacenter -- or a replacement re-running a delivered workflow
  for context -- sends for a slot it does not hold;
* tells the lost datacenter to stop, again every check until it answers;
* ends the job on the replacement's final result, counting the work the
  lost datacenter did before it was lost;
* passes on, when a replacement is lost in turn, only the share it had
  left -- never the workflows the datacenters before it delivered: a
  replacement that delivered its whole share needs no successor (one
  given an empty share was never sent the job, and the job waited on
  it until its timeout).

A real ``GateServer`` (never started) leads a job in dc-a and dc-b; the
routing view's verdicts (dc-b unhealthy, dc-c the best replacement) are
stood in for, and every manager is a recording transport.
"""

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import (
    CancelJob,
    GateJobReplica,
    GlobalJobResult,
    JobAck,
    JobFinalResult,
    JobProgress,
    JobStatus,
    JobSubmission,
    WorkflowResultPush,
)
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.runtime import RealClock

JOB_ID = "job-1"
HOST = "127.0.0.1"
GATE_ADDRESS = (HOST, 19471)
CLIENT_CALLBACK = (HOST, 19500)
MANAGERS = {
    "dc-a": (HOST, 19571),
    "dc-b": (HOST, 19671),
    "dc-c": (HOST, 19771),
    "dc-d": (HOST, 19871),
}
JOB_TIMEOUT_SECONDS = 600.0
# Delivered by both datacenters (aggregated), by dc-a alone, by dc-b
# alone, by neither.
AGGREGATED_WORKFLOW = "wf-login"
HALF_DELIVERED_WORKFLOW = "wf-browse"
DELIVERED_BY_LOST_WORKFLOW = "wf-search"
UNSTARTED_WORKFLOW = "wf-checkout"
WORKFLOW_IDS = [
    AGGREGATED_WORKFLOW,
    HALF_DELIVERED_WORKFLOW,
    DELIVERED_BY_LOST_WORKFLOW,
    UNSTARTED_WORKFLOW,
]
# dc-b's work before it was lost, as its last progress report gave it.
LOST_COMPLETED = 40
LOST_FAILED = 2


SETTINGS = Env(MERCURY_SYNC_AUTH_SECRET="mid-flight-failover-secret-0123456789")


class ScenarioClock:
    """The real clock, except that a sleep shorter than the gate's
    per-workflow result timeout only yields -- retry backoff costs the
    test nothing -- and that timeout never runs out on its own: each
    scenario ends well inside it."""

    def __init__(self) -> None:
        self._clock = RealClock()

    def monotonic(self) -> float:
        return self._clock.monotonic()

    def monotonic_ns(self) -> int:
        return self._clock.monotonic_ns()

    def time(self) -> float:
        return self._clock.time()

    async def sleep(self, seconds: float) -> None:
        if seconds >= SETTINGS.GATE_WORKFLOW_RESULT_TIMEOUT_SECONDS:
            await asyncio.Event().wait()
        await asyncio.sleep(0)

    async def wait_for(self, awaitable, timeout: float | None):
        return await self._clock.wait_for(awaitable, timeout)


class DatacenterTransport:
    """Every manager the gate sends to: all answer but dc-b's, which is
    unreachable; the client is out of reach (its updates are recorded for
    replay)."""

    def __init__(self) -> None:
        self.sent: list[tuple[tuple[str, int], str, bytes]] = []
        self.unreachable = {MANAGERS["dc-b"], CLIENT_CALLBACK}

    async def send_tcp(self, address, action, payload, timeout=None):
        self.sent.append((tuple(address), action, payload))
        if tuple(address) in self.unreachable:
            return ConnectionRefusedError(f"{address} is unreachable"), 0
        if action == "job_submission":
            return JobAck(job_id=JOB_ID, accepted=True).dump(), 0
        return b"ok", 0

    def submissions_to(self, datacenter: str) -> list[JobSubmission]:
        return [
            JobSubmission.load(payload)
            for address, action, payload in self.sent
            if address == MANAGERS[datacenter] and action == "job_submission"
        ]

    def cancels_to(self, datacenter: str) -> list[CancelJob]:
        return [
            CancelJob.load(payload)
            for address, action, payload in self.sent
            if address == MANAGERS[datacenter] and action == "cancel_job"
        ]


async def leading_gate(
    transport: DatacenterTransport,
    lost_datacenters: tuple[str, ...] = ("dc-b",),
) -> GateServer:
    gate = GateServer(
        host=HOST,
        tcp_port=GATE_ADDRESS[1],
        udp_port=GATE_ADDRESS[1] + 1,
        env=SETTINGS,
        datacenter_managers={datacenter: [address] for datacenter, address in MANAGERS.items()},
        datacenter_manager_udp={
            datacenter: [(address[0], address[1] + 1)] for datacenter, address in MANAGERS.items()
        },
        clock=ScenarioClock(),
    )
    gate.send_tcp = transport.send_tcp
    # As a started gate is.
    gate._accepting_requests = True
    gate._running = True
    gate._job_leadership_tracker.node_id = gate._node_id.full
    gate._job_leadership_tracker.node_addr = GATE_ADDRESS

    # The job as admitted: its replica committed (this gate alone is the
    # quorum), then dispatched to dc-a and dc-b and running.
    submission = JobSubmission(
        job_id=JOB_ID,
        workflows=b"pickled-workflows",
        vus=1,
        timeout_seconds=JOB_TIMEOUT_SECONDS,
        callback_addr=CLIENT_CALLBACK,
    )
    assert await gate._replication_coordinator.replicate_with_quorum(
        GateJobReplica(
            job_id=JOB_ID,
            sequence=1,
            fence_token=1,
            leader_id=gate._node_id.full,
            leader_addr=GATE_ADDRESS,
            origin_gate_addr=GATE_ADDRESS,
            callback_addr=CLIENT_CALLBACK,
            target_dcs=["dc-a", "dc-b"],
            target_dc_count=2,
            status_seed=JobStatus.SUBMITTED.value,
            submitted_wall_time=gate._clock.time(),
            raft_voters=[gate._node_id.full],
            workflow_ids=WORKFLOW_IDS,
            submission_payload=submission.dump(),
        ),
        [],
        1,
    )
    job = gate._job_manager.get_job(JOB_ID)
    job.status = JobStatus.RUNNING.value
    job.datacenters = [
        JobProgress(
            job_id=JOB_ID,
            datacenter="dc-b",
            status=JobStatus.RUNNING.value,
            total_completed=LOST_COMPLETED,
            total_failed=LOST_FAILED,
        )
    ]

    # The routing view: dc-b lost (unless the scenario reads the gate's
    # own); dc-c, then dc-d, the best datacenters the job is not in.
    if lost_datacenters:
        lose(gate, *lost_datacenters)
    gate._job_failover_coordinator._route_replacement = lambda job_id, constraint, occupied, latency_budget_ms: next(
        (datacenter for datacenter in ("dc-c", "dc-d") if datacenter not in occupied), None
    )
    return gate


def lose(gate: GateServer, *datacenters: str) -> None:
    """The routing view classifies these datacenters UNHEALTHY."""
    gate._job_failover_coordinator._classify_datacenter_health = lambda datacenter: (
        "unhealthy" if datacenter in datacenters else "healthy"
    )


def result_of(workflow_id: str, datacenter: str) -> bytes:
    return WorkflowResultPush(
        job_id=JOB_ID,
        workflow_id=workflow_id,
        workflow_name=workflow_id,
        datacenter=datacenter,
        status=JobStatus.COMPLETED.value,
        fence_token=1,
        results=[],
        callback_addr=CLIENT_CALLBACK,
    ).dump()


def final_result_of(datacenter: str, total_completed: int) -> bytes:
    return JobFinalResult(
        job_id=JOB_ID,
        datacenter=datacenter,
        status=JobStatus.COMPLETED.value,
        total_completed=total_completed,
    ).dump()


async def settle() -> None:
    """Let what the gate set off in the background run."""
    for _ in range(50):
        await asyncio.sleep(0)


def awaiting_result_timeouts(gate: GateServer) -> set[str]:
    """The job's workflows whose per-workflow result timeout runs."""
    return set(gate._workflow_result_timeout_tokens.get(JOB_ID, {}))


async def stop_result_timeouts(gate: GateServer) -> None:
    """End the scenario's per-workflow result timeouts, which never run
    out on their own here."""
    for job_tokens in list(gate._workflow_result_timeout_tokens.values()):
        for timeout_token in list(job_tokens.values()):
            await gate._cancel_workflow_result_timeout(timeout_token)


async def recorded_client_updates(gate: GateServer, message_type: str) -> list[bytes]:
    updates, _oldest_sequence, _latest_sequence = await gate._modular_state.get_client_updates_since(
        JOB_ID, 0
    )
    return [
        payload
        for _sequence, recorded_type, payload, _recorded_at in updates
        if recorded_type == message_type
    ]


async def on_leading_gate(
    scenario: Callable[[GateServer, DatacenterTransport], Awaitable[None]],
    lost_datacenters: tuple[str, ...] = ("dc-b",),
) -> None:
    """Run ``scenario`` on a gate leading the job, then end the result
    timeouts it left running."""
    transport = DatacenterTransport()
    gate = await leading_gate(transport, lost_datacenters)
    try:
        await scenario(gate, transport)
    finally:
        await stop_result_timeouts(gate)


@pytest.mark.asyncio
async def test_a_job_moves_its_unfinished_share_off_a_lost_datacenter() -> None:
    async def scenario(gate: GateServer, transport: DatacenterTransport) -> None:
        for workflow_id, datacenter in (
            (AGGREGATED_WORKFLOW, "dc-a"),
            (AGGREGATED_WORKFLOW, "dc-b"),
            (HALF_DELIVERED_WORKFLOW, "dc-a"),
            (DELIVERED_BY_LOST_WORKFLOW, "dc-b"),
        ):
            await gate.workflow_result_push(
                MANAGERS[datacenter], result_of(workflow_id, datacenter), 0
            )
        awaiting_before = awaiting_result_timeouts(gate)

        await gate._job_failover_coordinator.check_jobs()
        await settle()

        # The placement committed: dc-c holds dc-b's slots but those of the
        # workflows dc-b delivered; dc-b is released.
        [substitution] = gate._job_manager.get_datacenter_substitutions(JOB_ID)
        committed = gate._replication_coordinator.get_committed_replica(JOB_ID)
        assert gate._job_manager.get_target_dcs(JOB_ID) == {"dc-a", "dc-c"}
        assert (substitution.lost_datacenter, substitution.replacement_datacenter) == (
            "dc-b",
            "dc-c",
        )
        assert substitution.completed_workflow_ids == sorted(
            [AGGREGATED_WORKFLOW, DELIVERED_BY_LOST_WORKFLOW]
        )
        assert (substitution.total_completed, substitution.total_failed) == (
            LOST_COMPLETED,
            LOST_FAILED,
        )
        assert committed.datacenter_substitutions == [substitution]
        assert sorted(committed.target_dcs) == ["dc-a", "dc-c"]
        assert committed.released_datacenters == ["dc-b"]
        assert committed.status_seed == JobStatus.RUNNING.value
        # The moved workflow waits on the re-run, bounded by the job's
        # budget; the one dc-b holds still waits on dc-a, on its timeout.
        assert awaiting_before == {HALF_DELIVERED_WORKFLOW, DELIVERED_BY_LOST_WORKFLOW}
        assert awaiting_result_timeouts(gate) == {DELIVERED_BY_LOST_WORKFLOW}

        # dc-c runs what dc-b left unfinished, within the budget left.
        [rerun] = transport.submissions_to("dc-c")
        assert rerun.rerun_workflow_ids == sorted([HALF_DELIVERED_WORKFLOW, UNSTARTED_WORKFLOW])
        assert 0.0 < rerun.timeout_seconds <= JOB_TIMEOUT_SECONDS
        assert rerun.origin_gate_addr == GATE_ADDRESS
        # dc-b is told to stop, unanswered so far.
        assert [cancel.job_id for cancel in transport.cancels_to("dc-b")] == [JOB_ID]

        # Results: dc-b's late one and dc-c's context-only re-runs are
        # acked and dropped; dc-c's re-run completes the half-delivered
        # workflow, and dc-a the one dc-b delivered before it was lost.
        late_answer = await gate.workflow_result_push(
            MANAGERS["dc-b"], result_of(UNSTARTED_WORKFLOW, "dc-b"), 0
        )
        context_answers = [
            await gate.workflow_result_push(MANAGERS["dc-c"], result_of(workflow_id, "dc-c"), 0)
            for workflow_id in (AGGREGATED_WORKFLOW, DELIVERED_BY_LOST_WORKFLOW)
        ]
        rerun_answer = await gate.workflow_result_push(
            MANAGERS["dc-c"], result_of(HALF_DELIVERED_WORKFLOW, "dc-c"), 0
        )
        held_answer = await gate.workflow_result_push(
            MANAGERS["dc-a"], result_of(DELIVERED_BY_LOST_WORKFLOW, "dc-a"), 0
        )
        aggregates = {
            aggregate.workflow_id: aggregate
            for aggregate in map(
                WorkflowResultPush.load,
                await recorded_client_updates(gate, "workflow_result_push"),
            )
        }
        assert (late_answer, *context_answers, rerun_answer, held_answer) == (b"ok",) * 5
        assert set(aggregates) == {
            AGGREGATED_WORKFLOW,
            HALF_DELIVERED_WORKFLOW,
            DELIVERED_BY_LOST_WORKFLOW,
        }
        assert sorted(
            (result.datacenter, result.rerun_of)
            for result in aggregates[HALF_DELIVERED_WORKFLOW].per_dc_results
        ) == [("dc-a", ""), ("dc-c", "dc-b")]
        assert sorted(
            (result.datacenter, result.rerun_of)
            for result in aggregates[DELIVERED_BY_LOST_WORKFLOW].per_dc_results
        ) == [("dc-a", ""), ("dc-b", "")]
        assert UNSTARTED_WORKFLOW not in gate._workflow_dc_results.get(JOB_ID, {})

        # The job ends on dc-c's final result; dc-b's late one is dropped.
        await gate.job_final_result(MANAGERS["dc-a"], final_result_of("dc-a", 100), 0)
        await gate.job_final_result(MANAGERS["dc-b"], final_result_of("dc-b", 60), 0)
        await gate.job_final_result(MANAGERS["dc-c"], final_result_of("dc-c", 70), 0)
        await settle()

        [global_result] = map(
            GlobalJobResult.load,
            await recorded_client_updates(gate, "receive_global_job_result"),
        )
        assert global_result.status == JobStatus.COMPLETED.value
        assert sorted(
            result.datacenter for result in global_result.per_datacenter_results
        ) == ["dc-a", "dc-c"]
        assert global_result.datacenter_substitutions == [substitution]
        assert global_result.total_completed == 100 + 70 + LOST_COMPLETED
        assert global_result.total_failed == LOST_FAILED

    await on_leading_gate(scenario)


@pytest.mark.asyncio
async def test_a_lost_datacenter_that_delivered_every_workflow_is_no_longer_waited_on() -> None:
    async def scenario(gate: GateServer, transport: DatacenterTransport) -> None:
        for workflow_id in WORKFLOW_IDS:
            await gate.workflow_result_push(
                MANAGERS["dc-b"], result_of(workflow_id, "dc-b"), 0
            )

        await gate._job_failover_coordinator.check_jobs()
        await settle()
        for workflow_id in WORKFLOW_IDS:
            await gate.workflow_result_push(
                MANAGERS["dc-a"], result_of(workflow_id, "dc-a"), 0
            )
        await gate.job_final_result(MANAGERS["dc-a"], final_result_of("dc-a", 100), 0)
        await settle()

        [substitution] = gate._job_manager.get_datacenter_substitutions(JOB_ID)
        [global_result] = map(
            GlobalJobResult.load,
            await recorded_client_updates(gate, "receive_global_job_result"),
        )
        # Nothing re-ran: dc-b keeps every slot, and only its final result
        # was missing.
        assert substitution.replacement_datacenter == ""
        assert transport.submissions_to("dc-c") == []
        assert len(await recorded_client_updates(gate, "workflow_result_push")) == len(
            WORKFLOW_IDS
        )
        assert global_result.status == JobStatus.COMPLETED.value
        assert global_result.total_completed == 100 + LOST_COMPLETED

    await on_leading_gate(scenario)


@pytest.mark.asyncio
async def test_a_job_with_nowhere_to_go_waits_for_a_datacenter_to_take_it() -> None:
    async def scenario(gate: GateServer, transport: DatacenterTransport) -> None:
        coordinator = gate._job_failover_coordinator
        coordinator._route_replacement = lambda job_id, constraint, occupied, latency_budget_ms: None

        await coordinator.check_jobs()
        await settle()
        held_while_unplaceable = gate._job_manager.get_target_dcs(JOB_ID)

        # A datacenter can take it on a later check.
        coordinator._route_replacement = lambda job_id, constraint, occupied, latency_budget_ms: next(
            (datacenter for datacenter in ("dc-c",) if datacenter not in occupied), None
        )
        await coordinator.check_jobs()
        await settle()

        assert held_while_unplaceable == {"dc-a", "dc-b"}
        assert gate._job_manager.get_target_dcs(JOB_ID) == {"dc-a", "dc-c"}
        assert len(transport.submissions_to("dc-c")) == 1

    await on_leading_gate(scenario)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement_delivered_its_share", [False, True])
async def test_a_replacement_lost_in_turn_passes_on_only_its_own_share(
    replacement_delivered_its_share: bool,
) -> None:
    async def scenario(gate: GateServer, transport: DatacenterTransport) -> None:
        for workflow_id, datacenter in (
            (AGGREGATED_WORKFLOW, "dc-a"),
            (AGGREGATED_WORKFLOW, "dc-b"),
            (DELIVERED_BY_LOST_WORKFLOW, "dc-b"),
        ):
            await gate.workflow_result_push(
                MANAGERS[datacenter], result_of(workflow_id, datacenter), 0
            )
        await gate._job_failover_coordinator.check_jobs()
        await settle()
        if replacement_delivered_its_share:
            for workflow_id in (HALF_DELIVERED_WORKFLOW, UNSTARTED_WORKFLOW):
                await gate.workflow_result_push(
                    MANAGERS["dc-c"], result_of(workflow_id, "dc-c"), 0
                )

        # dc-c is lost in turn.
        transport.unreachable.add(MANAGERS["dc-c"])
        lose(gate, "dc-b", "dc-c")
        await gate._job_failover_coordinator.check_jobs()
        await settle()

        first, second = gate._job_manager.get_datacenter_substitutions(JOB_ID)
        assert (first.lost_datacenter, first.replacement_datacenter) == ("dc-b", "dc-c")
        if replacement_delivered_its_share:
            # Nothing left to re-run: dc-c keeps its slots, and the job
            # stops waiting on its final result.
            assert (second.lost_datacenter, second.replacement_datacenter) == ("dc-c", "")
            assert transport.submissions_to("dc-d") == []
            assert gate._job_manager.get_target_dcs(JOB_ID) == {"dc-a"}
            assert gate._job_manager.expected_workflow_datacenters(
                JOB_ID, HALF_DELIVERED_WORKFLOW
            ) == {"dc-a", "dc-c"}
        else:
            [rerun] = transport.submissions_to("dc-d")
            assert (second.lost_datacenter, second.replacement_datacenter) == ("dc-c", "dc-d")
            assert rerun.rerun_workflow_ids == sorted(
                [HALF_DELIVERED_WORKFLOW, UNSTARTED_WORKFLOW]
            )
            assert gate._job_manager.get_target_dcs(JOB_ID) == {"dc-a", "dc-d"}
            # dc-b's delivered workflow keeps its slot there along the chain.
            assert gate._job_manager.expected_workflow_datacenters(
                JOB_ID, DELIVERED_BY_LOST_WORKFLOW
            ) == {"dc-a", "dc-b"}
            assert gate._job_manager.rerun_origin(JOB_ID, "dc-d") == "dc-b"

    await on_leading_gate(scenario)


@pytest.mark.asyncio
async def test_a_datacenter_held_at_degraded_by_a_partition_is_not_lost() -> None:
    """The gate's own routing view decides what is lost: an UNHEALTHY
    verdict a detected partition holds at DEGRADED is suspect, not a
    loss (AD-36 Part 13) -- failing it over would re-run every job's share
    in a network-wide blip. Once the hold ends, the verdict stands."""

    async def scenario(gate: GateServer, transport: DatacenterTransport) -> None:
        merged_classification = gate._health_coordinator.classify_datacenter_health

        def dc_b_unhealthy(datacenter: str):
            status = merged_classification(datacenter)
            if datacenter == "dc-b":
                status.health = "unhealthy"
            return status

        gate._health_coordinator.classify_datacenter_health = dc_b_unhealthy
        gate._health_coordinator._partitioned_datacenters = {"dc-b"}
        await gate._job_failover_coordinator.check_jobs()
        held_substitutions = gate._job_manager.get_datacenter_substitutions(JOB_ID)

        gate._health_coordinator._partitioned_datacenters = set()
        await gate._job_failover_coordinator.check_jobs()
        await settle()

        assert held_substitutions == []
        [substitution] = gate._job_manager.get_datacenter_substitutions(JOB_ID)
        assert substitution.lost_datacenter == "dc-b"

    await on_leading_gate(scenario, lost_datacenters=())
