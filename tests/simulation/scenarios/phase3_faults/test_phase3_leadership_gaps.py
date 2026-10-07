"""
Phase 3 leadership scenarios from ``docs/SCENARIOS.md`` §1.

These cover leadership edges not exercised by the baseline kill/restart
faults: stable-lease pre-vote rejection, flapping backoff, synthetic term
exhaustion, SWIM DC-leader vs per-job Raft-leader divergence, and the
death-recovery rule that only the SWIM leader may take over a dead job
leader.
"""

import time

import pytest

from hyperscale.distributed.swim.leadership import LocalLeaderElection
from hyperscale.distributed.swim.leadership.leader_state import MAX_TERM, LeaderState
from hyperscale.distributed.testing.workflows import LongRunningWorkflow
from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    HarnessTimeouts,
    ServerHandle,
    Submission,
    SubmissionPattern,
    WorkloadSpec,
    dc_has_leader,
    wait_until,
)

# Long enough for the job to stay in flight through two leadership moves
# and a death detection, and the bound on that takeover.
_LONG_JOB_TIMEOUT_SECONDS = 120.0


def _l2_spec() -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=0),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


def _find_leader(managers: list[ServerHandle]) -> ServerHandle:
    for manager in managers:
        if manager.instance.is_leader():
            return manager
    raise AssertionError("no manager currently reports leadership")


def _find_follower(managers: list[ServerHandle]) -> ServerHandle:
    for manager in managers:
        if not manager.instance.is_leader():
            return manager
    raise AssertionError("no follower available")


def _find_followers(managers: list[ServerHandle]) -> list[ServerHandle]:
    followers = [manager for manager in managers if not manager.instance.is_leader()]
    if not followers:
        raise AssertionError("no followers available")
    return followers


def _manager_tcp_addr(manager: ServerHandle) -> tuple[str, int]:
    return manager.host, manager.tcp_port


def _manager_node_id(manager: ServerHandle) -> str:
    return manager.instance._node_id.full


def _all_managers_observe_job_leader(
    managers: list[ServerHandle],
    job_id: str,
    leader_id: str,
) -> bool:
    return all(
        manager.instance._manager_state.get_job_leader(job_id) == leader_id
        for manager in managers
    )


def _followers_observe_stable_leader(managers: list[ServerHandle]) -> bool:
    try:
        leader = _find_leader(managers)
    except AssertionError:
        return False

    leader_addr = (leader.host, leader.udp_port)
    for follower in managers:
        if follower is leader:
            continue
        state = follower.instance._leader_election.state
        if state.current_leader != leader_addr or not state.is_lease_valid():
            return False

    return True


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_pre_vote_rejected_during_stable_lease() -> None:
    """Follower rejects a pre-vote while a healthy leader lease is active."""
    spec = _l2_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="pre_vote_rejected_during_stable_lease",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            poll=0.5,
            description="initial leader elected",
        )
        await wait_until(
            lambda: _followers_observe_stable_leader(managers),
            timeout=60.0,
            poll=0.25,
            description="followers observe stable leader lease",
        )

        followers = _find_followers(managers)
        target_follower = followers[0]
        candidate_follower = followers[-1]
        target_election = target_follower.instance._leader_election
        original_term = target_election.state.current_term
        candidate_addr = (candidate_follower.host, candidate_follower.udp_port)

        response = target_election.handle_pre_vote_request(
            candidate=candidate_addr,
            term=original_term + 1,
            candidate_lhm=0,
        )

        assert response is not None
        assert f"pre-vote-resp:{original_term + 1}:0>".encode() in response
        assert target_election.state.current_term == original_term
        assert target_election.state.role == "follower"
        assert target_election.state.is_lease_valid()
        assert target_election.state.pre_voting_in_progress is False
        assert target_election.state.pre_votes_received == set()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_flapping_detector_backs_off_after_election_failures() -> None:
    """Repeated election failures trip flapping detection and delay elections."""
    spec = _l2_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="flapping_detector_backs_off_after_election_failures",
    ) as cluster:
        manager = cluster.managers("main")[0]
        election = manager.instance._leader_election
        detector = election.flapping_detector
        base_cooldown = detector.base_cooldown

        for _change_index in range(detector.max_changes_per_window):
            await election._record_election_failure("forced_election_failure")

        should_delay, delay_seconds = detector.should_delay_election()
        assert detector.get_stats()["is_flapping"] is True
        assert detector.current_cooldown > base_cooldown
        assert detector.current_cooldown <= detector.max_cooldown
        assert should_delay is True
        assert 0.0 < delay_seconds <= detector.current_cooldown
        assert election.get_election_timeout() >= detector.current_cooldown


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_term_exhaustion_bails_cleanly() -> None:
    """Near and at ``MAX_TERM`` election paths do not overflow terms."""
    state = LeaderState(current_term=MAX_TERM - 1)
    assert state.next_term() == MAX_TERM
    assert state.start_election(MAX_TERM) is True
    assert state.become_leader(MAX_TERM) is True

    election = LocalLeaderElection()
    election.self_addr = ("127.0.0.1", 1)
    election.state.current_term = MAX_TERM

    async def broadcast_noop(_message: bytes) -> None:
        return None

    election.set_callbacks(
        broadcast_message=broadcast_noop,
        get_member_count=lambda: 1,
        get_lhm_score=lambda: 0,
        self_addr=("127.0.0.1", 1),
    )

    assert election.state.is_term_exhausted() is True
    assert election.state.next_term() == MAX_TERM
    assert await election._run_pre_vote() is False
    await election._run_election()
    assert election.state.current_term == MAX_TERM
    assert election.state.is_leader() is False


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_swim_leader_and_raft_job_leader_can_diverge_safely() -> None:
    """Per-job Raft leadership may differ from the SWIM DC leader without split brain."""
    spec = _l2_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="swim_leader_and_raft_job_leader_can_diverge_safely",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            poll=0.5,
            description="initial SWIM leader elected",
        )

        swim_leader = _find_leader(managers)
        job_leader = _find_follower(managers)
        job_id = "synthetic-job"
        job_leader_id = _manager_node_id(job_leader)
        fence_token = 7

        # Job leadership is a fenced per-job claim (AD-10 fence tokens,
        # applied through ManagerLeaseCoordinator), independent of which
        # manager leads the SWIM tier.
        claim_results = [
            manager.instance._leases.apply_job_leadership(
                job_id=job_id,
                leader_id=job_leader_id,
                leader_addr=_manager_tcp_addr(job_leader),
                fencing_token=fence_token,
            )
            for manager in managers
        ]

        stale_claim_results = [
            manager.instance._leases.apply_job_leadership(
                job_id=job_id,
                leader_id=_manager_node_id(swim_leader),
                leader_addr=_manager_tcp_addr(swim_leader),
                fencing_token=fence_token - 1,
            )
            for manager in managers
        ]

        assert claim_results == [True, True, True]
        assert job_leader.instance.is_leader() is False
        assert swim_leader.node_id != job_leader.node_id
        assert stale_claim_results == [False, False, False]

        local_job_leaders = [
            manager
            for manager in managers
            if manager.instance._leases.is_job_leader(job_id)
        ]
        assert local_job_leaders == [job_leader]
        assert {
            manager.instance._leases.get_job_leader(job_id)
            for manager in managers
        } == {job_leader_id}
        assert {
            manager.instance._leases.get_fence_token(job_id)
            for manager in managers
        } == {fence_token}

        heartbeat_time = swim_leader.instance._leader_election.state.leader_lease_start
        assert heartbeat_time <= time.monotonic()
        assert swim_leader.instance._leader_election.state.is_lease_valid()
        assert dc_has_leader(managers)() is True


def _l2_workload_spec() -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=1, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


def _long_running_workload() -> WorkloadSpec:
    """One LongRunningWorkflow: the job is admitted and stays in flight
    across the leadership moves and the job leader's death."""
    return WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], LongRunningWorkflow)],
                dc_count=1,
                timeout_seconds=_LONG_JOB_TIMEOUT_SECONDS,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=[],
    )


def _job_leaders(managers: list[ServerHandle], job_id: str) -> list[ServerHandle]:
    return [manager for manager in managers if manager.instance._leases.is_job_leader(job_id)]


async def _move_swim_leadership_off(
    managers: list[ServerHandle],
    keeper: ServerHandle,
) -> ServerHandle:
    """Step ``keeper`` down until another manager wins the SWIM election;
    the new SWIM leader. Each step-down is one election, so the cohort
    size bounds the rounds."""
    for _ in managers:
        if not keeper.instance.is_leader():
            break
        await keeper.instance._leader_election._step_down()
        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            poll=0.5,
            description="a SWIM leader after the step-down",
        )
    swim_leader = _find_leader(managers)
    assert swim_leader is not keeper
    return swim_leader


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_dead_job_leader_failover_uses_swim_leader() -> None:
    """A dead job leader transfers to the SWIM leader, not the per-job Raft leader.

    Steady-state job leadership may diverge from SWIM leadership, but death
    recovery is intentionally centralized through the SWIM cluster leader so a
    busy manager tier does not split-brain job ownership under failover load.

    The job is admitted for real -- a job known only as a lease entry was
    never admitted, and the takeover settles it rather than claiming it
    (``ManagerServer._job_still_needs_takeover``). Its leader is then made
    a SWIM follower, the third manager the job's per-job Raft group leader,
    and the job leader dies.
    """
    spec = _l2_workload_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="dead_job_leader_failover_uses_swim_leader",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            poll=0.5,
            description="initial SWIM leader elected",
        )

        async with cluster.workload(_long_running_workload()) as driver:
            await driver.submit()
            await driver.wait_until_running(timeout=30.0)
            job_id = driver.observations.submitted_job_ids[-1]
            await wait_until(
                lambda: len(_job_leaders(managers, job_id)) == 1,
                timeout=10.0,
                poll=0.2,
                description="the admitted job has one leader",
            )
            [failed_job_leader] = _job_leaders(managers, job_id)
            original_fence_token = failed_job_leader.instance._leases.get_fence_token(job_id)

            swim_leader = await _move_swim_leadership_off(managers, failed_job_leader)
            [raft_job_leader] = [
                manager
                for manager in managers
                if manager is not swim_leader and manager is not failed_job_leader
            ]
            swim_leader_id = _manager_node_id(swim_leader)

            # The third manager leads the job's per-job Raft group: that is
            # no failover authority (AD-52 job groups), so neither the
            # callback nor the deprecated hook takes the job over.
            raft_job_leader.instance._on_job_raft_leader(job_id)
            await raft_job_leader.instance._check_raft_leader_takeover(job_id)

            await cluster.faults.kill(failed_job_leader)
            survivors = [swim_leader, raft_job_leader]

            await wait_until(
                lambda: _all_managers_observe_job_leader(
                    survivors,
                    job_id,
                    swim_leader_id,
                ),
                timeout=_LONG_JOB_TIMEOUT_SECONDS,
                poll=0.2,
                description="all survivors observe SWIM-leader job takeover",
                on_fail=lambda: cluster.dump_diagnostics(
                    reason="dead job leader did not transfer to SWIM leader"
                ),
            )

            assert swim_leader.instance._manager_state.get_job_leader_addr(
                job_id
            ) == _manager_tcp_addr(swim_leader)
            assert (
                swim_leader.instance._leases.get_fence_token(job_id)
                > original_fence_token
            )
            assert not raft_job_leader.instance._leases.is_job_leader(job_id)
