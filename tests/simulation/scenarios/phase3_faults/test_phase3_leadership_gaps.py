"""
Phase 3 leadership scenarios from ``docs/SCENARIOS.md`` §1.

These cover leadership edges not exercised by the baseline kill/restart
faults: stable-lease pre-vote rejection, flapping backoff, synthetic term
exhaustion, and SWIM DC-leader vs per-job Raft-leader divergence.
"""

import time

import pytest

from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
from hyperscale.distributed.swim.leadership import LocalLeaderElection
from hyperscale.distributed.swim.leadership.leader_state import MAX_TERM, LeaderState
from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    HarnessTimeouts,
    ServerHandle,
    dc_has_leader,
    wait_until,
)


def _l2_spec(base_port: int) -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=0),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=base_port,
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


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_pre_vote_rejected_during_stable_lease() -> None:
    """Follower rejects a pre-vote while a healthy leader lease is active."""
    spec = _l2_spec(base_port=31500)
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

        leader = _find_leader(managers)
        follower = _find_follower(managers)
        follower_election = follower.instance._leader_election
        original_term = follower_election.state.current_term
        candidate_addr = (leader.host, leader.udp_port)

        response = follower_election.handle_pre_vote_request(
            candidate=candidate_addr,
            term=original_term + 1,
            candidate_lhm=0,
        )

        assert response is not None
        assert f"pre-vote-resp:{original_term + 1}:0>".encode() in response
        assert follower_election.state.current_term == original_term
        assert follower_election.state.role == "follower"
        assert follower_election.state.is_lease_valid()
        assert follower_election.state.pre_voting_in_progress is False
        assert follower_election.state.pre_votes_received == set()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_flapping_detector_backs_off_after_election_failures() -> None:
    """Repeated election failures trip flapping detection and delay elections."""
    spec = _l2_spec(base_port=33000)
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
    spec = _l2_spec(base_port=34500)
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
        raft_job_leader = _find_follower(managers)
        job_id = "synthetic-job"
        tracker: JobLeadershipTracker[int] = (
            raft_job_leader.instance._raft_leadership_tracker
        )
        token = await tracker.assume_leadership_async(
            job_id,
            metadata=1,
            initial_token=7,
        )

        for manager in managers:
            manager_tracker: JobLeadershipTracker[int] = (
                manager.instance._raft_leadership_tracker
            )
            if manager is raft_job_leader:
                continue
            accepted = await manager_tracker.process_leadership_claim_async(
                job_id=job_id,
                claimer_id=tracker.node_id,
                claimer_addr=tracker.node_addr,
                fencing_token=token,
                metadata=1,
            )
            assert accepted is True

        stale_claim_results = [
            await manager.instance._raft_leadership_tracker.process_leadership_claim_async(
                job_id=job_id,
                claimer_id=swim_leader.node_id,
                claimer_addr=(swim_leader.host, swim_leader.tcp_port),
                fencing_token=token - 1,
                metadata=1,
            )
            for manager in managers
        ]

        leaderships = tracker.get_all_leaderships()
        assert raft_job_leader.instance.is_leader() is False
        assert swim_leader.node_id != raft_job_leader.node_id
        assert stale_claim_results == [False, False, False]
        assert len(leaderships) == 1
        assert leaderships[0][0] == job_id
        assert leaderships[0][1] == tracker.node_id
        assert leaderships[0][3] == token

        local_job_leaders = [
            manager
            for manager in managers
            if manager.instance._raft_leadership_tracker.is_leader(job_id)
        ]
        assert local_job_leaders == [raft_job_leader]
        assert {
            manager.instance._raft_leadership_tracker.get_leader(job_id)
            for manager in managers
        } == {tracker.node_id}
        assert {
            manager.instance._raft_leadership_tracker.get_fencing_token(job_id)
            for manager in managers
        } == {token}

        heartbeat_time = swim_leader.instance._leader_election.state.leader_lease_start
        assert heartbeat_time <= time.monotonic()
        assert swim_leader.instance._leader_election.state.is_lease_valid()
        assert dc_has_leader(managers)() is True
