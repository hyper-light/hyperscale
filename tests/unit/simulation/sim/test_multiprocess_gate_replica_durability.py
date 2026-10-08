"""
A2-G-266 end to end under multi-process SIM: a job's gate replica -- its
fence token, leader and idempotency key binding -- survives power loss.

Three peered gates, each with a Raft store on its own SimFilesystem, front
one datacenter; the client submits one job through gate-b, which leads it.

Measured on the old code: the replica's two-phase commit kept prepared and
committed replicas in memory only, so a gate that lost power came back
with none -- a power loss of the whole gate tier lost the job's replica,
its fence token and its key binding everywhere, and the leader that
crashed right after acknowledging the client came back without the job
it led.

Pinned:
* every gate loses power the moment the submission gate holds the
  committed replica: each gate of the quorum that committed it comes back
  from its own disk (its Raft store resumed) with the same replica --
  fence token, sequence, leader, key -- and the key decided for the job in
  its idempotency cache, before ``start()`` returns (no peer could have
  handed it back: every peer lost power too); a gate outside that quorum
  invents none;
* the leader gate loses power right after the client saw its job
  accepted: it comes back from its own disk with the replica it
  acknowledged, unchanged, before ``start()`` returns.
"""

from tests.simulation.harness.sim.multiprocess.gate_replica_durability_demo import (
    COORDINATOR_LATENCY,
    GATE_HOSTS,
    SUBMISSION_GATE,
    run_gate_replica_durability,
)

_CEILING = 40.0
_DOWN_SECONDS = 1.0


def _rows(log: list, tag: str) -> list[tuple[object, float]]:
    return [(entry[1], entry[2]) for entry in log if entry[0] == tag]


def _holds_one_replica(row: tuple) -> bool:
    return row[0] == "replicas" and len(row[1]) == 1


def _last_replicas_before_power_loss(log: list) -> tuple:
    """The committed replicas a generation last held: what its power loss
    must not lose."""
    held = [value for value, _ in _rows(log, "replicas") if value]
    assert held, log
    return held[-1]


def _assert_recovered_from_own_disk(host: str, before: tuple, rebooted_log: list) -> None:
    """The rebooted generation resumed its store and held ``before`` --
    replica and key binding -- before its ``start()`` returned."""
    assert [resumed for resumed, _ in _rows(rebooted_log, "raft-store-opened")] == [True], (host, rebooted_log)
    ((_, started_at),) = [(None, entry[1]) for entry in rebooted_log if entry[0] == "gate-started"]
    held = [(value, seen_at) for value, seen_at in _rows(rebooted_log, "replicas") if value]
    assert held, (host, rebooted_log)
    first_held, first_held_at = held[0]
    assert first_held == before, (host, before, rebooted_log)
    assert first_held_at <= started_at, (host, rebooted_log)
    # Adopted by the time start() returned: read at that instant.
    assert _rows(rebooted_log, "key-adopted-at-start") == [(1, started_at)], (host, rebooted_log)


def _run_whole_tier_power_loss() -> dict:
    def arm(coordinator) -> None:
        def power_off_every_gate(row: tuple) -> None:
            restart_at = row[-1] + COORDINATOR_LATENCY
            for host in GATE_HOSTS:
                coordinator.schedule_restart(host, restart_at, down_seconds=_DOWN_SECONDS)

        coordinator.schedule_on_event(SUBMISSION_GATE, _holds_one_replica, power_off_every_gate)

    return run_gate_replica_durability(_CEILING, arm)


def _run_leader_power_loss_after_ack() -> dict:
    def arm(coordinator) -> None:
        def power_off_the_leader(row: tuple) -> None:
            coordinator.schedule_restart(SUBMISSION_GATE, row[-1] + COORDINATOR_LATENCY, down_seconds=_DOWN_SECONDS)

        coordinator.schedule_on_event("client", lambda row: row[0] == "job-submitted", power_off_the_leader)

    return run_gate_replica_durability(_CEILING, arm)


def test_every_gate_recovers_the_replica_after_a_whole_tier_power_loss():
    results = _run_whole_tier_power_loss()

    holders = {
        host: _last_replicas_before_power_loss(results[f"{host}.gen1"])
        for host in GATE_HOSTS
        if any(value for value, _ in _rows(results[f"{host}.gen1"], "replicas"))
    }
    # A quorum committed it -- the submission gate among them -- one
    # replica, the same at each, led by the submission gate and binding
    # the client's idempotency key.
    assert SUBMISSION_GATE in holders and len(holders) >= 2, holders
    assert len(set(holders.values())) == 1, holders
    ((fence_token, _sequence, leader_host, binds_key),) = holders[SUBMISSION_GATE]
    assert (leader_host, binds_key) == (SUBMISSION_GATE, True), holders
    assert fence_token >= 1, holders

    for host, before in holders.items():
        _assert_recovered_from_own_disk(host, before, results[host])
    # A gate that never held it does not invent it.
    for host in set(GATE_HOSTS) - set(holders):
        assert [value for value, _ in _rows(results[host], "replicas") if value] == [], (host, results[host])


def test_the_leader_recovers_the_replica_it_acknowledged_after_a_power_loss():
    results = _run_leader_power_loss_after_ack()

    before = _last_replicas_before_power_loss(results[f"{SUBMISSION_GATE}.gen1"])
    ((_fence_token, _sequence, leader_host, binds_key),) = before
    assert (leader_host, binds_key) == (SUBMISSION_GATE, True), before

    _assert_recovered_from_own_disk(SUBMISSION_GATE, before, results[SUBMISSION_GATE])
    # A quorum committed it: at least one peer holds the same replica.
    peer_holders = [
        host
        for host in GATE_HOSTS
        if host != SUBMISSION_GATE and before in [value for value, _ in _rows(results[host], "replicas")]
    ]
    assert peer_holders, results


def test_the_leader_power_loss_is_replay_deterministic():
    assert _run_leader_power_loss_after_ack() == _run_leader_power_loss_after_ack()
