"""
Job control under multi-process SIM (``job_control_demo``): one gateless
manager -- the datacenter's leader, where gateless and gate-routed jobs
alike are admitted -- its workers, and clients.

D-65 concurrency caps. Client A's ping job is admitted; the instant the
manager admits it, client B arrives with another (an event-derived
arrival). The datacenter has room for one such job at a time -- by the
derived cap (two cores, a 4 core-second job, a 3s timeout: 4 <= 2 x 3 < 8)
or by a configured job-class cap of one -- so B's first submission is
refused with a retry hint, the manager never counts two jobs at once, and
B is admitted once A ended. Both complete, and nothing stays counted.

D-63 capacity reservation. Two of the worker's four cores are held back
for the burst class. A long job of another class fills the shared two; the
instant it is admitted, a burst job and an ordinary job of the same shape
arrive. The burst job is admitted into the reserve at once, the ordinary
one is refused with a hint and admitted only once the long job ended.

D-67 noisy-job breaker. The worker refuses the first dispatches of
``SimNoisyWorkflow`` -- one more than a workflow's retry budget -- so the
first noisy job fails with a refused retry, and the manager quarantines
its class. The instant the quarantine shows, a well-behaved ping client
and a second noisy client arrive. The ping job is admitted at once and
completes: the breaker isolates the noisy class alone. The noisy client
is refused with a retry hint, comes back when the breaker is half-open,
and its job -- the probe -- runs clean (the workers can start the class
again) and closes the breaker; its next job is admitted at once.
"""

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs.job_class_circuit_breaker import NOISY_JOB_BREAKER_CONTROL
from hyperscale.distributed.jobs.job_concurrency_caps import CONCURRENCY_CAP_CONTROL
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_control_demo import (
    job_control_client_entry,
    job_control_gate_entry,
    job_control_manager_entry,
    job_control_worker_entry,
)

_SEED = 61
_LATENCY = 0.01
_CEILING = 60.0
_MANAGER_ADDRESS = ("sim-mgr", 9000)
_PING_CLASS = "SimPingWorkflow"
_NOISY_CLASS = "SimNoisyWorkflow"
# SimPingWorkflow: two VUs (two cores) for a two-second duration -- four
# core-seconds. Two registered cores over a three-second timeout hold six:
# room for one such job, not two.
_CAPS_WORKER_CORES = 2
_DERIVED_CAP_TIMEOUT_SECONDS = 3.0
# A timeout the derived cap never binds at (two jobs: 8 <= 2 x 60), so
# only the configured class cap can refuse.
_CLASS_CAP_TIMEOUT_SECONDS = 60.0
_WAIT_TIMEOUT_SECONDS = 50.0
# The noisy class's first job fails once every dispatch its retry budget
# allows -- the first and one per retry -- was refused.
_NOISY_REFUSED_DISPATCHES = Env().RETRY_BUDGET_PER_WORKFLOW_DEFAULT + 1
# Room for the noisy, ping and probe jobs side by side.
_BREAKER_WORKER_CORES = 6
_BREAKER_TIMEOUT_SECONDS = 30.0


def _rows(log: list, tag: str) -> list[tuple]:
    return [entry for entry in log if entry[0] == tag]


def _finished(log: list) -> list[tuple[int, str]]:
    return [(ordinal, status) for _tag, ordinal, status, _at in _rows(log, "job-finished")]


def _add_manager_and_worker(coordinator: SimulationCoordinator, env_overrides: dict, cores: int, refused: int) -> None:
    coordinator.add_process(
        "manager", job_control_manager_entry, "sim-mgr", 9000, 9001, "sim-dc", env_overrides
    )
    coordinator.add_process(
        "worker", job_control_worker_entry, "sim-wkr", 9010, 9011, "sim-dc", [_MANAGER_ADDRESS], cores, refused
    )


def _client_args(host: str, workflow_kind: str, job_count: int, timeout_seconds: float) -> tuple:
    return (host, 9500, [_MANAGER_ADDRESS], workflow_kind, job_count, timeout_seconds, _WAIT_TIMEOUT_SECONDS)


def _run_caps(env_overrides: dict, timeout_seconds: float, seed: int = _SEED) -> dict:
    coordinator = SimulationCoordinator(latency=_LATENCY, max_virtual_time=_CEILING, seed=seed)
    _add_manager_and_worker(coordinator, env_overrides, _CAPS_WORKER_CORES, 0)
    coordinator.add_process(
        "client-a", job_control_client_entry, *_client_args("sim-cli-a", "ping", 1, timeout_seconds)
    )

    def admit_client_b(admission_row: tuple) -> None:
        coordinator.schedule_admission(
            "client-b",
            admission_row[-1] + _LATENCY,
            job_control_client_entry,
            *_client_args("sim-cli-b", "ping", 1, timeout_seconds),
        )

    coordinator.schedule_on_event(
        "manager", lambda row: row[0] == "admission-admitted", admit_client_b
    )
    return coordinator.run()


_CAP_SCENARIOS = {
    "derived-datacenter-cap": ({}, _DERIVED_CAP_TIMEOUT_SECONDS),
    "configured-job-class-cap": (
        {"JOB_CLASS_CONCURRENCY_CAPS": f"{_PING_CLASS}=1"},
        _CLASS_CAP_TIMEOUT_SECONDS,
    ),
}


@pytest.mark.parametrize("scenario", sorted(_CAP_SCENARIOS))
def test_a_job_beyond_the_cap_is_refused_with_a_hint_and_admitted_once_room_frees(scenario: str):
    env_overrides, timeout_seconds = _CAP_SCENARIOS[scenario]
    results = _run_caps(env_overrides, timeout_seconds)
    manager_log = results["manager"]

    admitted = _rows(manager_log, "admission-admitted")
    refused = _rows(manager_log, "admission-refused")
    # Two jobs admitted, each once; B's first submission came while A
    # held the room, and was refused by the cap with a hint.
    assert [job_class for _tag, job_class, _at in admitted] == [_PING_CLASS, _PING_CLASS], manager_log
    assert refused, manager_log
    first_admitted_at, second_admitted_at = (at_time for *_rest, at_time in admitted)
    for _tag, job_class, control, retry_after_seconds, refused_at in refused:
        assert (job_class, control) == (_PING_CLASS, CONCURRENCY_CAP_CONTROL), refused
        assert retry_after_seconds > 0.0, refused
        assert first_admitted_at < refused_at < second_admitted_at, (admitted, refused)

    # The cap held: never two jobs counted at once, and B was admitted
    # only once A no longer was.
    counted = [(count, at_time) for _tag, count, at_time in _rows(manager_log, "counted-jobs")]
    assert max(count for count, _at in counted) == 1, counted
    assert any(
        count == 0 and first_admitted_at < at_time <= second_admitted_at for count, at_time in counted
    ), (counted, admitted)
    # Nothing stays counted once both ended.
    assert counted[-1][0] == 0, counted

    # Both jobs ran once and completed.
    assert _finished(results["client-a"]) == [(0, "completed")], results["client-a"]
    assert _finished(results["client-b"]) == [(0, "completed")], results["client-b"]
    assert len(_rows(results["worker"], "dispatch-run")) == 2, results["worker"]


def test_gate_hold_scenario_is_replay_deterministic():
    assert _run_gate_hold() == _run_gate_hold()


def test_caps_scenario_is_replay_deterministic():
    env_overrides, timeout_seconds = _CAP_SCENARIOS["derived-datacenter-cap"]
    assert _run_caps(env_overrides, timeout_seconds) == _run_caps(env_overrides, timeout_seconds)


def _run_breaker(seed: int = _SEED) -> dict:
    coordinator = SimulationCoordinator(latency=_LATENCY, max_virtual_time=_CEILING, seed=seed)
    _add_manager_and_worker(coordinator, {}, _BREAKER_WORKER_CORES, _NOISY_REFUSED_DISPATCHES)
    coordinator.add_process(
        "client-noisy", job_control_client_entry, *_client_args("sim-cli-n", "noisy", 1, _BREAKER_TIMEOUT_SECONDS)
    )

    def admit_ping_and_noisy_again(quarantine_row: tuple) -> None:
        arrive_at = quarantine_row[-1] + _LATENCY
        coordinator.schedule_admission(
            "client-ping",
            arrive_at,
            job_control_client_entry,
            *_client_args("sim-cli-p", "ping", 1, _BREAKER_TIMEOUT_SECONDS),
        )
        coordinator.schedule_admission(
            "client-noisy-again",
            arrive_at,
            job_control_client_entry,
            *_client_args("sim-cli-m", "noisy", 2, _BREAKER_TIMEOUT_SECONDS),
        )

    coordinator.schedule_on_event(
        "manager",
        lambda row: row[0] == "quarantine" and (_NOISY_CLASS, "OPEN") in row[1],
        admit_ping_and_noisy_again,
    )
    return coordinator.run()


def test_a_noisy_job_class_is_quarantined_alone_and_recovers_through_a_probe():
    results = _run_breaker()
    manager_log = results["manager"]

    # The noisy job burned its retry budget and failed.
    assert _finished(results["client-noisy"]) == [(0, "failed")], results["client-noisy"]
    assert len(_rows(results["worker"], "noisy-dispatch-refused")) == _NOISY_REFUSED_DISPATCHES

    quarantine = [(states, at_time) for _tag, states, at_time in _rows(manager_log, "quarantine")]
    opened_at = next(at_time for states, at_time in quarantine if (_NOISY_CLASS, "OPEN") in states)
    closed_at = next(at_time for states, at_time in quarantine if at_time > opened_at and states == ())

    admitted = [(job_class, at_time) for _tag, job_class, at_time in _rows(manager_log, "admission-admitted")]
    refused = _rows(manager_log, "admission-refused")

    # Isolated alone: the ping job was admitted while the noisy class was
    # quarantined, never refused, and completed.
    ping_admitted_at = [at_time for job_class, at_time in admitted if job_class == _PING_CLASS]
    assert len(ping_admitted_at) == 1 and opened_at < ping_admitted_at[0] < closed_at, (admitted, quarantine)
    assert all(job_class == _NOISY_CLASS for _tag, job_class, *_rest in refused), refused
    assert _finished(results["client-ping"]) == [(0, "completed")], results["client-ping"]

    # The noisy class was refused, by the breaker, with a hint it was
    # admitted no sooner than.
    assert refused, manager_log
    noisy_admitted_at = [at_time for job_class, at_time in admitted if job_class == _NOISY_CLASS]
    probe_admitted_at = noisy_admitted_at[1]
    for _tag, _job_class, control, retry_after_seconds, refused_at in refused:
        assert control == NOISY_JOB_BREAKER_CONTROL, refused
        assert retry_after_seconds > 0.0, refused
        assert opened_at <= refused_at < probe_admitted_at, (refused, admitted)
    first_refused_at, first_retry_after_seconds = refused[0][-1], refused[0][-2]
    assert probe_admitted_at >= first_refused_at + first_retry_after_seconds, (refused, admitted)

    # Recovered through the probe: it ran clean and closed the breaker;
    # the class's next job was admitted at once -- no refusal came after
    # the probe's admission (the watch samples the close on its interval).
    assert len(noisy_admitted_at) == 3, admitted
    assert probe_admitted_at < closed_at, (admitted, quarantine)
    assert (_NOISY_CLASS, "HALF_OPEN") in quarantine[-2][0], quarantine
    assert _finished(results["client-noisy-again"]) == [(0, "completed"), (1, "completed")], results[
        "client-noisy-again"
    ]
    assert quarantine[-1][0] == (), quarantine
    assert _rows(manager_log, "counted-jobs")[-1][1] == 0, manager_log


def test_breaker_scenario_is_replay_deterministic():
    assert _run_breaker() == _run_breaker()


# -- A gate-routed job no datacenter has room for (D-65) ---------------------
#
# Two datacenters, each one two-core worker held by a gateless blocker job
# (SimLongWorkflow: two cores for 40s -- 80 core-seconds, admitted under a
# 120s timeout). A ping job through the gate (4 core-seconds, 30s timeout)
# fits neither: 84 > 2 x 30. Both refuse it with a hint ((84 - 60) / 2 =
# 12s), and the gate -- which acked the client -- holds it. The moment the
# gate holds it, a sixteen-core worker joins each datacenter (an
# event-derived healing): 84 <= 18 x 18 for what is left of the job's
# timeout, so the gate's next placement lands it, and it completes.
_GATE_ADDRESS = ("sim-gate", 9000)
_GATE_UDP_ADDRESS = ("sim-gate", 9001)
_DATACENTERS = ("dc-east", "dc-west")
_BLOCKER_TIMEOUT_SECONDS = 120.0
_GATED_JOB_TIMEOUT_SECONDS = 30.0
_JOINING_WORKER_CORES = 16
_GATE_CEILING = 90.0


def _manager_host(datacenter: str) -> str:
    return f"sim-mgr-{datacenter}"


def _run_gate_hold(seed: int = _SEED) -> dict:
    coordinator = SimulationCoordinator(latency=_LATENCY, max_virtual_time=_GATE_CEILING, seed=seed)
    coordinator.add_process(
        "gate",
        job_control_gate_entry,
        "sim-gate",
        9000,
        9001,
        {datacenter: [(_manager_host(datacenter), 9000)] for datacenter in _DATACENTERS},
        {datacenter: [(_manager_host(datacenter), 9001)] for datacenter in _DATACENTERS},
    )
    for datacenter in _DATACENTERS:
        manager_address = (_manager_host(datacenter), 9000)
        coordinator.add_process(
            f"manager-{datacenter}",
            job_control_manager_entry,
            _manager_host(datacenter),
            9000,
            9001,
            datacenter,
            {},
            [_GATE_ADDRESS],
            [_GATE_UDP_ADDRESS],
        )
        coordinator.add_process(
            f"worker-{datacenter}",
            job_control_worker_entry,
            f"sim-wkr-{datacenter}",
            9010,
            9011,
            datacenter,
            [manager_address],
            _CAPS_WORKER_CORES,
            0,
        )

    def blocker_args(datacenter: str) -> tuple:
        return (
            f"sim-blk-{datacenter}",
            9500,
            [(_manager_host(datacenter), 9000)],
            "long",
            1,
            _BLOCKER_TIMEOUT_SECONDS,
            _GATE_CEILING,
        )

    coordinator.add_process("blocker-dc-east", job_control_client_entry, *blocker_args("dc-east"))

    def is_blocker_admission(row: tuple) -> bool:
        return row[0] == "admission-admitted" and row[1] == "SimLongWorkflow"

    coordinator.schedule_on_event(
        "manager-dc-east",
        is_blocker_admission,
        lambda row: coordinator.schedule_admission(
            "blocker-dc-west", row[-1] + _LATENCY, job_control_client_entry, *blocker_args("dc-west")
        ),
    )
    coordinator.schedule_on_event(
        "manager-dc-west",
        is_blocker_admission,
        lambda row: coordinator.schedule_admission(
            "client-gated",
            row[-1] + _LATENCY,
            job_control_client_entry,
            "sim-cli-g",
            9500,
            [_GATE_ADDRESS],
            "ping",
            1,
            _GATED_JOB_TIMEOUT_SECONDS,
            _GATE_CEILING,
            "gate",
        ),
    )
    def free_room(held_row: tuple) -> None:
        for datacenter in _DATACENTERS:
            coordinator.schedule_admission(
                f"worker-{datacenter}-joining",
                held_row[-1] + _LATENCY,
                job_control_worker_entry,
                f"sim-wkr-{datacenter}-2",
                9010,
                9011,
                datacenter,
                [(_manager_host(datacenter), 9000)],
                _JOINING_WORKER_CORES,
                0,
            )

    coordinator.schedule_on_event("gate", lambda row: row[0] == "held", free_room)
    return coordinator.run()


def test_a_gated_job_no_datacenter_has_room_for_is_held_and_runs_once_room_frees():
    results = _run_gate_hold()
    gate_log = results["gate"]

    # Every datacenter the gate offered the job refused it for want of
    # room, with a hint, and the gate held the job.
    first_routed = _rows(gate_log, "routed")[0]
    offered = first_routed[1] + first_routed[2]
    held_at = _rows(gate_log, "held")[0][-1]
    for datacenter in offered:
        refused = [row for row in _rows(results[f"manager-{datacenter}"], "admission-refused") if row[1] == _PING_CLASS]
        assert refused and refused[0][-1] <= held_at, (datacenter, results[f"manager-{datacenter}"])
        assert refused[0][2] == CONCURRENCY_CAP_CONTROL and refused[0][3] > 0.0, refused

    # Room freed, and the held job was placed again -- no sooner than the
    # hint -- landed, and completed.
    placed = [
        (datacenter, at_time)
        for datacenter in _DATACENTERS
        for _tag, job_class, at_time in _rows(results[f"manager-{datacenter}"], "admission-admitted")
        if job_class == _PING_CLASS
    ]
    assert len(placed) == 1, placed
    smallest_hint = min(
        row[3]
        for datacenter in offered
        for row in _rows(results[f"manager-{datacenter}"], "admission-refused")
        if row[1] == _PING_CLASS
    )
    assert placed[0][1] >= held_at + smallest_hint - _LATENCY, (placed, held_at, smallest_hint)
    assert _finished(results["client-gated"]) == [(0, "completed")], results["client-gated"]

    # Held, not failed: the gate's job went from dispatching to running,
    # never through a terminal status before it completed.
    gate_statuses = [status for _tag, status, _at in _rows(gate_log, "gate-job")]
    assert "failed" not in gate_statuses, gate_statuses
    assert gate_statuses.index("dispatching") < gate_statuses.index("running"), gate_statuses


# -- The breaker survives its leader (D-67) ----------------------------------
#
# Three peered managers, one worker that cannot start the noisy class at
# first. The noisy job fails with a refused retry under the datacenter's
# leader, which quarantines the class. The instant the client sees the job
# end, that leader is killed, and a second noisy client arrives. The
# manager elected in its place refuses the class -- from the job's
# committed ledger terminal, mirrored on every member -- until its
# quarantine passes, then admits one probe, which runs clean and closes it.
_PEERED_MANAGER_HOSTS = ("sim-mgr-a", "sim-mgr-b", "sim-mgr-c")
_FAILOVER_CEILING = 120.0


def _run_breaker_failover(seed: int = _SEED) -> tuple[dict, str]:
    coordinator = SimulationCoordinator(latency=_LATENCY, max_virtual_time=_FAILOVER_CEILING, seed=seed)
    manager_addresses = [(host, 9000) for host in _PEERED_MANAGER_HOSTS]
    for host in _PEERED_MANAGER_HOSTS:
        coordinator.add_process(
            host,
            job_control_manager_entry,
            host,
            9000,
            9001,
            "sim-dc",
            {},
            None,
            None,
            [(peer, 9000) for peer in _PEERED_MANAGER_HOSTS if peer != host],
            [(peer, 9001) for peer in _PEERED_MANAGER_HOSTS if peer != host],
        )
    coordinator.add_process(
        "worker",
        job_control_worker_entry,
        "sim-wkr",
        9010,
        9011,
        "sim-dc",
        manager_addresses,
        _BREAKER_WORKER_CORES,
        _NOISY_REFUSED_DISPATCHES,
    )
    coordinator.add_process(
        "client-noisy",
        job_control_client_entry,
        "sim-cli-n",
        9500,
        manager_addresses,
        "noisy",
        1,
        _BREAKER_TIMEOUT_SECONDS,
        _FAILOVER_CEILING,
    )
    killed: list[str] = []

    def kill_leader_and_resubmit(ended_row: tuple) -> None:
        _tag, _ordinal, _status, leader_host, ended_at = ended_row
        killed.append(leader_host)
        coordinator.schedule_kill(leader_host, ended_at + _LATENCY)
        coordinator.schedule_admission(
            "client-noisy-again",
            ended_at + 2 * _LATENCY,
            job_control_client_entry,
            "sim-cli-m",
            9500,
            manager_addresses,
            "noisy",
            1,
            _BREAKER_TIMEOUT_SECONDS,
            _FAILOVER_CEILING,
        )

    coordinator.schedule_on_event("client-noisy", lambda row: row[0] == "job-ended-at", kill_leader_and_resubmit)
    return coordinator.run(), killed[0]


def test_a_new_leader_keeps_the_quarantine_until_a_probe_recovers_it():
    results, killed_host = _run_breaker_failover()

    assert _finished(results["client-noisy"]) == [(0, "failed")], results["client-noisy"]
    killed_at = next(at_time for *_rest, at_time in _rows(results["client-noisy"], "job-ended-at"))

    # Every surviving member mirrored the quarantine from the committed
    # terminal -- none of them led the job.
    for host in _PEERED_MANAGER_HOSTS:
        if host != killed_host:
            assert any(
                (_NOISY_CLASS, "OPEN") in states for _tag, states, _at in _rows(results[host], "quarantine")
            ), (host, results[host])

    # A surviving manager took the lead after the kill ...
    new_leaders = [
        host
        for host in _PEERED_MANAGER_HOSTS
        if host != killed_host
        and any(is_leader and at_time > killed_at for _tag, is_leader, at_time in _rows(results[host], "leader"))
    ]
    assert len(new_leaders) == 1, {host: _rows(results[host], "leader") for host in _PEERED_MANAGER_HOSTS}
    new_leader_log = results[new_leaders[0]]

    # ... and refused the noisy class before it admitted any job of it:
    # the quarantine outlived the leader that opened it.
    decisions = [
        row for row in new_leader_log
        if row[0] in ("admission-admitted", "admission-refused") and row[1] == _NOISY_CLASS
    ]
    assert decisions and decisions[0][0] == "admission-refused", decisions
    assert decisions[0][2] == NOISY_JOB_BREAKER_CONTROL and decisions[0][3] > 0.0, decisions

    # Recovered through a probe: one admission, clean, breaker closed.
    admitted = [row for row in decisions if row[0] == "admission-admitted"]
    assert len(admitted) == 1, decisions
    assert _finished(results["client-noisy-again"]) == [(0, "completed")], results["client-noisy-again"]
    assert _rows(new_leader_log, "quarantine")[-1][1] == (), new_leader_log


# -- Capacity reservation (D-63) ---------------------------------------------
#
# Four registered cores, two reserved for SimBurstWorkflow: two shared. The
# long job (two cores for 40s, 80 core-seconds) is admitted under a 60s
# timeout (80 <= 2 x 60). A burst job and a ping job -- the same shape, four
# core-seconds, under a 40s timeout -- then arrive together. The shared
# cores hold 2 x 40 = 80, all the long job's: the ping job is refused
# (84 > 80); the burst job's four core-seconds fit its reserve (4 <= 2 x 40)
# and it is admitted. Without the reserve the ping job would fit
# (84 <= 4 x 40).
_BURST_CLASS = "SimBurstWorkflow"
_LONG_CLASS = "SimLongWorkflow"
_RESERVATION_WORKER_CORES = 4
_RESERVED_BURST_CORES = 2
_LONG_JOB_TIMEOUT_SECONDS = 60.0
_ARRIVING_JOB_TIMEOUT_SECONDS = 40.0
_RESERVATION_CEILING = 90.0


def _run_reservation(seed: int = _SEED) -> dict:
    coordinator = SimulationCoordinator(latency=_LATENCY, max_virtual_time=_RESERVATION_CEILING, seed=seed)
    _add_manager_and_worker(
        coordinator,
        {"JOB_CLASS_RESERVED_CORES": f"{_BURST_CLASS}={_RESERVED_BURST_CORES}"},
        _RESERVATION_WORKER_CORES,
        0,
    )
    coordinator.add_process(
        "client-long",
        job_control_client_entry,
        "sim-cli-l",
        9500,
        [_MANAGER_ADDRESS],
        "long",
        1,
        _LONG_JOB_TIMEOUT_SECONDS,
        _RESERVATION_CEILING,
    )

    def admit_burst_and_ordinary(admission_row: tuple) -> None:
        for name, host, workflow_kind in (("client-burst", "sim-cli-b", "burst"), ("client-ping", "sim-cli-p", "ping")):
            coordinator.schedule_admission(
                name,
                admission_row[-1] + _LATENCY,
                job_control_client_entry,
                host,
                9500,
                [_MANAGER_ADDRESS],
                workflow_kind,
                1,
                _ARRIVING_JOB_TIMEOUT_SECONDS,
                _RESERVATION_CEILING,
            )

    coordinator.schedule_on_event(
        "manager",
        lambda row: row[0] == "admission-admitted" and row[1] == _LONG_CLASS,
        admit_burst_and_ordinary,
    )
    return coordinator.run()


def test_a_burst_class_is_admitted_into_its_reserve_while_ordinary_jobs_are_refused():
    results = _run_reservation()
    manager_log = results["manager"]

    decisions = [row for row in manager_log if row[0] in ("admission-admitted", "admission-refused")]
    long_admitted_at = next(row[-1] for row in decisions if row[1] == _LONG_CLASS)
    long_ended_at = next(at_time for *_rest, at_time in _rows(results["client-long"], "job-finished"))

    # The burst job: admitted at its first submission, into the reserve,
    # while the long job held every shared core.
    burst_decisions = [row for row in decisions if row[1] == _BURST_CLASS]
    assert [row[0] for row in burst_decisions] == ["admission-admitted"], decisions
    assert long_admitted_at < burst_decisions[0][-1] < long_ended_at, (decisions, long_ended_at)

    # The ordinary job of the same shape: refused by the work cap with a
    # hint while the long job ran, admitted only after it ended.
    ping_decisions = [row for row in decisions if row[1] == _PING_CLASS]
    ping_refusals = [row for row in ping_decisions if row[0] == "admission-refused"]
    ping_admitted_at = [row[-1] for row in ping_decisions if row[0] == "admission-admitted"]
    assert ping_decisions[0][0] == "admission-refused", decisions
    for _tag, _job_class, control, retry_after_seconds, refused_at in ping_refusals:
        assert control == CONCURRENCY_CAP_CONTROL and retry_after_seconds > 0.0, ping_refusals
        assert refused_at < ping_admitted_at[0], (ping_refusals, ping_admitted_at)
    assert ping_refusals[0][-1] < long_ended_at, (ping_refusals, long_ended_at)
    assert len(ping_admitted_at) == 1, decisions

    # Every job ran once and completed; nothing stays counted.
    for client in ("client-long", "client-burst", "client-ping"):
        assert _finished(results[client]) == [(0, "completed")], results[client]
    assert _rows(manager_log, "counted-jobs")[-1][1] == 0, manager_log


def test_reservation_scenario_is_replay_deterministic():
    assert _run_reservation() == _run_reservation()
