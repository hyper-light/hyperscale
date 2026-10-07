"""
Staggered starts across a gate cluster under multi-process SIM (SCENARIOS
§7 "Staggered. Submissions at offsets across multiple gates
concurrently"), over ``staggered_submission_demo``: three peered gates
fronting one datacenter (a manager registered with all three, two
four-core workers), and one client per gate, each submitting a
``SimPingWorkflow`` job every second through ITS gate.

The first client starts with the run. Its first acceptance -- the
cluster's readiness, an event of the run -- anchors the others: client
``k`` is admitted ``k / (rate * gates)`` later, so the three schedules
interleave evenly and every gate has jobs in flight at once.

* Staggered: each client's jobs go out on its own schedule, which starts
  no earlier than its offset from the anchor.
* Concurrent: at some instant all three gates have a job in flight.
* Exactly once: every job is accepted once and completes once -- at the
  manager (AD-54 lifecycle, by job ordinal), at the workers (one run per
  job) and at each client -- whichever gate took it.
* Bounded: once the cluster is formed, no job takes more than two gated
  rounds from submission to terminal.
* Drained: the manager ends with no job, lifecycle record, dispatch queue
  entry, dispatch loop or per-job Raft group; the gate tier stays whole.

The run has a replay twin.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.fanout_demo import (
    WATCH_INTERVAL_SECONDS,
    fanout_worker_entry,
)
from tests.simulation.harness.sim.multiprocess.gate_cluster_demo import gate_tier_entry
from tests.simulation.harness.sim.multiprocess.staggered_submission_demo import (
    gate_paced_client_entry,
    gated_lifecycle_manager_entry,
)
from tests.simulation.oracle import WorkflowLifecycleOracle

_SEED = 67
_LINK_LATENCY_SECONDS = 0.01
_GATE_HOSTS = ("sim-gate-a", "sim-gate-b", "sim-gate-c")
_CLIENTS = [f"client-{index}" for index in range(len(_GATE_HOSTS))]
_WORKER_HOSTS = ["sim-wkr-0", "sim-wkr-1"]
_CORES_PER_WORKER = 4
_JOBS_PER_SECOND_PER_CLIENT = 1.0
_JOBS_PER_CLIENT = 20
_TOTAL_JOBS = _JOBS_PER_CLIENT * len(_CLIENTS)
_STAGGER_SECONDS = 1 / (_JOBS_PER_SECOND_PER_CLIENT * len(_GATE_HOSTS))
# SimPingWorkflow: one 0.5s action. Through a gate a job crosses one more
# hop each way than gateless (client -> gate -> manager, manager -> gate
# -> client), so its round allows twice the gateless ten latencies.
_GATED_ROUND_SECONDS = 0.5 + 20 * _LINK_LATENCY_SECONDS
_SOJOURN_BOUND_SECONDS = 2 * _GATED_ROUND_SECONDS
_JOB_RETENTION_SECONDS = 5.0
_JOB_CLEANUP_INTERVAL_SECONDS = 2.0
_JOB_TIMEOUT_SECONDS = 120.0
# Gate-cluster formation before the first acceptance (probed: 7.2s), the
# last client's offset and schedule, its last round, and the sweep.
_FORMATION_ALLOWANCE_SECONDS = 15.0
_CEILING = (
    _FORMATION_ALLOWANCE_SECONDS
    + (len(_CLIENTS) - 1) * _STAGGER_SECONDS
    + _JOBS_PER_CLIENT / _JOBS_PER_SECOND_PER_CLIENT
    + _SOJOURN_BOUND_SECONDS
    + _JOB_RETENTION_SECONDS
    + 2 * _JOB_CLEANUP_INTERVAL_SECONDS
    + 2 * WATCH_INTERVAL_SECONDS
)


def _client_args(index: int) -> tuple:
    return (
        f"sim-cli-{index}",
        9500,
        (_GATE_HOSTS[index], 9000),
        _JOBS_PER_SECOND_PER_CLIENT,
        _JOBS_PER_CLIENT,
        _JOB_TIMEOUT_SECONDS,
    )


def _add_gate_tier(coordinator: SimulationCoordinator) -> None:
    datacenter_managers = {"sim-dc": [("sim-mgr", 9000)]}
    datacenter_manager_udp = {"sim-dc": [("sim-mgr", 9001)]}
    for gate_host in _GATE_HOSTS:
        peer_hosts = [host for host in _GATE_HOSTS if host != gate_host]
        coordinator.add_process(
            gate_host,
            gate_tier_entry,
            gate_host,
            9000,
            9001,
            datacenter_managers,
            datacenter_manager_udp,
            [(peer_host, 9000) for peer_host in peer_hosts],
            [(peer_host, 9001) for peer_host in peer_hosts],
        )


def _add_datacenter(coordinator: SimulationCoordinator) -> None:
    coordinator.add_process(
        "manager",
        gated_lifecycle_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        [(gate_host, 9000) for gate_host in _GATE_HOSTS],
        [(gate_host, 9001) for gate_host in _GATE_HOSTS],
        _JOB_RETENTION_SECONDS,
        _JOB_CLEANUP_INTERVAL_SECONDS,
    )
    for worker_host in _WORKER_HOSTS:
        coordinator.add_process(
            worker_host, fanout_worker_entry, worker_host, 9000, 9001, "sim-dc", ("sim-mgr", 9000), _CORES_PER_WORKER
        )


def _run_staggered() -> tuple[dict, dict[str, float]]:
    """Run the scenario; also returns each later client's admission instant."""
    coordinator = SimulationCoordinator(latency=_LINK_LATENCY_SECONDS, max_virtual_time=_CEILING, seed=_SEED)
    _add_gate_tier(coordinator)
    _add_datacenter(coordinator)
    coordinator.add_process(_CLIENTS[0], gate_paced_client_entry, *_client_args(0))
    admitted_at: dict[str, float] = {}

    def admit_the_others(anchor_row: tuple) -> None:
        for index in range(1, len(_CLIENTS)):
            admitted_at[_CLIENTS[index]] = anchor_row[1] + index * _STAGGER_SECONDS
            coordinator.schedule_admission(
                _CLIENTS[index], admitted_at[_CLIENTS[index]], gate_paced_client_entry, *_client_args(index)
            )

    coordinator.schedule_on_event(_CLIENTS[0], lambda row: row[0] == "anchor", admit_the_others)
    return coordinator.run(), admitted_at


def _times(log: list, tag: str) -> dict[int, float]:
    return {row[1]: row[-1] for row in log if row[0] == tag}


def _anchor(log: list) -> float:
    return next(row[1] for row in log if row[0] == "anchor")


def _assert_staggered(results: dict, admitted_at: dict[str, float]) -> None:
    """Each later client starts no earlier than its offset, and each keeps
    its own schedule from its own first acceptance."""
    for client in _CLIENTS[1:]:
        assert _anchor(results[client]) >= admitted_at[client], (client, admitted_at)
    for client in _CLIENTS:
        submitted = _times(results[client], "job-submitted")
        anchor = _anchor(results[client])
        for ordinal in range(1, _JOBS_PER_CLIENT):
            assert abs(submitted[ordinal] - (anchor + ordinal / _JOBS_PER_SECOND_PER_CLIENT)) <= 1e-6


def _gates_in_flight_peak(results: dict) -> int:
    """The most gates with a job in flight at one instant."""
    events = sorted(
        (at_time, change, client)
        for client in _CLIENTS
        for tag, change in (("job-submitted", 1), ("job-finished", -1))
        for at_time in _times(results[client], tag).values()
    )
    in_flight = {client: 0 for client in _CLIENTS}
    peak = 0
    for _at_time, change, client in events:
        in_flight[client] += change
        peak = max(peak, sum(1 for count in in_flight.values() if count > 0))
    return peak


def _assert_exactly_once(results: dict) -> None:
    oracle = WorkflowLifecycleOracle()
    assert oracle.check_manager_log(results["manager"]) == [], results["manager"]
    histories = oracle.workflow_histories(results["manager"])
    assert len(histories) == _TOTAL_JOBS, len(histories)
    for history in histories.values():
        assert [to_value for _from, to_value, _at in history] == ["pending", "dispatched", "running", "completed"]
    for client in _CLIENTS:
        finished = sorted((row[1], row[2]) for row in results[client] if row[0] == "job-finished")
        assert finished == [(ordinal, "completed") for ordinal in range(_JOBS_PER_CLIENT)], (client, finished)
    runs = sum(len([row for row in results[host] if row[0] == "dispatch-run"]) for host in _WORKER_HOSTS)
    assert runs == _TOTAL_JOBS, runs


def _assert_bounded(results: dict) -> None:
    """Past the formation (client 0's first job), every job's terminal
    comes within two gated rounds of its submission."""
    for client in _CLIENTS:
        submitted = _times(results[client], "job-submitted")
        finished = _times(results[client], "job-finished")
        first = 1 if client == _CLIENTS[0] else 0
        for ordinal in range(first, _JOBS_PER_CLIENT):
            sojourn = finished[ordinal] - submitted[ordinal]
            assert sojourn <= _SOJOURN_BOUND_SECONDS, (client, ordinal, sojourn)


def _assert_drained_and_whole(results: dict) -> None:
    for tag in ("jobs", "lifecycle-records", "dispatcher-pending", "dispatch-loops"):
        counts = [row[1] for row in results["manager"] if row[0] == tag]
        assert counts[-1] == 0, (tag, counts)
    for gate_host in _GATE_HOSTS:
        peer_counts = [row[1] for row in results[gate_host] if row[0] == "gate-peers"]
        assert peer_counts[-1] == len(_GATE_HOSTS) - 1, (gate_host, peer_counts)
        final_health = {row[1]: row[2] for row in results[gate_host] if row[0] == "dc-health"}
        assert final_health == {"sim-dc": "healthy"}, (gate_host, final_health)


def test_staggered_submissions_through_every_gate_complete_exactly_once():
    results, admitted_at = _run_staggered()

    _assert_staggered(results, admitted_at)
    assert _gates_in_flight_peak(results) == len(_GATE_HOSTS)
    _assert_exactly_once(results)
    _assert_bounded(results)
    for client in _CLIENTS:
        assert not [row for row in results[client] if row[0] == "submit-rejected"], results[client]
    _assert_drained_and_whole(results)


def test_staggered_submission_is_replay_deterministic():
    assert _run_staggered() == _run_staggered()
