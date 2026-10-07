"""
A dead gate's death reaches a surviving gate that a partition has cut off
from the other surviving gate — through the MANAGER's DEAD gossip, inside
one manager probe cycle of the manager's commit.

The interleaving (the long-horizon gate-fault chaos composite, seed 220,
reduced to its four SWIM members): gate-c dies; the manager — whose
suspicion bracket is far shorter than a gate's (``SWIM_SUSPICION_*`` vs
``GATE_SWIM_GLOBAL_*``) — commits it DEAD first; at that instant gate-a is
partitioned from gate-b, so gate-b's relay of the news can never reach
gate-a. gate-a's only timely source is the manager's own gossip.

The defect this pins: the gossip buffer charged a copy sent TO an
update's own subject against the update's lambda*log(n) relay budget, and
the dead subject draws most of the manager's traffic right after the
commit (its in-flight probe round, the suspicion notice, a proxy probe
per indirect-probe requester). Measured before the fix: 3 of the
manager's 5 DEAD copies went to gate-c itself, 2 to gate-b, none to
gate-a — which then sat on its own no-confirmation suspicion leg
(gate bracket ``[30, 120]`` stretched to 124.8s) and declared gate-c dead
133s after the kill instead of 22s.
"""

from collections.abc import Callable

from hyperscale.distributed.env import Env
from hyperscale.distributed.swim.health.local_health_multiplier import (
    LocalHealthMultiplier,
)
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.swim_death_watch_demo import (
    swim_death_watch_gate_entry,
    swim_death_watch_manager_entry,
)

import pytest

# 220 is the long-horizon chaos composite's seed (test_multiprocess_gate_faults)
# and the mutation check: with copies to the subject charged again, its
# gate-a never learns the death inside the run (the manager's budget goes to
# gate-c and gate-b), while 217 and 200 (that suite's other distinct-layout
# seeds) disseminate in time either way and guard the bound across schedules.
_SEEDS = (220, 217, 200)
# Coordinator link latency, one way: the earliest instant any reaction to
# an observed row can take effect.
_LATENCY = 0.01
_GATE_PIDS = ("gate-a", "gate-b", "gate-c")
_GATE_HOSTS = {gate_pid: f"sim-{gate_pid}" for gate_pid in _GATE_PIDS}
_MANAGER_HOST = "sim-mgr"
_VICTIM = "gate-c"
_SUSPECTING_GATE = "gate-a"
_OTHER_CONFIRMER = "gate-b"

_ENV = Env()
# The dissemination deadline: the manager's next message to gate-a carries
# the DEAD update (with copies to the subject uncharged, its relay budget
# int(3 * ln(n + 1)) outlasts the cycle), and its round-robin probe cycle
# (ProbeScheduler) reaches gate-a within one round per member — the round
# in flight to the dead gate at commit, the other survivor's, gate-a's own.
# A round is one protocol period (SWIM_UDP_POLL_INTERVAL) plus its probe
# exchange: the direct phase's budget (at most the LHM-stretched timeout
# times the LHM saturation multiplier, ``_compute_direct_probe_budget``)
# and the indirect wait (one LHM-stretched timeout); the stretched timeout
# is SWIM_CURRENT_TIMEOUT times that same saturation multiplier
# (``get_lhm_adjusted_timeout``).
_LHM_SATURATION_MULTIPLIER = LocalHealthMultiplier().get_max_multiplier()
_STRETCHED_PROBE_TIMEOUT = _ENV.SWIM_CURRENT_TIMEOUT * _LHM_SATURATION_MULTIPLIER
_PROBE_ROUND_BOUND = (
    _ENV.SWIM_UDP_POLL_INTERVAL
    + _STRETCHED_PROBE_TIMEOUT * _LHM_SATURATION_MULTIPLIER
    + _STRETCHED_PROBE_TIMEOUT
)
_ROUNDS_TO_REACH_SUSPECTING_GATE = len(_GATE_PIDS)
_DISSEMINATION_DEADLINE_SECONDS = _PROBE_ROUND_BOUND * _ROUNDS_TO_REACH_SUSPECTING_GATE
# The run ends before any gate's OWN no-confirmation suspicion could expire
# (a gate suspicion's leg is at least GATE_SWIM_GLOBAL_MAX_TIMEOUT after it
# starts, and it starts after the kill), so a death row for gate-c at
# gate-a inside the run can only have come from dissemination.
_CEILING = _ENV.GATE_SWIM_GLOBAL_MAX_TIMEOUT


def _build_cluster(seed: int) -> SimulationCoordinator:
    """Three peered gates and one manager registered with all of them."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_CEILING, seed=seed
    )
    for gate_pid in _GATE_PIDS:
        peer_hosts = [
            _GATE_HOSTS[peer_pid] for peer_pid in _GATE_PIDS if peer_pid != gate_pid
        ]
        coordinator.add_process(
            gate_pid,
            swim_death_watch_gate_entry,
            _GATE_HOSTS[gate_pid],
            9000,
            9001,
            {"dc-1": [(_MANAGER_HOST, 9000)]},
            {"dc-1": [(_MANAGER_HOST, 9001)]},
            [(peer_host, 9000) for peer_host in peer_hosts],
            [(peer_host, 9001) for peer_host in peer_hosts],
        )
    coordinator.add_process(
        "manager",
        swim_death_watch_manager_entry,
        _MANAGER_HOST,
        9000,
        9001,
        "dc-1",
        [(_GATE_HOSTS[gate_pid], 9000) for gate_pid in _GATE_PIDS],
        [(_GATE_HOSTS[gate_pid], 9001) for gate_pid in _GATE_PIDS],
    )
    return coordinator


def _manager_can_suspect_every_gate() -> Callable[[tuple], bool]:
    """A row predicate true once the manager can suspect its LAST gate."""
    suspectable_hosts: set[str] = set()
    gate_hosts = set(_GATE_HOSTS.values())

    def matches(row: tuple) -> bool:
        if row[0] == "swim-suspectable" and row[1] in gate_hosts:
            suspectable_hosts.add(row[1])
        return suspectable_hosts == gate_hosts

    return matches


def _is_victim_death(row: tuple) -> bool:
    return row[0] == "swim-dead" and row[1] == _GATE_HOSTS[_VICTIM]


def _run_kill_then_cut_suspecting_gate(seed: int) -> tuple[dict, dict[str, float]]:
    """Kill gate-c once the manager can suspect every gate; partition gate-a from
    gate-b (for good) the instant the manager commits gate-c DEAD."""
    coordinator = _build_cluster(seed)
    fault_instants: dict[str, float] = {}

    def kill_victim(row: tuple) -> None:
        fault_instants["kill_at"] = row[-1] + _LATENCY
        coordinator.schedule_kill(_VICTIM, fault_instants["kill_at"])

    def cut_suspecting_gate(row: tuple) -> None:
        fault_instants["manager_commit_at"] = row[-1]
        fault_instants["cut_at"] = row[-1] + _LATENCY
        coordinator.schedule_partition(
            _SUSPECTING_GATE, _OTHER_CONFIRMER, fault_instants["cut_at"]
        )

    coordinator.schedule_on_event("manager", _manager_can_suspect_every_gate(), kill_victim)
    coordinator.schedule_on_event("manager", _is_victim_death, cut_suspecting_gate)
    return coordinator.run(), fault_instants


def _first_row_time(rows: list, tag: str, host: str) -> float | None:
    return next((row[-1] for row in rows if row[0] == tag and row[1] == host), None)


@pytest.mark.parametrize("seed", _SEEDS)
def test_cut_off_gate_learns_a_dead_peer_from_the_manager_within_one_probe_cycle(seed: int):
    results, fault_instants = _run_kill_then_cut_suspecting_gate(seed)
    victim_host = _GATE_HOSTS[_VICTIM]
    suspecting_rows = results[_SUSPECTING_GATE]

    # Premise: both surviving gates could suspect the victim before it died
    # (registered and confirmed, AD-29), and the suspecting gate had not
    # learned of the death before the cut — the manager was first.
    for gate_pid in (_SUSPECTING_GATE, _OTHER_CONFIRMER):
        suspectable_at = _first_row_time(results[gate_pid], "swim-suspectable", victim_host)
        assert suspectable_at is not None and suspectable_at < fault_instants["kill_at"], (
            gate_pid,
            results[gate_pid],
        )
    death_at = _first_row_time(suspecting_rows, "swim-dead", victim_host)
    assert death_at is None or death_at > fault_instants["cut_at"], (fault_instants, suspecting_rows)

    deadline = fault_instants["manager_commit_at"] + _DISSEMINATION_DEADLINE_SECONDS
    assert death_at is not None and death_at <= deadline, (
        "the cut-off gate must learn the death from the manager's gossip "
        f"within one probe cycle ({_DISSEMINATION_DEADLINE_SECONDS}s) of the commit",
        fault_instants,
        suspecting_rows,
        results["manager"],
    )


def test_kill_then_cut_is_replay_deterministic():
    assert _run_kill_then_cut_suspecting_gate(_SEEDS[0]) == _run_kill_then_cut_suspecting_gate(
        _SEEDS[0]
    )
