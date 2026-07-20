"""
C5 pause/resume — ``SimulationCoordinator.schedule_pause``, the
SIGSTOP-style freeze over the multi-process lockstep.

Two symmetric heartbeat processes ping each other every virtual second;
one is frozen for ``[2.5, 6.5)``. The world-visible contract asserted
against exact logs:

* SILENCE — the survivor hears nothing from the victim for the whole
  window (its own sends continue, so the silence is the victim's);
* deliveries toward the frozen victim BUFFER (the host is alive — its
  NIC queue holds) and arrive in the thaw grant;
* BURST — the victim's frozen-span output reaches the survivor in a
  burst at the resume instant (four receipts at one virtual instant);
* the victim's own log pins the documented local-sweep model: its
  timers fire at their originally scheduled LOCAL virtual times inside
  the single thaw window (see ``schedule_pause``'s model note — the
  victim's own timeline replays the span; every OTHER process observes
  freeze-then-burst).

The interaction rules are pinned too: SIGKILL of a frozen victim kills
it un-thawed (no burst, ever), power loss of a frozen victim reboots it
un-paused with the frozen span executed by NOBODY, back-to-back windows
sharing an edge compose into one continuous freeze byte-identically,
and pausing a corpse or double-pausing raise loudly. Determinism is
free (the schedule is data) and asserted by full twin-run equality.
"""

import pytest

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.demo_endpoints import (
    ping_pong_entry,
    repeating_ping_entry,
)

_ALPHA_ADDRESS = ("proc", 1)
_BETA_ADDRESS = ("proc", 2)


def _run_pause_burst(pause_windows: list) -> dict:
    """Two mutual heartbeaters (interval 1.0, latency 0.5), ``alpha``
    frozen per ``pause_windows``, ceiling 9.0."""
    coordinator = SimulationCoordinator(latency=0.5, max_virtual_time=9.0)
    coordinator.add_process(
        "alpha", repeating_ping_entry, _ALPHA_ADDRESS, _BETA_ADDRESS, 1.0
    )
    coordinator.add_process(
        "beta", repeating_ping_entry, _BETA_ADDRESS, _ALPHA_ADDRESS, 1.0
    )
    for at_time, resume_time in pause_windows:
        coordinator.schedule_pause(
            "alpha", at_time=at_time, resume_time=resume_time
        )
    return coordinator.run()


def _sends(times: list, peer) -> list:
    return [(time, "send", "ping", peer) for time in times]


def _recvs(times: list, peer) -> list:
    return [(time, "recv", "ping", peer) for time in times]


def test_paused_process_is_silent_then_bursts_at_resume():
    results = _run_pause_burst([(2.5, 6.5)])

    # The survivor: normal exchange to 2.5, then SILENCE from alpha
    # while its own sends continue (3.0-6.0 unanswered), then the
    # frozen span's four pings land in a BURST at one instant (6.5 —
    # the resume), then steady state resumes.
    assert results["beta"] == [
        (1.0, "send", "ping", _ALPHA_ADDRESS),
        (1.5, "recv", "ping", _ALPHA_ADDRESS),
        (2.0, "send", "ping", _ALPHA_ADDRESS),
        (2.5, "recv", "ping", _ALPHA_ADDRESS),
        *_sends([3.0, 4.0, 5.0, 6.0], _ALPHA_ADDRESS),
        *_recvs([6.5, 6.5, 6.5, 6.5], _ALPHA_ADDRESS),
        (7.0, "send", "ping", _ALPHA_ADDRESS),
        (7.5, "recv", "ping", _ALPHA_ADDRESS),
        (8.0, "send", "ping", _ALPHA_ADDRESS),
        (8.5, "recv", "ping", _ALPHA_ADDRESS),
        (9.0, "send", "ping", _ALPHA_ADDRESS),
    ], results["beta"]

    # The victim: pre-freeze exchange, then the frozen span replayed in
    # the single thaw window — its own sends at their LOCAL 3.0-6.0
    # instants interleaved with the buffered deliveries (2.5-6.5) — the
    # documented local-sweep model; then steady state. Every send in
    # the 3.0-6.0 range reached beta only at 6.5 (the burst above).
    assert results["alpha"] == [
        (1.0, "send", "ping", _BETA_ADDRESS),
        (1.5, "recv", "ping", _BETA_ADDRESS),
        (2.0, "send", "ping", _BETA_ADDRESS),
        (2.5, "recv", "ping", _BETA_ADDRESS),
        (3.0, "send", "ping", _BETA_ADDRESS),
        (3.5, "recv", "ping", _BETA_ADDRESS),
        (4.0, "send", "ping", _BETA_ADDRESS),
        (4.5, "recv", "ping", _BETA_ADDRESS),
        (5.0, "send", "ping", _BETA_ADDRESS),
        (5.5, "recv", "ping", _BETA_ADDRESS),
        (6.0, "send", "ping", _BETA_ADDRESS),
        (6.5, "recv", "ping", _BETA_ADDRESS),
        (7.0, "send", "ping", _BETA_ADDRESS),
        (7.5, "recv", "ping", _BETA_ADDRESS),
        (8.0, "send", "ping", _BETA_ADDRESS),
        (8.5, "recv", "ping", _BETA_ADDRESS),
        (9.0, "send", "ping", _BETA_ADDRESS),
    ], results["alpha"]


def test_pause_is_replay_deterministic():
    assert _run_pause_burst([(2.5, 6.5)]) == _run_pause_burst([(2.5, 6.5)])


def test_back_to_back_pause_windows_compose_into_one_freeze():
    """Resume at T + next pause at T re-freezes before any thaw grant
    fires: the split schedule is byte-identical to the single window."""
    assert _run_pause_burst([(2.5, 4.5), (4.5, 6.5)]) == _run_pause_burst(
        [(2.5, 6.5)]
    )


def _run_sink_scenario(configure) -> dict:
    """A repeating pinger toward a passive sink, faults applied by
    ``configure`` — the shape the kill/restart interaction tests share."""
    coordinator = SimulationCoordinator(latency=0.5, max_virtual_time=8.0)
    coordinator.add_process(
        "alpha", repeating_ping_entry, _ALPHA_ADDRESS, _BETA_ADDRESS, 1.0
    )
    coordinator.add_process(
        "beta", ping_pong_entry, _BETA_ADDRESS, _ALPHA_ADDRESS, "sink"
    )
    configure(coordinator)
    return coordinator.run()


def test_kill_during_pause_window_kills_without_thaw():
    """SIGKILL of a stopped process: the victim dies frozen — its
    buffered span is discarded, the burst NEVER arrives, and it
    produces no RESULT (like any kill)."""

    def configure(coordinator: SimulationCoordinator) -> None:
        coordinator.schedule_pause("alpha", at_time=2.5, resume_time=6.5)
        coordinator.schedule_kill("alpha", at_time=4.0)

    results = _run_sink_scenario(configure)

    assert "alpha" not in results
    assert results["beta"] == _recvs([1.5, 2.5], _ALPHA_ADDRESS), results[
        "beta"
    ]


def test_restart_during_pause_window_reboots_unpaused():
    """Power loss of a frozen victim: the generation-1 snapshot
    reflects execution up to the freeze instant (a stopped process
    does no work), the frozen span is executed by NOBODY (no 3.0-5.0
    sends exist anywhere), and the rebooted generation runs un-paused
    — the stale armed thaw is skipped, never applied to gen-2."""

    def configure(coordinator: SimulationCoordinator) -> None:
        coordinator.schedule_pause("alpha", at_time=2.5, resume_time=7.5)
        coordinator.schedule_restart("alpha", at_time=4.0, down_seconds=1.0)

    results = _run_sink_scenario(configure)

    assert results["alpha.gen1"] == _sends([1.0, 2.0], _BETA_ADDRESS), (
        results["alpha.gen1"]
    )
    # Gen-2 boots at 5.0, heartbeats from 6.0 — never frozen.
    assert results["alpha"] == _sends([6.0, 7.0, 8.0], _BETA_ADDRESS), (
        results["alpha"]
    )
    assert results["beta"] == _recvs([1.5, 2.5, 6.5, 7.5], _ALPHA_ADDRESS), (
        results["beta"]
    )


def test_pausing_a_dead_process_raises():
    """Freezing a corpse is a scenario bug — refused loudly, never a
    silent no-op."""

    def configure(coordinator: SimulationCoordinator) -> None:
        coordinator.schedule_kill("alpha", at_time=2.0)
        coordinator.schedule_pause("alpha", at_time=3.0, resume_time=6.0)

    with pytest.raises(ValueError, match="unknown or already-dead"):
        _run_sink_scenario(configure)


def test_overlapping_pause_windows_raise():
    def configure(coordinator: SimulationCoordinator) -> None:
        coordinator.schedule_pause("alpha", at_time=2.0, resume_time=6.0)
        coordinator.schedule_pause("alpha", at_time=3.0, resume_time=7.0)

    with pytest.raises(ValueError, match="already paused"):
        _run_sink_scenario(configure)


def test_pause_schedule_validation():
    """``resume_time`` must be strictly after ``at_time`` — rejected at
    schedule time, before any process spawns."""
    coordinator = SimulationCoordinator(latency=0.5)
    with pytest.raises(ValueError, match="strictly after"):
        coordinator.schedule_pause("alpha", at_time=5.0, resume_time=5.0)
    with pytest.raises(ValueError, match="strictly after"):
        coordinator.schedule_pause("alpha", at_time=5.0, resume_time=4.0)
