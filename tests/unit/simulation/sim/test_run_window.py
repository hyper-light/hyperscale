"""
Unit tests for ``SimulationLoop.run_window`` — the bounded-execution
primitive that the multi-process ``SimulationCoordinator`` drives.

``run_window(deadline)`` must: process every event with ``when <=
deadline`` (including callbacks chained within the window) in the same
order the single-process scheduler would; never advance virtual time
past ``deadline``; report the next pending timer (``> deadline``) or
``None`` when idle; and never raise the deadlock error (an idle process
awaiting a cross-process message is normal, not a bug).
"""

from tests.simulation.harness.sim import SimulationLoop


def test_processes_events_up_to_deadline_and_reports_next():
    loop = SimulationLoop()
    fired: list[float] = []
    loop.call_at(1.0, lambda: fired.append(1.0))
    loop.call_at(3.0, lambda: fired.append(3.0))
    loop.call_at(5.0, lambda: fired.append(5.0))

    next_event = loop.run_window(3.0)

    assert fired == [1.0, 3.0]
    assert next_event == 5.0
    assert loop.time() == 3.0


def test_second_window_drains_remaining_then_idle():
    loop = SimulationLoop()
    fired: list[float] = []
    loop.call_at(1.0, lambda: fired.append(1.0))
    loop.call_at(5.0, lambda: fired.append(5.0))

    assert loop.run_window(3.0) == 5.0
    # Second window past the last event: fires it, then idle -> None.
    assert loop.run_window(10.0) is None
    assert fired == [1.0, 5.0]


def test_advances_to_window_edge_when_next_event_is_beyond():
    loop = SimulationLoop()
    loop.call_at(10.0, lambda: None)

    next_event = loop.run_window(3.0)

    # No event fired, but the clock advanced to the granted edge and the
    # coordinator learns the next event is at 10.0.
    assert next_event == 10.0
    assert loop.time() == 3.0


def test_chained_callbacks_within_window_run_same_window():
    loop = SimulationLoop()
    fired: list = []

    def outer():
        fired.append(loop.time())
        loop.call_soon(lambda: fired.append(("soon", loop.time())))

    loop.call_at(2.0, outer)

    next_event = loop.run_window(5.0)

    assert fired == [2.0, ("soon", 2.0)]
    assert next_event is None


def test_idle_loop_returns_none_without_raising():
    loop = SimulationLoop()
    # No work scheduled at all: a quiescent process, not a deadlock.
    assert loop.run_window(100.0) is None
    assert loop.time() == 0.0


def test_timer_exactly_at_deadline_fires():
    loop = SimulationLoop()
    fired: list[float] = []
    loop.call_at(3.0, lambda: fired.append(3.0))

    next_event = loop.run_window(3.0)

    assert fired == [3.0]
    assert next_event is None


def test_start_time_initializes_virtual_clock():
    """A loop built with ``start_time`` (a process admitted mid-run by
    the coordinator) begins at that virtual time: relative scheduling is
    offset from it and nothing time-travels into the global past."""
    loop = SimulationLoop(start_time=7.5)
    fired: list[float] = []

    assert loop.time() == 7.5

    loop.call_later(0.5, lambda: fired.append(loop.time()))
    next_event = loop.run_window(8.0)

    assert fired == [8.0]
    assert next_event is None
    assert loop.time() == 8.0


def test_windowed_execution_matches_single_pass_ordering():
    """Driving a schedule window-by-window yields the same fire order as
    running it in one pass — the coordinator must not perturb ordering."""

    def build_schedule(loop: SimulationLoop, sink: list):
        # Mix of same-instant and staggered timers, with a chained soon.
        loop.call_at(1.0, lambda: sink.append("a@1"))
        loop.call_at(1.0, lambda: sink.append("b@1"))
        loop.call_at(2.0, lambda: (sink.append("c@2"), loop.call_soon(
            lambda: sink.append("c-soon@2"))))
        loop.call_at(4.0, lambda: sink.append("d@4"))

    one_pass: list = []
    loop_a = SimulationLoop()
    build_schedule(loop_a, one_pass)
    # One big window covers everything.
    assert loop_a.run_window(10.0) is None

    windowed: list = []
    loop_b = SimulationLoop()
    build_schedule(loop_b, windowed)
    # Many small windows.
    for edge in (0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0):
        loop_b.run_window(edge)

    assert one_pass == windowed
    assert one_pass == ["a@1", "b@1", "c@2", "c-soon@2", "d@4"]
