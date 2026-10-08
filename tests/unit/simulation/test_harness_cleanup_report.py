"""ClusterHarness teardown never drops a finding (simulation_framework.md
"Errors collect into a CleanupReport attached to the test failure").

Each test drives the harness's real ``__aexit__`` with a real
``InvariantChecker`` that observes a violated safety invariant and a real
``Supervisor`` whose cleanup report holds an error, then checks where the
findings land for a failed and for a passing test body.
"""

import asyncio
import pathlib
import tempfile

import pytest

from tests.simulation.harness.cluster_harness import ClusterHarness
from tests.simulation.harness.cluster_spec import ClusterSpec
from tests.simulation.harness.invariants import (
    InvariantChecker,
    InvariantResult,
    InvariantViolation,
    SafetyInvariant,
)
from tests.simulation.harness.port_allocator import PortAllocator
from tests.simulation.harness.scenario_signal_router import ScenarioSignalRouter
from tests.simulation.harness.supervisor import Supervisor


VIOLATED_INVARIANT_NAME = "always_violated"
LEAKED_TASK_ERROR = "leaked task: sim-test-leak"


async def _harness_with_findings(
    violated: bool,
    cleanup_errors: list[str],
) -> ClusterHarness:
    """A harness whose teardown will find ``cleanup_errors`` and, when ``violated``, a pending violation."""
    spec = ClusterSpec(gates=0, datacenters={})
    harness = ClusterHarness(spec=spec)
    supervisor = Supervisor(timeouts=spec.timeouts, ports=PortAllocator(host=spec.host))
    supervisor.cleanup_errors.extend(cleanup_errors)
    checker = InvariantChecker(harness=harness, poll_interval=spec.timeouts.invariant_poll_interval)
    checker.add_safety(
        SafetyInvariant(
            name=VIOLATED_INVARIANT_NAME,
            evaluate=lambda _harness: InvariantResult(holds=not violated, detail="forced by test"),
        )
    )
    harness._supervisor = supervisor
    harness._invariants = checker
    # The rest of the state ``__aenter__`` gives a harness its teardown uses.
    harness._signal_router = ScenarioSignalRouter(asyncio.current_task())
    harness._node_data_root = pathlib.Path(tempfile.mkdtemp(prefix="hyperscale-harness-"))
    await checker.start()
    await _wait_for_first_tick(checker, violated)
    return harness


async def _wait_for_first_tick(checker: InvariantChecker, violated: bool) -> None:
    """Yield until the checker has recorded the forced violation (or ticked once when none is forced)."""
    while violated and checker.violation is None:
        await asyncio.sleep(0)
    await asyncio.sleep(0)


async def test_failed_body_carries_violation_and_cleanup_errors_as_notes() -> None:
    harness = await _harness_with_findings(violated=True, cleanup_errors=[LEAKED_TASK_ERROR])
    body_failure = AssertionError("scenario assertion failed")

    await harness.__aexit__(type(body_failure), body_failure, None)

    notes = getattr(body_failure, "__notes__", [])
    assert any(VIOLATED_INVARIANT_NAME in note and "InvariantViolation" in note for note in notes), notes
    assert f"harness cleanup error: {LEAKED_TASK_ERROR}" in notes


async def test_failed_body_keeps_its_own_exception_type() -> None:
    harness = await _harness_with_findings(violated=True, cleanup_errors=[LEAKED_TASK_ERROR])

    with pytest.raises(ValueError) as raised:
        async with _ExitOnly(harness):
            raise ValueError("body failure")

    assert len(raised.value.__notes__) == 2


async def test_passing_body_raises_violation_carrying_cleanup_errors() -> None:
    harness = await _harness_with_findings(violated=True, cleanup_errors=[LEAKED_TASK_ERROR])

    with pytest.raises(InvariantViolation) as raised:
        await harness.__aexit__(None, None, None)

    assert VIOLATED_INVARIANT_NAME in str(raised.value)
    assert f"harness cleanup error: {LEAKED_TASK_ERROR}" in raised.value.__notes__


async def test_passing_body_raises_cleanup_errors_alone() -> None:
    harness = await _harness_with_findings(violated=False, cleanup_errors=[LEAKED_TASK_ERROR])

    with pytest.raises(RuntimeError, match=LEAKED_TASK_ERROR):
        await harness.__aexit__(None, None, None)


async def test_clean_teardown_raises_nothing() -> None:
    harness = await _harness_with_findings(violated=False, cleanup_errors=[])

    await harness.__aexit__(None, None, None)


class _ExitOnly:
    """Async context manager running only a harness's real ``__aexit__`` around a body."""

    def __init__(self, harness: ClusterHarness) -> None:
        self._harness = harness

    async def __aenter__(self) -> ClusterHarness:
        return self._harness

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._harness.__aexit__(exc_type, exc_val, exc_tb)
