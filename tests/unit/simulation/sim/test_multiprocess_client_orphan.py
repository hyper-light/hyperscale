"""
FIXED-BUG PIN — the manager's client-orphan virtual-time spin.

The last committed member of the frozen-instant livelock family: after
a client process vanished (power loss — a leaf death the servers must
absorb), the manager's orphan machinery re-armed a ``Timeout``-family
wait on a composed-float remainder at ~vanish+150s and spun at one
frozen virtual instant forever. Every SIM suite in the corpus carried
the workaround as a CEILING CAP ("ceiling held below the manager's
client-orphan virtual-time spin" — the gates suite docstring;
``vopr_gates`` was capped at 145s for the same reason). The epsilon-
expiry contract (``protocol/time_quantum.py``, applied tier-wide in
b4bc1784 and finished by the 9fa652b0 boundary guards) cured it:
sub-quantum remainders ARE expiry, and no wait re-arms at an instant
the quantized clock cannot honor.

This scenario is the reproducer shape, un-capped: gateless L2, ONE
genuinely-long (60s) workflow, the client SIGKILLed at t=20 — inside
live execution, so the manager owns a running job whose submitter no
longer exists — and a 420s ceiling, vanish+400, nearly three times the
old spin instant. THE RUN COMPLETING AT ALL is the pin: the harness's
runaway diagnostic aborts any child that spins 500001 times at one
instant, so reaching the ceiling proves no frozen-instant re-arm fires
anywhere in the orphan path. The behavioral assertions pin the rest:
the workflow runs to natural completion on the worker (the vanish must
not kill in-flight work), the worker drains and stays registered to
the ceiling (orphan cleanup must not poison worker health), and the
whole timeline replays byte-identically.

Measured (seed 311): dispatch 9.5, client killed 20.0, workflow drains
75.25 (dispatch + 60s + push-failure slack — the completion push has
no live destination and fails loudly manager-side), worker-count holds
1 from 3.5 to the 420 ceiling.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.l2_workload_demo import (
    sustained_client_entry,
)
from tests.simulation.harness.sim.multiprocess.soak_multi_job_demo import (
    soak_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)
from tests.simulation.oracle import ClusterTraceOracle

_SEED = 311
_WORKFLOW_DURATION_SECONDS = 60.0
_CLIENT_KILL_AT = 20.0
# Nearly 3x the old ~vanish+150 spin instant: the ceiling IS the test.
_CEILING = 420.0
# Probed: dispatch 9.5 + 60s execution + completion-push failure slack.
_NATURAL_DRAIN = 75.25


def _run_client_orphan() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=_SEED
    )
    coordinator.add_process(
        "manager", soak_manager_entry, "sim-mgr", 9000, 9001, "sim-dc", ()
    )
    coordinator.add_process(
        "worker",
        worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )
    coordinator.add_process(
        "client",
        sustained_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        _WORKFLOW_DURATION_SECONDS,
        1,
        90.0,
        150.0,
    )
    coordinator.schedule_kill("client", _CLIENT_KILL_AT)
    return coordinator.run()


def test_client_vanish_mid_execution_never_spins_the_manager():
    """The pin proper: the run REACHES its vanish+400 ceiling (the
    harness runaway diagnostic would abort a frozen-instant spin), the
    orphaned workflow completes at its natural length, and the worker
    stays healthy and registered to the end."""
    results = _run_client_orphan()

    # SIGKILLed client returns no result log.
    assert "client" not in results, sorted(results)

    worker_log = results["worker"]
    drain_rows = [
        entry
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] == 0 and entry[2] > 0.0
    ]
    assert drain_rows, worker_log
    assert abs(drain_rows[0][2] - _NATURAL_DRAIN) <= 2.0, (
        f"orphaned workflow drained at {drain_rows[0][2]}, expected the "
        f"natural length ~{_NATURAL_DRAIN}: {worker_log}"
    )

    # Worker registration holds to the ceiling: orphan cleanup must not
    # poison worker health (worker-count transitions are the soak
    # manager entry's watcher — final value 1, no eviction dip after
    # registration).
    manager_log = results["manager"]
    count_rows = [entry for entry in manager_log if entry[0] == "worker-count"]
    assert count_rows[-1][1] == 1, manager_log
    post_registration_dips = [
        entry for entry in count_rows if entry[1] == 0 and entry[2] > 5.0
    ]
    assert not post_registration_dips, manager_log

    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_client_orphan_is_replay_deterministic():
    assert _run_client_orphan() == _run_client_orphan()
