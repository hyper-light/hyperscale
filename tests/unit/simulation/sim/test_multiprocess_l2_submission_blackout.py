"""
L5 — submission retry under a SUSTAINED client<->manager blackout
(gateless L2 topology), probed then pinned.

The client is partitioned from its ONLY submission surface for a long
window spanning the whole submission phase; the entry retries
``submit_job`` forever (1s between calls). The pinned truth of the
retry SHAPE comes from the traced mechanism, not the checklist's
"~1/s rejection" sketch — that cadence is impossible under a CUT:

* Each ``submit_job`` call runs an INTERNAL retry cycle
  (``ClientJobSubmitter._submit_with_retry``): ``1 + submission_max_retries``
  attempts, each a ``send_tcp`` with the dial INSIDE a
  ``CLIENT_SUBMISSION_TIMEOUT`` timeout, joined by un-hinted back-offs on the
  RFC 6298 retransmission timeout -- under a cut no round trip lowers it
  below ``CLIENT_RETRANSMISSION_TIMEOUT_MIN_SECONDS`` -- doubling per back-off
  and equal-jittered by ``0.5 + r``, ``r in [0, 1)``. Under total silence
  every attempt times out, so one call's rejection cycle is timeout-paced --
  NEVER a hot spin and NEVER a give-up (the entry's outer loop is unbounded).
  The bounds below are derived from those settings, not traced.
* Only the exhausted call surfaces: ``RuntimeError("Job submission
  failed after 5 retries: ...")`` -- logged as the milestone's
  exception type.
* An attempt whose dial straddles the heal still times out (its SYN
  was already dropped; the harness models no retransmit), so
  acceptance lands within ONE attempt timeout + one max back-off + the
  entry sleep of the heal.
* The cut lasts until exactly two full cycles have been refused: the heal
  comes after the latest the second rejection can land and before the
  earliest a third could.
"""

from hyperscale.distributed.env import Env
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.l2_workload_demo import (
    dag_worker_entry,
    SUSTAINED_ACTION_STEP_COUNT,
    sustained_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
)
from tests.simulation.oracle import ClusterTraceOracle, JobStatusOracle

_SEED = 103
_CUT_AT = 1.0
_WORKFLOW_DURATION_SECONDS = 6.0

_CLIENT_ENV = Env()
_ATTEMPT_TIMEOUT = _CLIENT_ENV.CLIENT_SUBMISSION_TIMEOUT
_ATTEMPTS = _CLIENT_ENV.CLIENT_SUBMISSION_MAX_RETRIES + 1
_BACKOFF_BASE = _CLIENT_ENV.CLIENT_RETRANSMISSION_TIMEOUT_MIN_SECONDS
# The entry's sleep between submit_job calls.
_ENTRY_SLEEP = 1.0
# One un-hinted back-off per retry, doubling, jittered by [0.5, 1.5).
_BACKOFF_SUM = _BACKOFF_BASE * sum(2**backoff for backoff in range(_ATTEMPTS - 1))
_MAX_BACKOFF = _BACKOFF_BASE * 2 ** (_ATTEMPTS - 2) * 1.5
# The INTER-rejection gap is a fully cut cycle plus the entry sleep.
_MIN_REJECTION_GAP = _ATTEMPTS * _ATTEMPT_TIMEOUT + 0.5 * _BACKOFF_SUM + _ENTRY_SLEEP
_MAX_REJECTION_GAP = _ATTEMPTS * _ATTEMPT_TIMEOUT + 1.5 * _BACKOFF_SUM + _ENTRY_SLEEP
# The first cycle's first attempt is refused at once, before the cut.
_MIN_FIRST_REJECTION = (_ATTEMPTS - 1) * _ATTEMPT_TIMEOUT + 0.5 * _BACKOFF_SUM
_MAX_FIRST_REJECTION = _CUT_AT + (_ATTEMPTS - 1) * _ATTEMPT_TIMEOUT + 1.5 * _BACKOFF_SUM
# Two full cycles refused, never a third.
_HEAL_AT = 210.0
assert _MAX_FIRST_REJECTION + _MAX_REJECTION_GAP < _HEAL_AT < _MIN_FIRST_REJECTION + 2 * _MIN_REJECTION_GAP
# Acceptance after heal: one straddling attempt timeout + one max back-off
# + the entry sleep + round-trip slop.
_MAX_ACCEPTANCE_AFTER_HEAL = _ATTEMPT_TIMEOUT + _MAX_BACKOFF + _ENTRY_SLEEP + 1.0
_CEILING = _HEAL_AT + _MAX_ACCEPTANCE_AFTER_HEAL + _WORKFLOW_DURATION_SECONDS + 50.0


def _run_submission_blackout() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=_SEED
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker",
        dag_worker_entry,
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
        60.0,
        150.0,
    )
    coordinator.schedule_partition(
        "manager", "client", _CUT_AT, heal_time=_HEAL_AT
    )
    return coordinator.run()


def test_submission_survives_blackout_with_bounded_timeout_paced_retries():
    results = _run_submission_blackout()
    client_log = results["client"]

    rejections = [
        entry for entry in client_log if entry[0] == "submit-rejected"
    ]
    # The blackout spans two full internal retry cycles: each surfaces
    # exactly one loud entry-level rejection (retries-exhausted
    # RuntimeError), never silence and never an abandoned loop.
    assert len(rejections) == 2, client_log
    assert all(
        rejection[1] == "RuntimeError" for rejection in rejections
    ), client_log

    first_rejection_time = rejections[0][2]
    second_rejection_time = rejections[1][2]
    # No hot spin: rejections are TIMEOUT-paced (a cycle cannot finish
    # faster than its five-plus 10s attempt timeouts), and no cycle
    # stretches past the max-backoff bracket.
    rejection_gap = second_rejection_time - first_rejection_time
    assert _MIN_REJECTION_GAP <= rejection_gap <= _MAX_REJECTION_GAP, (
        f"inter-rejection gap {rejection_gap}s is outside the traced "
        f"internal-cycle bracket: {client_log}"
    )
    # The first cycle began with the pre-cut formation rejection, so it
    # must still be timeout-paced overall — a sub-30s first rejection
    # would mean attempts are failing instantly (hot spin).
    assert first_rejection_time >= 30.0, client_log

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert len(submitted) == 1, client_log
    # Nothing can be accepted while the submission surface is dark;
    # acceptance lands within one straddling-attempt timeout + one max
    # backoff + the entry sleep of the heal.
    assert _HEAL_AT < submitted[0][1] <= _HEAL_AT + _MAX_ACCEPTANCE_AFTER_HEAL, (
        client_log
    )

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", client_log
    # Completion follows acceptance by execution length + push latency.
    assert (
        submitted[0][1]
        < finished[0][2]
        <= submitted[0][1] + _WORKFLOW_DURATION_SECONDS + 2.0
    ), client_log

    # The client-observed history linearizes and the final stats are the
    # ACTION chain's deterministic total: one completed action per step.
    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    final_stats = [entry for entry in client_log if entry[0] == "final-stats"]
    assert final_stats == [
        ("final-stats", SUSTAINED_ACTION_STEP_COUNT, 0, finished[0][2])
    ], client_log

    # G3/G4: exactly-once execution evidence and audit absence.
    trace_oracle = ClusterTraceOracle(
        worker_process_ids_by_datacenter={"sim-dc": ("worker",)},
        retry_budget=0,
    )
    assert trace_oracle.check_workflow_execution(results) == [], results
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_submission_blackout_is_replay_deterministic():
    assert _run_submission_blackout() == _run_submission_blackout()
