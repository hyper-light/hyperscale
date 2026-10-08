"""
An IDLE pool executor dies: the pool leader stops handing it out at the
reap, the next dispatch runs on the survivor alone, and the pool refills
the slot through its own spawn path once the replacement's ready
handshake lands.

The regression for the chaos-VOPR seed-3 defect: executor 9011 was
killed between jobs, the worker's pool-health loop dropped it from the
worker's own view, but the leader's ``Provisioner`` kept handing out its
node and ``LocalServerPool`` never refilled the slot — the probe job's
ping was dispatched onto the dead executor and failed at the AD-34
timeout.

Topology: one manager, one worker (2 executors, ports 9009/9011), one
dispatch client. Everything is event-driven from the worker's own
``pool-ready`` row (``WorkerServer.start()`` returned — every executor
acknowledged):

* executor 9011 is SIGKILLed one sampler step later, while idle;
* its replacement (``executor-sim-wkr-9011-respawn-1``) is partitioned
  from the worker for ``_REPLACEMENT_PARTITION_SECONDS``, so the
  client's job is dispatched while the slot is empty — the dispatch the
  defect sent to the dead executor;
* after the partition heals, the replacement's start acknowledgement
  reaches the leader and only then does the slot return.

The worker samples its pool leader's provisioner every
``_SAMPLE_INTERVAL`` of virtual time — far finer than the worker's
0.25s pool-health poll — so the removal instant shows whether it came
from the reap event or from a later poll.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.executor_respawn_demo import (
    slot_watching_worker_entry,
)
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
)

_CEILING = 60.0
_LATENCY = 0.01
_SAMPLE_INTERVAL = 0.01
# The pool-health poll the event-driven removal must beat
# (``WorkerServer._worker_pool_health_iteration``).
_POOL_HEALTH_POLL_INTERVAL = 0.25
# Long enough to hold the replacement out across the client's whole job
# (dispatched ~1.2s after the kill, drained ~0.6s later), short of the
# executor's 120s connect budget so the replacement still joins.
_REPLACEMENT_PARTITION_SECONDS = 15.0
_KILL_DELAY = 0.05

_SURVIVOR_PORT = 9009
_VICTIM_PORT = 9011
_VICTIM_PROCESS_ID = f"executor-sim-wkr-{_VICTIM_PORT}"
_REPLACEMENT_PROCESS_ID = f"executor-sim-wkr-{_VICTIM_PORT}-respawn-1"
_FULL_POOL = (_SURVIVOR_PORT, _VICTIM_PORT)


def _run_with_idle_executor_kill() -> tuple[dict, float]:
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_CEILING, seed=23
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker",
        slot_watching_worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
        _SAMPLE_INTERVAL,
    )
    coordinator.add_process(
        "client", dispatch_client_entry, "sim-cli", 9500, ("sim-mgr", 9000)
    )

    kill_times: list[float] = []

    def kill_idle_executor(pool_ready_row: tuple) -> None:
        kill_at = pool_ready_row[-1] + _KILL_DELAY
        kill_times.append(kill_at)
        coordinator.schedule_kill(_VICTIM_PROCESS_ID, at_time=kill_at)
        coordinator.schedule_partition(
            "worker",
            _REPLACEMENT_PROCESS_ID,
            at_time=kill_at,
            heal_time=kill_at + _REPLACEMENT_PARTITION_SECONDS,
        )

    coordinator.schedule_on_event(
        "worker", lambda row: row[0] == "pool-ready", kill_idle_executor
    )
    results = coordinator.run()
    return results, kill_times[0]


def _slot_rows(worker_log: list) -> list[tuple]:
    return [row for row in worker_log if row[0] == "executor-slots"]


def _readmission_row(slot_rows: list[tuple], kill_time: float) -> tuple:
    return next(
        row for row in slot_rows if row[-1] > kill_time and _VICTIM_PORT in row[1]
    )


def test_idle_executor_kill_is_withdrawn_at_reap_and_refilled_after_handshake():
    results, kill_time = _run_with_idle_executor_kill()
    heal_time = kill_time + _REPLACEMENT_PARTITION_SECONDS

    # The victim died for good; its slot was refilled by a NEW process
    # through the pool's spawn path.
    assert _VICTIM_PROCESS_ID not in results
    assert _REPLACEMENT_PROCESS_ID in results
    assert f"executor-sim-wkr-{_SURVIVOR_PORT}" in results

    worker_log = results["worker"]
    slot_rows = _slot_rows(worker_log)

    # Before the kill the leader hands out the full pool.
    pre_kill_rows = [row for row in slot_rows if row[-1] < kill_time]
    assert pre_kill_rows[-1][1:3] == (_FULL_POOL, _FULL_POOL), worker_log

    # Withdrawn at the reap: the first sample after the kill already
    # lacks the victim — within one sampler step, far inside the
    # pool-health poll a poll-driven removal would wait for.
    first_post_kill_row = next(row for row in slot_rows if row[-1] >= kill_time)
    assert first_post_kill_row[1:3] == ((_SURVIVOR_PORT,), (_SURVIVOR_PORT,)), worker_log
    assert first_post_kill_row[-1] - kill_time <= _SAMPLE_INTERVAL < _POOL_HEALTH_POLL_INTERVAL

    # Unavailable until the replacement's ready handshake: no sample
    # between the kill and the readmission registers or offers the
    # victim's slot, and the readmission waits for the partition that
    # blocks the handshake to heal.
    readmission_row = _readmission_row(slot_rows, kill_time)
    withdrawn_rows = [
        row for row in slot_rows if kill_time <= row[-1] < readmission_row[-1]
    ]
    assert all(
        _VICTIM_PORT not in row[1] and _VICTIM_PORT not in row[2]
        for row in withdrawn_rows
    ), worker_log
    assert readmission_row[-1] >= heal_time, worker_log

    # The next dispatch arrived while the slot was empty and ran on the
    # survivor alone.
    dispatch_time = next(
        row[-1]
        for row in worker_log
        if row[0] == "workflows-active" and row[1] == 1
    )
    assert kill_time < dispatch_time < heal_time, worker_log
    held_at_dispatch = [
        tuple(sorted(set(row[1]) - set(row[2])))
        for row in withdrawn_rows
        if row[-1] == dispatch_time
    ]
    assert held_at_dispatch == [(_SURVIVOR_PORT,)], worker_log

    # ...and completed, long before the partition healed (the defect
    # failed it at the AD-34 timeout instead).
    client_log = results["client"]
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    (_tag, job_status, finished_time) = finished[0]
    assert job_status == "completed", client_log
    assert finished_time < heal_time, client_log

    # The pool is back to size once the replacement acknowledged.
    assert slot_rows[-1][1:3] == (_FULL_POOL, _FULL_POOL), worker_log


def test_idle_executor_kill_is_replay_deterministic():
    assert _run_with_idle_executor_kill() == _run_with_idle_executor_kill()
