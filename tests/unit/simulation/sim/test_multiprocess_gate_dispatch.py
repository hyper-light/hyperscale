"""
The gate tier under multi-process SIM: client -> gate -> manager ->
worker -> executor pool, end to end through a cold-started L3 topology.

Six real OS processes. The client submits through a real ``GateServer``
whose datacenter is cold: health classifies ``initializing`` (no manager
heartbeat has ever arrived), then ``busy`` (managers alive, zero
workers), then ``healthy``. While the job's workflow holds every core the
gate may see the datacenter at full utilization, which the AD-16
classifier rates ``degraded`` (load-aware, not a fault): a heartbeat
taken during execution can reach the gate's sampler up to one SWIM probe
interval plus one sample period after the workflow ends. The gate's
dispatch rides out the
manager's leader election with transient-rejection retries ("Not DC
leader" / "no quorum") instead of insta-failing the accepted job. The
manager accepts once elected, the worker's executor children run the
workflow, and completion flows worker -> manager -> gate-registered
callback -> client. Byte-identical replay.
"""

from hyperscale.distributed.env import Env
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    gate_dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    gate_entry,
    manager_entry,
    worker_entry,
)

_CEILING = 90.0
# The harness samples the gate's datacenter health every 0.5s
# (worker_manager_demo.gate_entry); a manager heartbeat reaches the gate
# on every SWIM probe.
_HEALTH_SAMPLE_INTERVAL_SECONDS = 0.5
_HEARTBEAT_VIEW_LAG_SECONDS = Env().SWIM_UDP_POLL_INTERVAL + _HEALTH_SAMPLE_INTERVAL_SECONDS


def _run_gate_dispatch() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=31
    )
    coordinator.add_process(
        "gate",
        gate_entry,
        "sim-gate",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        ("sim-mgr", 9001),
    )
    coordinator.add_process(
        "manager",
        manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        ("sim-gate", 9000),
        ("sim-gate", 9001),
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
        "client", gate_dispatch_client_entry, "sim-cli", 9500, ("sim-gate", 9000)
    )
    return coordinator.run()


def test_job_completes_through_gate_from_cold_start():
    results = _run_gate_dispatch()

    assert "executor-sim-wkr-9009" in results
    assert "executor-sim-wkr-9011" in results

    gate_log = results["gate"]
    client_log = results["client"]

    # The gate's view of its datacenter walks the warmup ladder — never
    # "unhealthy": the cold DC is initializing, a worker-less-but-alive
    # manager tier is busy, and capacity makes it healthy.
    health_changes = [(entry[1], entry[2]) for entry in gate_log if entry[0] == "dc-health"]
    health_progression = [health for health, _ in health_changes]
    assert health_progression[:3] == ["initializing", "busy", "healthy"], gate_log
    assert "unhealthy" not in health_progression, gate_log
    assert health_progression[-1] == "healthy", gate_log

    # Any later departure from healthy is full utilization by the job --
    # seen no earlier than its execution and no later than the gate's view
    # of the last heartbeat taken during it.
    active_changes = [
        (entry[1], entry[2]) for entry in results["worker"] if entry[0] == "workflows-active"
    ]
    execution_started = next(time for count, time in active_changes if count > 0)
    execution_ended = next(
        time for count, time in active_changes if count == 0 and time > execution_started
    )
    for health, time in health_changes[3:]:
        if health == "healthy":
            continue
        assert health == "degraded", gate_log
        assert execution_started <= time <= execution_ended + _HEARTBEAT_VIEW_LAG_SECONDS, (
            time,
            execution_started,
            execution_ended,
            gate_log,
        )

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(submitted) == 1, client_log
    assert len(finished) == 1, client_log

    (_tag, job_status, finished_time) = finished[0]
    assert job_status == "completed", client_log
    # Acceptance precedes the manager's election; the gate's dispatch
    # retries carry the job across it and completion lands well before
    # the ceiling.
    assert submitted[0][1] < finished_time < _CEILING


def test_gate_dispatch_is_replay_deterministic():
    assert _run_gate_dispatch() == _run_gate_dispatch()
