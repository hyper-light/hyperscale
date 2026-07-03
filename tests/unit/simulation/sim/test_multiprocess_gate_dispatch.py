"""
The gate tier under multi-process SIM: client -> gate -> manager ->
worker -> executor pool, end to end through a cold-started L3 topology.

Six real OS processes. The client submits through a real ``GateServer``
whose datacenter is cold: health classifies ``initializing`` (no manager
heartbeat has ever arrived), then ``busy`` (managers alive, zero
workers), then ``healthy`` — and the gate's dispatch rides out the
manager's leader election with transient-rejection retries ("Not DC
leader" / "no quorum") instead of insta-failing the accepted job. The
manager accepts once elected, the worker's executor children run the
workflow, and completion flows worker -> manager -> gate-registered
callback -> client. Byte-identical replay.
"""

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
    health_progression = [entry[1] for entry in gate_log if entry[0] == "dc-health"]
    assert health_progression == ["initializing", "busy", "healthy"], gate_log

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
