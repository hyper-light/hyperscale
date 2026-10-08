"""
The manager's record of a two-core worker, sampled every window under
multi-process SIM, idle and through a job.

A real ``ManagerServer``, a real two-core ``WorkerServer`` (its executor
pool as coordinator children) and a ``HyperscaleClient`` submitting one
workflow: the manager samples its worker record every quarter virtual
second. The total must be the worker's own two cores in every sample --
before, during and after the job -- and the free count (available minus
reserved, as the dashboard reads it) never above it.

Pins the fix to the heartbeat path that derived the total from the free
count plus the active workflow count: mid-job it read ``total 1, free 0``
(one workflow holding both cores), and a heartbeat whose free count was
older than the one applied read ``total 1, free 2``.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_core_record_demo import (
    sampling_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)

CEILING_SECONDS = 20.0
SAMPLING_WINDOW_SECONDS = 0.25
WORKER_CORES = 2


def _run_sampled_job() -> dict:
    coordinator = SimulationCoordinator(latency=0.01, max_virtual_time=CEILING_SECONDS, seed=23)
    coordinator.add_process(
        "manager", sampling_manager_entry, "sim-mgr", 9000, 9001, "sim-dc", SAMPLING_WINDOW_SECONDS
    )
    coordinator.add_process(
        "worker", worker_entry, "sim-wkr", 9000, 9001, "sim-dc", ("sim-mgr", 9000), WORKER_CORES
    )
    coordinator.add_process("client", dispatch_client_entry, "sim-cli", 9500, ("sim-mgr", 9000))
    return coordinator.run()


def _core_samples(manager_log: list) -> list[tuple[int, int, int, float]]:
    return [entry[1:] for entry in manager_log if entry[0] == "worker-cores"]


def _job_window(client_log: list) -> tuple[float, float]:
    (submitted_time,) = [entry[1] for entry in client_log if entry[0] == "job-submitted"]
    (finished_time,) = [entry[2] for entry in client_log if entry[0] == "job-finished"]
    return submitted_time, finished_time


def test_manager_worker_record_holds_the_worker_total_idle_and_through_a_job() -> None:
    results = _run_sampled_job()
    samples = _core_samples(results["manager"])
    submitted_time, finished_time = _job_window(results["client"])

    assert samples, results["manager"]
    assert [sample for sample in samples if sample[0] != WORKER_CORES] == []
    assert [sample for sample in samples if not 0 <= sample[1] <= sample[0]] == []

    # The samples span the idle worker before the job, the busy worker
    # (both cores held or reserved) during it, and the idle one after.
    assert any(sample[3] < submitted_time and sample[1] == WORKER_CORES for sample in samples)
    assert any(submitted_time <= sample[3] <= finished_time and sample[1] == 0 for sample in samples)
    assert any(sample[3] > finished_time and sample[1] == WORKER_CORES for sample in samples)


def test_sampled_job_is_replay_deterministic() -> None:
    assert _run_sampled_job() == _run_sampled_job()
