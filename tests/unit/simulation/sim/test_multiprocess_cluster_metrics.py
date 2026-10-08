"""
D-5 / D-68 under multi-process SIM: every role answers ``cluster
--metrics`` in the one shared schema, with live values, and the manager's
dispatch round trips are kept per worker.

Real processes: a gate fronting one datacenter, its manager, two workers
and a client. Every frame the manager sends worker-b is held an extra
``INJECTED_DELAY_SECONDS``; worker-a's are not. The client runs enough jobs
at once to occupy both workers, waits for them, then asks each node for its
metrics.

* each node names its role; the gate and manager are cluster members, a
  worker is not (no formation);
* each worker reports the cores it was started with, all free and nothing
  running once its jobs are done;
* the manager's per-worker digests tell the workers apart by the injected
  delay alone: every worker-b round trip carries the delay and both
  coordinator hops, no worker-a round trip carries the delay;
* the manager's datacenter digest is the sum of its per-worker ones, each
  sample an answered dispatch it counted by outcome; the gate's view of the
  datacenter (from the manager's heartbeats) is never ahead of it.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.telemetry_demo import telemetry_client_entry
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    gate_entry,
    manager_entry,
    worker_entry,
)

CEILING_SECONDS = 120.0
COORDINATOR_LATENCY_SECONDS = 0.01
INJECTED_DELAY_SECONDS = 0.25
WORKER_CORES = 2
JOB_COUNT = 4
DATACENTER = "sim-dc"
WORKERS = {"worker-a": "sim-wkr-a", "worker-b": "sim-wkr-b"}
ANSWERED_OUTCOMES = ("accepted", "not_ready", "rejected")


def run_cluster() -> dict:
    coordinator = SimulationCoordinator(latency=COORDINATOR_LATENCY_SECONDS, max_virtual_time=CEILING_SECONDS, seed=41)
    coordinator.add_process(
        "gate", gate_entry, "sim-gate", 9000, 9001, DATACENTER, ("sim-mgr", 9000), ("sim-mgr", 9001)
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, DATACENTER, ("sim-gate", 9000), ("sim-gate", 9001)
    )
    for process_name, host in WORKERS.items():
        coordinator.add_process(
            process_name, worker_entry, host, 9000, 9001, DATACENTER, ("sim-mgr", 9000), WORKER_CORES
        )
    coordinator.add_process(
        "client",
        telemetry_client_entry,
        "sim-cli",
        9500,
        ("sim-gate", 9000),
        {
            "gate": ("sim-gate", 9000),
            "manager": ("sim-mgr", 9000),
            **{process_name: (host, 9000) for process_name, host in WORKERS.items()},
        },
        JOB_COUNT,
    )
    coordinator.schedule_delay("manager", "worker-b", INJECTED_DELAY_SECONDS)
    return coordinator.run()


def metrics_by_node(client_log: list) -> dict[str, dict]:
    return {entry[1]: entry[2] for entry in client_log if entry[0] == "metrics"}


def test_every_role_answers_the_shared_schema_and_digests_key_per_worker() -> None:
    client_log = run_cluster()["client"]

    assert [entry for entry in client_log if entry[0] == "metrics-failed"] == [], client_log
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert [entry[1] for entry in finished] == ["completed"] * JOB_COUNT, client_log
    metrics = metrics_by_node(client_log)
    assert set(metrics) == {"gate", "manager", *WORKERS}, client_log

    assert (metrics["gate"]["role"], metrics["manager"]["role"]) == ("gate", "manager")
    assert metrics["gate"]["formation"] and metrics["manager"]["formation"]
    assert set(metrics["gate"]["workload"]) == {"active_jobs"}
    assert 0 <= metrics["gate"]["workload"]["active_jobs"] <= JOB_COUNT, metrics["gate"]
    for worker_name in WORKERS:
        worker = metrics[worker_name]
        assert (worker["role"], worker["formation"], worker["node_state"]) == ("worker", "", "healthy"), worker
        assert worker["capacity"] == {"total_cores": WORKER_CORES, "available_cores": WORKER_CORES}, worker
        assert worker["workload"] == {"active_workflows": 0, "pending_workflows": 0}, worker
        assert set(worker["resources"]) == {"cpu_percent", "memory_percent"}, worker

    manager = metrics["manager"]
    assert manager["capacity"]["workers"] == len(WORKERS), manager
    assert manager["capacity"]["total_cores"] == len(WORKERS) * WORKER_CORES, manager
    per_worker = manager["dispatch_latency"]
    assert set(per_worker) == set(WORKERS), manager
    fewest_delayed_ms = (INJECTED_DELAY_SECONDS + 2 * COORDINATOR_LATENCY_SECONDS) * 1000.0
    assert per_worker["worker-b"]["p50_ms"] >= fewest_delayed_ms, per_worker
    assert per_worker["worker-a"]["p99_ms"] < INJECTED_DELAY_SECONDS * 1000.0, per_worker

    manager_samples = manager["slo"][DATACENTER]["sample_count"]
    assert manager_samples == sum(latency["sample_count"] for latency in per_worker.values()), manager
    answered = sum(manager["dispatch_outcomes"].get(outcome, 0) for outcome in ANSWERED_OUTCOMES)
    assert manager_samples == answered >= JOB_COUNT, manager
    gate_samples = metrics["gate"]["slo"][DATACENTER]["sample_count"]
    assert 0 < gate_samples <= manager_samples, (metrics["gate"], manager)


def test_the_cluster_metrics_run_is_replay_deterministic() -> None:
    assert run_cluster() == run_cluster()
