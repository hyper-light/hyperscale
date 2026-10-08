"""
A full gate CLUSTER under multi-process SIM: three peered ``GateServer``
processes form a tier over SWIM (peer discovery + gate leader election —
the exact leadership stack the dedup-class-separation and monotonic-
lease fixes hardened), front one datacenter, and carry a job from a
client through a NON-SEED gate to completion. Byte-identical replay.

Six real OS processes plus two executor children: gates a/b/c (each
peered with the other two), one manager registered upstream with ALL
three gates, one worker with a 2-core executor pool, and the client
submitting through gate-b — exercising cross-gate job handling rather
than always the first-listed gate.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.gate_cluster_demo import (
    gate_tier_entry,
    multi_gate_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    gate_dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)

_CEILING = 120.0

_GATE_HOSTS = ("sim-gate-a", "sim-gate-b", "sim-gate-c")


def _run_gate_cluster_dispatch() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=43
    )

    datacenter_managers = {"sim-dc": [("sim-mgr", 9000)]}
    datacenter_manager_udp = {"sim-dc": [("sim-mgr", 9001)]}

    for gate_host in _GATE_HOSTS:
        peer_hosts = [host for host in _GATE_HOSTS if host != gate_host]
        coordinator.add_process(
            gate_host,
            gate_tier_entry,
            gate_host,
            9000,
            9001,
            datacenter_managers,
            datacenter_manager_udp,
            [(peer_host, 9000) for peer_host in peer_hosts],
            [(peer_host, 9001) for peer_host in peer_hosts],
        )

    coordinator.add_process(
        "manager",
        multi_gate_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        [(gate_host, 9000) for gate_host in _GATE_HOSTS],
        [(gate_host, 9001) for gate_host in _GATE_HOSTS],
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
    # Submit through gate-b, not the first-listed gate, so the job path
    # exercises whichever-gate-receives ownership, not a fixed seed gate.
    coordinator.add_process(
        "client",
        gate_dispatch_client_entry,
        "sim-cli",
        9500,
        ("sim-gate-b", 9000),
    )
    return coordinator.run()


def test_gate_cluster_forms_and_completes_job():
    results = _run_gate_cluster_dispatch()

    # The worker's executor pool came up under the coordinator.
    executor_ids = [key for key in results if key.startswith("executor-sim-wkr-")]
    assert len(executor_ids) == 2, sorted(results)

    # Cluster formation: every gate discovered BOTH peers and held them
    # (final observed active-peer count is 2 on all three gates).
    for gate_host in _GATE_HOSTS:
        peer_counts = [
            entry[1] for entry in results[gate_host] if entry[0] == "gate-peers"
        ]
        assert peer_counts, results[gate_host]
        assert peer_counts[-1] == 2, (
            f"{gate_host} ended with {peer_counts[-1]} active peers "
            f"(progression {peer_counts})"
        )

    # Every gate warmed the shared datacenter to healthy.
    for gate_host in _GATE_HOSTS:
        final_health = {
            entry[1]: entry[2]
            for entry in results[gate_host]
            if entry[0] == "dc-health"
        }
        assert final_health == {"sim-dc": "healthy"}, results[gate_host]

    # The job submitted through gate-b completed before the ceiling.
    client_log = results["client"]
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    (_tag, job_status, finished_time) = finished[0]
    assert job_status == "completed", client_log
    assert finished_time < _CEILING


def test_gate_cluster_dispatch_is_replay_deterministic():
    # Peer discovery order, gate leader election, DC health polling, and
    # the job's whole path must land on identical virtual timestamps
    # across two independent same-seed runs.
    assert _run_gate_cluster_dispatch() == _run_gate_cluster_dispatch()
