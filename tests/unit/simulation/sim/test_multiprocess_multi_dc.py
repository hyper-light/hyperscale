"""
Multi-DATACENTER dispatch under multi-process SIM: client -> gate ->
one of TWO datacenters, end to end, replaying byte-identically.

Seven real OS processes plus four executor children: one ``GateServer``
fronting dc-east and dc-west, each datacenter a real ``ManagerServer``
plus a ``WorkerServer`` (2 executor-pool children each), and a client
submitting through the gate. The gate warms both DCs to ``healthy``,
picks a datacenter for the job, and completion flows back through the
gate to the client.

The replay-determinism assertion here is the one this whole layer
existed to make pass: WHICH datacenter wins the dispatch is downstream
of node identity, hash seeding, and SWIM probe order — with any of them
unseeded, two identical-seed runs disagreed on the winning DC (~50/50)
and every virtual timestamp after the divergence shifted. Topology-
derived NodeIds, the coordinator's PYTHONHASHSEED pin, and the seeded
probe scheduler are what make this equality hold.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.gate_cluster_demo import (
    gate_tier_entry,
    multi_gate_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    gate_dispatch_client_entry,
    pinned_gate_dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)

_CEILING = 120.0


def _run_multi_dc_dispatch() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=41
    )
    coordinator.add_process(
        "gate",
        gate_tier_entry,
        "sim-gate",
        9000,
        9001,
        {
            "dc-east": [("sim-mgr-east", 9000)],
            "dc-west": [("sim-mgr-west", 9000)],
        },
        {
            "dc-east": [("sim-mgr-east", 9001)],
            "dc-west": [("sim-mgr-west", 9001)],
        },
    )
    for datacenter_id, manager_host, worker_host in (
        ("dc-east", "sim-mgr-east", "sim-wkr-east"),
        ("dc-west", "sim-mgr-west", "sim-wkr-west"),
    ):
        coordinator.add_process(
            f"manager-{datacenter_id}",
            multi_gate_manager_entry,
            manager_host,
            9000,
            9001,
            datacenter_id,
            [("sim-gate", 9000)],
            [("sim-gate", 9001)],
        )
        coordinator.add_process(
            f"worker-{datacenter_id}",
            worker_entry,
            worker_host,
            9000,
            9001,
            datacenter_id,
            (manager_host, 9000),
            2,
        )
    coordinator.add_process(
        "client", gate_dispatch_client_entry, "sim-cli", 9500, ("sim-gate", 9000)
    )
    return coordinator.run()


def test_job_completes_across_multi_dc_topology():
    results = _run_multi_dc_dispatch()

    # Both datacenters' executor pools were admitted as coordinator
    # children — two per worker.
    east_executors = [key for key in results if key.startswith("executor-sim-wkr-east-")]
    west_executors = [key for key in results if key.startswith("executor-sim-wkr-west-")]
    assert len(east_executors) == 2, sorted(results)
    assert len(west_executors) == 2, sorted(results)

    # The gate warmed BOTH datacenters to healthy — and they STAY
    # healthy through the 120s horizon. This pins the stale-deadline
    # regression: the manager's AD-26 deadline-enforcement loop used to
    # evict the worker that ran the job ~30s after it drained (the
    # granted deadline was never cleared on completion), the dead-node
    # reaper then deregistered it, and the job's DC flipped to "busy"
    # on an idle cluster.
    gate_log = results["gate"]
    final_health = {
        entry[1]: entry[2] for entry in gate_log if entry[0] == "dc-health"
    }
    assert final_health == {"dc-east": "healthy", "dc-west": "healthy"}, gate_log

    # AD-35 -> AD-36: the gate learns each datacenter's Vivaldi
    # coordinate from its manager's SWIM traffic and keeps it, so the
    # router's latency estimate has the coordinate input. Looked up under
    # the manager's node id (SWIM keys coordinates by UDP address), it
    # never found one.
    final_coordinate_known = {
        entry[1]: entry[2] for entry in gate_log if entry[0] == "dc-coordinate"
    }
    assert final_coordinate_known == {"dc-east": True, "dc-west": True}, gate_log

    # Same invariant from the managers' side: no fault was injected, so
    # neither manager may lose its worker at any point in the run.
    for manager_name in ("manager-dc-east", "manager-dc-west"):
        lost = [entry for entry in results[manager_name] if entry[0] == "worker-lost"]
        assert not lost, (manager_name, results[manager_name])

    # The job completed through the gate before the ceiling.
    client_log = results["client"]
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    (_tag, job_status, finished_time) = finished[0]
    assert job_status == "completed", client_log
    assert finished_time < _CEILING

    # Exactly ONE datacenter ran the workflow: its worker's active count
    # rose to 1; the other stayed at 0 throughout.
    def workflow_ran(worker_log: list) -> bool:
        return any(
            entry[0] == "workflows-active" and entry[1] > 0
            for entry in worker_log
        )

    ran_in_east = workflow_ran(results["worker-dc-east"])
    ran_in_west = workflow_ran(results["worker-dc-west"])
    assert ran_in_east != ran_in_west, (
        f"job must run in exactly one DC (east={ran_in_east}, west={ran_in_west})"
    )


def test_multi_dc_dispatch_is_replay_deterministic():
    # Full-dict equality pins the winning datacenter, every health
    # transition, every executor id, and every virtual timestamp across
    # two independent same-seed runs — the multi-DC determinism claim.
    assert _run_multi_dc_dispatch() == _run_multi_dc_dispatch()


def _run_pinned_multi_dc_dispatch() -> dict:
    """Same topology and seed as the free-selection run, but the client
    pins ``datacenters=["dc-east"]`` — the DC the free run does NOT
    pick (seed 41 lands in dc-west), so the placement constraint is
    provably what decides the outcome."""
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=41
    )
    coordinator.add_process(
        "gate",
        gate_tier_entry,
        "sim-gate",
        9000,
        9001,
        {
            "dc-east": [("sim-mgr-east", 9000)],
            "dc-west": [("sim-mgr-west", 9000)],
        },
        {
            "dc-east": [("sim-mgr-east", 9001)],
            "dc-west": [("sim-mgr-west", 9001)],
        },
    )
    for datacenter_id, manager_host, worker_host in (
        ("dc-east", "sim-mgr-east", "sim-wkr-east"),
        ("dc-west", "sim-mgr-west", "sim-wkr-west"),
    ):
        coordinator.add_process(
            f"manager-{datacenter_id}",
            multi_gate_manager_entry,
            manager_host,
            9000,
            9001,
            datacenter_id,
            [("sim-gate", 9000)],
            [("sim-gate", 9001)],
        )
        coordinator.add_process(
            f"worker-{datacenter_id}",
            worker_entry,
            worker_host,
            9000,
            9001,
            datacenter_id,
            (manager_host, 9000),
            2,
        )
    coordinator.add_process(
        "client",
        pinned_gate_dispatch_client_entry,
        "sim-cli",
        9500,
        ("sim-gate", 9000),
        ["dc-east"],
    )
    return coordinator.run()


def test_pinned_datacenter_constraint_is_honored():
    """``datacenters=[...]`` is a placement constraint, not a hint.

    With free selection, seed 41 runs the job in dc-west; pinning
    dc-east on the same seed must land it in dc-east — previously the
    pin was a 10% score nudge in steady state and ignored outright in
    bootstrap mode and the legacy selector, so this exact scenario ran
    in dc-west despite the pin.
    """
    results = _run_pinned_multi_dc_dispatch()

    client_log = results["client"]
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", client_log

    def workflow_ran(worker_log: list) -> bool:
        return any(
            entry[0] == "workflows-active" and entry[1] > 0
            for entry in worker_log
        )

    assert workflow_ran(results["worker-dc-east"]), results["worker-dc-east"]
    assert not workflow_ran(results["worker-dc-west"]), results["worker-dc-west"]


def test_pinned_dispatch_is_replay_deterministic():
    assert _run_pinned_multi_dc_dispatch() == _run_pinned_multi_dc_dispatch()
