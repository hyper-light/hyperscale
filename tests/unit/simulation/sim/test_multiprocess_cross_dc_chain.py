"""
A cross-datacenter dependency chain under multi-process SIM (SCENARIOS §7
"Cross-DC dependencies (L3). Workflow B in DC-east depends on A in
DC-west"), over ``cross_dc_chain_demo``: one gate fronting two
single-manager datacenters (2-core workers), and a client submitting the
chain ``SimChainA -> SimChainB`` placed in either datacenter, one of
them.

A job's workflows are placed together, so B depends on an A from another
datacenter exactly when the datacenter running the chain is lost between
them. The scenario loses it there: once A's result has reached the
client -- the event, whichever datacenter the gate chose -- that whole
datacenter (manager, worker, executors) is killed while B runs.

* A's result is the lost datacenter's: delivered before the loss,
  counted once, never replaced.
* B's result is the other datacenter's, which re-ran the lost share
  (AD-36 Part 13: B, and A again only for the context B reads).
* The replacement starts only after the gate classifies the loss (within
  the heartbeat-staleness bound), at its next failover check, and the job
  completes -- one terminal, ``completed`` -- within A's and B's run
  times of that start.

The run has a replay twin.
"""

from hyperscale.distributed.env import Env
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.cross_dc_chain_demo import chain_client_entry
from tests.simulation.harness.sim.multiprocess.gate_cluster_demo import gate_tier_entry
from tests.simulation.harness.sim.multiprocess.multi_dc_fault_demo import faulted_multi_gate_manager_entry
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import worker_entry
from tests.simulation.oracle import JobStatusOracle

_SEED = 61
_LINK_LATENCY_SECONDS = 0.01
_ENV = Env()
_DATACENTERS = {"dc-east": ("sim-mgr-east", "sim-wkr-east"), "dc-west": ("sim-mgr-west", "sim-wkr-west")}
_UPSTREAM_SECONDS = 2.0
# Long enough that B is still running when its datacenter dies.
_DOWNSTREAM_SECONDS = 20.0
_JOB_TIMEOUT_SECONDS = 240.0
# The gate's datacenter-death classification (as test_multiprocess_dc_loss):
# never before the heartbeat pause it tolerates, always inside the 30s
# staleness window it replaced.
_DEATH_CLASSIFY_MIN = _ENV.PHI_ACCRUAL_ACCEPTABLE_HEARTBEAT_PAUSE_SECONDS
_DEATH_CLASSIFY_MAX = 30.0
# The leader gate checks its jobs for lost datacenters every manager
# heartbeat interval (AD-36 Part 13).
_FAILOVER_CHECK_SECONDS = _ENV.MANAGER_HEARTBEAT_INTERVAL
# Dispatch to the replacement and the results' way back: room for one
# reordered hop each way over the gated path (twice the gateless ten).
_DELIVERY_SECONDS = 20 * _LINK_LATENCY_SECONDS
# worker_entry samples its active workflows every quarter second.
_WORKER_WATCH_SECONDS = 0.25
_CEILING = 60.0 + _DEATH_CLASSIFY_MAX + _FAILOVER_CHECK_SECONDS + _UPSTREAM_SECONDS + _DOWNSTREAM_SECONDS


def _victims(datacenter: str) -> tuple[str, ...]:
    """Every process of a datacenter: manager, worker and its two executors."""
    _manager_host, worker_host = _DATACENTERS[datacenter]
    return (
        f"manager-{datacenter}",
        f"worker-{datacenter}",
        f"executor-{worker_host}-9009",
        f"executor-{worker_host}-9011",
    )


def _add_topology(coordinator: SimulationCoordinator) -> None:
    coordinator.add_process(
        "sim-gate-a",
        gate_tier_entry,
        "sim-gate-a",
        9000,
        9001,
        {datacenter: [(hosts[0], 9000)] for datacenter, hosts in _DATACENTERS.items()},
        {datacenter: [(hosts[0], 9001)] for datacenter, hosts in _DATACENTERS.items()},
    )
    for datacenter, (manager_host, worker_host) in _DATACENTERS.items():
        coordinator.add_process(
            f"manager-{datacenter}",
            faulted_multi_gate_manager_entry,
            manager_host,
            9000,
            9001,
            datacenter,
            [("sim-gate-a", 9000)],
            [("sim-gate-a", 9001)],
            (),
        )
        coordinator.add_process(
            f"worker-{datacenter}", worker_entry, worker_host, 9000, 9001, datacenter, (manager_host, 9000), 2
        )


def _is_upstream_result(row: tuple) -> bool:
    return row[:2] == ("workflow-result", "SimChainA")


def _run_chain() -> tuple[dict, dict[str, object]]:
    """Run the chain; lose A's datacenter one latency after A's result
    reached the client. Returns the results and the loss (``datacenter``,
    ``at``)."""
    coordinator = SimulationCoordinator(latency=_LINK_LATENCY_SECONDS, max_virtual_time=_CEILING, seed=_SEED)
    _add_topology(coordinator)
    coordinator.add_process(
        "client",
        chain_client_entry,
        "sim-cli",
        9500,
        ("sim-gate-a", 9000),
        sorted(_DATACENTERS),
        _UPSTREAM_SECONDS,
        _DOWNSTREAM_SECONDS,
        _JOB_TIMEOUT_SECONDS,
    )
    loss: dict[str, object] = {}

    def lose_the_upstream_datacenter(row: tuple) -> None:
        (datacenter,) = row[3]
        loss["datacenter"] = datacenter
        loss["at"] = row[5] + _LINK_LATENCY_SECONDS
        for victim in _victims(datacenter):
            coordinator.schedule_kill(victim, loss["at"])

    coordinator.schedule_on_event("client", _is_upstream_result, lose_the_upstream_datacenter)
    return coordinator.run(), loss


def _workflow_result(client_log: list, workflow_name: str) -> tuple:
    rows = [row for row in client_log if row[:2] == ("workflow-result", workflow_name)]
    assert len(rows) == 1, (workflow_name, client_log)
    return rows[0]


def test_a_chain_split_across_datacenters_by_a_loss_completes_once():
    results, loss = _run_chain()
    client_log = results["client"]
    lost_datacenter = loss["datacenter"]
    (replacement,) = [datacenter for datacenter in _DATACENTERS if datacenter != lost_datacenter]

    # A: the lost datacenter's own result, counted once.
    _tag, _name, upstream_status, upstream_from, upstream_reran, _at = _workflow_result(client_log, "SimChainA")
    assert (upstream_status, upstream_from, upstream_reran) == ("completed", (lost_datacenter,), ()), client_log

    # B: the replacement's, which re-ran the lost share.
    _tag, _name, downstream_status, downstream_from, downstream_reran, _at = _workflow_result(client_log, "SimChainB")
    assert (downstream_status, downstream_from, downstream_reran) == ("completed", (replacement,), (replacement,))

    # The loss is classified inside its bound.
    unhealthy_at = [
        row[3] for row in results["sim-gate-a"] if row[:3] == ("dc-health", lost_datacenter, "unhealthy")
    ][-1]
    assert _DEATH_CLASSIFY_MIN <= unhealthy_at - loss["at"] <= _DEATH_CLASSIFY_MAX, (loss, unhealthy_at)

    # The replacement ran nothing before the loss was classified, and took
    # the share at the next failover check.
    replacement_starts = [
        row[2] for row in results[f"worker-{replacement}"] if row[0] == "workflows-active" and row[1] > 0
    ]
    assert replacement_starts, results[f"worker-{replacement}"]
    assert unhealthy_at <= replacement_starts[0], (unhealthy_at, replacement_starts)
    assert replacement_starts[0] <= unhealthy_at + _FAILOVER_CHECK_SECONDS + _WORKER_WATCH_SECONDS

    # One loud terminal: completed, A's context re-run then B after the start.
    finished = [row for row in client_log if row[0] == "job-finished"]
    assert [row[1] for row in finished] == ["completed"], client_log
    latest = replacement_starts[0] + _UPSTREAM_SECONDS + _DOWNSTREAM_SECONDS + _DELIVERY_SECONDS
    assert finished[0][2] <= latest, (finished, latest)
    assert JobStatusOracle().check_client_log(client_log) == [], client_log


def test_cross_dc_chain_is_replay_deterministic():
    assert _run_chain() == _run_chain()
