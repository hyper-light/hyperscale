"""Seeded VOPR: a gate's AD-34 global timeout reaches the job's current leader
in a datacenter, however many times the job was taken over.

A manager fences the gate's decision by the job's timeout fence
(``GateCoordinatedTimeout.handle_global_timeout``), and a takeover resumes
tracking at the job's leadership fence (``resume_tracking``), which strictly
rises with each takeover. At base commit 657e460b the gate stamped every
decision with its own decision counter -- 1 for every job
(``jobs/gates/gate_job_timeout_tracker.py:563``) -- so the decision was stale
at a leader that had taken the job over: the job never timed out there. And
a manager the job was taken from still held its strategy at its old fence:
a decision the gate addressed to it, before hearing of the takeover, timed
the job out under its new leader.

Each seed takes the job over between zero and five times, each takeover at a
higher leadership fence, and loses the new leader's transfer report to the
gate at random. The real manager strategies report to the real gate tracker;
each manager answers ``job_global_timeout`` through the manager server's own
decision path. Invariants, per seed:

* INV1 -- when the gate heard of the job's current leader, that leader times
  the job out, once;
* INV2 -- a manager the job was taken from never times it out.
"""

import random
from types import SimpleNamespace

import pytest

from hyperscale.distributed.jobs import gate_coordinated_timeout as gate_coordinated_timeout_module
from hyperscale.distributed.jobs.gate_coordinated_timeout import GateCoordinatedTimeout
from hyperscale.distributed.jobs.gates import gate_job_timeout_tracker as gate_job_timeout_tracker_module
from hyperscale.distributed.jobs.gates.gate_job_timeout_tracker import GateJobTimeoutTracker
from hyperscale.distributed.models.distributed import (
    JobGlobalTimeout,
    JobLeaderTransfer,
    JobProgressReport,
)
from hyperscale.distributed.models.jobs import JobInfo, TrackingToken
from hyperscale.distributed.nodes.manager.server import ManagerServer

JOB_ID = "job-takeovers"
DATACENTER = "dc-east"
GATE_ADDR = ("10.0.0.1", 9000)
SEED_COUNT = 200
MAX_TAKEOVERS = 5
JOB_TIMEOUT_SECONDS = 300.0
STUCK_THRESHOLD_SECONDS = 1.0e9
# The first leader tracks at the strategy's initial fence; a takeover claims
# the job at a leadership fence of at least 2 (``_claim_job_leadership_takeover``).
FIRST_TAKEOVER_FENCE = 2


class SteppedClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


class SilentLogger:
    async def log(self, entry: object) -> None:
        return None


class DatacenterManager:
    """One manager of the datacenter: its job copy and strategy, whether it
    leads the job, and the jobs it timed out."""

    def __init__(self, index: int, cluster: "Cluster") -> None:
        self.index = index
        self.address = ("10.0.1.1", 8000 + index)
        self.leads_job = False
        self.timed_out_jobs: list[str] = []
        self.job = JobInfo(
            token=TrackingToken.for_job(DATACENTER, f"manager-{index}", JOB_ID),
            submission=None,
            # Running: a report of every workflow done marks the
            # datacenter completed at the gate.
            workflows_total=4,
        )
        self.server = SimpleNamespace(
            _job_manager=SimpleNamespace(get_job_by_id=lambda job_id: self.job),
            _node_id=SimpleNamespace(datacenter=DATACENTER, short=f"manager-{index}"),
            _host=self.address[0],
            _tcp_port=self.address[1],
            _udp_logger=SilentLogger(),
            env=SimpleNamespace(JOB_STUCK_THRESHOLD=STUCK_THRESHOLD_SECONDS),
            send_tcp=cluster.deliver_to_gate,
            _timeout_job=self.time_out_job,
            _leases=SimpleNamespace(is_job_leader=lambda job_id: self.leads_job),
            _manager_state=SimpleNamespace(remove_job_timeout_strategy=lambda job_id: None),
        )
        self.strategy = GateCoordinatedTimeout(self.server)

    async def time_out_job(self, job_id: str, reason: str) -> bool:
        self.timed_out_jobs.append(job_id)
        return True

    async def receive_global_timeout(self, payload: bytes) -> bytes:
        """The manager's ``job_global_timeout`` handler, past its strategy lookup."""
        await ManagerServer._apply_global_timeout_decision(
            self.server, self.strategy, JobGlobalTimeout.load(payload)
        )
        return b"ok"


class Cluster:
    """The gate and a datacenter's managers, wired through their handlers."""

    def __init__(self) -> None:
        self.managers: dict[tuple[str, int], DatacenterManager] = {}
        self.drop_reports_to_gate = False
        self.gate = SimpleNamespace(
            _udp_logger=SilentLogger(),
            _host=GATE_ADDR[0],
            _tcp_port=GATE_ADDR[1],
            _node_id=SimpleNamespace(short="gate-1"),
            send_tcp=self.deliver_to_manager,
            handle_global_timeout=self.cancel_datacenters,
            handle_exception=self.raise_error,
        )
        self.tracker = GateJobTimeoutTracker(gate=self.gate, stuck_threshold=STUCK_THRESHOLD_SECONDS)

    async def deliver_to_gate(
        self, gate_addr: tuple[str, int], handler_name: str, payload: bytes
    ) -> tuple[bytes | None, float]:
        if self.drop_reports_to_gate:
            return None, 0.0
        if handler_name == "receive_job_leader_transfer":
            await self.tracker.record_leader_transfer(JobLeaderTransfer.load(payload))
        else:
            await self.tracker.record_progress(JobProgressReport.load(payload))
        return b"ok", 0.0

    async def deliver_to_manager(
        self, manager_addr: tuple[str, int], handler_name: str, payload: bytes, timeout: float
    ) -> tuple[bytes, float]:
        return await self.managers[manager_addr].receive_global_timeout(payload), 0.0

    async def cancel_datacenters(self, *args: object) -> None:
        return None

    async def raise_error(self, error: Exception, operation: str) -> None:
        raise error


async def lead_job(cluster: Cluster, index: int, fence_token: int | None) -> DatacenterManager:
    """Make manager ``index`` the job's leader: the first tracks it from
    submission; a later one takes it over at ``fence_token``."""
    for manager in cluster.managers.values():
        manager.leads_job = False
    manager = DatacenterManager(index, cluster)
    cluster.managers[manager.address] = manager
    manager.leads_job = True
    await manager.strategy.start_tracking(JOB_ID, JOB_TIMEOUT_SECONDS, gate_addr=GATE_ADDR)
    if fence_token is not None:
        await manager.strategy.resume_tracking(JOB_ID, fence_token)
    return manager


async def run_seed(seed: int, monkeypatch: pytest.MonkeyPatch) -> tuple[Cluster, DatacenterManager, bool]:
    """Run one takeover schedule to the gate's global timeout; returns the
    cluster, the job's current leader, and whether the gate heard of it."""
    seeded_random = random.Random(seed)
    clock = SteppedClock()
    monkeypatch.setattr(gate_job_timeout_tracker_module, "_DEFAULT_CLOCK", clock)
    monkeypatch.setattr(gate_coordinated_timeout_module, "_DEFAULT_CLOCK", clock)

    cluster = Cluster()
    await cluster.tracker.start_tracking_job(JOB_ID, timeout_seconds=JOB_TIMEOUT_SECONDS, target_dcs=[DATACENTER])
    leader = await lead_job(cluster, 0, fence_token=None)
    await leader.strategy._send_progress_report(JOB_ID)

    fence_token = FIRST_TAKEOVER_FENCE - 1
    gate_heard_of_leader = True
    for takeover in range(1, seeded_random.randint(0, MAX_TAKEOVERS) + 1):
        clock.now += seeded_random.uniform(1.0, JOB_TIMEOUT_SECONDS / (MAX_TAKEOVERS + 1))
        fence_token += seeded_random.randint(1, 3)
        cluster.drop_reports_to_gate = seeded_random.random() < 0.3
        leader = await lead_job(cluster, takeover, fence_token)
        gate_heard_of_leader = not cluster.drop_reports_to_gate
        await leader.strategy._send_progress_report(JOB_ID)

    cluster.drop_reports_to_gate = False
    clock.now = JOB_TIMEOUT_SECONDS + 1.0
    await cluster.tracker._check_tracked_jobs()
    return cluster, leader, gate_heard_of_leader


@pytest.mark.asyncio
async def test_the_global_timeout_reaches_the_current_leader_only(monkeypatch: pytest.MonkeyPatch) -> None:
    heard_takeovers = 0
    for seed in range(SEED_COUNT):
        cluster, leader, gate_heard_of_leader = await run_seed(seed, monkeypatch)

        deposed_timeouts = {
            manager.index: manager.timed_out_jobs
            for manager in cluster.managers.values()
            if manager is not leader and manager.timed_out_jobs
        }
        assert deposed_timeouts == {}, f"seed {seed}: deposed leaders timed the job out: {deposed_timeouts}"
        if gate_heard_of_leader:
            assert leader.timed_out_jobs == [JOB_ID], (
                f"seed {seed}: leader {leader.index} (fence "
                f"{leader.job.timeout_tracking.timeout_fence_token}) did not time the job out"
            )
            heard_takeovers += leader.index > 0

    # The corpus takes jobs over and has the gate hear of it.
    assert heard_takeovers > 0
