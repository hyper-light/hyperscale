"""Seeded VOPR: AD-34's gate stuck check holds whatever the hosts' clocks read.

At base commit 2e6d0532 the gate stored each ``JobProgressReport.timestamp``
-- the manager's ``monotonic()`` (``jobs/gate_coordinated_timeout.py:452``)
-- as the datacenter's last progress
(``jobs/gates/gate_job_timeout_tracker.py:215``) and compared it with its own
``monotonic()`` (``:439``, ``:495-503``). Monotonic clocks are boot-relative,
so the difference between two hosts' readings is meaningless: a manager
booted after the gate looked stuck from its first report (a false global
timeout), and one booted before it never looked stuck at all.

Each seed gives the gate and the manager their own monotonic origin, up to
30 days apart, on one shared timeline. The real manager report path
(``GateCoordinatedTimeout._send_progress_report``) feeds the real tracker,
and the gate's check runs every ``check_interval``. Invariants, per seed:

* INV1 -- while the manager keeps reporting, the job is never declared stuck;
* INV2 -- once reports stop, the stuck verdict comes at the first check at
  least ``stuck_threshold`` after the gate last heard from the datacenter
  (AD-34 Part 5: "All DCs stuck (no progress 3+ min)").
"""

import random
from types import SimpleNamespace

import pytest

from hyperscale.distributed.jobs import gate_coordinated_timeout as gate_coordinated_timeout_module
from hyperscale.distributed.jobs.gate_coordinated_timeout import GateCoordinatedTimeout
from hyperscale.distributed.jobs.gates import gate_job_timeout_tracker as gate_job_timeout_tracker_module
from hyperscale.distributed.jobs.gates.gate_job_timeout_tracker import GateJobTimeoutTracker
from hyperscale.distributed.models.distributed import JobProgressReport

JOB_ID = "job-clock-skew"
DATACENTER = "dc-east"
GATE_ADDR = ("10.0.0.1", 9000)
SEED_COUNT = 200
MAX_CLOCK_ORIGIN_SECONDS = 30 * 24 * 3600.0
REPORT_INTERVAL_SECONDS = 10.0
CHECK_INTERVAL_SECONDS = 15.0
STUCK_THRESHOLD_SECONDS = 180.0
JOB_TIMEOUT_SECONDS = 1.0e9


class SharedTimeline:
    """The one true elapsed time every host's clock advances with."""

    def __init__(self) -> None:
        self.elapsed_seconds = 0.0


class HostMonotonicClock:
    """A host's monotonic clock: the shared timeline plus its own boot-relative origin."""

    def __init__(self, timeline: SharedTimeline, origin_seconds: float) -> None:
        self._timeline = timeline
        self._origin_seconds = origin_seconds

    def monotonic(self) -> float:
        return self._origin_seconds + self._timeline.elapsed_seconds


class RecordingLogger:
    async def log(self, entry: object) -> None:
        return None


def stub_gate() -> SimpleNamespace:
    return SimpleNamespace(
        _udp_logger=RecordingLogger(),
        _host=GATE_ADDR[0],
        _tcp_port=GATE_ADDR[1],
        _node_id=SimpleNamespace(short="gate-1"),
    )


def stub_manager(tracker: GateJobTimeoutTracker) -> SimpleNamespace:
    """A manager whose job is tracked for ``GATE_ADDR`` and whose sends reach ``tracker``."""
    job = SimpleNamespace(
        workflows_total=4,
        workflows_completed=1,
        workflows_failed=0,
        timeout_tracking=SimpleNamespace(
            last_progress_at=0.0,
            timeout_fence_token=1,
            total_extensions_granted=0.0,
            max_worker_extension=0.0,
            active_workers_with_extensions=set(),
            gate_addr=GATE_ADDR,
        ),
    )

    async def send_tcp(gate_addr: tuple[str, int], handler_name: str, payload: bytes) -> tuple[bytes, float]:
        await tracker.record_progress(JobProgressReport.load(payload))
        return b"ok", 0.0

    return SimpleNamespace(
        _job_manager=SimpleNamespace(get_job_by_id=lambda job_id: job),
        _node_id=SimpleNamespace(datacenter=DATACENTER, short="manager-1"),
        _host="10.0.1.1",
        _tcp_port=8000,
        _udp_logger=RecordingLogger(),
        send_tcp=send_tcp,
    )


async def run_seed(seed: int, monkeypatch: pytest.MonkeyPatch) -> tuple[list[float], float, float]:
    """Run one skewed-clock schedule; returns (stuck verdict times while
    reporting, last report time, first stuck verdict time after reports stop)."""
    seeded_random = random.Random(seed)
    timeline = SharedTimeline()
    gate_clock = HostMonotonicClock(timeline, seeded_random.uniform(0.0, MAX_CLOCK_ORIGIN_SECONDS))
    manager_clock = HostMonotonicClock(timeline, seeded_random.uniform(0.0, MAX_CLOCK_ORIGIN_SECONDS))
    monkeypatch.setattr(gate_job_timeout_tracker_module, "_DEFAULT_CLOCK", gate_clock)
    monkeypatch.setattr(gate_coordinated_timeout_module, "_DEFAULT_CLOCK", manager_clock)

    tracker = GateJobTimeoutTracker(
        gate=stub_gate(), check_interval=CHECK_INTERVAL_SECONDS, stuck_threshold=STUCK_THRESHOLD_SECONDS
    )
    manager_strategy = GateCoordinatedTimeout(stub_manager(tracker))
    await tracker.start_tracking_job(JOB_ID, timeout_seconds=JOB_TIMEOUT_SECONDS, target_dcs=[DATACENTER])

    report_count = seeded_random.randint(1, 60)
    report_times = [index * REPORT_INTERVAL_SECONDS + seeded_random.uniform(0.0, 1.0) for index in range(report_count)]
    last_report_time = report_times[-1]
    check_times = [index * CHECK_INTERVAL_SECONDS for index in range(1, 400)]
    events = sorted([(time, "report") for time in report_times] + [(time, "check") for time in check_times])

    stuck_while_reporting: list[float] = []
    for event_time, event_kind in events:
        timeline.elapsed_seconds = event_time
        if event_kind == "report":
            await manager_strategy._send_progress_report(JOB_ID)
            continue
        is_stuck, _ = await tracker._check_global_timeout(tracker._tracked_jobs[JOB_ID])
        if is_stuck and event_time <= last_report_time:
            stuck_while_reporting.append(event_time)
        if is_stuck and event_time > last_report_time:
            return stuck_while_reporting, last_report_time, event_time
    return stuck_while_reporting, last_report_time, float("inf")


@pytest.mark.asyncio
async def test_stuck_verdict_follows_the_gates_own_clock_under_skew(monkeypatch: pytest.MonkeyPatch) -> None:
    for seed in range(SEED_COUNT):
        stuck_while_reporting, last_report_time, first_stuck_time = await run_seed(seed, monkeypatch)

        assert stuck_while_reporting == [], f"seed {seed}: declared stuck while reporting at {stuck_while_reporting}"
        silence_seconds = first_stuck_time - last_report_time
        assert STUCK_THRESHOLD_SECONDS <= silence_seconds < STUCK_THRESHOLD_SECONDS + CHECK_INTERVAL_SECONDS, (
            f"seed {seed}: stuck verdict {silence_seconds:.1f}s after the last report"
        )
