"""
Fence-token validation in the gate's job timeout tracker (FIX.md 1.2,
AD-34 Part 5 / Part 7).

The tracker STORED ``report.fence_token`` on every progress, timeout,
and leader-transfer report without ever comparing it to the fence it
already held for that datacenter. The global timeout decision is
driven by ``dc_last_progress``, so a straggling report from a manager
that had since lost leadership refreshed the progress clock — evidence
of life from a node no longer running the work — and pushed the global
timeout out past where it belonged. After a leadership transfer, the
old leader's in-flight reports are exactly that shape, and Scenario
11.1's timeout detection skewed accordingly.

These tests pin the guard from both directions: reports at or above
the known fence flow exactly as before (first contact, steady state,
and legitimate fence advancement), while a report behind the known
fence mutates NOTHING — not the progress clock, not the routing
address, not the DC status — and is logged as superseded.
"""

from __future__ import annotations

import pytest

from hyperscale.distributed.jobs.gates import gate_job_timeout_tracker as gate_job_timeout_tracker_module
from hyperscale.distributed.jobs.gates.gate_job_timeout_tracker import (
    GateJobTimeoutTracker,
)
from hyperscale.distributed.models.distributed import (
    JobLeaderTransfer,
    JobProgressReport,
    JobTimeoutReport,
)
from hyperscale.logging.hyperscale_logging_models import ServerWarning

JOB_ID = "job-fence-test"
DATACENTER = "dc-east"
# A report's own timestamp is the manager's monotonic clock, which the gate
# never compares with its own; the gate records when it received the report.
MANAGER_CLOCK_READING = 7.0


class _SteppedClock:
    """The gate's monotonic clock, set by each test to a report's receipt time."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture(autouse=True)
def gate_clock(monkeypatch: pytest.MonkeyPatch) -> _SteppedClock:
    clock = _SteppedClock()
    monkeypatch.setattr(gate_job_timeout_tracker_module, "_DEFAULT_CLOCK", clock)
    return clock


async def _receive_progress(
    tracker: GateJobTimeoutTracker,
    gate_clock: _SteppedClock,
    received_at: float,
    report: JobProgressReport,
) -> None:
    """Deliver ``report`` to the tracker at gate time ``received_at``."""
    gate_clock.now = received_at
    await tracker.record_progress(report)


class _RecordingLogger:
    """Captures every entry the tracker logs."""

    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


class _NodeId:
    short = "gate-1"


class _StubGate:
    """The four attributes the tracker reads off its parent gate."""

    def __init__(self) -> None:
        self._udp_logger = _RecordingLogger()
        self._host = "127.0.0.1"
        self._tcp_port = 9000
        self._node_id = _NodeId()


def _progress_report(
    fence_token: int,
    manager_port: int = 8000,
    workflows_completed: int = 1,
) -> JobProgressReport:
    return JobProgressReport(
        job_id=JOB_ID,
        datacenter=DATACENTER,
        manager_id=f"manager-fence-{fence_token}",
        manager_host="10.0.0.1",
        manager_port=manager_port,
        workflows_total=4,
        workflows_completed=workflows_completed,
        workflows_failed=0,
        has_recent_progress=True,
        timestamp=MANAGER_CLOCK_READING,
        fence_token=fence_token,
        total_extensions_granted=float(fence_token),
    )


def _timeout_report(fence_token: int) -> JobTimeoutReport:
    return JobTimeoutReport(
        job_id=JOB_ID,
        datacenter=DATACENTER,
        manager_id=f"manager-fence-{fence_token}",
        manager_host="10.0.0.1",
        manager_port=8000,
        reason="stuck",
        elapsed_seconds=120.0,
        fence_token=fence_token,
    )


def _leader_transfer(fence_token: int, new_leader_port: int) -> JobLeaderTransfer:
    return JobLeaderTransfer(
        job_id=JOB_ID,
        datacenter=DATACENTER,
        new_leader_id=f"manager-fence-{fence_token}",
        new_leader_host="10.0.0.2",
        new_leader_port=new_leader_port,
        fence_token=fence_token,
    )


async def _tracked_job() -> tuple[GateJobTimeoutTracker, _StubGate]:
    gate = _StubGate()
    tracker = GateJobTimeoutTracker(gate=gate)
    await tracker.start_tracking_job(
        JOB_ID, timeout_seconds=300.0, target_dcs=[DATACENTER]
    )
    return tracker, gate


def _superseded_warnings(gate: _StubGate) -> list[ServerWarning]:
    return [
        entry
        for entry in gate._udp_logger.entries
        if isinstance(entry, ServerWarning) and "superseded" in entry.message
    ]


@pytest.mark.asyncio
async def test_first_report_for_a_datacenter_is_accepted(gate_clock: _SteppedClock) -> None:
    """No known fence means nothing to be behind — first contact from a
    DC must record, or a freshly tracked job would never hear anything."""
    tracker, gate = await _tracked_job()

    await _receive_progress(tracker, gate_clock, 100.0, _progress_report(fence_token=5))

    info = tracker._tracked_jobs[JOB_ID]
    assert info.dc_last_progress[DATACENTER] == 100.0
    assert info.dc_fence_tokens[DATACENTER] == 5
    assert _superseded_warnings(gate) == []


@pytest.mark.asyncio
async def test_equal_fence_token_keeps_reporting(gate_clock: _SteppedClock) -> None:
    """The same leader reports every ~10s at the same fence; equality
    must pass or steady-state progress tracking dies after one report."""
    tracker, _gate = await _tracked_job()

    await _receive_progress(tracker, gate_clock, 100.0, _progress_report(fence_token=5))
    await _receive_progress(tracker, gate_clock, 110.0, _progress_report(fence_token=5))

    info = tracker._tracked_jobs[JOB_ID]
    assert info.dc_last_progress[DATACENTER] == 110.0


@pytest.mark.asyncio
async def test_newer_fence_supersedes_and_advances(gate_clock: _SteppedClock) -> None:
    """A legitimate leadership transfer raises the fence; the new
    leader's reports must both record and become the new bar."""
    tracker, _gate = await _tracked_job()

    await _receive_progress(tracker, gate_clock, 100.0, _progress_report(fence_token=5))
    await _receive_progress(tracker, gate_clock, 105.0, _progress_report(fence_token=6))

    info = tracker._tracked_jobs[JOB_ID]
    assert info.dc_fence_tokens[DATACENTER] == 6
    assert info.dc_last_progress[DATACENTER] == 105.0


@pytest.mark.asyncio
async def test_stale_progress_cannot_refresh_the_progress_clock(gate_clock: _SteppedClock) -> None:
    """The core of FIX.md 1.2: a report behind the known fence is
    evidence from a deposed leader and must not touch ANY tracking
    state — before the guard, its timestamp bumped ``dc_last_progress``
    and delayed the global timeout decision."""
    tracker, gate = await _tracked_job()

    await _receive_progress(
        tracker, gate_clock, 100.0, _progress_report(fence_token=5, manager_port=8005)
    )
    await _receive_progress(
        tracker,
        gate_clock,
        200.0,
        _progress_report(
            fence_token=3,
            manager_port=8003,
            workflows_completed=4,
        ),
    )

    info = tracker._tracked_jobs[JOB_ID]
    assert info.dc_last_progress[DATACENTER] == 100.0, (
        "a stale-fence report refreshed the progress clock — the global "
        "timeout decision is now delayed on a deposed leader's evidence"
    )
    assert info.dc_fence_tokens[DATACENTER] == 5, "fence moved backwards"
    assert info.dc_manager_addrs[DATACENTER] == ("10.0.0.1", 8005), (
        "routing rewound to the deposed leader"
    )
    assert info.dc_total_extensions[DATACENTER] == 5.0, (
        "extension tracking took the stale report's values"
    )
    assert info.dc_status[DATACENTER] == "running", (
        "a stale report with workflows_completed == total marked the DC "
        "completed"
    )
    assert len(_superseded_warnings(gate)) == 1, (
        "the rejection must be logged — silent drops hide fencing activity"
    )


@pytest.mark.asyncio
async def test_stale_timeout_report_cannot_mark_a_datacenter_timed_out(gate_clock: _SteppedClock) -> None:
    """A deposed leader declaring a timeout is the inverse skew: it
    would push the gate TOWARD a global timeout on stale evidence."""
    tracker, _gate = await _tracked_job()

    await _receive_progress(tracker, gate_clock, 100.0, _progress_report(fence_token=5))
    await tracker.record_timeout(_timeout_report(fence_token=3))

    info = tracker._tracked_jobs[JOB_ID]
    assert info.dc_status[DATACENTER] == "running", (
        "a stale-fence timeout report marked the DC timed_out"
    )
    assert info.dc_fence_tokens[DATACENTER] == 5


@pytest.mark.asyncio
async def test_stale_leader_transfer_cannot_rewind_routing() -> None:
    """Transfers race: the announcement for fence 6 can arrive after
    fence 7's. Applying it would route timeout decisions to a manager
    that has already lost the job again."""
    tracker, _gate = await _tracked_job()

    await tracker.record_leader_transfer(
        _leader_transfer(fence_token=7, new_leader_port=8007)
    )
    await tracker.record_leader_transfer(
        _leader_transfer(fence_token=6, new_leader_port=8006)
    )

    info = tracker._tracked_jobs[JOB_ID]
    assert info.dc_manager_addrs[DATACENTER] == ("10.0.0.2", 8007), (
        "an out-of-order transfer announcement rewound routing"
    )
    assert info.dc_fence_tokens[DATACENTER] == 7
