"""
Each job's progress windows have exactly one consumer.

The manager's windowed stats had two consumers: a 50 ms loop that drained
every job's closed windows and discarded those of jobs with no origin
gate, and a 250 ms client push that found almost nothing left -- so a
client submitting without a gate saw sparse progress -- and could itself
drain a gate-routed job's windows.

* a gate-routed job's closed windows go to its origin gate, per worker;
  a directly submitted job's are left for the client push, which takes
  only those;
* flushing one job leaves its open windows and every other job's in
  place, and drops a window that outlived the maximum age without
  closing (stats stamped by a clock running ahead).
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.jobs import windowed_stats_collector as collector_module
from hyperscale.distributed.jobs.windowed_stats_collector import (
    WindowBucket,
    WindowedStatsCollector,
)
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.nodes.manager.stats import ManagerStatsCoordinator

GATE_ROUTED_JOB = "job-gate"
CLIENT_JOB = "job-client"
ORIGIN_GATE = ("10.0.0.9", 9100)
WINDOW_SIZE_MS = 100.0
DRIFT_TOLERANCE_MS = 50.0
MAX_WINDOW_AGE_MS = 5000.0


class SteppedClock:
    def __init__(self, now: float) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now


def add_window(
    collector: WindowedStatsCollector,
    job_id: str,
    bucket_number: int,
    created_at: float,
) -> None:
    collector._buckets[(job_id, "workflow-1", bucket_number)] = WindowBucket(
        window_start=bucket_number * WINDOW_SIZE_MS / 1000,
        window_end=(bucket_number + 1) * WINDOW_SIZE_MS / 1000,
        job_id=job_id,
        workflow_id="workflow-1",
        workflow_name="Workflow",
        worker_stats={},
        created_at=created_at,
    )


def make_collector() -> WindowedStatsCollector:
    return WindowedStatsCollector(
        window_size_ms=WINDOW_SIZE_MS,
        drift_tolerance_ms=DRIFT_TOLERANCE_MS,
        max_window_age_ms=MAX_WINDOW_AGE_MS,
    )


@pytest.mark.asyncio
async def test_flushing_one_job_leaves_its_open_windows_and_other_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 100.0
    monkeypatch.setattr(collector_module, "_DEFAULT_CLOCK", SteppedClock(now))
    collector = make_collector()
    closed_bucket = int((now - 1.0) * 1000 / WINDOW_SIZE_MS)
    open_bucket = int(now * 1000 / WINDOW_SIZE_MS)
    add_window(collector, CLIENT_JOB, closed_bucket, created_at=now - 1.0)
    add_window(collector, CLIENT_JOB, open_bucket, created_at=now)
    add_window(collector, GATE_ROUTED_JOB, closed_bucket, created_at=now - 1.0)

    flushed = await collector.flush_closed_job_windows(CLIENT_JOB, aggregate=True)

    assert [(push.job_id, push.window_start) for push in flushed] == [
        (CLIENT_JOB, closed_bucket * WINDOW_SIZE_MS / 1000)
    ]
    assert set(collector._buckets) == {
        (CLIENT_JOB, "workflow-1", open_bucket),
        (GATE_ROUTED_JOB, "workflow-1", closed_bucket),
    }


@pytest.mark.asyncio
async def test_a_window_that_never_closes_is_dropped_at_the_maximum_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 100.0
    monkeypatch.setattr(collector_module, "_DEFAULT_CLOCK", SteppedClock(now))
    collector = make_collector()
    future_bucket = int((now + 60.0) * 1000 / WINDOW_SIZE_MS)
    add_window(collector, CLIENT_JOB, future_bucket, created_at=now - MAX_WINDOW_AGE_MS / 1000 - 1.0)

    flushed = await collector.flush_closed_job_windows(CLIENT_JOB, aggregate=False)

    assert flushed == []
    assert collector._buckets == {}
    assert collector.get_metrics().windows_dropped_late == 1


class RecordingCollector:
    def __init__(self) -> None:
        self.flushed: list[tuple[str, bool]] = []

    def get_jobs_with_pending_stats(self) -> list[str]:
        return [GATE_ROUTED_JOB, CLIENT_JOB]

    async def flush_closed_job_windows(self, job_id: str, aggregate: bool) -> list[SimpleNamespace]:
        self.flushed.append((job_id, aggregate))
        return [SimpleNamespace(job_id=job_id)]


def origin_gates() -> SimpleNamespace:
    return SimpleNamespace(
        get_job_origin_gate={GATE_ROUTED_JOB: ORIGIN_GATE}.get,
    )


@pytest.mark.asyncio
async def test_a_gate_routed_jobs_windows_go_only_to_its_origin_gate() -> None:
    collector = RecordingCollector()
    forwarded: list[tuple[str, tuple[str, int]]] = []
    manager = object.__new__(ManagerServer)
    manager._windowed_stats = collector
    manager._manager_state = origin_gates()

    async def forward(stats_push: SimpleNamespace, origin_gate_addr: tuple[str, int]) -> None:
        forwarded.append((stats_push.job_id, origin_gate_addr))

    manager._push_windowed_stats_to_gate = forward

    await ManagerServer._flush_windowed_stats(manager)

    assert collector.flushed == [(GATE_ROUTED_JOB, False)]
    assert forwarded == [(GATE_ROUTED_JOB, ORIGIN_GATE)]


@pytest.mark.asyncio
async def test_the_client_push_takes_only_directly_submitted_jobs() -> None:
    pushed_jobs: list[str] = []
    stats = object.__new__(ManagerStatsCoordinator)
    stats._windowed_stats = RecordingCollector()
    stats._state = origin_gates()

    async def push_job_stats(job_id: str) -> int:
        pushed_jobs.append(job_id)
        return 0

    stats._push_job_stats = push_job_stats

    await stats.push_batch_stats()

    assert pushed_jobs == [CLIENT_JOB]
