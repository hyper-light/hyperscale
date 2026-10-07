"""
AD-30 job-layer silence detection.

The detector's reader still consulted a per-job dead map that an earlier
cleanup deleted, so every pass raised AttributeError and no silent
(job, worker) pair was ever suspected. The worker-removal paths filtered
the ``(job_id, worker_id)`` progress keys on the job id, so a removed
worker's entries were never pruned.

* a pair silent past the threshold is found; a suspected one is not;
* a pair whose suspicion expired is not found again (its workflows are
  reassigned, so it owes the job no progress) until it reports again;
* unregistering a worker, or removing its state, prunes exactly its
  progress entries.

A job suspicion was never refuted: ``record_job_progress`` left it in
place, so every suspicion expired into job-death and a worker that resumed
reporting had its workflows reassigned and run twice.

* a suspected pair whose worker reports again is not declared dead when
  its suspicion window passes;
* a report landing between the loop finding a pair silent and suspecting
  it refutes the silence: no suspicion starts;
* a report landing while an expiry pass logs its deaths does not break
  the pass: every pair it found expired is returned for reassignment.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.manager import (
    job_suspicion as job_suspicion_module,
    manager_health_monitor as manager_health_monitor_module,
)
from hyperscale.distributed.nodes.manager.health import ManagerHealthMonitor
from hyperscale.distributed.nodes.manager.registry import ManagerRegistry
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.slo import SLOConfig

RESPONSIVENESS_THRESHOLD_SECONDS = 30.0
SUSPICION_TIMEOUT_SECONDS = 5.0
JOB_ID = "job-1"
SILENT_WORKER = "worker-silent"
TALKING_WORKER = "worker-talking"


class SteppedClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


def make_config() -> SimpleNamespace:
    return SimpleNamespace(
        job_responsiveness_threshold_seconds=RESPONSIVENESS_THRESHOLD_SECONDS,
        host="127.0.0.1",
        tcp_port=9000,
    )


def make_monitor(state: ManagerState) -> ManagerHealthMonitor:
    return ManagerHealthMonitor(
        state=state,
        config=make_config(),
        registry=SimpleNamespace(),
        logger=RecordingLogger(),
        node_id="manager-1",
        task_runner=SimpleNamespace(run=lambda *args, **kwargs: None),
    )


@pytest.fixture
def stepped_clock(monkeypatch: pytest.MonkeyPatch) -> SteppedClock:
    clock = SteppedClock()
    # The manager health classes read the clock where each is defined.
    for health_class_module in (manager_health_monitor_module, job_suspicion_module):
        monkeypatch.setattr(health_class_module, "_DEFAULT_CLOCK", clock)
    return clock


@pytest.mark.asyncio
async def test_a_silent_pair_is_found_and_a_suspected_one_is_not(stepped_clock: SteppedClock) -> None:
    state = ManagerState(slo_config=SLOConfig.from_env(Env()))
    monitor = make_monitor(state)
    monitor.record_job_progress(JOB_ID, SILENT_WORKER)
    stepped_clock.now += RESPONSIVENESS_THRESHOLD_SECONDS
    monitor.record_job_progress(JOB_ID, TALKING_WORKER)

    silent_pairs = monitor.find_silent_worker_jobs(RESPONSIVENESS_THRESHOLD_SECONDS)

    assert silent_pairs == [(JOB_ID, SILENT_WORKER)]

    await monitor.suspect_job(JOB_ID, SILENT_WORKER, timeout_seconds=SUSPICION_TIMEOUT_SECONDS)

    assert monitor.find_silent_worker_jobs(RESPONSIVENESS_THRESHOLD_SECONDS) == []


@pytest.mark.asyncio
async def test_an_expired_pair_is_not_suspected_again_until_it_reports(stepped_clock: SteppedClock) -> None:
    state = ManagerState(slo_config=SLOConfig.from_env(Env()))
    monitor = make_monitor(state)
    monitor.record_job_progress(JOB_ID, SILENT_WORKER)
    stepped_clock.now += RESPONSIVENESS_THRESHOLD_SECONDS
    await monitor.suspect_job(JOB_ID, SILENT_WORKER, timeout_seconds=SUSPICION_TIMEOUT_SECONDS)
    stepped_clock.now += SUSPICION_TIMEOUT_SECONDS

    expired = await monitor.check_job_suspicion_expiry()

    assert expired == [(JOB_ID, SILENT_WORKER)]
    assert monitor.find_silent_worker_jobs(RESPONSIVENESS_THRESHOLD_SECONDS) == []

    monitor.record_job_progress(JOB_ID, SILENT_WORKER)
    stepped_clock.now += RESPONSIVENESS_THRESHOLD_SECONDS

    assert monitor.find_silent_worker_jobs(RESPONSIVENESS_THRESHOLD_SECONDS) == [(JOB_ID, SILENT_WORKER)]


def test_unregistering_a_worker_prunes_exactly_its_progress_entries() -> None:
    state = ManagerState(slo_config=SLOConfig.from_env(Env()))
    state._worker_job_last_progress[(JOB_ID, SILENT_WORKER)] = 1.0
    state._worker_job_last_progress[("job-2", SILENT_WORKER)] = 1.0
    state._worker_job_last_progress[(JOB_ID, TALKING_WORKER)] = 1.0
    registry = ManagerRegistry(
        state=state,
        config=make_config(),
        logger=RecordingLogger(),
        node_id="manager-1",
        task_runner=SimpleNamespace(run=lambda *args, **kwargs: None),
        on_worker_unregistered=lambda worker_id: None,
    )

    registry.unregister_worker(SILENT_WORKER)

    assert state._worker_job_last_progress == {(JOB_ID, TALKING_WORKER): 1.0}


def test_removing_a_workers_state_prunes_exactly_its_progress_entries() -> None:
    state = ManagerState(slo_config=SLOConfig.from_env(Env()))
    state._worker_job_last_progress[(JOB_ID, SILENT_WORKER)] = 1.0
    state._worker_job_last_progress[("job-2", SILENT_WORKER)] = 1.0
    state._worker_job_last_progress[(JOB_ID, TALKING_WORKER)] = 1.0

    state.remove_worker_state(SILENT_WORKER)

    assert state._worker_job_last_progress == {(JOB_ID, TALKING_WORKER): 1.0}


@pytest.mark.asyncio
async def test_a_report_refutes_the_suspicion_of_its_pair(stepped_clock: SteppedClock) -> None:
    state = ManagerState(slo_config=SLOConfig.from_env(Env()))
    monitor = make_monitor(state)
    monitor.record_job_progress(JOB_ID, SILENT_WORKER)
    stepped_clock.now += RESPONSIVENESS_THRESHOLD_SECONDS
    await monitor.suspect_job(JOB_ID, SILENT_WORKER, timeout_seconds=SUSPICION_TIMEOUT_SECONDS)

    stepped_clock.now += SUSPICION_TIMEOUT_SECONDS / 2
    monitor.record_job_progress(JOB_ID, SILENT_WORKER)
    stepped_clock.now += SUSPICION_TIMEOUT_SECONDS

    assert await monitor.check_job_suspicion_expiry() == []
    # The pair is tracked again from its report, not forgotten.
    assert (JOB_ID, SILENT_WORKER) in state._worker_job_last_progress


@pytest.mark.asyncio
async def test_a_report_after_the_silence_was_found_starts_no_suspicion(stepped_clock: SteppedClock) -> None:
    state = ManagerState(slo_config=SLOConfig.from_env(Env()))
    monitor = make_monitor(state)
    monitor.record_job_progress(JOB_ID, SILENT_WORKER)
    stepped_clock.now += RESPONSIVENESS_THRESHOLD_SECONDS
    [silent_pair] = monitor.find_silent_worker_jobs(RESPONSIVENESS_THRESHOLD_SECONDS)

    monitor.record_job_progress(*silent_pair)
    await monitor.suspect_job(*silent_pair, timeout_seconds=SUSPICION_TIMEOUT_SECONDS)
    stepped_clock.now += SUSPICION_TIMEOUT_SECONDS

    assert monitor._job_suspicions == {}
    assert await monitor.check_job_suspicion_expiry() == []


class ReportingDuringLogLogger(RecordingLogger):
    """Delivers a worker's progress report while the first expiry is
    logged -- the await an expiry pass makes between its deaths."""

    def __init__(self, monitor_reports: list[tuple[str, str]]) -> None:
        super().__init__()
        self.monitor: ManagerHealthMonitor | None = None
        self.monitor_reports = monitor_reports

    async def log(self, entry: object) -> None:
        await super().log(entry)
        while self.monitor is not None and self.monitor_reports:
            self.monitor.record_job_progress(*self.monitor_reports.pop())


@pytest.mark.asyncio
async def test_a_report_during_an_expiry_pass_does_not_break_it(stepped_clock: SteppedClock) -> None:
    state = ManagerState(slo_config=SLOConfig.from_env(Env()))
    logger = ReportingDuringLogLogger([(JOB_ID, TALKING_WORKER)])
    monitor = ManagerHealthMonitor(
        state=state,
        config=make_config(),
        registry=SimpleNamespace(),
        logger=logger,
        node_id="manager-1",
        task_runner=SimpleNamespace(run=lambda *args, **kwargs: None),
    )
    for worker_id in (SILENT_WORKER, TALKING_WORKER):
        monitor.record_job_progress(JOB_ID, worker_id)
    stepped_clock.now += RESPONSIVENESS_THRESHOLD_SECONDS
    for worker_id in (SILENT_WORKER, TALKING_WORKER):
        await monitor.suspect_job(JOB_ID, worker_id, timeout_seconds=SUSPICION_TIMEOUT_SECONDS)
    stepped_clock.now += SUSPICION_TIMEOUT_SECONDS
    logger.entries.clear()
    logger.monitor = monitor

    expired = await monitor.check_job_suspicion_expiry()

    # Both were expired when the pass looked; both are reassigned.
    assert sorted(expired) == [(JOB_ID, SILENT_WORKER), (JOB_ID, TALKING_WORKER)]
    assert len(logger.entries) == 2
    assert monitor._job_suspicions == {}
