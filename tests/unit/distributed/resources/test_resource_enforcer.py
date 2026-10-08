"""
AD-41 ResourceEnforcer: graduated, uncertainty-aware enforcement of a
workflow's resource budget, driven on a stepped clock so every grace
boundary is exact.

Rules pinned: under the warning threshold nothing is tracked; a sustained
warning-zone violation warns exactly once, at the (uncertainty-stretched)
warning grace; only a CERTAIN violation (estimate minus 2 sigma above the
kill threshold) sustained for the kill grace kills, so a noisy estimate
over the limit is never killed; a kill that is lost or ignored is retried
once and then the worker is evicted; dropping under the warning
threshold ends the violation; per-job budgets apply; released workflows
and jobs leave no state.
"""

import math

import pytest

from hyperscale.distributed.resources.enforcement_action import EnforcementAction
from hyperscale.distributed.resources.resource_budget import ResourceBudget
from hyperscale.distributed.resources.resource_enforcer import (
    KILL_CONFIDENCE_SIGMAS,
    MAX_KILL_ATTEMPTS,
    ResourceEnforcer,
)
from hyperscale.distributed.resources.resource_violation_type import ResourceViolationType

LIMIT_CPU = 400.0
LIMIT_MEMORY = 1_000_000_000
BUDGET = ResourceBudget(
    max_cpu_percent=LIMIT_CPU,
    max_memory_bytes=LIMIT_MEMORY,
    warning_threshold=0.8,
    throttle_threshold=0.85,
    kill_threshold=1.0,
    warning_grace_seconds=10.0,
    kill_grace_seconds=2.0,
)
CPU = ResourceViolationType.CPU_EXCEEDED


class SteppedClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now


class Recorder:
    def __init__(self, kill_succeeds: bool = True, throttle_succeeds: bool = True, release_succeeds: bool = True) -> None:
        self.kill_succeeds = kill_succeeds
        self.throttle_succeeds = throttle_succeeds
        self.release_succeeds = release_succeeds
        self.warnings: list[tuple] = []
        self.throttles: list[tuple] = []
        self.releases: list[tuple] = []
        self.kills: list[tuple] = []
        self.evictions: list[tuple] = []

    async def warn(self, workflow_id, worker_id, violation_type, value, limit) -> None:
        self.warnings.append((workflow_id, worker_id, violation_type, value, limit))

    async def throttle(self, workflow_id, worker_id, job_id, scale) -> bool:
        self.throttles.append((workflow_id, worker_id, job_id, scale))
        return self.throttle_succeeds

    async def release(self, workflow_id, worker_id, job_id) -> bool:
        self.releases.append((workflow_id, worker_id, job_id))
        return self.release_succeeds

    async def kill(self, workflow_id, worker_id, job_id, violation_type) -> bool:
        self.kills.append((workflow_id, worker_id, job_id, violation_type))
        return self.kill_succeeds

    async def evict(self, worker_id, violation_type) -> bool:
        self.evictions.append((worker_id, violation_type))
        return True


def _enforcer(recorder: Recorder, clock: SteppedClock) -> ResourceEnforcer:
    return ResourceEnforcer(
        clock=clock,
        default_budget=BUDGET,
        on_warn=recorder.warn,
        on_throttle_workflow=recorder.throttle,
        on_release_throttle=recorder.release,
        on_kill_workflow=recorder.kill,
        on_evict_worker=recorder.evict,
    )


async def _check(enforcer, cpu, cpu_sigma=0.0, memory=0.0, memory_sigma=0.0, job_id="job-1"):
    return await enforcer.check_workflow(
        workflow_id="wf-1",
        worker_id="worker-1",
        job_id=job_id,
        cpu_percent=cpu,
        cpu_uncertainty=cpu_sigma,
        memory_bytes=memory,
        memory_uncertainty=memory_sigma,
    )


async def _run(enforcer, clock, seconds, step, **reading):
    """Feed one reading every ``step`` seconds; stops at an eviction (the
    worker is gone -- anything after would be a new violation)."""
    actions = []
    elapsed = 0.0
    while elapsed <= seconds:
        action = await _check(enforcer, **reading)
        actions.append((elapsed, action))
        if action is EnforcementAction.EVICT_WORKER:
            break
        clock.now += step
        elapsed += step
    return actions


@pytest.mark.asyncio
async def test_usage_under_the_warning_threshold_tracks_nothing() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)

    actions = await _run(enforcer, clock, 60.0, 1.0, cpu=LIMIT_CPU * 0.8)

    assert {action for _, action in actions} == {EnforcementAction.NONE}
    assert enforcer.tracked_violation_count == 0


@pytest.mark.asyncio
async def test_warning_zone_warns_exactly_once_at_the_grace() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)

    actions = await _run(enforcer, clock, 60.0, 0.5, cpu=LIMIT_CPU * 0.9)

    warn_times = [elapsed for elapsed, action in actions if action is EnforcementAction.WARN]
    assert warn_times == [BUDGET.warning_grace_seconds]
    assert recorder.kills == [] and recorder.evictions == []


@pytest.mark.asyncio
async def test_uncertainty_stretches_the_grace() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)
    value, sigma = LIMIT_CPU * 0.9, LIMIT_CPU * 0.9

    actions = await _run(enforcer, clock, 60.0, 0.5, cpu=value, cpu_sigma=sigma)

    (warn_time,) = [elapsed for elapsed, action in actions if action is EnforcementAction.WARN]
    assert warn_time == BUDGET.warning_grace_seconds * (1.0 + sigma / value)


@pytest.mark.asyncio
async def test_certain_violation_is_killed_after_the_warning_and_kill_grace() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)

    value, sigma, step = LIMIT_CPU * 1.5, 5.0, 0.5
    actions = await _run(enforcer, clock, 30.0, step, cpu=value, cpu_sigma=sigma)

    outcomes = [(elapsed, action) for elapsed, action in actions if action is not EnforcementAction.NONE]
    (warn_time, warn_action), (kill_time, kill_action) = outcomes[0], outcomes[1]
    stretched_grace = BUDGET.warning_grace_seconds * (1.0 + sigma / value)
    assert warn_action is EnforcementAction.WARN
    assert warn_time == math.ceil(stretched_grace / step) * step
    assert kill_action is EnforcementAction.KILL_WORKFLOW
    assert kill_time > warn_time
    assert recorder.kills[0] == ("wf-1", "worker-1", "job-1", CPU)


@pytest.mark.asyncio
async def test_uncertain_violation_is_warned_and_throttled_but_never_killed() -> None:
    """Over the limit but not certainly so: never killed -- the graduated
    response it gets is the throttle, repeated every (stretched) warning
    grace while it stays above the throttle line."""
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)
    value = LIMIT_CPU * 1.05
    sigma = (value - LIMIT_CPU) / KILL_CONFIDENCE_SIGMAS + 1.0  # value - 2 sigma < limit

    actions = await _run(enforcer, clock, 120.0, 0.5, cpu=value, cpu_sigma=sigma)

    acted = [(elapsed, action) for elapsed, action in actions if action is not EnforcementAction.NONE]
    assert acted[0][1] is EnforcementAction.WARN
    assert {action for _, action in acted[1:]} == {EnforcementAction.THROTTLE_WORKFLOW}
    interval = BUDGET.warning_grace_seconds * (1.0 + sigma / value)
    times = [elapsed for elapsed, _ in acted]
    assert all(later - earlier >= interval for earlier, later in zip(times, times[1:]))
    assert recorder.kills == []


@pytest.mark.asyncio
async def test_lost_kill_is_retried_once_then_the_worker_is_evicted() -> None:
    recorder, clock = Recorder(kill_succeeds=False), SteppedClock()
    enforcer = _enforcer(recorder, clock)

    actions = await _run(enforcer, clock, 60.0, 0.5, cpu=LIMIT_CPU * 1.5, cpu_sigma=5.0)

    escalation = [action for _, action in actions if action is not EnforcementAction.NONE]
    assert escalation == [EnforcementAction.WARN, EnforcementAction.EVICT_WORKER]
    assert len(recorder.kills) == MAX_KILL_ATTEMPTS
    assert recorder.evictions == [("worker-1", CPU)]
    assert enforcer.tracked_violation_count == 0


@pytest.mark.asyncio
async def test_ignored_kill_is_retried_once_then_the_worker_is_evicted() -> None:
    recorder, clock = Recorder(kill_succeeds=True), SteppedClock()
    enforcer = _enforcer(recorder, clock)

    actions = await _run(enforcer, clock, 60.0, 0.5, cpu=LIMIT_CPU * 1.5, cpu_sigma=5.0)

    escalation = [action for _, action in actions if action is not EnforcementAction.NONE]
    assert escalation == [
        EnforcementAction.WARN,
        EnforcementAction.KILL_WORKFLOW,
        EnforcementAction.KILL_WORKFLOW,
        EnforcementAction.EVICT_WORKER,
    ]
    kill_times = [elapsed for elapsed, action in actions if action is EnforcementAction.KILL_WORKFLOW]
    assert kill_times[1] - kill_times[0] >= BUDGET.kill_grace_seconds


@pytest.mark.asyncio
async def test_recovery_under_the_warning_threshold_ends_the_violation() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)

    await _run(enforcer, clock, 12.0, 0.5, cpu=LIMIT_CPU * 0.9)
    await _check(enforcer, cpu=LIMIT_CPU * 0.5)
    assert enforcer.tracked_violation_count == 0
    actions = await _run(enforcer, clock, 12.0, 0.5, cpu=LIMIT_CPU * 0.9)

    assert len(recorder.warnings) == 2
    assert [action for _, action in actions].count(EnforcementAction.WARN) == 1


@pytest.mark.asyncio
async def test_memory_is_judged_when_cpu_is_within_budget() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)

    await _run(enforcer, clock, 30.0, 0.5, cpu=1.0, memory=LIMIT_MEMORY * 1.5, memory_sigma=1.0)

    assert recorder.kills and recorder.kills[0][3] is ResourceViolationType.MEMORY_EXCEEDED


@pytest.mark.asyncio
async def test_job_budget_overrides_the_default_and_releases_cleanly() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)
    enforcer.assign_budget(
        "job-strict",
        ResourceBudget(
            max_cpu_percent=100.0,
            max_memory_bytes=LIMIT_MEMORY,
            warning_threshold=0.8,
            throttle_threshold=0.85,
            kill_threshold=1.0,
            warning_grace_seconds=10.0,
            kill_grace_seconds=2.0,
        ),
    )

    await _run(enforcer, clock, 30.0, 0.5, cpu=200.0, cpu_sigma=1.0, job_id="job-strict")
    assert recorder.kills, "200% CPU is over the strict job's 100% budget"

    enforcer.release_workflow("wf-1")
    enforcer.release_job("job-strict")
    assert enforcer.tracked_violation_count == 0
    assert await _check(enforcer, cpu=200.0, job_id="job-strict") is EnforcementAction.NONE


# ------------------------------------------------------------------ THROTTLE


@pytest.mark.asyncio
async def test_sustained_throttle_zone_usage_is_throttled_toward_the_throttle_line() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)
    value = LIMIT_CPU * 0.9

    actions = await _run(enforcer, clock, 30.0, 0.5, cpu=value)

    acted = [(elapsed, action) for elapsed, action in actions if action is not EnforcementAction.NONE]
    assert acted == [
        (BUDGET.warning_grace_seconds, EnforcementAction.WARN),
        (2 * BUDGET.warning_grace_seconds, EnforcementAction.THROTTLE_WORKFLOW),
        (3 * BUDGET.warning_grace_seconds, EnforcementAction.THROTTLE_WORKFLOW),
    ]
    expected_scale = (LIMIT_CPU * BUDGET.throttle_threshold) / value
    assert [scale for *_, scale in recorder.throttles] == [expected_scale, expected_scale]
    assert enforcer.throttled_workflow_count == 1


@pytest.mark.asyncio
async def test_usage_between_warning_and_throttle_lines_is_only_warned() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)

    await _run(enforcer, clock, 60.0, 0.5, cpu=LIMIT_CPU * 0.82)

    assert len(recorder.warnings) == 1
    assert recorder.throttles == [] and recorder.kills == []


@pytest.mark.asyncio
async def test_falling_back_under_the_warning_line_releases_the_throttle_once() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)
    await _run(enforcer, clock, 20.0, 0.5, cpu=LIMIT_CPU * 0.9)
    assert len(recorder.throttles) == 1

    await _run(enforcer, clock, 5.0, 0.5, cpu=LIMIT_CPU * 0.5)

    assert recorder.releases == [("wf-1", "worker-1", "job-1")]
    assert enforcer.throttled_workflow_count == 0
    assert enforcer.tracked_violation_count == 0


@pytest.mark.asyncio
async def test_a_lost_release_is_retried_until_the_worker_answers() -> None:
    recorder, clock = Recorder(release_succeeds=False), SteppedClock()
    enforcer = _enforcer(recorder, clock)
    await _run(enforcer, clock, 20.0, 0.5, cpu=LIMIT_CPU * 0.9)

    await _run(enforcer, clock, 1.0, 0.5, cpu=LIMIT_CPU * 0.5)
    assert len(recorder.releases) == 3 and enforcer.throttled_workflow_count == 1

    recorder.release_succeeds = True
    await _check(enforcer, cpu=LIMIT_CPU * 0.5)
    await _check(enforcer, cpu=LIMIT_CPU * 0.5)
    assert len(recorder.releases) == 4 and enforcer.throttled_workflow_count == 0


@pytest.mark.asyncio
async def test_a_workflow_throttled_on_two_resources_is_released_only_when_both_recover() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)
    over = {"cpu": LIMIT_CPU * 0.9, "memory": LIMIT_MEMORY * 0.9}
    # CPU is checked first and returns early on action, so drive both
    # resources' violations long enough that each is throttled.
    await _run(enforcer, clock, 60.0, 0.5, **over)
    assert enforcer.throttled_workflow_count == 1

    await _run(enforcer, clock, 1.0, 0.5, cpu=LIMIT_CPU * 0.5, memory=LIMIT_MEMORY * 0.9)
    assert recorder.releases == []

    await _check(enforcer, cpu=LIMIT_CPU * 0.5, memory=LIMIT_MEMORY * 0.5)
    assert len(recorder.releases) == 1


@pytest.mark.asyncio
async def test_an_unappliable_throttle_still_escalates_to_kill() -> None:
    """An ACTION workflow has no concurrency to cut: its worker reports the
    throttle unapplied, nothing is recorded as throttled, and a certain
    violation is still killed."""
    recorder, clock = Recorder(throttle_succeeds=False), SteppedClock()
    enforcer = _enforcer(recorder, clock)

    actions = await _run(enforcer, clock, 30.0, 0.5, cpu=LIMIT_CPU * 1.5)

    assert EnforcementAction.KILL_WORKFLOW in [action for _, action in actions]
    assert enforcer.throttled_workflow_count == 0


@pytest.mark.asyncio
async def test_a_finished_workflow_leaves_no_throttle_state() -> None:
    recorder, clock = Recorder(), SteppedClock()
    enforcer = _enforcer(recorder, clock)
    await _run(enforcer, clock, 20.0, 0.5, cpu=LIMIT_CPU * 0.9)

    enforcer.release_workflow("wf-1")

    assert enforcer.throttled_workflow_count == 0 and enforcer.tracked_violation_count == 0
    assert recorder.releases == []
