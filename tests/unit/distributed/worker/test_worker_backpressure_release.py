"""AD-23/AD-37: a worker's backpressure releases when its manager's clears.

At base commit 2e6d0532, ``WorkerProgressReporter._apply_ack_backpressure``
(``nodes/worker/worker_progress_reporter.py:1168-1180``) applied an ack only
when ``backpressure_level > 0`` and kept the delay as a running maximum, so a
manager's NONE was dropped and the worker stayed at its peak level and delay
until restart. AD-37's worker state diagram returns to NO_BACKPRESSURE once
the level falls below THROTTLE.

The worker's level and delay are the maxima over the managers' current
signals; a reaped manager's last signal no longer counts.

A signal holds only for the backoff its level imposes (RFC 9293 section
3.8.6.1, the persist timer): under REJECT the worker drops the progress
whose acks would clear it, so a REJECT held until renewed never lapsed --
in a Kubernetes run an idle worker kept the previous job's peak REJECT,
reported itself overloaded, and the next job never ran.
"""

import pytest

from unittest.mock import AsyncMock, MagicMock

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import WorkflowProgressAck
from hyperscale.distributed.nodes.worker.models.worker_config import WorkerConfig
from hyperscale.distributed.nodes.worker import state as worker_state_module
from hyperscale.distributed.nodes.worker.backpressure import WorkerBackpressureManager
from hyperscale.distributed.nodes.worker.progress import WorkerProgressReporter
from hyperscale.distributed.nodes.worker.registry import WorkerRegistry
from hyperscale.distributed.nodes.worker.state import WorkerState
from hyperscale.distributed.reliability.backpressure_level import BackpressureLevel
from hyperscale.distributed.reliability.backpressure_signal import BackpressureSignal


def backpressure_hold_policy(env: Env):
    """The worker's hold policy for a manager's signal, configured from Env."""
    return WorkerBackpressureManager(
        state=None,
        throttle_delay_ms=env.WORKER_BACKPRESSURE_THROTTLE_DELAY_MS,
        batch_delay_ms=env.WORKER_BACKPRESSURE_BATCH_DELAY_MS,
        reject_delay_ms=env.WORKER_BACKPRESSURE_REJECT_DELAY_MS,
    ).hold_seconds


class SteppedClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> SteppedClock:
    stepped_clock = SteppedClock()
    monkeypatch.setattr(worker_state_module, "_DEFAULT_CLOCK", stepped_clock)
    return stepped_clock


def make_reporter() -> tuple[WorkerProgressReporter, WorkerRegistry, WorkerState]:
    logger = MagicMock()
    logger.log = AsyncMock()
    state = WorkerState(
        core_allocator=MagicMock(),
        throughput_interval_seconds=Env().WORKER_THROUGHPUT_INTERVAL_SECONDS,
        completion_times_max_samples=Env().WORKER_COMPLETION_TIMES_MAX_SAMPLES,
    )
    registry = WorkerRegistry(
        logger,
        circuit_breaker_config=Env().get_circuit_breaker_config(),
        select_manager=lambda manager_ids: None,
        forget_manager_backpressure=state.remove_manager_backpressure,
    )
    config = WorkerConfig.from_env(env=Env(), host="127.0.0.1", tcp_port=9000, udp_port=9001)
    reporter = WorkerProgressReporter(
        registry=registry,
        state=state,
        config=config,
        backpressure_hold_seconds=backpressure_hold_policy(Env()),
        logger=logger,
    )
    return reporter, registry, state


def ack_at(manager_id: str, level: BackpressureLevel) -> bytes:
    """The ack a manager sends at ``level``, built as the manager builds it."""
    signal = BackpressureSignal.from_level(level)
    return WorkflowProgressAck(
        manager_id=manager_id,
        is_leader=False,
        healthy_managers=[],
        backpressure_level=signal.level.value,
        backpressure_delay_ms=signal.delay_ms,
        backpressure_batch_only=signal.batch_only,
    ).dump()


def test_reject_releases_when_the_manager_signals_none() -> None:
    reporter, _, state = make_reporter()

    reporter._process_ack(ack_at("manager-1", BackpressureLevel.REJECT))
    assert state.get_max_backpressure_level() == BackpressureLevel.REJECT
    assert state.get_backpressure_delay_ms() == 1000

    reporter._process_ack(ack_at("manager-1", BackpressureLevel.NONE))

    assert state.get_max_backpressure_level() == BackpressureLevel.NONE
    assert state.get_backpressure_delay_ms() == 0


def test_delay_follows_the_managers_current_signals() -> None:
    reporter, _, state = make_reporter()
    reporter._process_ack(ack_at("manager-1", BackpressureLevel.BATCH))
    reporter._process_ack(ack_at("manager-2", BackpressureLevel.THROTTLE))
    assert (state.get_max_backpressure_level(), state.get_backpressure_delay_ms()) == (BackpressureLevel.BATCH, 500)

    reporter._process_ack(ack_at("manager-1", BackpressureLevel.NONE))

    assert (state.get_max_backpressure_level(), state.get_backpressure_delay_ms()) == (BackpressureLevel.THROTTLE, 100)


def test_a_reaped_managers_signal_no_longer_counts() -> None:
    reporter, registry, state = make_reporter()
    reporter._process_ack(ack_at("manager-1", BackpressureLevel.REJECT))
    reporter._process_ack(ack_at("manager-2", BackpressureLevel.THROTTLE))

    registry.remove_manager_state("manager-1", None)

    assert (state.get_max_backpressure_level(), state.get_backpressure_delay_ms()) == (BackpressureLevel.THROTTLE, 100)
    assert set(state._manager_backpressure) == {"manager-2"}


def test_a_reject_lapses_after_its_backoff_so_the_worker_sends_again(clock: SteppedClock) -> None:
    """Unrenewed, a manager's REJECT holds exactly for the backoff it
    imposes, then lapses; an ack renews it for another backoff."""
    reporter, _, state = make_reporter()
    hold = backpressure_hold_policy(Env())(BackpressureLevel.REJECT, BackpressureSignal.from_level(BackpressureLevel.REJECT).delay_ms)
    assert hold > 0

    reporter._process_ack(ack_at("manager-1", BackpressureLevel.REJECT))
    clock.now += hold * 0.999
    assert state.get_max_backpressure_level() == BackpressureLevel.REJECT

    clock.now += hold * 0.002
    assert state.get_max_backpressure_level() == BackpressureLevel.NONE
    assert state.get_backpressure_delay_ms() == 0

    reporter._process_ack(ack_at("manager-1", BackpressureLevel.REJECT))
    clock.now += hold * 0.999
    assert state.get_max_backpressure_level() == BackpressureLevel.REJECT


def test_a_lapsed_signal_leaves_the_others_holding(clock: SteppedClock) -> None:
    reporter, _, state = make_reporter()
    hold_policy = backpressure_hold_policy(Env())
    reject_hold = hold_policy(BackpressureLevel.REJECT, BackpressureSignal.from_level(BackpressureLevel.REJECT).delay_ms)
    throttle_hold = hold_policy(BackpressureLevel.THROTTLE, BackpressureSignal.from_level(BackpressureLevel.THROTTLE).delay_ms)
    assert throttle_hold < reject_hold

    reporter._process_ack(ack_at("manager-1", BackpressureLevel.REJECT))
    reporter._process_ack(ack_at("manager-2", BackpressureLevel.THROTTLE))
    clock.now += (throttle_hold + reject_hold) / 2

    # Past THROTTLE's backoff, inside REJECT's.
    assert state.get_max_backpressure_level() == BackpressureLevel.REJECT
    clock.now += reject_hold
    assert state.get_max_backpressure_level() == BackpressureLevel.NONE
