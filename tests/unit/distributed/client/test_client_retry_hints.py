"""
Client retry pacing (AD-21, AD-24, AD-32).

A node that refuses a submission with ``retry_after_seconds`` (a shed
submission carries one OVERLOAD_SAMPLE_INTERVAL_SECONDS, a gate-replication
quorum refusal one standard gate TCP timeout, a rate limit its token
refill time) must not see the client's next attempt before the hint has
passed. A refusal with no hint backs off from the configured base, which
is one overload sample interval. Time runs on a stepped clock: each sleep
advances it by its length, so the gap between two sends is exactly the
back-off the client chose.
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.env import Env
from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKeyGenerator
from hyperscale.distributed.jobs.logical_id_generator import LogicalIdGenerator
from hyperscale.distributed.models import JobAck, JobCancelResponse, RateLimitResponse
from hyperscale.distributed.nodes.client import cancellation as cancellation_module
from hyperscale.distributed.nodes.client import submission as submission_module
from hyperscale.distributed.nodes.client.cancellation import ClientCancellationManager
from hyperscale.distributed.nodes.client.config import ClientConfig
from hyperscale.distributed.nodes.client.protocol import ClientProtocol
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.submission import ClientJobSubmitter
from hyperscale.distributed.nodes.client.targets import ClientTargetSelector
from hyperscale.distributed.nodes.client.tracking import ClientJobTracker
from hyperscale.distributed.runtime import RealClock
from hyperscale.logging import Logger


class SteppedClock:
    """Each sleep advances the time by its length."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        await asyncio.sleep(0)


class FixedRandom:
    """A random source whose every draw is the same value in [0, 1)."""

    def __init__(self, value: float) -> None:
        self.value = value

    def random(self) -> float:
        return self.value


class RecordingTransport:
    """Answers each send with the next scripted reply and records when it was sent."""

    def __init__(self, clock: SteppedClock, replies: list[bytes]) -> None:
        self._clock = clock
        self._replies = replies
        self.send_times: list[float] = []

    async def send_tcp(
        self,
        target: tuple[str, int],
        action: str,
        payload: bytes,
        timeout: float,
    ) -> tuple[bytes, None]:
        self.send_times.append(self._clock.now)
        return (self._replies[len(self.send_times) - 1], None)

    def gap_between_sends(self) -> float:
        return self.send_times[1] - self.send_times[0]


def make_config(env: Env) -> ClientConfig:
    return ClientConfig.from_env(
        env=env,
        host="localhost",
        tcp_port=8000,
        managers=[("m1", 7000)],
        gates=[],
    )


def make_logger() -> Logger:
    logger = Mock(spec=Logger)
    logger.log = AsyncMock()
    return logger


def make_targets(env: Env, config: ClientConfig, state: ClientState) -> ClientTargetSelector:
    discovery = DiscoveryService(
        env.get_discovery_config(node_role="client", static_seeds=[], allow_dynamic_registration=True)
    )
    return ClientTargetSelector(config, state, discovery)


def make_tracker(env: Env, state: ClientState) -> ClientJobTracker:
    return ClientJobTracker(state, make_logger(), result_drain_timeout_seconds=env.CLIENT_RESULT_DRAIN_TIMEOUT)


def make_submitter(env: Env, transport: RecordingTransport) -> ClientJobSubmitter:
    config = make_config(env)
    state = ClientState()
    return ClientJobSubmitter(
        state,
        config,
        make_logger(),
        make_targets(env, config, state),
        make_tracker(env, state),
        ClientProtocol(state, make_logger()),
        transport.send_tcp,
        IdempotencyKeyGenerator(client_id="retry-hint-client"),
        LogicalIdGenerator(scope="retry-hint-client", clock=RealClock()),
    )


async def submit_one_job(submitter: ClientJobSubmitter) -> str:
    workflow = Mock()
    workflow.reporting = None
    return await submitter.submit_job([([], workflow)])


def install_time(monkeypatch: pytest.MonkeyPatch, module: object, random_value: float) -> SteppedClock:
    clock = SteppedClock()
    monkeypatch.setattr(module, "_DEFAULT_CLOCK", clock)
    monkeypatch.setattr(module, "_DEFAULT_RANDOM", FixedRandom(random_value))
    return clock


def shed_refusal(retry_after_seconds: float) -> bytes:
    return JobAck(
        job_id="",
        accepted=False,
        error="System under load (overloaded), please retry later",
        retry_after_seconds=retry_after_seconds,
    ).dump()


def accepted_ack() -> bytes:
    return JobAck(job_id="job-retry-hint", accepted=True).dump()


@pytest.mark.asyncio
@pytest.mark.parametrize("random_value", [0.0, 0.5, 0.999])
async def test_a_shed_submission_is_not_retried_before_its_hint(
    monkeypatch: pytest.MonkeyPatch,
    random_value: float,
) -> None:
    """The next attempt lands no sooner than the hint and within one hint of jitter after it."""
    env = Env()
    retry_after_seconds = env.OVERLOAD_SAMPLE_INTERVAL_SECONDS
    clock = install_time(monkeypatch, submission_module, random_value)
    transport = RecordingTransport(clock, [shed_refusal(retry_after_seconds), accepted_ack()])

    await submit_one_job(make_submitter(env, transport))

    assert len(transport.send_times) == 2
    assert retry_after_seconds <= transport.gap_between_sends() < 2 * retry_after_seconds


@pytest.mark.asyncio
async def test_a_quorum_refusal_hint_longer_than_the_backoff_is_honored(monkeypatch: pytest.MonkeyPatch) -> None:
    """A gate-replication quorum refusal is retried (its hint makes it retryable), not before the hint."""
    env = Env()
    retry_after_seconds = float(env.GATE_TCP_TIMEOUT_STANDARD)
    clock = install_time(monkeypatch, submission_module, 0.0)
    quorum_refusal = JobAck(
        job_id="job-retry-hint",
        accepted=False,
        error="gate_replication_quorum_unavailable",
        retry_after_seconds=retry_after_seconds,
    ).dump()
    transport = RecordingTransport(clock, [quorum_refusal, accepted_ack()])

    await submit_one_job(make_submitter(env, transport))

    assert transport.gap_between_sends() >= retry_after_seconds


@pytest.mark.asyncio
async def test_a_rate_limited_submission_is_not_retried_before_its_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    """A RateLimitResponse's hint paces the retry the same way, jittered at or above it."""
    env = Env()
    retry_after_seconds = env.OVERLOAD_SAMPLE_INTERVAL_SECONDS * 3
    clock = install_time(monkeypatch, submission_module, 0.25)
    rate_limited = RateLimitResponse(operation="job_submit", retry_after_seconds=retry_after_seconds).dump()
    transport = RecordingTransport(clock, [rate_limited, accepted_ack()])

    await submit_one_job(make_submitter(env, transport))

    assert transport.gap_between_sends() == pytest.approx(retry_after_seconds * 1.25)


@pytest.mark.asyncio
async def test_an_unhinted_refusal_backs_off_from_the_derived_base(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no hint the first back-off is the configured base (one overload sample interval), equal-jittered."""
    env = Env()
    clock = install_time(monkeypatch, submission_module, 0.5)
    unhinted_refusal = JobAck(job_id="job-retry-hint", accepted=False, error="syncing").dump()
    transport = RecordingTransport(clock, [unhinted_refusal, accepted_ack()])

    await submit_one_job(make_submitter(env, transport))

    assert make_config(env).retry_base_delay_seconds == env.OVERLOAD_SAMPLE_INTERVAL_SECONDS
    assert transport.gap_between_sends() == pytest.approx(env.OVERLOAD_SAMPLE_INTERVAL_SECONDS)


@pytest.mark.asyncio
async def test_the_final_hinted_refusal_fails_without_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    """No attempt follows the last one, so the client does not sleep out its hint before failing."""
    env = Env()
    clock = install_time(monkeypatch, submission_module, 0.0)
    attempts = env.CLIENT_SUBMISSION_MAX_RETRIES + 1
    retry_after_seconds = env.OVERLOAD_SAMPLE_INTERVAL_SECONDS
    transport = RecordingTransport(clock, [shed_refusal(retry_after_seconds)] * attempts)

    with pytest.raises(RuntimeError, match="Job submission failed"):
        await submit_one_job(make_submitter(env, transport))

    assert len(transport.send_times) == attempts
    assert clock.now == transport.send_times[-1]


@pytest.mark.asyncio
async def test_cancel_backs_off_from_the_derived_base(monkeypatch: pytest.MonkeyPatch) -> None:
    """A cancel sweep that met only transient refusals backs off from the configured base, not a literal."""
    env = Env()
    clock = install_time(monkeypatch, cancellation_module, 0.5)
    replies = [
        JobCancelResponse(job_id="cancel-retry-hint", success=False, error="syncing").dump(),
        JobCancelResponse(job_id="cancel-retry-hint", success=True, cancelled_workflow_count=1).dump(),
    ]
    transport = RecordingTransport(clock, replies)
    config = make_config(env)
    state = ClientState()
    tracker = make_tracker(env, state)
    tracker.initialize_job_tracking("cancel-retry-hint", expected_workflow_ids=frozenset())
    manager = ClientCancellationManager(
        state,
        config,
        make_logger(),
        make_targets(env, config, state),
        tracker,
        transport.send_tcp,
    )

    response = await manager.cancel_job("cancel-retry-hint")

    assert response.success is True
    assert transport.gap_between_sends() == pytest.approx(config.retry_base_delay_seconds)
