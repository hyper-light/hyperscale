"""
Client retry pacing (AD-21, AD-24, AD-32).

A node that refuses a submission with ``retry_after_seconds`` (a shed
submission carries one OVERLOAD_SAMPLE_INTERVAL_SECONDS, a gate-replication
quorum refusal one standard gate TCP timeout, a rate limit its token
refill time, a manager without a known leader the time until its election
next decides) must not see the client's next attempt before the hint has
passed. A refusal with no hint, and a failed exchange, back off from the
RFC 6298 retransmission timeout of the round trips measured so far --
never below CLIENT_RETRANSMISSION_TIMEOUT_MIN_SECONDS -- doubling per
un-hinted back-off. Time runs on a stepped clock: each sleep advances it by
its length, so the gap between two sends is exactly the back-off the
client chose.
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from hyperscale.distributed.discovery import DiscoveryConfig, DiscoveryService
from hyperscale.distributed.env import Env
from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKeyGenerator
from hyperscale.distributed.jobs.logical_id_generator import LogicalIdGenerator
from hyperscale.distributed.models import JobAck, JobCancelResponse, RateLimitResponse
from hyperscale.distributed.nodes.client import cancellation as cancellation_module
from hyperscale.distributed.nodes.client import submission as submission_module
from hyperscale.distributed.nodes.client.cancellation import ClientCancellationManager
from hyperscale.distributed.nodes.client.models.client_config import ClientConfig
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
    """Answers each send with the next scripted reply, ``round_trip_seconds``
    after it, and records when it was sent."""

    def __init__(self, clock: SteppedClock, replies: list[bytes], round_trip_seconds: float = 0.0) -> None:
        self._clock = clock
        self._replies = replies
        self._round_trip_seconds = round_trip_seconds
        self.send_times: list[float] = []

    async def send_tcp(
        self,
        target: tuple[str, int],
        action: str,
        payload: bytes,
        timeout: float,
    ) -> tuple[bytes, None]:
        self.send_times.append(self._clock.now)
        self._clock.now += self._round_trip_seconds
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
        DiscoveryConfig.from_env(env, node_role="client", static_seeds=[], allow_dynamic_registration=True),
        Logger(),
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
    """
    With no hint, over a path whose round trips are far below it, the first back-off is RFC 6298's minimum
    retransmission timeout, equal-jittered.
    """
    env = Env()
    clock = install_time(monkeypatch, submission_module, 0.5)
    unhinted_refusal = JobAck(job_id="job-retry-hint", accepted=False, error="syncing").dump()
    transport = RecordingTransport(clock, [unhinted_refusal, accepted_ack()])

    await submit_one_job(make_submitter(env, transport))

    assert make_config(env).retry_base_delay_seconds == env.CLIENT_RETRANSMISSION_TIMEOUT_MIN_SECONDS
    assert transport.gap_between_sends() == pytest.approx(env.CLIENT_RETRANSMISSION_TIMEOUT_MIN_SECONDS)


@pytest.mark.asyncio
async def test_slow_round_trips_raise_the_unhinted_base(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    A round trip R above the minimum sets the retransmission timeout to R + 4 * R/2 = 3R (RFC 6298 sections 2.2,
    2.3): the client waits that, not the minimum, before asking again -- sooner would duplicate an exchange the
    path takes that long to complete.
    """
    env = Env()
    round_trip_seconds = 2 * env.CLIENT_RETRANSMISSION_TIMEOUT_MIN_SECONDS
    clock = install_time(monkeypatch, submission_module, 0.5)
    unhinted_refusal = JobAck(job_id="job-retry-hint", accepted=False, error="syncing").dump()
    transport = RecordingTransport(clock, [unhinted_refusal, accepted_ack()], round_trip_seconds)

    await submit_one_job(make_submitter(env, transport))

    assert transport.gap_between_sends() == pytest.approx(round_trip_seconds + 3 * round_trip_seconds)


@pytest.mark.asyncio
async def test_a_hinted_wait_does_not_double_the_next_unhinted_back_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    A manager without a known leader says when its election next decides; when the client comes back to a
    refusal no server can time (no worker registered yet), it backs off from the base -- the hinted wait was the
    server's schedule, not a failed attempt to back off from.
    """
    env = Env()
    election_hint_seconds = env.LEADER_ELECTION_TIMEOUT_JITTER
    clock = install_time(monkeypatch, submission_module, 0.5)
    leader_unknown = JobAck(
        job_id="job-retry-hint",
        accepted=False,
        error="Not DC leader, retry at leader: unknown",
        retry_after_seconds=election_hint_seconds,
    ).dump()
    no_capacity = JobAck(
        job_id="job-retry-hint",
        accepted=False,
        error="No workers registered in this datacenter; rejecting job submission",
    ).dump()
    transport = RecordingTransport(clock, [leader_unknown, no_capacity, accepted_ack()])

    await submit_one_job(make_submitter(env, transport))

    first_gap, second_gap = (
        later_send - earlier_send
        for earlier_send, later_send in zip(transport.send_times, transport.send_times[1:])
    )
    assert first_gap == pytest.approx(election_hint_seconds * 1.5)
    assert second_gap == pytest.approx(env.CLIENT_RETRANSMISSION_TIMEOUT_MIN_SECONDS)


@pytest.mark.asyncio
async def test_the_final_unhinted_refusal_fails_without_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    """No attempt follows the last one and the server gave no hint, so the failure is raised at once."""
    env = Env()
    clock = install_time(monkeypatch, submission_module, 0.0)
    attempts = env.CLIENT_SUBMISSION_MAX_RETRIES + 1
    unhinted_refusal = JobAck(job_id="job-retry-hint", accepted=False, error="syncing").dump()
    transport = RecordingTransport(clock, [unhinted_refusal] * attempts)

    with pytest.raises(RuntimeError, match="Job submission failed"):
        await submit_one_job(make_submitter(env, transport))

    assert len(transport.send_times) == attempts
    assert clock.now == transport.send_times[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize("random_value", [0.0, 0.5, 0.999])
@pytest.mark.parametrize("hinted_refusal", ["shed", "rate_limited"])
async def test_a_caller_resubmitting_after_the_final_hinted_refusal_waits_out_the_hint(
    monkeypatch: pytest.MonkeyPatch,
    random_value: float,
    hinted_refusal: str,
) -> None:
    """
    Regression (rejection storm): a caller that calls ``submit_job`` again as soon as one fails -- the storm's
    back-to-back submit loop -- must not reach the server inside the hint its last refused attempt carried.
    The final hinted refusal is waited out before the failure is raised.
    """
    env = Env()
    retry_after_seconds = env.OVERLOAD_SAMPLE_INTERVAL_SECONDS
    refusal = (
        shed_refusal(retry_after_seconds)
        if hinted_refusal == "shed"
        else RateLimitResponse(operation="job_submission", retry_after_seconds=retry_after_seconds).dump()
    )
    clock = install_time(monkeypatch, submission_module, random_value)
    attempts = env.CLIENT_SUBMISSION_MAX_RETRIES + 1
    transport = RecordingTransport(clock, [refusal] * attempts + [accepted_ack()])
    submitter = make_submitter(env, transport)

    with pytest.raises(RuntimeError, match="Job submission failed"):
        await submit_one_job(submitter)
    await submit_one_job(submitter)

    assert len(transport.send_times) == attempts + 1
    gaps = [
        later_send - earlier_send
        for earlier_send, later_send in zip(transport.send_times, transport.send_times[1:])
    ]
    assert all(retry_after_seconds <= gap < 2 * retry_after_seconds for gap in gaps), gaps


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
