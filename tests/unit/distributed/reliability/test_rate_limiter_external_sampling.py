"""
Health-gated rate limiting (AD-24) over a detector the node's resource
sampler owns.

The limiter sampled its overload detector with no resource readings
(zero CPU and memory) on every check; on a detector the node's sampler
also feeds, those samples counted toward its de-escalation hysteresis --
measured: after a CPU-overload sample, 100 checks admitted 99 and left
the detector HEALTHY. Manager and gate now hand their limiter the
detector their sampler feeds, marked externally sampled: checks read
the state it settled on.

* an overloaded node keeps refusing non-critical requests through a
  burst, and its detector stays overloaded;
* critical requests are never refused;
* a standalone limiter (default) still samples per check.
"""

import pytest

from hyperscale.distributed.reliability.overload import HybridOverloadDetector, OverloadState
from hyperscale.distributed.reliability.priority import RequestPriority
from hyperscale.distributed.reliability.rate_limiting import AdaptiveRateLimiter, ServerRateLimiter

QUIET_LATENCY_MS = 10.0
LATENCY_SAMPLES = 200
OVERLOADED_CPU_PERCENT = 99.0
REQUEST_BURST = 100


def cpu_overloaded_detector() -> HybridOverloadDetector:
    detector = HybridOverloadDetector()
    for _ in range(LATENCY_SAMPLES):
        detector.record_latency(QUIET_LATENCY_MS)
    detector.get_state(OVERLOADED_CPU_PERCENT, 0.0)  # the resource sampler's tick
    return detector


@pytest.mark.asyncio
async def test_a_burst_does_not_talk_an_overloaded_node_out_of_its_limits() -> None:
    detector = cpu_overloaded_detector()
    limiter = AdaptiveRateLimiter(detector, detector_sampled_externally=True)

    admitted = [
        (await limiter.check(f"client-{index}", "job_submit", RequestPriority.NORMAL)).allowed
        for index in range(REQUEST_BURST)
    ]

    assert not any(admitted)
    assert detector.current_state is OverloadState.OVERLOADED


@pytest.mark.asyncio
async def test_critical_requests_are_never_refused_when_overloaded() -> None:
    limiter = AdaptiveRateLimiter(cpu_overloaded_detector(), detector_sampled_externally=True)
    result = await limiter.check("client-1", "job_submit", RequestPriority.CRITICAL)
    assert result.allowed


@pytest.mark.asyncio
async def test_the_server_limiter_passes_external_sampling_through() -> None:
    limiter = ServerRateLimiter(overload_detector=cpu_overloaded_detector(), detector_sampled_externally=True)
    result = await limiter.check_rate_limit("client-1", "job_submit")
    assert not result.allowed
