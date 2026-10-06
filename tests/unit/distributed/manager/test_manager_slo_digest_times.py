"""
AD-42 dispatch latency digest, read on the manager's clock.

The digest windowed its samples and aged its windows on a module-global
clock rather than the manager's injected one, so a manager on any other
clock (a SIM's virtual clock, a test's stepped clock) mixed two time
axes. The manager now passes its own clock readings.

* samples land in the window of the time the manager recorded them;
* the summary keeps only windows recent at the manager's time, so a
  digest whose windows have all aged out summarizes as empty;
* an observation is stale relative to the time it is judged at.
"""

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.slo import LatencyObservation, SLOConfig

RECORDED_AT = 1000.0
LATENCIES_MS = [12.0, 15.0, 18.0]


def make_state() -> tuple[ManagerState, SLOConfig]:
    slo_config = SLOConfig.from_env(Env())
    return ManagerState(slo_config=slo_config), slo_config


def test_samples_land_in_the_window_of_the_managers_time() -> None:
    state, _ = make_state()
    for latency_ms in LATENCIES_MS:
        state.record_dispatch_latency(latency_ms, RECORDED_AT)

    observation = state.get_dispatch_latency_observation(RECORDED_AT)

    assert observation is not None
    assert observation.sample_count == len(LATENCIES_MS)
    assert observation.window_start <= RECORDED_AT < observation.window_end
    assert state.get_slo_summary(RECORDED_AT).sample_count == len(LATENCIES_MS)


def test_windows_aged_out_at_the_managers_time_summarize_as_empty() -> None:
    state, slo_config = make_state()
    for latency_ms in LATENCIES_MS:
        state.record_dispatch_latency(latency_ms, RECORDED_AT)
    retention_seconds = slo_config.window_duration_seconds * (slo_config.max_windows + 1)

    aged_out_at = RECORDED_AT + retention_seconds

    assert state.get_dispatch_latency_observation(aged_out_at) is None
    assert state.get_slo_summary(aged_out_at).sample_count == 0


def test_an_observation_is_stale_relative_to_the_given_time() -> None:
    observation = LatencyObservation(
        target_id="dispatches",
        p50_ms=10.0,
        p95_ms=20.0,
        p99_ms=30.0,
        sample_count=3,
        window_start=RECORDED_AT - 60.0,
        window_end=RECORDED_AT,
    )

    assert observation.is_stale(max_age_seconds=30.0, now=RECORDED_AT + 30.0) is False
    assert observation.is_stale(max_age_seconds=30.0, now=RECORDED_AT + 31.0) is True
