"""
D-5: the manager's AD-42 dispatch round trips, kept per worker as well as
per datacenter.

Every dispatch's round trip feeds the datacenter digest (the SLO summary a
gate's health classification and routing read) and, while the worker it
timed is registered, that worker's own digest (the metrics surface: which
worker answers slowly):

* each worker's observation holds only its own samples; the datacenter's
  holds all of them;
* a worker's digest opens when it registers, survives a re-registration,
  and closes when it unregisters -- a sample arriving after that counts
  toward the datacenter only and opens nothing (churn leaves no digest
  behind);
* a worker whose windows all aged out reports no observation.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.manager.models.manager_config import ManagerConfig
from hyperscale.distributed.nodes.manager.registry import ManagerRegistry
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.slo import SLOConfig

RECORDED_AT = 1000.0
FAST_WORKER_LATENCIES_MS = [10.0, 11.0, 12.0]
SLOW_WORKER_LATENCIES_MS = [400.0, 410.0, 420.0, 430.0]
CHURNED_WORKER_COUNT = 64


def make_registration(worker_id: str, port: int) -> MagicMock:
    registration = MagicMock()
    registration.node.node_id = worker_id
    registration.node.host = "10.0.0.7"
    registration.node.port = port
    registration.node.udp_port = port + 1
    registration.total_cores = 2
    return registration


def make_registry() -> tuple[ManagerState, ManagerRegistry, SLOConfig]:
    slo_config = SLOConfig.from_env(Env())
    state = ManagerState(slo_config=slo_config)
    state.initialize_locks()
    logger = MagicMock()
    logger.log = AsyncMock()
    registry = ManagerRegistry(
        state=state,
        config=ManagerConfig(host="127.0.0.1", tcp_port=8000, udp_port=8001, datacenter_id="dc-test"),
        logger=logger,
        node_id="manager-1",
        task_runner=MagicMock(),
        on_worker_unregistered=lambda worker_id: None,
    )
    return state, registry, slo_config


def record_all(state: ManagerState, worker_id: str, latencies_ms: list[float]) -> None:
    for latency_ms in latencies_ms:
        state.record_dispatch_latency(worker_id, latency_ms, RECORDED_AT)


@pytest.mark.asyncio
async def test_each_worker_observes_only_its_own_round_trips() -> None:
    state, registry, _slo_config = make_registry()
    await registry.register_worker(make_registration("worker-fast", 9100))
    await registry.register_worker(make_registration("worker-slow", 9200))

    record_all(state, "worker-fast", FAST_WORKER_LATENCIES_MS)
    record_all(state, "worker-slow", SLOW_WORKER_LATENCIES_MS)

    observations = state.get_worker_dispatch_latency_observations(RECORDED_AT)
    assert set(observations) == {"worker-fast", "worker-slow"}
    fast, slow = observations["worker-fast"], observations["worker-slow"]
    assert fast.sample_count == len(FAST_WORKER_LATENCIES_MS)
    assert slow.sample_count == len(SLOW_WORKER_LATENCIES_MS)
    assert min(FAST_WORKER_LATENCIES_MS) <= fast.p50_ms <= fast.p99_ms <= max(FAST_WORKER_LATENCIES_MS)
    assert min(SLOW_WORKER_LATENCIES_MS) <= slow.p50_ms <= slow.p99_ms <= max(SLOW_WORKER_LATENCIES_MS)
    datacenter = state.get_dispatch_latency_observation(RECORDED_AT)
    assert datacenter is not None
    assert datacenter.sample_count == len(FAST_WORKER_LATENCIES_MS) + len(SLOW_WORKER_LATENCIES_MS)


@pytest.mark.asyncio
async def test_an_unregistered_workers_digest_closes_and_late_samples_open_nothing() -> None:
    state, registry, _slo_config = make_registry()
    await registry.register_worker(make_registration("worker-gone", 9100))
    record_all(state, "worker-gone", FAST_WORKER_LATENCIES_MS)

    registry.unregister_worker("worker-gone")
    # A dispatch in flight when the worker left answers afterwards.
    state.record_dispatch_latency("worker-gone", SLOW_WORKER_LATENCIES_MS[0], RECORDED_AT)

    assert state._worker_dispatch_latency_digests == {}
    assert state.get_worker_dispatch_latency_observations(RECORDED_AT) == {}
    datacenter = state.get_dispatch_latency_observation(RECORDED_AT)
    assert datacenter is not None
    assert datacenter.sample_count == len(FAST_WORKER_LATENCIES_MS) + 1


@pytest.mark.asyncio
async def test_a_re_registration_keeps_the_workers_history() -> None:
    state, registry, _slo_config = make_registry()
    registration = make_registration("worker-1", 9100)
    await registry.register_worker(registration)
    record_all(state, "worker-1", FAST_WORKER_LATENCIES_MS)

    await registry.register_worker(registration)

    observation = state.get_worker_dispatch_latency_observations(RECORDED_AT)["worker-1"]
    assert observation.sample_count == len(FAST_WORKER_LATENCIES_MS)


@pytest.mark.asyncio
async def test_worker_churn_leaves_no_digest_behind() -> None:
    state, registry, _slo_config = make_registry()
    for worker_index in range(CHURNED_WORKER_COUNT):
        worker_id = f"worker-{worker_index}"
        await registry.register_worker(make_registration(worker_id, 9100 + 2 * worker_index))
        record_all(state, worker_id, FAST_WORKER_LATENCIES_MS)
        registry.unregister_worker(worker_id)

    assert state._worker_dispatch_latency_digests == {}
    datacenter = state.get_dispatch_latency_observation(RECORDED_AT)
    assert datacenter is not None
    assert datacenter.sample_count == CHURNED_WORKER_COUNT * len(FAST_WORKER_LATENCIES_MS)


@pytest.mark.asyncio
async def test_a_worker_whose_windows_aged_out_reports_no_observation() -> None:
    state, registry, slo_config = make_registry()
    await registry.register_worker(make_registration("worker-1", 9100))
    record_all(state, "worker-1", FAST_WORKER_LATENCIES_MS)
    retention_seconds = slo_config.window_duration_seconds * (slo_config.max_windows + 1)

    assert state.get_worker_dispatch_latency_observations(RECORDED_AT + retention_seconds) == {}
