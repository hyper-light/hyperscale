"""
AD-11: state sync retries a target that refuses, or answers that it is
still starting, with backoff -- and does not re-spend a budget on one that
timed out.

A peer restarting refuses connections for a moment; the sync used to try
each target once, so a cluster-leader takeover that met a restarting peer
went without its state. A target that times out has already spent a whole
request budget unresponsive -- retrying it would only stretch the
takeover, which syncs its peers in turn -- so it gets one attempt and
SWIM's verdict governs.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import StateSyncResponse
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.nodes.manager.sync import ManagerStateSync
from hyperscale.distributed.slo import SLOConfig

PEER = ("10.0.0.2", 9000)
SYNC_RETRIES = Env().MANAGER_STATE_SYNC_RETRIES
# Short, so the derived backoffs (spanning one timeout) keep the test fast.
SYNC_TIMEOUT_SECONDS = 0.07


class SilentLogger:
    async def log(self, entry) -> None:
        return None


# A failure is an exception the transport returns, or NOT_READY: an answer
# with ``responder_ready=False``.
NOT_READY = "not-ready"


async def peer_sync_attempts(failures: list[Exception | str]) -> tuple[int, int]:
    """Sync from one peer that fails as given, then answers ready; returns
    the attempts made and how many failures preceded the answer."""
    state = ManagerState(slo_config=SLOConfig.from_env(Env()))
    await state.add_active_peer(PEER, "manager-b")
    attempts = 0

    async def send_tcp(addr, action, payload, timeout):
        nonlocal attempts
        attempts += 1
        failure = failures[attempts - 1] if attempts <= len(failures) else None
        if isinstance(failure, Exception):
            return (failure, 0)
        return (
            StateSyncResponse(responder_id="manager-b", current_version=0, responder_ready=failure != NOT_READY).dump(),
            0,
        )

    state_sync = ManagerStateSync(
        state=state,
        config=SimpleNamespace(
            cluster_id="hyperscale",
            environment_id="default",
            datacenter_id="dc-east",
            state_sync_retries=SYNC_RETRIES,
            state_sync_timeout_seconds=SYNC_TIMEOUT_SECONDS,
        ),
        registry=None,
        leases=None,
        job_manager=SimpleNamespace(iter_jobs=lambda: []),
        logger=SilentLogger(),
        node_id=SimpleNamespace(full="manager-a-full", short="manager-a"),
        node_host="127.0.0.1",
        node_port=9000,
        task_runner=None,
        send_tcp=send_tcp,
        is_cluster_leader=lambda: True,
        get_current_term=lambda: 3,
        build_job_state_sync_message=lambda job_id, job: None,
        apply_job_state_sync_message=None,
        get_job_callback_addr=lambda job_id: None,
        validate_mtls_claims=None,
    )
    await state_sync.sync_state_from_manager_peers(force_full=True)
    return attempts, len(failures)


@pytest.mark.asyncio
async def test_a_refusing_peer_is_retried_until_it_answers() -> None:
    attempts, failures = await peer_sync_attempts([ConnectionRefusedError("restarting")] * SYNC_RETRIES)
    assert attempts == failures + 1


@pytest.mark.asyncio
async def test_a_peer_refusing_past_the_retries_is_given_up() -> None:
    attempts, _failures = await peer_sync_attempts([ConnectionRefusedError("down")] * (SYNC_RETRIES + 5))
    assert attempts == SYNC_RETRIES + 1


@pytest.mark.asyncio
async def test_a_peer_that_timed_out_is_not_retried() -> None:
    attempts, _failures = await peer_sync_attempts([TimeoutError("unresponsive")])
    assert attempts == 1


@pytest.mark.asyncio
async def test_a_peer_still_starting_is_retried_until_ready() -> None:
    attempts, failures = await peer_sync_attempts([NOT_READY, ConnectionResetError("restarting"), NOT_READY])
    assert attempts == failures + 1
