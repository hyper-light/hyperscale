"""
A gate's global-timeout cancellation reaches the job's managers unfenced.

The gate sent its own job fence -- its lease's counter -- as the cancel's
``fence_token``, while a manager refuses a cancel whose nonzero fence does
not EQUAL its own lease fence for the job, an unrelated counter. Almost
every timeout cancel was refused and the job ran on past its global
timeout. Cancels the gate forwards for clients go out unfenced; a cancel is
fail-safe and needs no leadership epoch to authorize it.

The manager's rule is applied to what the gate actually sends, for a gate
fence (7) that differs from the manager's (3).
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import CancelJob, JobCancelResponse
from hyperscale.distributed.nodes.gate.server import GateServer

GATE_FENCE = 7
MANAGER_FENCE = 3
MANAGER_ADDRESS = ("10.0.0.5", 9000)


def manager_accepts(fence_token: int, stored_fence: int) -> bool:
    """The manager's fence rule for a cancel (manager/cancellation.py)."""
    return not (fence_token > 0 and stored_fence != fence_token)


@pytest.mark.asyncio
async def test_a_timeout_cancel_passes_every_managers_fence_check() -> None:
    sent: list[tuple[tuple[str, int], str, bytes]] = []

    async def send_tcp(address, action, payload, timeout):
        sent.append((address, action, payload))
        cancel = CancelJob.load(payload)
        return (
            JobCancelResponse(
                job_id=cancel.job_id,
                success=manager_accepts(cancel.fence_token, MANAGER_FENCE),
            ).dump(),
            0,
        )

    logged: list[object] = []

    async def log(entry: object) -> None:
        logged.append(entry)

    gate = object.__new__(GateServer)
    gate._job_manager = SimpleNamespace(get_fence_token=lambda job_id: GATE_FENCE)
    gate._modular_state = SimpleNamespace(get_job_dc_managers=lambda job_id: {})
    gate._send_tcp = send_tcp
    gate._tcp_timeout_standard = 5.0
    gate._udp_logger = SimpleNamespace(log=log)
    gate._host = "127.0.0.1"
    gate._tcp_port = 9100
    gate._node_id = SimpleNamespace(short="gate-a")

    await gate._cancel_job_for_timeout(
        "job-1",
        "global_timeout",
        ["dc-east"],
        {"dc-east": MANAGER_ADDRESS},
    )

    [(address, action, payload)] = sent
    assert (address, action) == (MANAGER_ADDRESS, "cancel_job")
    assert CancelJob.load(payload).fence_token == 0
    assert logged == []
