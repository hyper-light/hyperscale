"""
Stopping a gate stops every background loop before it closes what they
write to.

``GateServer.stop`` closes the job ledger, the idempotency cache and the
orphan coordinator after ``_stop_background_loops``. That used to cancel
only the resource-sampling loop: the job-cleanup loop (which checkpoints
the ledger), the failover coordinator and six more kept running into the
closed ledger until the task runner itself shut down, and discovery
maintenance and job lease cleanup ran on raw tasks outside the runner --
the lease cleanup never stopped at all. Every loop the gate
starts must have ended -- not merely been asked to -- when it returns.
"""

import asyncio
from types import SimpleNamespace

import pytest

from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.taskex import TaskRunner

LOOP_NAMES = (
    "_job_cleanup_loop",
    "_rate_limit_cleanup_loop",
    "_batch_stats_loop",
    "_windowed_stats_push_loop",
    "_dead_peer_reap_loop",
    "_datacenter_correlation_loop",
    "_resource_sampling_loop",
    "_discovery_maintenance_loop",
    "_gate_peer_readmission_loop",
    # Re-drives cancels a datacenter has not confirmed; it records their
    # confirmations in the ledger.
    "_cancellation_redrive_loop",
)


class SilentLogger:
    async def log(self, entry) -> None:
        return None


@pytest.mark.asyncio
async def test_every_background_loop_has_ended_when_the_stop_returns() -> None:
    running: set[str] = set()
    ended: set[str] = set()

    def stub_loop(name: str):
        async def loop() -> None:
            running.add(name)
            try:
                while True:
                    await asyncio.sleep(0)
            finally:
                ended.add(name)

        # The task runner keys tasks by name, as it does the real loops.
        loop.__name__ = name
        return loop

    gate = object.__new__(GateServer)
    gate._task_runner = TaskRunner()
    gate._udp_logger = SilentLogger()
    gate._gate_udp_peers = [("10.0.0.2", 9001)]
    gate._background_loop_tokens = []
    for name in LOOP_NAMES:
        setattr(gate, name, stub_loop(name))
    gate._job_failover_coordinator = SimpleNamespace(run=stub_loop("job_failover_coordinator"))
    gate._job_lease_manager = SimpleNamespace(run_cleanup=stub_loop("job_lease_cleanup"))

    gate._start_background_loops()
    started = {*LOOP_NAMES, "job_failover_coordinator", "job_lease_cleanup"}
    while running != started:
        await asyncio.sleep(0)

    await gate._stop_background_loops()

    assert ended == started
    assert gate._background_loop_tokens == []
    await gate._task_runner.shutdown()
