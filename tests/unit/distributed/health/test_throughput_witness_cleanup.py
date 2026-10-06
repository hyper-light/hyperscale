"""
AD-26 H6: the throughput witness keeps one BOCPD stream per (worker,
workflow) it has seen, and must drop each when its workflow ends or its
worker leaves. Nothing called ``reset_stream`` -- wiring the witness into
the manager as it stood would have leaked a stream per workflow for the
manager's lifetime.

Driven through ``WorkerHealthManager``'s own cleanup paths (the ones the
manager calls on workflow outcomes and, via its registry, on worker
removal) over a real witness.
"""

import itertools
import random

import pytest

from hyperscale.distributed.health import WorkerHealthManager
from hyperscale.distributed.health.progress_witness import ThroughputWitness

SEEDS = range(8)


def observe(witness: ThroughputWitness, worker_id: str, workflow_id: str) -> None:
    witness.observe(
        worker_id=worker_id,
        workflow_id=workflow_id,
        throughput=1.0,
        active_in_cluster=1,
        active_in_dc=1,
        active_on_manager=1,
        active_on_worker=1,
    )


@pytest.mark.parametrize("seed", SEEDS)
def test_streams_end_with_their_workflow_or_their_worker(seed: int) -> None:
    draw = random.Random(seed)
    witness = ThroughputWitness()
    health_manager = WorkerHealthManager(throughput_witness=witness)
    workers = [f"worker-{index}" for index in range(5)]
    workflows = [f"workflow-{index}" for index in range(12)]
    live: set[tuple[str, str]] = set()

    for _ in range(400):
        action = draw.random()
        if action < 0.6:
            worker_id, workflow_id = draw.choice(workers), draw.choice(workflows)
            observe(witness, worker_id, workflow_id)
            live.add((worker_id, workflow_id))
        elif action < 0.85:
            workflow_id = draw.choice(workflows)
            health_manager.forget_workflow(workflow_id)
            live = {stream for stream in live if stream[1] != workflow_id}
        else:
            worker_id = draw.choice(workers)
            health_manager.on_worker_removed(worker_id)
            live = {stream for stream in live if stream[0] != worker_id}
        assert witness.stream_count == len(live)

    for worker_id in workers:
        health_manager.on_worker_removed(worker_id)
    assert witness.stream_count == 0
    assert witness._workers_by_workflow == {} and witness._workflows_by_worker == {}


def test_a_reset_stream_leaves_no_index_behind() -> None:
    witness = ThroughputWitness()
    for worker_id, workflow_id in itertools.product(["worker-a", "worker-b"], ["workflow-1", "workflow-2"]):
        observe(witness, worker_id, workflow_id)
    for worker_id, workflow_id in itertools.product(["worker-a", "worker-b"], ["workflow-1", "workflow-2"]):
        witness.reset_stream(worker_id, workflow_id)

    assert witness.stream_count == 0
    assert witness._workers_by_workflow == {} and witness._workflows_by_worker == {}
