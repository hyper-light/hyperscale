"""
AD-26: a worker's extension budget is per cycle, not per lifetime.

``WorkerHealthManager`` keeps one ``ExtensionTracker`` per worker. Its
reset (``on_worker_healthy``) had no caller, so after ``max_extensions``
lifetime grants every later workflow on that worker was denied from its
first request, and a healthy worker in a long workflow was evicted. The
AD (AD_26.md, ``on_worker_healthy``: "Reset extension tracker when worker
completes successfully") resets the tracker when the worker completes a
workflow. Every terminating workflow already passes through the outcome
path (``record_workflow_outcome`` on the manager that saw the result,
``ingest_remote_outcome_event`` on every peer), so a COMPLETED outcome is
where the cycle ends.

Driven through the real ``WorkerHealthManager`` entry points the manager
server calls:

* a COMPLETED workflow restores the worker's full grant budget;
* a FAILED / TIMED_OUT / EVICTED / UNKNOWN outcome does not;
* a peer resets on the first gossiped copy only -- a late duplicate never
  wipes grants made after it;
* a seeded model over several workers: every grant decision matches a
  model that budgets ``max_extensions`` per worker per cycle.
"""

import random

import pytest

from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeEvent, ExtensionOutcomeKind
from hyperscale.distributed.health.worker_health_manager import WorkerHealthManager
from hyperscale.distributed.health.worker_health_manager_config import WorkerHealthManagerConfig
from hyperscale.distributed.models import HealthcheckExtensionRequest

SEEDS = range(40)
NON_SUCCESS_KINDS = (
    ExtensionOutcomeKind.FAILED,
    ExtensionOutcomeKind.TIMED_OUT,
    ExtensionOutcomeKind.EVICTED,
    ExtensionOutcomeKind.UNKNOWN,
)


def extension_request(worker_id: str, completed_items: int) -> HealthcheckExtensionRequest:
    return HealthcheckExtensionRequest(
        worker_id=worker_id,
        reason="long workflow",
        current_progress=float(completed_items),
        estimated_completion=10.0,
        active_workflow_count=1,
        completed_items=completed_items,
    )


def outcome_event(worker_id: str, workflow_id: str, outcome_kind: ExtensionOutcomeKind) -> ExtensionOutcomeEvent:
    return ExtensionOutcomeEvent(
        job_id="job-1",
        workflow_id=workflow_id,
        workflow_class="LongWorkflow",
        worker_id=worker_id,
        outcome_kind=outcome_kind,
        granted_extension_count=0,
        denied_extension_count=0,
        total_extended_seconds=0.0,
        final_progress_fraction=1.0 if outcome_kind is ExtensionOutcomeKind.COMPLETED else 0.0,
        completed_at=0.0,
        fence_token=1,
        leader_term=1,
    )


def record_outcome(
    health_manager: WorkerHealthManager,
    worker_id: str,
    workflow_id: str,
    outcome_kind: ExtensionOutcomeKind,
) -> ExtensionOutcomeEvent:
    return health_manager.record_workflow_outcome(
        job_id="job-1",
        workflow_id=workflow_id,
        workflow_class="LongWorkflow",
        worker_id=worker_id,
        outcome_kind=outcome_kind,
        final_progress_fraction=1.0 if outcome_kind is ExtensionOutcomeKind.COMPLETED else 0.0,
        completed_at=0.0,
        fence_token=1,
        leader_term=1,
    )


def exhaust_extensions(health_manager: WorkerHealthManager, worker_id: str, max_extensions: int) -> int:
    """Request extensions, each with progress, until one is denied; returns the last completed count."""
    completed_items = 0
    for _ in range(max_extensions):
        completed_items += 1
        assert health_manager.handle_extension_request(extension_request(worker_id, completed_items), 0.0).granted
    completed_items += 1
    assert not health_manager.handle_extension_request(extension_request(worker_id, completed_items), 0.0).granted
    return completed_items


def test_a_completed_workflow_restores_the_full_grant_budget() -> None:
    config = WorkerHealthManagerConfig()
    health_manager = WorkerHealthManager(config)
    exhaust_extensions(health_manager, "worker-1", config.max_extensions)

    record_outcome(health_manager, "worker-1", "workflow-1", ExtensionOutcomeKind.COMPLETED)

    first_request_of_next_workflow = health_manager.handle_extension_request(extension_request("worker-1", 1), 0.0)
    assert first_request_of_next_workflow.granted
    assert first_request_of_next_workflow.extension_seconds == config.base_deadline
    assert first_request_of_next_workflow.remaining_extensions == config.max_extensions - 1
    assert health_manager.should_evict_worker("worker-1") == (False, None)


@pytest.mark.parametrize("outcome_kind", NON_SUCCESS_KINDS)
def test_a_workflow_that_did_not_complete_keeps_the_budget_spent(outcome_kind: ExtensionOutcomeKind) -> None:
    config = WorkerHealthManagerConfig()
    health_manager = WorkerHealthManager(config)
    completed_items = exhaust_extensions(health_manager, "worker-1", config.max_extensions)

    record_outcome(health_manager, "worker-1", "workflow-1", outcome_kind)

    assert not health_manager.handle_extension_request(
        extension_request("worker-1", completed_items + 1), 0.0
    ).granted


def test_another_workers_completion_leaves_this_workers_budget_spent() -> None:
    config = WorkerHealthManagerConfig()
    health_manager = WorkerHealthManager(config)
    completed_items = exhaust_extensions(health_manager, "worker-1", config.max_extensions)

    record_outcome(health_manager, "worker-2", "workflow-2", ExtensionOutcomeKind.COMPLETED)

    assert not health_manager.handle_extension_request(
        extension_request("worker-1", completed_items + 1), 0.0
    ).granted


def test_a_peer_resets_on_the_first_gossiped_completion_only() -> None:
    config = WorkerHealthManagerConfig()
    peer = WorkerHealthManager(config)
    exhaust_extensions(peer, "worker-1", config.max_extensions)
    completion = outcome_event("worker-1", "workflow-1", ExtensionOutcomeKind.COMPLETED)

    assert peer.ingest_remote_outcome_event(completion)
    for completed_items in (1, 2):
        assert peer.handle_extension_request(extension_request("worker-1", completed_items), 0.0).granted

    assert not peer.ingest_remote_outcome_event(completion)
    assert peer.get_worker_extension_state("worker-1")["extension_count"] == 2


@pytest.mark.parametrize("seed", SEEDS)
def test_grants_follow_a_per_cycle_budget_model(seed: int) -> None:
    """Random interleavings of extension requests (always with progress)
    and workflow outcomes over three workers, each outcome delivered one
    to three times. A request is granted exactly when the model's
    per-worker grant count for the current cycle is under the cap; the
    first copy of a COMPLETED outcome starts a new cycle."""
    draw = random.Random(seed)
    config = WorkerHealthManagerConfig(max_extensions=draw.randint(1, 6))
    health_manager = WorkerHealthManager(config)
    worker_ids = ("worker-a", "worker-b", "worker-c")
    grants_this_cycle = {worker_id: 0 for worker_id in worker_ids}
    completed_items = {worker_id: 0 for worker_id in worker_ids}
    delivered_workflow_ids: set[str] = set()

    for step_index in range(draw.randint(20, 120)):
        worker_id = draw.choice(worker_ids)
        if draw.random() < 0.75:
            completed_items[worker_id] += 1
            response = health_manager.handle_extension_request(
                extension_request(worker_id, completed_items[worker_id]), 0.0
            )
            expected_grant = grants_this_cycle[worker_id] < config.max_extensions
            assert response.granted == expected_grant, (seed, step_index, worker_id)
            grants_this_cycle[worker_id] += int(response.granted)
            continue

        workflow_id = f"workflow-{step_index}"
        outcome_kind = draw.choice((ExtensionOutcomeKind.COMPLETED, *NON_SUCCESS_KINDS))
        event = outcome_event(worker_id, workflow_id, outcome_kind)
        for _ in range(draw.randint(1, 3)):
            health_manager.ingest_remote_outcome_event(event)
        if outcome_kind is ExtensionOutcomeKind.COMPLETED and workflow_id not in delivered_workflow_ids:
            grants_this_cycle[worker_id] = 0
            completed_items[worker_id] = 0
        delivered_workflow_ids.add(workflow_id)
