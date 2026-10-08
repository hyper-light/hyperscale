"""
AD-24 transport admission by handler (``ServerRateLimiter.check_handler``).

Every TCP request is admitted against the sending peer's budget for the
handler's AD-24 operation, at the handler's AD-37 priority. These tests
pin the properties the transport relies on:

- CONTROL traffic (consensus, SWIM, cancellation) is never refused, however
  fast it arrives -- its volume is set by the protocol, and losing it costs
  elections.
- One handler's burst cannot exhaust another handler's budget from the
  same peer (a progress flood must not starve final results).
- One peer's burst cannot exhaust another peer's budget.
- High-frequency handlers draw from their AD-24 operation's budget, and
  a refusal tells the sender how long to wait.
"""

import pytest

from hyperscale.distributed.reliability import ServerRateLimiter
from hyperscale.distributed.reliability.load_shedding import (
    classify_handler_to_priority,
)
from hyperscale.distributed.reliability.rate_limiting import (
    AdaptiveRateLimitConfig,
    HANDLER_RATE_LIMIT_OPERATIONS,
)

WORKER_PEER = ("10.0.0.5", 41000)
OTHER_WORKER_PEER = ("10.0.0.6", 41000)


async def admit_burst(
    limiter: ServerRateLimiter,
    peer: tuple[str, int],
    handler_name: str,
    request_count: int,
) -> list[bool]:
    priority = classify_handler_to_priority(handler_name)
    return [
        (await limiter.check_handler(peer, handler_name, priority)).allowed
        for _ in range(request_count)
    ]


# Finite budgets, distinct per operation, over a window no test outlives:
# what is pinned here is which budget a handler draws from, not its size
# (the derived sizes are pinned by test_rate_limit_derivation).
BUDGET_WINDOW_SECONDS = 600.0
TEST_OPERATION_BUDGETS = {
    operation: budget
    for budget, operation in enumerate(
        sorted({*HANDLER_RATE_LIMIT_OPERATIONS.values(), "default"}),
        start=3,
    )
}


def budgeted_limits() -> AdaptiveRateLimitConfig:
    return AdaptiveRateLimitConfig(
        default_max_requests=TEST_OPERATION_BUDGETS["default"],
        default_window_size=BUDGET_WINDOW_SECONDS,
        operation_limits={
            operation: (budget, BUDGET_WINDOW_SECONDS)
            for operation, budget in TEST_OPERATION_BUDGETS.items()
        },
    )


def budgeted_limiter() -> ServerRateLimiter:
    return ServerRateLimiter(adaptive_config=budgeted_limits())


def operation_budget(handler_name: str) -> int:
    operation = HANDLER_RATE_LIMIT_OPERATIONS.get(handler_name, handler_name)
    max_requests, _window_seconds = budgeted_limits().get_operation_limits(operation)
    return max_requests


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "handler_name",
    [
        "raft_append_entries",
        "raft_request_vote",
        "raft_ledger_proposal",
        "gate_raft_append_entries",
        "gate_raft_ledger_placement",
        "cancel_workflow",
    ],
)
async def test_control_handlers_are_never_refused(handler_name: str) -> None:
    limiter = budgeted_limiter()
    burst = 10 * operation_budget(handler_name)

    admissions = await admit_burst(limiter, WORKER_PEER, handler_name, burst)

    assert all(admissions)


@pytest.mark.asyncio
async def test_progress_burst_does_not_consume_final_result_budget() -> None:
    limiter = budgeted_limiter()
    progress_budget = operation_budget("workflow_progress")

    progress_admissions = await admit_burst(
        limiter, WORKER_PEER, "workflow_progress", progress_budget + 1
    )
    assert progress_admissions[-1] is False

    final_result = await limiter.check_handler(
        WORKER_PEER,
        "workflow_final_result",
        classify_handler_to_priority("workflow_final_result"),
    )
    assert final_result.allowed


@pytest.mark.asyncio
async def test_one_peer_burst_does_not_consume_another_peers_budget() -> None:
    limiter = budgeted_limiter()
    progress_budget = operation_budget("workflow_progress")

    await admit_burst(limiter, WORKER_PEER, "workflow_progress", progress_budget + 1)
    other_peer_admissions = await admit_burst(
        limiter, OTHER_WORKER_PEER, "workflow_progress", progress_budget
    )

    assert all(other_peer_admissions)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler_name", "operation"),
    sorted(HANDLER_RATE_LIMIT_OPERATIONS.items()),
)
async def test_mapped_handlers_draw_their_operation_budget(
    handler_name: str,
    operation: str,
) -> None:
    limiter = budgeted_limiter()
    budget = operation_budget(handler_name)
    assert budget == budgeted_limits().get_operation_limits(operation)[0]

    admissions = await admit_burst(limiter, WORKER_PEER, handler_name, budget + 1)

    assert admissions[:budget] == [True] * budget
    assert admissions[budget] is False


@pytest.mark.asyncio
async def test_refusal_carries_retry_after() -> None:
    limiter = budgeted_limiter()
    budget = operation_budget("workflow_progress")
    priority = classify_handler_to_priority("workflow_progress")

    await admit_burst(limiter, WORKER_PEER, "workflow_progress", budget)
    refusal = await limiter.check_handler(WORKER_PEER, "workflow_progress", priority)

    assert refusal.allowed is False
    assert refusal.retry_after_seconds > 0.0


@pytest.mark.asyncio
async def test_unmapped_handler_has_its_own_default_budget() -> None:
    limiter = budgeted_limiter()
    default_budget = budgeted_limits().default_max_requests

    first_handler = await admit_burst(
        limiter, WORKER_PEER, "workflow_query", default_budget + 1
    )
    second_handler = await admit_burst(
        limiter, WORKER_PEER, "job_status", default_budget
    )

    assert first_handler[-1] is False
    assert all(second_handler)
