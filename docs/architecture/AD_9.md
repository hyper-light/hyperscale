---
ad_number: 9
name: Retry Requeues the Pending Workflow
description: A retried workflow is requeued as its original PendingWorkflow, excluding the worker it left, and redispatched with a fresh fence token
---

# AD-9: Retry Requeues the Pending Workflow

**Decision**: A retried workflow is requeued as the same `PendingWorkflow` the dispatcher already holds (its parsed workflow, VUs, timeout and context), with the worker it left excluded, and is dispatched again with a fresh fence token.

*History (2026-10-05):* the original design stored the first `WorkflowDispatch` bytes in `_workflow_retries` and replayed them; that state was never read and was deleted. Replaying stored bytes would also carry a stale fence token.

**Rationale**:
- Retry has exactly the same parameters (VUs, timeout, context) because the pending workflow is reused
- No serialization round trip; no second copy of dispatch state to keep consistent
- A fresh fence token per dispatch fences out the failed attempt

**Implementation**:
- `WorkflowDispatcher.requeue_workflow(sub_workflow_token, excluded_worker_id)` (`jobs/workflow_dispatcher.py:1808`)
  requeues only a workflow the AD-54 lifecycle has returned to PENDING, resets its dispatch backoff, and restarts
  the job's dispatch loop if it has exited.
- `PendingWorkflow.excluded_worker_ids` (`models/pending_workflow.py:48`) collects the workers it left
  (`jobs/workflow_dispatcher.py:1894`); `WorkerPool` allocation skips them (`jobs/worker_pool.py:1101-1118`).
- Each dispatch draws a new fence token from `JobManager.get_next_fence_token` (`jobs/workflow_dispatcher.py:970`).
- Test: `tests/unit/distributed/jobs/test_workflow_dispatch_routing.py`.
