# TODO: Job Leadership Transfer and Cancellation Improvements

## Overview

This document tracks the remaining work for robust job leadership transfer and workflow cancellation when managers fail.

---

## 1. Fix Job Leadership Takeover When SWIM Leader IS Job Leader (Option A)

**Problem**: When Manager A is both the SWIM cluster leader AND job leader, and Manager A fails:
1. SWIM detects failure (probe → suspicion → confirmed dead)
2. `_on_node_dead` callback fires on surviving managers
3. SWIM leader election begins (may take seconds)
4. `_handle_job_leader_failure()` checks `is_leader()` - returns False during election
5. **No one takes over orphaned jobs**

**Solution**: Add orphaned job scanning to `_on_manager_become_leader` callback.

### Tasks

- [ ] **1.1** Add `_dead_managers` tracking set to manager
  - Track managers confirmed dead via SWIM
  - Populate in `_on_node_dead` callback
  - Clear entries when manager rejoins via `_on_node_join`

- [ ] **1.2** Add `_scan_for_orphaned_jobs()` method
  - Called from `_on_manager_become_leader`
  - For each job in `_job_leader_addrs`, check if leader is in `_dead_managers`
  - Take over any orphaned jobs found

- [ ] **1.3** Update `_on_manager_become_leader` to call `_scan_for_orphaned_jobs()`
  - Run after initial leader stabilization
  - Log jobs being taken over

- [ ] **1.4** Handle edge case: new leader fails during takeover
  - The next elected leader will also scan for orphaned jobs
  - Fencing tokens prevent duplicate takeover

### Files
- `hyperscale/distributed_rewrite/nodes/manager.py`

---

## 2. Refactor Workflow Cancellation to Event-Based Approach

**Status**: ✅ Core cancellation mechanism implemented

**Problem**: Current cancellation uses polling and callbacks. This needs to be event-based for proper integration with job leader failure handling.

### Completed: WorkflowRunner Bool Flag Cancellation

The minimal-impact bool flag approach has been implemented:

- [x] **2.0a** Add `_cancelled: bool` flag to `WorkflowRunner.__init__`
- [x] **2.0b** Add `request_cancellation()` method to `WorkflowRunner`
- [x] **2.0c** Update `_generate()` while loop: `while elapsed < duration and not self._cancelled`
- [x] **2.0d** Update `_generate_constant()` while loop: same pattern
- [x] **2.0e** Reset `_cancelled = False` at start of `run()`
- [x] **2.0f** `RemoteGraphController.cancel_workflow_background()` calls `request_cancellation()` before task cancel
- [x] **2.0g** Fix `Run.cancel()` to use timeout and always update status
- [x] **2.0h** Add event-driven workflow completion signaling

**Files modified**:
- `hyperscale/core/jobs/graphs/workflow_runner.py`
- `hyperscale/core/jobs/graphs/remote_graph_controller.py` (already updated)
- `hyperscale/core/jobs/tasks/run.py` - Added timeout parameter to prevent hangs
- `hyperscale/core/jobs/tasks/task_hook.py` - Pass through timeout parameter
- `hyperscale/core/jobs/tasks/task_runner.py` - Pass through timeout parameter

### Completed: Task Runner Cancellation Fix

**Problem**: `Run.cancel()` could hang indefinitely if a task didn't respond to cancellation. The status was only updated after awaiting the task, so timeouts left status unchanged.

**Solution**:
- Added `timeout` parameter to `Run.cancel()` (default: 5.0 seconds)
- Uses `asyncio.wait_for(asyncio.shield(task), timeout)` to prevent indefinite hangs
- Always updates `status = CANCELLED`, `end`, and `elapsed` regardless of timeout/exception
- Propagated timeout parameter through `Task.cancel()` and `TaskRunner.cancel()`

```python
# Before (could hang forever):
async def cancel(self):
    if self._task and not self._task.done():
        self._task.cancel()
        await self._task  # <-- Could hang!
    self.status = RunStatus.CANCELLED  # <-- Never reached on hang

# After (bounded wait, always updates status):
async def cancel(self, timeout: float = 5.0):
    if self._task and not self._task.done():
        self._task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(self._task), timeout=timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
            pass
    # Always update status, even if timeout occurred
    self.status = RunStatus.CANCELLED
    self.end = time.monotonic()
    self.elapsed = self.end - self.start
```

### Completed: Event-Driven Workflow Completion Signaling

**Problem**: `cancel_workflow_background()` used polling via `tasks.cancel()` to wait for workflow termination. This was converted to event-driven but had gaps.

**Solution**:
- Added `_is_cancelled: asyncio.Event` to WorkflowRunner
- Added `await_cancellation()` method that waits on the event
- Event is set at the end of both `_execute_test_workflow` AND `_execute_non_test_workflow`
- Event is cleared at start of `run()` alongside the bool flag reset
- `cancel_workflow_background()` now uses `await_cancellation()` instead of `tasks.cancel()`

**Flow**:
```
cancel_workflow_background()
    │
    ├─► request_cancellation()  # Sets _cancelled = True
    │       │
    │       └─► Generators stop yielding new VUs
    │
    └─► await_cancellation()  # Waits on _is_cancelled event
            │
            └─► Event fires when _execute_*_workflow completes
```

### Current Architecture Documentation

#### 2.1 RemoteGraphManager.cancel_workflow() Flow

**File**: `hyperscale/core/jobs/graphs/remote_graph_manager.py`

```
cancel_workflow(run_id, workflow, timeout, update_rate)
    │
    ▼
RemoteGraphController.submit_workflow_cancellation()
    │
    ├─► Finds nodes running the workflow (status == RUNNING)
    ├─► Sends request_workflow_cancellation() to each node
    │   │
    │   └─► @send() method sends to "cancel_workflow" receiver
    │
    └─► Starts background task: get_latest_cancelled_status()
            │
            ├─► Polls _cancellations dict every `rate` seconds
            ├─► Calls update_callback with aggregated status counts
            └─► Runs until timeout expires
```

**Key data structures**:
- `_cancellations: NodeData[WorkflowCancellationUpdate]` - stores cancellation status per (run_id, workflow, node_id)
- `_cancellation_write_lock` - per-node locks for cancellation updates
- `_statuses` - tracks workflow status per node (RUNNING, COMPLETED, etc.)

#### 2.2 Worker-Side Cancellation Handler

**File**: `hyperscale/core/jobs/graphs/remote_graph_controller.py`

```
@receive()
cancel_workflow(shard_id, cancelation: JobContext[WorkflowCancellation])
    │
    ├─► Looks up workflow_run_id from _run_workflow_run_id_map
    │
    └─► Spawns background task: cancel_workflow_background()
            │
            ├─► Calls self.tasks.cancel("run_workflow", workflow_run_id)
            │       │
            │       └─► This cancels the asyncio task running the workflow
            │
            ├─► On success: sends receive_cancellation_update with CANCELLED status
            │
            └─► On failure/timeout: sends receive_cancellation_update with FAILED status
```

**Cancellation statuses** (from `WorkflowCancellationStatus`):
- `REQUESTED` - Cancellation request received
- `IN_PROGRESS` - Cancellation in progress
- `CANCELLED` - Successfully cancelled
- `FAILED` - Cancellation failed
- `NOT_FOUND` - Workflow not found on this node

#### 2.3 WorkflowRunner Cancellation Handling

**File**: `hyperscale/core/jobs/graphs/workflow_runner.py`

The WorkflowRunner doesn't have explicit cancellation handling. Cancellation works via:

1. **Task cancellation**: `tasks.cancel("run_workflow", run_id)` cancels the asyncio.Task
2. **asyncio.CancelledError propagation**: When the task is cancelled, `CancelledError` propagates through:
   - `_run_workflow()`
   - `_execute_test_workflow()` or `_execute_non_test_workflow()`
   - The `asyncio.wait()` call returns pending tasks

3. **Pending task cleanup**: The `cancel_pending()` helper function cleans up remaining tasks:
   ```python
   async def cancel_pending(pend: asyncio.Task):
       if pend.done():
           pend.exception()
           return pend
       pend.cancel()
       await asyncio.sleep(0)
       if not pend.cancelled():
           await pend
       return pend
   ```

4. **Status tracking**: `run_statuses[run_id][workflow_name]` is set to `WorkflowStatus.FAILED` on exception

**Current limitations**:
- No explicit cancellation event/flag that generators check
- Duration-based execution (`_generate`, `_generate_constant`) runs until elapsed time
- CPU monitor locks can delay cancellation propagation

### Refactoring Tasks

- [ ] **2.4** Add cancellation event to WorkflowRunner
  - Add `_cancellation_events: Dict[int, Dict[str, asyncio.Event]]`
  - Set event in new `cancel_workflow()` method
  - Check event in `_generate()` and `_generate_constant()` loops

- [ ] **2.5** Replace polling with event subscription in RemoteGraphController
  - Add `_cancellation_complete_events: Dict[int, Dict[str, asyncio.Event]]`
  - Signal event when cancellation completes
  - `get_latest_cancelled_status` waits on event instead of polling

- [ ] **2.6** Add cancellation acknowledgment flow
  - Worker sends explicit "cancellation complete" message
  - Manager updates status immediately on receipt
  - No need for periodic polling

- [ ] **2.7** Integrate with job leader failure
  - When worker detects job leader failure → check for orphaned workflows
  - Grace period before cancellation (wait for `JobLeaderWorkerTransfer`)
  - If transfer arrives → update routing, continue execution
  - If grace expires → trigger cancellation via event system

### Files
- `hyperscale/core/jobs/graphs/workflow_runner.py`
- `hyperscale/core/jobs/graphs/remote_graph_controller.py`
- `hyperscale/core/jobs/graphs/remote_graph_manager.py`
- `hyperscale/distributed_rewrite/nodes/worker.py`

---

## 3. Worker-Side Job Leader Failure Handling

**Problem**: When workers learn their job leader has failed, they need to:
1. Wait for potential `JobLeaderWorkerTransfer` (new leader taking over)
2. If transfer arrives → update `_workflow_job_leader` mapping, continue
3. If grace period expires → trigger workflow cancellation

### Tasks

- [ ] **3.1** Add orphaned workflow tracking to worker
  ```python
  _orphaned_workflows: dict[str, float]  # workflow_id -> orphan_timestamp
  ```

- [ ] **3.2** Modify `_on_node_dead` to mark workflows as orphaned
  - Find all workflows for the dead manager
  - Add to `_orphaned_workflows` with current timestamp
  - Do NOT immediately cancel

- [ ] **3.3** Modify `job_leader_worker_transfer` handler
  - Clear workflow from `_orphaned_workflows` if present
  - Update `_workflow_job_leader` mapping
  - Log successful transfer

- [ ] **3.4** Add orphan grace period checker
  - Periodic task or integrate with existing cleanup task
  - For each orphaned workflow, check if grace period expired
  - If expired → trigger cancellation via event system (from item 2)

- [ ] **3.5** Configuration
  - `WORKER_ORPHAN_GRACE_PERIOD` env var (default: 5.0 seconds)
  - Tune based on expected election + takeover time

### Files
- `hyperscale/distributed_rewrite/nodes/worker.py`
- `hyperscale/distributed_rewrite/env.py` (for config)

---

## 4. Integration Testing

- [ ] **4.1** Test: SWIM leader + job leader fails
  - Start 3 managers, submit job to leader
  - Kill leader manager
  - Verify new leader takes over job
  - Verify workers receive transfer notification
  - Verify job completes successfully

- [ ] **4.2** Test: Job leader fails (not SWIM leader)
  - Start 3 managers, submit job to non-leader
  - Kill job leader manager
  - Verify SWIM leader takes over job
  - Verify gate receives transfer notification

- [ ] **4.3** Test: Worker orphan grace period
  - Start manager + worker, submit job
  - Kill manager before new leader elected
  - Verify worker waits grace period
  - Verify cancellation if no transfer received

- [ ] **4.4** Test: Worker receives transfer before grace expires
  - Start manager + worker, submit job
  - Kill manager, new leader takes over quickly
  - Verify worker receives transfer
  - Verify workflow continues (not cancelled)

### Files
- `tests/integration/test_job_leader_failover.py` (new)

---

## Dependencies

- Item 1 can be done independently
- Item 2 (event-based cancellation) should be done before Item 3
- Item 3 depends on Item 2 for the cancellation mechanism
- Item 4 depends on Items 1, 2, 3

---

## Appendix: Key Code Locations

### Cancellation-Related

| Component | File | Key Methods |
|-----------|------|-------------|
| RemoteGraphManager | `hyperscale/core/jobs/graphs/remote_graph_manager.py:1458` | `cancel_workflow()` |
| RemoteGraphController | `hyperscale/core/jobs/graphs/remote_graph_controller.py:428` | `submit_workflow_cancellation()` |
| RemoteGraphController | `hyperscale/core/jobs/graphs/remote_graph_controller.py:941` | `cancel_workflow()` (receive) |
| RemoteGraphController | `hyperscale/core/jobs/graphs/remote_graph_controller.py:1154` | `cancel_workflow_background()` |
| WorkflowRunner | `hyperscale/core/jobs/graphs/workflow_runner.py:55` | `cancel_pending()` |

### Job Leadership-Related

| Component | File | Key Methods |
|-----------|------|-------------|
| Manager | `hyperscale/distributed_rewrite/nodes/manager.py:614` | `_on_manager_become_leader()` |
| Manager | `hyperscale/distributed_rewrite/nodes/manager.py:1078` | `_handle_job_leader_failure()` |
| Manager | `hyperscale/distributed_rewrite/nodes/manager.py:1170` | `_notify_gate_of_leadership_transfer()` |
| Manager | `hyperscale/distributed_rewrite/nodes/manager.py:1257` | `_notify_workers_of_leadership_transfer()` |
| Worker | `hyperscale/distributed_rewrite/nodes/worker.py` | `job_leader_worker_transfer()` handler |

---

## Notes

- All changes must be asyncio-safe (use locks where needed)
- Follow existing patterns in codebase (TaskRunner for background tasks, structured logging)
- Fencing tokens must be respected throughout to prevent stale operations
- Memory cleanup is critical - track and clean up orphaned state
