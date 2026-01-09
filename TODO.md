# TODO: Job Leadership Transfer and Cancellation Improvements

## Overview

This document tracks the remaining work for robust job leadership transfer and workflow cancellation when managers fail.

---

## 1. Fix Job Leadership Takeover When SWIM Leader IS Job Leader (Option A)

**Status**: ✅ Complete

**Problem**: When Manager A is both the SWIM cluster leader AND job leader, and Manager A fails:
1. SWIM detects failure (probe → suspicion → confirmed dead)
2. `_on_node_dead` callback fires on surviving managers
3. SWIM leader election begins (may take seconds)
4. `_handle_job_leader_failure()` checks `is_leader()` - returns False during election
5. **No one takes over orphaned jobs**

**Solution**: Add orphaned job scanning to `_on_manager_become_leader` callback.

### Tasks

- [x] **1.1** Add `_dead_managers` tracking set to manager
  - Track managers confirmed dead via SWIM
  - Populate in `_on_node_dead` callback
  - Clear entries when manager rejoins via `_on_node_join`

- [x] **1.2** Add `_scan_for_orphaned_jobs()` method
  - Called from `_on_manager_become_leader`
  - For each job in `_job_leader_addrs`, check if leader is in `_dead_managers`
  - Take over any orphaned jobs found

- [x] **1.3** Update `_on_manager_become_leader` to call `_scan_for_orphaned_jobs()`
  - Run after initial leader stabilization
  - Log jobs being taken over

- [x] **1.4** Handle edge case: new leader fails during takeover
  - The next elected leader will also scan for orphaned jobs
  - Fencing tokens prevent duplicate takeover

### Files
- `hyperscale/distributed_rewrite/nodes/manager.py`

---

## 2. Refactor Workflow Cancellation to Event-Based Approach

**Status**: ✅ Complete

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
- [x] **2.0i** Fix `cancel_pending()` to use timeout and consistent return type
- [x] **2.0j** Add done callback to prevent memory leaks in hung task cancellation

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
            # No shield - we already cancelled it, just waiting for cleanup
            await asyncio.wait_for(self._task, timeout=timeout)
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

### Completed: Memory-Leak-Free Task Cancellation

**Problem**: Fire-and-forget task cancellation could leak memory when tasks are stuck in syscalls (SSL, network operations). Python's asyncio keeps task objects alive if their exception is never retrieved. This is critical when cancelling millions of hung network requests.

**Solution**: Use `add_done_callback` to ensure exception retrieval even for stuck tasks.

```python
def _retrieve_task_exception(task: asyncio.Task) -> None:
    """
    Done callback to retrieve a task's exception and prevent memory leaks.
    """
    try:
        task.exception()
    except (asyncio.CancelledError, asyncio.InvalidStateError, Exception):
        pass


def cancel_and_release_task(pend: asyncio.Task) -> None:
    """
    Cancel a task and guarantee no memory leaks, even for hung tasks.
    """
    try:
        if pend.done():
            # Task already finished - retrieve exception now
            try:
                pend.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError, Exception):
                pass
        else:
            # Task still running - cancel and add callback for when it finishes
            # The callback ensures exception is retrieved even if task is stuck
            pend.add_done_callback(_retrieve_task_exception)
            pend.cancel()
    except Exception:
        pass
```

**Key insight**: The done callback fires when the task eventually finishes (even if stuck for a long time), ensuring:
1. Exception is retrieved → no "exception never retrieved" warnings
2. Task object can be garbage collected → no memory leaks
3. Works even for tasks stuck in SSL/network syscalls

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

- [x] **2.4** Add cancellation event to WorkflowRunner
  - `_is_cancelled: asyncio.Event` already exists for completion signaling
  - Bool flag `_running` is checked in `_generate()` and `_generate_constant()` loops
  - Single workflow per runner, so event pattern is sufficient

- [x] **2.5** Replace polling with event subscription in RemoteGraphController
  - `_cancellation_completion_events: Dict[int, Dict[str, asyncio.Event]]` exists
  - `_cancellation_expected_nodes` tracks pending workers
  - Event fires in `receive_cancellation_update()` when all nodes report terminal status
  - `await_workflow_cancellation()` waits on event instead of polling

- [x] **2.6** Add cancellation acknowledgment flow
  - Worker sends `WorkflowCancellationComplete` via `_push_cancellation_complete()`
  - Manager receives and tracks via `receive_cancellation_update()`
  - Status updates immediately on receipt

- [x] **2.7** Integrate with job leader failure
  - Worker tracks orphaned workflows in `_orphaned_workflows: dict[str, float]`
  - `_handle_manager_failure()` marks workflows as orphaned when job leader fails
  - `job_leader_worker_transfer()` clears orphaned workflows when transfer arrives
  - `_orphan_check_loop()` cancels workflows after `WORKER_ORPHAN_GRACE_PERIOD` expires
  - Configuration via `WORKER_ORPHAN_GRACE_PERIOD` (default 5.0s) and `WORKER_ORPHAN_CHECK_INTERVAL` (default 1.0s)

### Files
- `hyperscale/core/jobs/graphs/workflow_runner.py`
- `hyperscale/core/jobs/graphs/remote_graph_controller.py`
- `hyperscale/core/jobs/graphs/remote_graph_manager.py`
- `hyperscale/distributed_rewrite/nodes/worker.py`
- `hyperscale/distributed_rewrite/env/env.py`

---

## 3. Worker-Side Job Leader Failure Handling

**Status**: ✅ Complete

**Problem**: When workers learn their job leader has failed, they need to:
1. Wait for potential `JobLeaderWorkerTransfer` (new leader taking over)
2. If transfer arrives → update `_workflow_job_leader` mapping, continue
3. If grace period expires → trigger workflow cancellation

### Tasks

- [x] **3.1** Add orphaned workflow tracking to worker
  ```python
  _orphaned_workflows: dict[str, float]  # workflow_id -> orphan_timestamp
  ```

- [x] **3.2** Modify `_on_node_dead` to mark workflows as orphaned
  - Find all workflows for the dead manager
  - Add to `_orphaned_workflows` with current timestamp
  - Do NOT immediately cancel

- [x] **3.3** Modify `job_leader_worker_transfer` handler
  - Clear workflow from `_orphaned_workflows` if present
  - Update `_workflow_job_leader` mapping
  - Log successful transfer

- [x] **3.4** Add orphan grace period checker
  - Periodic task or integrate with existing cleanup task
  - For each orphaned workflow, check if grace period expired
  - If expired → trigger cancellation via event system (from item 2)

- [x] **3.5** Configuration
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

## 5. Event-Driven Cancellation Push Notification Chain

**Problem**: Currently, when a manager sends a cancellation request to workers, the manager does not receive push notification when the cancellation is actually complete. The flow is request/ack only, not request/ack/completion. We need:

1. Workers to push completion notification to managers when cancellation finishes
2. Managers to move cancelled workflows to a "cancelled" data structure for cleanup
3. Managers to push cancellation errors to the originating gate/client
4. Gates to support submitting cancellation requests (already partial)
5. Clients to submit cancellation requests to gate OR manager

**Architecture**: Worker → Manager → Gate → Client push notification chain

### Tasks

- [ ] **5.1** Add `WorkflowCancellationComplete` message type
  - `job_id: str`
  - `workflow_id: str`
  - `success: bool`
  - `errors: list[str]`
  - `cancelled_at: float`
  - `node_id: str` (worker that cancelled)

- [ ] **5.2** Add `cancel_workflow_complete` TCP handler to Worker
  - After `_cancel_workflow()` completes, send `WorkflowCancellationComplete` to manager
  - Include any errors from the cancellation process
  - Use the existing task runner pattern (spawn task, don't block cancel flow)

- [ ] **5.3** Add `receive_workflow_cancellation_complete` handler to Manager
  - Receive push from worker
  - Update `SubWorkflowInfo.status = CANCELLED`
  - Track in `_cancelled_workflows: dict[str, CancellationResult]`
  - If all sub-workflows for a job are cancelled, mark job as cancelled
  - Call `_push_cancellation_complete_to_origin()` if errors present

- [ ] **5.4** Add `_push_cancellation_complete_to_origin()` to Manager
  - Lookup origin gate/client from `_job_origin_gates[job_id]` or `_job_callbacks[job_id]`
  - Push `JobCancellationComplete` message with aggregated errors
  - Use existing push notification pattern (fire-and-forget with retry)

- [ ] **5.5** Add `JobCancellationComplete` message type
  - `job_id: str`
  - `success: bool`
  - `cancelled_workflow_count: int`
  - `errors: list[str]` (aggregated from all workers)
  - `cancelled_at: float`

- [ ] **5.6** Add `receive_job_cancellation_complete` handler to Gate
  - Receive push from manager
  - Update local job cache status
  - Forward to client callback if registered
  - Log any errors for debugging

- [ ] **5.7** Add `receive_job_cancellation_complete` handler to Client
  - Receive push from gate/manager
  - Update local job state
  - Set completion event for any `await_job_cancellation()` waiters
  - Expose errors via `get_cancellation_errors(job_id)`

- [ ] **5.8** Add `await_job_cancellation()` to Client
  - Event-driven wait for cancellation completion
  - Returns `tuple[bool, list[str]]` (success, errors)
  - Times out if no completion received

- [ ] **5.9** Update Manager cleanup to handle cancelled workflows
  - Move cancelled workflows to `_cancelled_workflows` with timestamp
  - Cleanup after `_cancelled_workflow_max_age` (use existing cleanup loop)
  - Ensure proper memory cleanup for all cancellation tracking structures

- [ ] **5.10** Integration: Wire Worker `_cancel_workflow()` to push completion
  - After successful cancellation, push `WorkflowCancellationComplete`
  - After failed cancellation, push with errors
  - Handle edge cases (worker disconnect, manager unreachable)

### Message Flow

```
Client                Gate                  Manager               Worker
  |                    |                      |                     |
  |--CancelJob-------->|                      |                     |
  |                    |--CancelJob---------->|                     |
  |                    |                      |--CancelJob--------->|
  |                    |                      |<--CancelAck---------|
  |                    |<--CancelAck----------|                     |
  |<--CancelAck--------|                      |                     |
  |                    |                      |     (cancellation   |
  |                    |                      |      in progress)   |
  |                    |                      |                     |
  |                    |                      |<--CancellationComplete
  |                    |<--JobCancellationComplete                  |
  |<--JobCancellationComplete                 |                     |
  |                    |                      |                     |
```

### Files
- `hyperscale/distributed_rewrite/models/distributed.py` (new message types)
- `hyperscale/distributed_rewrite/nodes/worker.py` (push completion)
- `hyperscale/distributed_rewrite/nodes/manager.py` (receive & forward)
- `hyperscale/distributed_rewrite/nodes/gate.py` (receive & forward)
- `hyperscale/distributed_rewrite/nodes/client.py` (receive & await)

---

## 6. Workflow-Level Cancellation from Gates (Single Workflow Cancellation)

**Problem**: Currently, cancellation is at the job level. We need fine-grained workflow-level cancellation where:
1. Clients can request cancellation of a specific workflow (not entire job)
2. Gates dispatch to ALL datacenters with matching job
3. Managers check workflow state (pending, running, not found)
4. ALL dependent workflows are also cancelled
5. Cancellation is race-condition safe with proper locking
6. Peer notification ensures consistency across cluster

### Architecture Overview

```
Client                Gate                  Manager               Worker
  |                    |                      |                     |
  |--CancelWorkflow--->|                      |                     |
  |                    |--CancelWorkflow----->| (to all DCs)        |
  |                    |                      |                     |
  |                    |   (notify peers)     |--CancelWorkflow---->|
  |                    |      |               |<--CancelAck---------|
  |                    |      v               |                     |
  |                    | Gate Peers           | Manager Peers       |
  |                    | (register for        | (move workflow+deps |
  |                    |  failover)           |  to cancelled bucket)|
  |                    |                      |                     |
  |                    |                      | (wait ALL workers)  |
  |                    |<--CancellationResult-|                     |
  |<--CancellationResult (aggregate all DCs) |                     |
```

### Tasks

#### 6.1 Message Types

- [ ] **6.1.1** Add `SingleWorkflowCancelRequest` message type
  - `job_id: str`
  - `workflow_id: str`
  - `origin_gate_id: str | None` (for result push)
  - `origin_client_id: str | None`
  - `cancel_dependents: bool = True`
  - `request_id: str` (for deduplication and tracking)

- [ ] **6.1.2** Add `SingleWorkflowCancelResponse` message type
  - `job_id: str`
  - `workflow_id: str`
  - `status: WorkflowCancellationStatus` (CANCELLED, NOT_FOUND, PENDING_CANCELLED, etc.)
  - `cancelled_dependents: list[str]` (workflow IDs of cancelled dependents)
  - `errors: list[str]`
  - `request_id: str`

- [ ] **6.1.3** Add `WorkflowCancellationPeerNotification` message type
  - For gate-to-gate and manager-to-manager peer sync
  - `job_id: str`
  - `workflow_id: str`
  - `cancelled_workflows: list[str]` (workflow + all dependents)
  - `request_id: str`
  - `origin_node_id: str`

#### 6.2 Manager Cancellation Handler

- [ ] **6.2.1** Add `receive_cancel_workflow` handler to Manager
  - Check if workflow is PENDING (in queue): remove from queue, mark cancelled
  - Check if workflow is RUNNING: dispatch cancellation to workers
  - Check if NOT FOUND: return empty response with message
  - Acquire per-workflow lock before any state mutation

- [ ] **6.2.2** Add workflow dependency graph traversal
  - Use existing `_workflow_dependencies` structure
  - Recursively find ALL dependent workflows
  - Cancel entire dependency subtree atomically

- [ ] **6.2.3** Add `_cancelled_workflows` bucket
  ```python
  _cancelled_workflows: dict[str, CancelledWorkflowInfo] = {}
  # CancelledWorkflowInfo contains: job_id, workflow_id, cancelled_at, dependents
  ```
  - Cleanup at `Env.CANCELLED_WORKFLOW_CLEANUP_INTERVAL` (configurable)
  - TTL: `Env.CANCELLED_WORKFLOW_TTL` (default: 1 hour)

- [ ] **6.2.4** Add pre-dispatch cancellation check
  - Before dispatching ANY workflow, check `_cancelled_workflows`
  - If workflow_id in bucket, reject dispatch immediately
  - This prevents "resurrection" of cancelled workflows

- [ ] **6.2.5** Add per-workflow asyncio.Lock for race safety
  ```python
  _workflow_cancellation_locks: dict[str, asyncio.Lock] = {}
  ```
  - Acquire lock before checking/modifying workflow state
  - Prevents race between cancellation and dispatch

#### 6.3 Manager Peer Notification

- [ ] **6.3.1** Add manager peer notification on cancellation
  - When cancellation received, immediately notify ALL manager peers
  - Use existing peer TCP connections

- [ ] **6.3.2** Add `receive_workflow_cancellation_peer_notification` handler
  - Manager peers receive notification
  - Move workflow + ALL dependents to `_cancelled_workflows` bucket (atomic)
  - Use same per-workflow lock pattern

- [ ] **6.3.3** Ensure atomic bucket updates
  - All dependents must be added to cancelled bucket in one operation
  - No partial cancellation states

#### 6.4 Gate Cancellation Handler

- [ ] **6.4.1** Add `cancel_workflow` to Gate
  - Receive request from client
  - Dispatch to ALL datacenters with matching job
  - Track pending responses per datacenter

- [ ] **6.4.2** Add gate peer notification
  - When cancellation received, notify ALL gate peers
  - Gate peers register the cancellation request

- [ ] **6.4.3** Add gate peer failover handling
  - If job leader gate fails, peer gates have the cancellation registered
  - Re-dispatch cancellation request to datacenters if leader fails mid-cancellation

- [ ] **6.4.4** Gates push cancellation results to clients
  - Once ALL datacenters respond, aggregate results
  - Push `SingleWorkflowCancelResponse` to originating client
  - Include all cancelled dependents across all datacenters

#### 6.5 Worker Completion Await

- [ ] **6.5.1** Manager waits for ALL workers before pushing result
  - Use existing event-driven completion tracking pattern
  - Track expected workers for the workflow
  - Only push result to gate when ALL workers confirm

- [ ] **6.5.2** Handle worker timeout/failure during cancellation
  - If worker doesn't respond within timeout, mark as failed
  - Include in error list pushed to gate/client

#### 6.6 Client Multi-Datacenter Handling

- [ ] **6.6.1** Clients wait for all datacenters to return cancellation results
  - Track pending datacenters
  - Aggregate results from all DCs
  - Fire completion event when ALL DCs respond

- [ ] **6.6.2** Add `await_workflow_cancellation` to Client
  - Event-driven wait for all DC responses
  - Returns aggregated `(success, cancelled_workflows, errors)`

### Files

| File | Changes |
|------|---------|
| `hyperscale/distributed_rewrite/models/distributed.py` | New message types (6.1) |
| `hyperscale/distributed_rewrite/nodes/manager.py` | Cancellation handler, peer notification, cancelled bucket (6.2, 6.3) |
| `hyperscale/distributed_rewrite/nodes/gate.py` | Cancel workflow handler, peer notification, result aggregation (6.4) |
| `hyperscale/distributed_rewrite/nodes/worker.py` | Worker completion push (already exists, verify integration) |
| `hyperscale/distributed_rewrite/nodes/client.py` | Multi-DC await (6.6) |
| `hyperscale/distributed_rewrite/env.py` | `CANCELLED_WORKFLOW_CLEANUP_INTERVAL`, `CANCELLED_WORKFLOW_TTL` |

### Race Condition Protection

This implementation must be race-condition proof in the asyncio environment:

1. **Per-workflow locks**: Each workflow has its own `asyncio.Lock`
2. **Atomic bucket updates**: All dependents added in single operation
3. **Pre-dispatch checks**: Always check cancelled bucket before dispatch
4. **Peer sync before response**: Wait for peer acknowledgment before confirming to caller
5. **Request deduplication**: Use `request_id` to prevent duplicate processing

---

## 7. Gate Job Leadership Takeover Handling

**Problem**: When a manager that is the job leader fails, gates need to handle the transition:
1. Gates track which manager is the job leader for each job via `_job_leader_addrs`
2. When job leader manager fails, gates receive `JobLeaderGateTransfer` from the new leader
3. Gates need to handle edge cases: concurrent failures, delayed transfers, stale state

**Solution**: Similar to Section 1's approach for managers, gates need orphaned job scanning when they become aware of manager failures.

### Tasks

- [ ] **7.1** Add `_dead_job_leaders` tracking set to GateServer
  - Track managers confirmed dead that were job leaders
  - Populate when SWIM detects manager death via `_on_node_dead`
  - Clear entries when transfer received via `job_leader_gate_transfer`

- [ ] **7.2** Add `_orphaned_jobs` tracking to GateServer
  ```python
  _orphaned_jobs: dict[str, float]  # job_id -> orphan_timestamp
  ```
  - Track jobs whose leader is in `_dead_job_leaders`
  - Add timestamp when orphaned detected

- [ ] **7.3** Add `_scan_for_orphaned_jobs()` method to GateServer
  - Called when gate detects manager failure
  - For each job in `_job_leader_addrs`, check if leader is dead
  - Mark matching jobs as orphaned with current timestamp
  - Do NOT cancel jobs immediately (wait for transfer)

- [ ] **7.4** Add grace period handling for orphaned jobs
  - `GATE_ORPHAN_GRACE_PERIOD` env var (default: 10.0 seconds)
  - Grace period should be longer than manager election + takeover time
  - Periodic checker (or integrate with existing task) monitors orphaned jobs
  - If grace expires without transfer → mark job as failed

- [ ] **7.5** Update `job_leader_gate_transfer` handler
  - Clear job from `_orphaned_jobs` if present
  - Clear old leader from `_dead_job_leaders` for this job
  - Update `_job_leader_addrs` with new leader
  - Log successful transfer

- [ ] **7.6** Handle concurrent manager failures
  - If new job leader also fails during transfer
  - Gate should handle multiple transfer notifications
  - Use fencing tokens/incarnation to determine latest valid leader

- [ ] **7.7** Add `_handle_job_orphan_timeout()` method
  - Called when grace period expires
  - Notify client of job failure (push notification)
  - Clean up job state from gate
  - Log detailed failure information

### Files
- `hyperscale/distributed_rewrite/nodes/gate.py`
- `hyperscale/distributed_rewrite/env.py` (for `GATE_ORPHAN_GRACE_PERIOD`)

---

## 8. Worker Robust Response to Job Leadership Takeover

**Problem**: When a job leader manager fails and a new manager takes over, workers must robustly handle the `JobLeaderWorkerTransfer` message. Current implementation may have edge cases:
1. Race between transfer message and ongoing workflow operations
2. Multiple transfers in rapid succession (cascading failures)
3. Transfer arriving for unknown workflow (stale message)
4. Transfer validation (is the new leader legitimate?)

**Solution**: Add comprehensive validation, state machine handling, and race condition protection.

### Tasks

- [ ] **8.1** Add `_job_leader_transfer_locks` to WorkerServer
  ```python
  _job_leader_transfer_locks: dict[str, asyncio.Lock]  # job_id -> lock
  ```
  - Per-job locks to prevent race conditions during transfer
  - Acquire lock before processing transfer or workflow operations

- [ ] **8.2** Add transfer validation in `job_leader_worker_transfer` handler
  - Verify job_id exists in `_workflow_job_leader`
  - Verify fencing token is newer than current (prevent stale transfers)
  - Verify new leader is in known managers list
  - Reject invalid transfers with detailed error response

- [ ] **8.3** Add `_pending_transfers` tracking
  ```python
  _pending_transfers: dict[str, PendingTransfer]  # job_id -> transfer info
  ```
  - Track transfers that arrived before job was known (late arrival handling)
  - Check pending transfers when new job is assigned
  - Clean up stale pending transfers periodically

- [ ] **8.4** Add transfer acknowledgment flow
  - After processing transfer, send explicit `JobLeaderTransferAck` to new leader
  - Include worker's current workflow state for the job
  - New leader can verify all workers acknowledged

- [ ] **8.5** Handle in-flight operations during transfer
  - If workflow operation is in progress when transfer arrives
  - Queue transfer, apply after operation completes
  - Prevent partial state updates

- [ ] **8.6** Add transfer metrics
  - `worker_job_transfers_received` counter
  - `worker_job_transfers_accepted` counter
  - `worker_job_transfers_rejected` counter (with reason labels)
  - `worker_job_transfer_latency` histogram

- [ ] **8.7** Add detailed logging for transfer events
  - Log old leader, new leader, job_id, fencing token
  - Log rejection reasons clearly
  - Log time between job leader death detection and transfer receipt

- [ ] **8.8** Update `_on_node_dead` for defensive handling
  - When manager dies, don't immediately assume it's job leader
  - Wait for explicit transfer or orphan timeout
  - Handle case where dead node was NOT the job leader

### Files
- `hyperscale/distributed_rewrite/nodes/worker.py`
- `hyperscale/distributed_rewrite/models/distributed.py` (for `JobLeaderTransferAck`)

---

## 9. Client Robust Response to Gate and Manager Job Leadership Takeovers

**Problem**: Clients interact with both gates and managers for job operations. When leadership changes occur at either level, clients must handle the transitions robustly:

1. **Gate Job Leadership Transfer**: When the gate acting as job leader fails, another gate takes over
2. **Manager Job Leadership Transfer**: When a manager job leader fails, another manager takes over
3. Clients may have in-flight requests to the old leader
4. Clients may receive stale responses from old leaders
5. Clients need to re-route subsequent requests to new leaders

**Solution**: Add comprehensive tracking, validation, and re-routing logic for both gate and manager leadership changes.

### Tasks

#### 9.1 Gate Leadership Tracking

- [ ] **9.1.1** Add `_gate_job_leaders` tracking to HyperscaleClient
  ```python
  _gate_job_leaders: dict[str, GateLeaderInfo]  # job_id -> gate info
  # GateLeaderInfo contains: gate_addr, fencing_token, last_updated
  ```
  - Track which gate is the job leader for each job
  - Update on job submission response
  - Update on transfer notification

- [ ] **9.1.2** Add `receive_gate_job_leader_transfer` handler to Client
  - Receive push notification from new gate leader
  - Validate fencing token is newer than current
  - Update `_gate_job_leaders` mapping
  - Cancel any pending requests to old gate leader
  - Re-queue failed requests to new leader

- [ ] **9.1.3** Add `_pending_gate_requests` tracking
  ```python
  _pending_gate_requests: dict[str, list[PendingRequest]]  # gate_addr -> requests
  ```
  - Track in-flight requests per gate
  - On gate failure, identify affected requests
  - Re-route to new leader or fail gracefully

- [ ] **9.1.4** Add gate failure detection at client level
  - Monitor connection state to gates
  - On disconnect, mark gate as potentially failed
  - Wait for transfer notification or timeout
  - If timeout → fail affected jobs with clear error

#### 9.2 Manager Leadership Tracking

- [ ] **9.2.1** Add `_manager_job_leaders` tracking to HyperscaleClient
  ```python
  _manager_job_leaders: dict[str, ManagerLeaderInfo]  # job_id -> manager info
  # ManagerLeaderInfo contains: manager_addr, fencing_token, datacenter_id, last_updated
  ```
  - Track which manager is the job leader per datacenter
  - Update on job dispatch acknowledgment
  - Update on transfer notification (via gate)

- [ ] **9.2.2** Add `receive_manager_job_leader_transfer` handler to Client
  - Receive notification (typically forwarded by gate)
  - Validate fencing token
  - Update `_manager_job_leaders` mapping
  - Log transition for debugging

- [ ] **9.2.3** Handle multi-datacenter manager leadership
  - Each datacenter has independent manager leadership
  - Track per-datacenter manager leaders
  - Handle partial failures (one DC's manager fails, others ok)

#### 9.3 Request Re-routing and Retry Logic

- [ ] **9.3.1** Add automatic request re-routing on leadership change
  - Intercept responses from old leaders
  - Check if leadership changed during request
  - Re-route to new leader if safe (idempotent operations)
  - Fail with clear error if not safe (non-idempotent)

- [ ] **9.3.2** Add `_request_routing_locks` per job
  ```python
  _request_routing_locks: dict[str, asyncio.Lock]  # job_id -> lock
  ```
  - Prevent race between leadership update and request routing
  - Acquire lock before sending request or processing transfer

- [ ] **9.3.3** Add retry policy configuration
  ```python
  @dataclass
  class LeadershipRetryPolicy:
      max_retries: int = 3
      retry_delay: float = 0.5
      exponential_backoff: bool = True
      max_delay: float = 5.0
  ```
  - Configurable retry behavior on leadership changes
  - Exponential backoff to avoid thundering herd

- [ ] **9.3.4** Add idempotency key support
  - Generate unique idempotency key per request
  - Include in request headers
  - Leaders use key to deduplicate retried requests
  - Safe re-routing even for non-idempotent operations

#### 9.4 Stale Response Handling

- [ ] **9.4.1** Add fencing token validation on all responses
  - Check response fencing token against current known leader
  - Reject responses from stale leaders
  - Log stale response events for debugging

- [ ] **9.4.2** Add response freshness timeout
  - Track request send time
  - If response arrives after leadership change AND after timeout
  - Discard response, retry with new leader

- [ ] **9.4.3** Handle split-brain scenarios
  - If receiving responses from multiple "leaders"
  - Use fencing token to determine authoritative response
  - Log split-brain detection for investigation

#### 9.5 Client-Side Orphan Job Handling

- [ ] **9.5.1** Add `_orphaned_jobs` tracking to Client
  ```python
  _orphaned_jobs: dict[str, OrphanedJobInfo]  # job_id -> orphan info
  # OrphanedJobInfo contains: orphan_timestamp, last_known_gate, last_known_manager
  ```
  - Track jobs whose leaders are unknown/failed
  - Grace period before marking as failed

- [ ] **9.5.2** Add orphan job recovery
  - When new leader is discovered, check orphaned jobs
  - Query new leader for job status
  - Resume tracking or mark as failed

- [ ] **9.5.3** Add `CLIENT_ORPHAN_GRACE_PERIOD` configuration
  - Default: 15.0 seconds (longer than gate/worker grace periods)
  - Allows time for full leadership cascade: manager → gate → client

#### 9.6 Metrics and Observability

- [ ] **9.6.1** Add client-side leadership transfer metrics
  - `client_gate_transfers_received` counter
  - `client_manager_transfers_received` counter
  - `client_requests_rerouted` counter
  - `client_requests_failed_leadership_change` counter
  - `client_leadership_transfer_latency` histogram

- [ ] **9.6.2** Add detailed logging for leadership events
  - Log old leader, new leader, job_id, fencing token
  - Log request re-routing decisions
  - Log orphan job lifecycle

- [ ] **9.6.3** Add client health reporting
  - Track number of healthy gate connections
  - Track number of jobs with known leaders
  - Expose via status endpoint or callback

### Files
- `hyperscale/distributed_rewrite/nodes/client.py`
- `hyperscale/distributed_rewrite/models/distributed.py` (for `GateLeaderInfo`, `ManagerLeaderInfo`, `OrphanedJobInfo`, `LeadershipRetryPolicy`)
- `hyperscale/distributed_rewrite/env.py` (for `CLIENT_ORPHAN_GRACE_PERIOD`)

---

## Dependencies

- Item 1 can be done independently
- Item 2 (event-based cancellation) should be done before Item 3
- Item 3 depends on Item 2 for the cancellation mechanism
- Item 4 depends on Items 1, 2, 3
- Item 5 can be done after Item 2 (uses event-driven cancellation completion)
- Item 6 builds on Item 5's push notification chain
- Item 7 (gate takeover) can be done after Item 1 (follows same pattern)
- Item 8 (worker robust response) can be done after Item 3, integrates with Item 7
- Item 9 (client robust response) depends on Items 7 and 8 (receives transfers from both gate and manager layers)

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
