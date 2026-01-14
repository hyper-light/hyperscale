# Hyperscale Distributed Bug Fixes TODO

**Generated**: 2026-01-14  
**Progress**: 40/64 completed (63%)

---

## Overview

Systematic bug fixes for the Hyperscale distributed performance testing framework across three node types: **Gate**, **Manager**, and **Worker**.

### Constraints
- Do NOT modify `RemoteGraphManager`, `LocalServerPool`, or any classes in `hyperscale/core/`
- Only modify files in `hyperscale/distributed/`
- Use `asyncio.Lock`, NEVER threading locks
- Follow modular delegation architecture - changes go in coordinator/handler classes, NOT directly in server.py
- Use TaskRunner for background tasks, never raw asyncio tasks

---

## Completed Tasks (30)

- [x] **Task 1**: Fix Gate parameter mismatch (handle_exception vs active_peer_count)
- [x] **Task 2**: Fix Gate idempotency race condition - check_or_insert not atomic, TOCTOU vulnerability
- [x] **Task 3**: Fix Gate _job_submissions memory leak
- [x] **Task 4**: Fix Gate WindowedStatsCollector memory leak
- [x] **Task 5**: Fix Gate WorkflowResultPush aggregation race - _cleanup_single_job has no lock
- [x] **Task 6**: Fix Worker final results - pending result retry loop NEVER INVOKED
- [x] **Task 7**: Fix Worker core leak on dispatch failure
- [x] **Task 11**: Implement circuit breaker for gate-to-gate peer forwarding
- [x] **Task 12**: Add CircuitBreakerManager.remove_circuit calls for dead managers and peers
- [x] **Task 15**: Add retry logic for client callback pushes instead of best-effort swallow
- [x] **Task 20**: Add GateJobLeaderTransfer emission from gate to client
- [x] **Task 21**: Add ManagerJobLeaderTransfer emission from gate to client
- [x] **Task 24**: Add guard against progress updates after job completion
- [x] **Task 25**: Add windowed_stats job existence check before recording
- [x] **Task 26**: Add timeout path for missing DC workflow results
- [x] **Task 27**: Add exactly-once completion guard for duplicate final results
- [x] **Task 28**: Add TCP handler for job_leader_gate_transfer in GateServer
- [x] **Task 35**: Add GlobalJobResult aggregation path in gate
- [x] **Task 37**: Global timeout trigger gate-side cancellation/completion
- [x] **Task 39**: Add orphan job timeout -> failed path
- [x] **Task 42**: Extend state sync to include workflow results, progress callbacks
- [x] **Task 44**: Manager: Implement _cancel_workflow to send WorkflowCancelRequest
- [x] **Task 46**: Manager: Wire stats backpressure to actual stats recording
- [x] **Task 47**: Manager: Add windowed stats flush/push loop
- [x] **Task 51**: Manager: Connect StatsBuffer recording to stats handling
- [x] **Task 52**: Cross-DC correlation - wire check_correlation to gate routing
- [x] **Task 53**: Partition callbacks - wire to routing changes in health coordinator
- [x] **Task 55**: WorkflowResultPush - add fence tokens for stale rejection
- [x] **Task 56**: Manager idempotency ledger - wire to job submission dedup
- [x] **Task 57**: Gate idempotency wait_for_pending timeout -> duplicate jobs fix
- [x] **Task 58**: Manager stats backpressure - wire to windowed stats
- [x] **Task 64**: Gate process resource sampling loop - add ProcessResourceMonitor
- [x] **Task 8**: Fix Manager health state race condition
- [x] **Task 9**: Fix Manager circuit breaker auto-transition bug (verified - already correct in ErrorStats)
- [x] **Task 10**: Fix Manager dispatch counter race
- [x] **Task 19**: Add client-side fallback to query gate for leader on missed transfers
- [x] **Task 22**: Fix dead peer reaping - remove from _gate_peer_unhealthy_since (verified - already handled)
- [x] **Task 23**: Fix peer cleanup to fully purge UDP-TCP mapping (verified - already handled)
- [x] **Task 13**: Add JobFinalResult peer-forwarding for gate resilience (verified - already implemented in tcp_state_sync.py)
- [x] **Task 14**: Add immediate status replay after client reconnect/register_callback (verified - already implemented)
- [x] **Task 16**: Add job_status_push retry/peer-forward on failure (verified - already implemented in stats_coordinator.py)
- [x] **Task 17**: Invoke progress callbacks on batch updates (verified - already implemented in stats_coordinator.py)
- [x] **Task 18**: Add client poll-on-reconnect or replay mechanism (verified - already implemented with last_sequence)

---

## High Priority Tasks (10 remaining)

### Task 36: Implement mixed final status resolution across DCs
**Status:** Pending  
**Priority:** HIGH  
**Files:** `hyperscale/distributed/nodes/gate/` directory

**Problem:**  
When job runs across multiple DCs, they may report different final statuses (one COMPLETED, one FAILED). Need resolution logic.

**Requirements:**
1. Find where multi-DC job status is aggregated in gate
2. Implement status resolution rules:
   - Any FAILED → overall FAILED
   - Any CANCELLED → overall CANCELLED (unless FAILED)
   - All COMPLETED → overall COMPLETED
   - Timeout → overall TIMEOUT
3. Record per-DC status in final result for debugging
4. Handle partial responses (some DCs didn't respond)

**Commit message:** `Gate: Implement mixed final status resolution across DCs`

---

### Task 40: Integrate job lease acquisition/renewal in gate submission
**Status:** Pending  
**Priority:** HIGH  
**Files:** `hyperscale/distributed/nodes/gate/` directory

**Problem:**  
Job submission should acquire a lease for distributed coordination. Leases should be renewed periodically.

**Requirements:**
1. Find lease management code in `distributed/` (likely in `leasing/` directory)
2. On job submission in gate:
   - Acquire lease for the job
   - Store lease token with job info
   - Start renewal loop using TaskRunner
3. On job completion:
   - Release the lease
   - Stop renewal loop
4. Handle lease acquisition failures

**Commit message:** `Gate: Integrate job lease acquisition and renewal`

---

### Task 43: Manager: Add cluster/environment/mTLS validation
**Status:** Pending  
**Priority:** HIGH  
**Files:** `hyperscale/distributed/nodes/manager/` directory

**Problem:**  
Manager should validate that incoming connections are from the same cluster/environment and have valid mTLS credentials.

**Requirements:**
1. Find where manager accepts connections (likely in `server.py` or connection handler)
2. Add cluster ID validation - reject connections from different clusters
3. Add environment validation - reject prod/staging mismatch
4. Ensure mTLS is properly validated (if configured)
5. Log rejected connections with reason

**Commit message:** `Manager: Add cluster/environment/mTLS validation`

---

### Task 45: Manager: Fix WorkflowProgressAck structure mismatch
**Status:** Pending  
**Priority:** HIGH  
**Files:** `hyperscale/distributed/nodes/manager/` and `hyperscale/distributed/models/` directories

**Problem:**  
WorkflowProgressAck message structure may not match what's expected by receivers, causing deserialization failures.

**Requirements:**
1. Find `WorkflowProgressAck` model in `distributed/models`
2. Find where it's created in manager
3. Find where it's consumed (likely in gate or worker)
4. Ensure all fields match between producer and consumer
5. Fix any mismatches in field names, types, or optionality

**Commit message:** `Manager: Fix WorkflowProgressAck structure alignment`

---

### Task 48: Manager: Implement workflow reassignment to dispatch state
**Status:** Pending  
**Priority:** HIGH  
**Files:** `hyperscale/distributed/nodes/manager/` directory

**Problem:**  
When a worker fails, its workflows need to be reassigned. The reassignment needs to update dispatch state properly.

**Requirements:**
1. Find workflow reassignment logic in manager
2. When reassigning:
   - Update dispatch state to remove old worker assignment
   - Add new worker assignment
   - Update workflow tracking token if needed
   - Notify gate of reassignment
3. Handle case where no workers are available
4. Ensure atomic state updates

**Commit message:** `Manager: Implement workflow reassignment with dispatch state update`

---

### Task 49: Manager: Implement _apply_worker_state in sync.py
**Status:** Pending  
**Priority:** HIGH  
**Files:** `hyperscale/distributed/nodes/manager/sync.py` and related files

**Problem:**  
`_apply_worker_state` method in `sync.py` may be a stub or incomplete. It needs to properly apply synced worker state.

**Requirements:**
1. Find `_apply_worker_state` in manager `sync.py`
2. Implement full worker state application:
   - Update worker registry with synced workers
   - Update worker health states
   - Update worker capacity/load info
   - Handle worker removals (in sync but not local)
   - Handle new workers (in sync but not known locally)
3. Ensure thread-safe updates

**Commit message:** `Manager: Implement _apply_worker_state for sync`

---

### Task 50: Manager: Add job leader transfer sender to workers
**Status:** Pending  
**Priority:** HIGH  
**Files:** `hyperscale/distributed/nodes/manager/` directory

**Problem:**  
When job leadership transfers (manager failover), workers need to be notified of the new leader so they can send results to the right place.

**Requirements:**
1. Find where job leader transfer happens in manager
2. After transfer, send notification to all workers assigned to that job
3. Notification should include: new leader address, new fencing token
4. Handle case where worker is unreachable
5. Use existing message types if available (`JobLeaderTransfer` or similar)

**Commit message:** `Manager: Add job leader transfer notification to workers`

---

### Task 54: Manager peer state sync - reconcile leadership/fence tokens
**Status:** Pending  
**Priority:** HIGH  
**Files:** `hyperscale/distributed/nodes/manager/` directory

**Problem:**  
When manager syncs state with peers, leadership and fence tokens may conflict and need reconciliation.

**Requirements:**
1. Find peer state sync in manager
2. When syncing:
   - Compare fence tokens - higher token wins
   - Reconcile leadership based on term/election state
   - Handle split-brain scenarios
   - Update local state to match reconciled state
3. Log reconciliation decisions for debugging

**Commit message:** `Manager: Reconcile leadership/fence tokens in peer state sync`

---

### Task 59: Reporter submission flow - complete distributed path
**Status:** Pending  
**Priority:** HIGH  
**Files:** `hyperscale/distributed/` directory

**Problem:**  
Reporter result submission in distributed mode may be incomplete - results may not flow properly from workers through managers to gate to client.

**Requirements:**
1. Trace the reporter result flow:
   - Worker generates reporter results
   - Worker sends to manager
   - Manager aggregates and sends to gate
   - Gate forwards to client
2. Find and fix any gaps in this flow
3. Add `ReporterResultPush` message handling if missing
4. Ensure results are not lost on node failures

**Commit message:** `Distributed: Complete reporter result submission flow`

---

## Medium Priority Tasks (14 remaining)

### Task 29: Integrate DatacenterCapacityAggregator into routing/dispatch
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/routing/` directory

**Problem:**  
`DatacenterCapacityAggregator` exists but may not be wired into routing decisions.

**Requirements:**
1. Find `DatacenterCapacityAggregator` implementation
2. Wire capacity data into routing decision logic
3. Use capacity info to avoid overloaded DCs
4. Add fallback behavior when capacity data is stale

**Commit message:** `Routing: Integrate DatacenterCapacityAggregator into dispatch`

---

### Task 30: Integrate SpilloverEvaluator into routing decisions
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/routing/` directory

**Problem:**  
`SpilloverEvaluator` exists but may not be used in routing.

**Requirements:**
1. Find `SpilloverEvaluator` implementation
2. Wire into routing decision logic
3. Trigger spillover when primary DC is overloaded
4. Log spillover events for debugging

**Commit message:** `Routing: Integrate SpilloverEvaluator into decisions`

---

### Task 31: Add ordering/dedup for JobProgress beyond fence token
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/nodes/gate/` directory

**Problem:**  
JobProgress updates may arrive out of order or duplicated. Fence token helps but may not be sufficient.

**Requirements:**
1. Find JobProgress handling in gate
2. Add sequence number tracking per job
3. Reject out-of-order updates (or reorder if buffering is acceptable)
4. Deduplicate based on sequence + fence token

**Commit message:** `Gate: Add ordering and dedup for JobProgress updates`

---

### Task 32: Add explicit progress percentage calculation in gate
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/nodes/gate/` directory

**Problem:**  
Progress percentage may not be calculated or may be inaccurate.

**Requirements:**
1. Find where progress is tracked in gate
2. Calculate percentage based on completed/total work units
3. Handle multi-DC jobs (aggregate progress across DCs)
4. Include in progress callbacks to client

**Commit message:** `Gate: Add explicit progress percentage calculation`

---

### Task 33: Add recovery path for manager dies with pending stats
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/nodes/manager/` directory

**Problem:**  
If manager dies with pending stats, those stats are lost.

**Requirements:**
1. Find stats buffering in manager
2. Add periodic checkpoint of pending stats
3. On manager recovery, reload checkpointed stats
4. Or: forward stats to peer manager before death

**Commit message:** `Manager: Add recovery path for pending stats on failure`

---

### Task 34: Add ReporterResultPush forwarding path in gate
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/nodes/gate/` directory

**Problem:**  
`ReporterResultPush` may not have a proper forwarding path in gate.

**Requirements:**
1. Find `ReporterResultPush` handling in gate
2. Add forwarding to registered client callbacks
3. Handle case where client is disconnected
4. Buffer results if needed for reconnecting clients

**Commit message:** `Gate: Add ReporterResultPush forwarding path`

---

### Task 38: Add reporter task creation and result dispatch in gate
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/nodes/gate/` directory

**Problem:**  
Reporter tasks may not be properly created or results may not be dispatched.

**Requirements:**
1. Find reporter task handling in gate
2. Ensure tasks are created when job requests reporting
3. Dispatch results to appropriate handlers
4. Clean up reporter tasks on job completion

**Commit message:** `Gate: Add reporter task creation and result dispatch`

---

### Task 41: Add LeaseTransfer sender in gate code
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/nodes/gate/` directory

**Problem:**  
When job leadership transfers between gates, lease should transfer too.

**Requirements:**
1. Find where gate leadership transfer happens
2. Add lease transfer as part of the handoff
3. Include lease token and expiry in transfer
4. Handle transfer failures gracefully

**Commit message:** `Gate: Add LeaseTransfer sender for leadership handoff`

---

### Task 60: Routing SLO-constraint gating - filter by SLO targets
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/routing/` directory

**Problem:**  
Routing may not respect SLO constraints when selecting destinations.

**Requirements:**
1. Find routing decision logic
2. Add SLO constraint checking (latency, throughput targets)
3. Filter out destinations that can't meet SLO
4. Fallback behavior when no destination meets SLO

**Commit message:** `Routing: Add SLO-constraint gating for destination selection`

---

### Task 61: Latency handling - add percentile/jitter control
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/` directory

**Problem:**  
Latency tracking may not include percentile calculations or jitter handling.

**Requirements:**
1. Find latency tracking code
2. Add percentile calculations (p50, p95, p99)
3. Add jitter detection and smoothing
4. Use in routing and health decisions

**Commit message:** `Distributed: Add latency percentile and jitter control`

---

### Task 62: Connection storm mitigation - add explicit connection caps
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/` directory

**Problem:**  
Connection storms can overwhelm nodes. Need explicit caps.

**Requirements:**
1. Find connection acceptance code in each node type
2. Add configurable connection limits
3. Reject new connections when at limit
4. Add backoff/retry guidance in rejection response

**Commit message:** `Distributed: Add connection storm mitigation with explicit caps`

---

### Task 63: Protocol size violations - send structured error response
**Status:** Pending  
**Priority:** MEDIUM  
**Files:** `hyperscale/distributed/` directory

**Problem:**  
When protocol messages exceed size limits, error response may not be helpful.

**Requirements:**
1. Find message size validation code
2. On size violation, send structured error with:
   - Actual size vs limit
   - Which field is too large (if detectable)
   - Suggested remediation
3. Log violations for debugging

**Commit message:** `Distributed: Add structured error response for protocol size violations`

---

## Verification Checklist

After implementing fixes, verify:

### High Priority
- [ ] All Manager race conditions fixed with asyncio.Lock
- [ ] Circuit breaker state transitions are correct
- [ ] JobFinalResult forwards to leader gate
- [ ] Client reconnect replays missed status
- [ ] Dead peer cleanup removes all tracking data
- [ ] Multi-DC status resolution works correctly
- [ ] Job leases are acquired and renewed
- [ ] Manager validates cluster/environment
- [ ] WorkflowProgressAck structure matches consumers
- [ ] Workflow reassignment updates dispatch state
- [ ] Worker state sync applies correctly
- [ ] Job leader transfers notify workers
- [ ] Peer sync reconciles fence tokens
- [ ] Reporter results flow end-to-end

### Medium Priority
- [ ] DatacenterCapacityAggregator influences routing
- [ ] SpilloverEvaluator triggers when needed
- [ ] JobProgress is ordered and deduplicated
- [ ] Progress percentage is calculated correctly
- [ ] Manager stats survive failure
- [ ] ReporterResultPush reaches clients
- [ ] Reporter tasks are created properly
- [ ] LeaseTransfer happens on gate handoff
- [ ] SLO constraints gate routing
- [ ] Latency percentiles are tracked
- [ ] Connection limits prevent storms
- [ ] Protocol size errors are helpful

---

## Notes

- All changes must pass `lsp_diagnostics` before committing
- Run integration tests after completing related task groups
- Use TaskRunner for background tasks, never raw asyncio tasks
- Follow existing code patterns in each file
- One class per file rule applies
- Memory leaks are unacceptable - always clean up
