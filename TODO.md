# AD-38 to AD-45: Critical Fixes and Integration TODO

Generated: 2026-01-12
Audit Reference: `docs/architecture/AUDIT_DISTRIBUTED_2026_01_11.md`

---

## Priority Legend

- **P0 (CRITICAL)**: Must fix immediately - causes data loss, crashes, memory leaks, or security issues
- **P1 (HIGH)**: Should fix soon - causes significant degradation or incorrect behavior
- **P2 (MEDIUM)**: Should fix - causes minor issues or technical debt
- **P3 (LOW)**: Nice to have - code quality improvements

---

## Executive Summary

| Category | Count | Highest Priority |
|----------|-------|------------------|
| Memory Leaks | 4 | P0 |
| Race Conditions | 8 | P0 |
| Silent Failures | 149 | P0 |
| Orphaned Tasks | 59 | P0 |
| Missing AD Integration | 6 ADs | P1 |

---

# Part 1: Critical Fixes (P0)

## Section 1.1: Memory Leaks

### 1.1.1 [P0] Gate Server Missing Job Cleanup

**File**: `hyperscale/distributed/nodes/gate/server.py`
**Lines**: 2768-2777

**Problem**: The `_job_cleanup_loop` removes completed jobs but fails to clean up two dictionaries, causing unbounded memory growth.

**Current Code**:
```python
for job_id in jobs_to_remove:
    self._job_manager.delete_job(job_id)
    self._workflow_dc_results.pop(job_id, None)
    self._job_workflow_ids.pop(job_id, None)
    self._progress_callbacks.pop(job_id, None)
    self._job_leadership_tracker.release_leadership(job_id)
    self._job_dc_managers.pop(job_id, None)
    # MISSING CLEANUP
```

**Fix**: Add cleanup for `_job_reporter_tasks` and `_job_stats_crdt` after line 2774:
```python
for job_id in jobs_to_remove:
    self._job_manager.delete_job(job_id)
    self._workflow_dc_results.pop(job_id, None)
    self._job_workflow_ids.pop(job_id, None)
    self._progress_callbacks.pop(job_id, None)
    self._job_leadership_tracker.release_leadership(job_id)
    self._job_dc_managers.pop(job_id, None)
    
    # Cancel and remove reporter tasks for this job
    reporter_tasks = self._job_reporter_tasks.pop(job_id, None)
    if reporter_tasks:
        for task in reporter_tasks.values():
            if task and not task.done():
                task.cancel()
    
    # Remove CRDT stats for this job
    self._job_stats_crdt.pop(job_id, None)
```

**References**:
- `_job_reporter_tasks` initialized at line 418
- `_job_stats_crdt` initialized at line 421
- Manager server properly cleans up in `_cleanup_reporter_tasks()` at line 2030

---

### 1.1.2 [P2] Unbounded Latency Sample Lists

**File**: `hyperscale/distributed/nodes/manager/state.py`
**Lines**: 135-137

**Problem**: Latency sample lists grow indefinitely without bounds.

**Current Code**:
```python
self._gate_latency_samples: list[tuple[float, float]] = []
self._peer_manager_latency_samples: dict[str, list[tuple[float, float]]] = {}
self._worker_latency_samples: dict[str, list[tuple[float, float]]] = {}
```

**Fix**: Use bounded deques with max size:
```python
from collections import deque

MAX_LATENCY_SAMPLES = 1000

self._gate_latency_samples: deque[tuple[float, float]] = deque(maxlen=MAX_LATENCY_SAMPLES)
self._peer_manager_latency_samples: dict[str, deque[tuple[float, float]]] = {}
self._worker_latency_samples: dict[str, deque[tuple[float, float]]] = {}

# Update getter methods to create bounded deques:
def _get_peer_latency_samples(self, peer_id: str) -> deque[tuple[float, float]]:
    if peer_id not in self._peer_manager_latency_samples:
        self._peer_manager_latency_samples[peer_id] = deque(maxlen=MAX_LATENCY_SAMPLES)
    return self._peer_manager_latency_samples[peer_id]
```

---

### 1.1.3 [P2] Lock Dictionaries Grow Unboundedly

**Files**:
- `hyperscale/distributed/nodes/manager/state.py:49, 61, 108`
- `hyperscale/distributed/nodes/gate/state.py:44`
- `hyperscale/distributed/nodes/worker/state.py:65, 162, 277`
- `hyperscale/distributed/nodes/gate/models/gate_peer_state.py:80`

**Problem**: Lock dictionaries are created on-demand but never removed when peers/jobs disconnect.

**Fix**: Add cleanup methods and call them when peers/jobs are removed:
```python
def remove_peer_lock(self, peer_addr: tuple[str, int]) -> None:
    """Remove lock when peer disconnects."""
    self._peer_state_locks.pop(peer_addr, None)

def remove_job_lock(self, job_id: str) -> None:
    """Remove lock when job completes."""
    self._job_locks.pop(job_id, None)
```

Call these in the appropriate cleanup paths (peer disconnect handlers, job cleanup loops).

---

### 1.1.4 [P3] Inefficient Event History in HierarchicalFailureDetector

**File**: `hyperscale/distributed/swim/detection/hierarchical_failure_detector.py`
**Lines**: 740-744

**Problem**: Using `list.pop(0)` is O(n) for a bounded buffer.

**Current Code**:
```python
def _record_event(self, event: FailureEvent) -> None:
    self._recent_events.append(event)
    if len(self._recent_events) > self._max_event_history:
        self._recent_events.pop(0)
```

**Fix**: Use `collections.deque` with maxlen:
```python
from collections import deque

# In __init__:
self._recent_events: deque[FailureEvent] = deque(maxlen=self._max_event_history)

# In _record_event:
def _record_event(self, event: FailureEvent) -> None:
    self._recent_events.append(event)  # Automatically drops oldest when full
```

---

## Section 1.2: Race Conditions

### 1.2.1 [P0] Double-Checked Locking Race in Context

**File**: `hyperscale/distributed/server/context/context.py`
**Lines**: 20-27

**Problem**: First check is unprotected, allowing two coroutines to create different locks for the same key.

**Current Code**:
```python
async def get_value_lock(self, key: str) -> asyncio.Lock:
    if key in self._value_locks:  # RACE: Check without lock
        return self._value_locks[key]
    
    async with self._value_locks_creation_lock:
        if key not in self._value_locks:
            self._value_locks[key] = asyncio.Lock()
        return self._value_locks[key]
```

**Fix**: Always acquire the creation lock:
```python
async def get_value_lock(self, key: str) -> asyncio.Lock:
    async with self._value_locks_creation_lock:
        if key not in self._value_locks:
            self._value_locks[key] = asyncio.Lock()
        return self._value_locks[key]
```

---

### 1.2.2 [P0] Unprotected Counter Increments in GateRuntimeState

**File**: `hyperscale/distributed/nodes/gate/state.py`
**Lines**: 106-111, 186-189, 244-246, 261-264

**Problem**: Read-modify-write operations are not atomic, causing lost increments under concurrency.

**Affected Methods**:
- `increment_peer_epoch()` (lines 106-111)
- `next_fence_token()` (lines 186-189)
- `record_forward()` (line 246)
- `increment_state_version()` (lines 261-264)

**Fix**: Add lock and make methods async:
```python
# Add to __init__:
self._counter_lock = asyncio.Lock()

# Update methods:
async def increment_peer_epoch(self, peer_addr: tuple[str, int]) -> int:
    async with self._counter_lock:
        current_epoch = self._peer_state_epoch.get(peer_addr, 0)
        new_epoch = current_epoch + 1
        self._peer_state_epoch[peer_addr] = new_epoch
        return new_epoch

async def next_fence_token(self) -> int:
    async with self._counter_lock:
        self._fence_token_counter += 1
        return self._fence_token_counter

async def record_forward(self) -> None:
    async with self._counter_lock:
        self._forward_throughput_count += 1

async def increment_state_version(self) -> int:
    async with self._counter_lock:
        self._state_version += 1
        return self._state_version
```

**Note**: Update all callers to `await` these methods.

---

### 1.2.3 [P0] Unprotected Counter Increments in ClientState

**File**: `hyperscale/distributed/nodes/client/state.py`
**Lines**: 173-187

**Problem**: Four counter increment methods are not thread-safe.

**Affected Methods**:
- `increment_gate_transfers()`
- `increment_manager_transfers()`
- `increment_rerouted()`
- `increment_failed_leadership_change()`

**Fix**: Add lock and make methods async (same pattern as 1.2.2):
```python
# Add to __init__:
self._metrics_lock = asyncio.Lock()

# Update methods:
async def increment_gate_transfers(self) -> None:
    async with self._metrics_lock:
        self._gate_transfers_received += 1
```

---

### 1.2.4 [P0] Unprotected Counter Increments in ManagerState

**File**: `hyperscale/distributed/nodes/manager/state.py`
**Lines**: 174-192

**Problem**: Critical counters including fence_token are not protected.

**Affected Methods**:
- `increment_fence_token()` - **CRITICAL: affects at-most-once semantics**
- `increment_state_version()`
- `increment_external_incarnation()`
- `increment_context_lamport_clock()`

**Fix**: Add lock and make methods async (same pattern as 1.2.2).

---

### 1.2.5 [P0] Unprotected Counter Increment in WorkerState

**File**: `hyperscale/distributed/nodes/worker/state.py`
**Lines**: 108-111

**Problem**: State version increment is not protected.

**Fix**: Add lock and make method async (same pattern as 1.2.2).

---

### 1.2.6 [P1] TOCTOU Race in GateJobManager Fence Token

**File**: `hyperscale/distributed/jobs/gates/gate_job_manager.py`
**Lines**: 211-221

**Problem**: Time-of-check-time-of-use race in fence token update.

**Fix**: Add lock or document that caller must hold job lock:
```python
async def update_fence_token_if_higher(self, job_id: str, token: int) -> bool:
    """
    Update fence token only if new token is higher.
    
    MUST be called with job lock held via lock_job(job_id).
    """
    async with self._fence_token_lock:
        current = self._job_fence_tokens.get(job_id, 0)
        if token > current:
            self._job_fence_tokens[job_id] = token
            return True
        return False
```

---

### 1.2.7 [P1] TOCTOU Race in JobManager.get_next_fence_token

**File**: `hyperscale/distributed/jobs/job_manager.py`
**Lines**: 160-191

**Fix**: Add lock protection (same pattern as 1.2.6).

---

### 1.2.8 [P2] TOCTOU Race in ConnectionPool.acquire

**File**: `hyperscale/distributed/discovery/pool/connection_pool.py`
**Lines**: 160-212

**Problem**: Connection limits can be exceeded between releasing and re-acquiring lock.

**Fix**: Re-check limits after creating connection:
```python
async def acquire(self, peer_id: str, timeout: float | None = None) -> PooledConnection[T]:
    # ... create connection outside lock ...
    
    async with self._get_lock():
        # RE-CHECK LIMITS after creating connection
        if self._total_connections >= self.config.max_total_connections:
            await self.close_fn(connection)
            raise RuntimeError("Connection pool exhausted (limit reached during creation)")
        
        peer_connections = self._connections.get(peer_id, [])
        if len(peer_connections) >= self.config.max_connections_per_peer:
            await self.close_fn(connection)
            raise RuntimeError(f"Max connections per peer reached for {peer_id}")
        
        # ... add connection ...
```

---

## Section 1.3: Silent/Dropped Failures

### 1.3.1 [P0] Manager Server Background Tasks Without Error Handling

**File**: `hyperscale/distributed/nodes/manager/server.py`
**Lines**: 712-730

**Problem**: 19 background tasks created with `asyncio.create_task()` without error callbacks. Any exception crashes silently.

**Affected Tasks**:
- `_dead_node_reap_task`
- `_orphan_scan_task`
- `_discovery_maintenance_task`
- `_job_responsiveness_task`
- `_stats_push_task`
- `_gate_heartbeat_task`
- `_rate_limit_cleanup_task`
- `_job_cleanup_task`
- `_unified_timeout_task`
- `_deadline_enforcement_task`
- `_peer_job_state_sync_task`
- And 8 more...

**Fix**: Create helper to add error callback:
```python
def _create_background_task(self, coro, name: str) -> asyncio.Task:
    """Create background task with error logging."""
    task = asyncio.create_task(coro, name=name)
    task.add_done_callback(lambda t: self._handle_task_error(t, name))
    return task

def _handle_task_error(self, task: asyncio.Task, name: str) -> None:
    """Log background task errors."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        # Fire-and-forget logging (task runner handles async)
        self._task_runner.run(
            self._udp_logger.log(
                ServerError(
                    message=f"Background task '{name}' failed: {exc}",
                    node_id=self._node_id.short,
                    error_type=type(exc).__name__,
                )
            )
        )

# Usage in _start_background_tasks():
self._dead_node_reap_task = self._create_background_task(
    self._dead_node_reap_loop(), "dead_node_reap"
)
```

---

### 1.3.2 [P0] Worker Server Background Tasks Without Error Handling

**File**: `hyperscale/distributed/nodes/worker/server.py`
**Lines**: 532, 546, 558, 577, 589, 597, 986

**Problem**: 7 background tasks without error callbacks.

**Fix**: Apply same pattern as 1.3.1.

---

### 1.3.3 [P0] WAL Writer Tasks Without Error Handling

**File**: `hyperscale/distributed/ledger/wal/wal_writer.py`
**Lines**: 155, 297

**Problem**: WAL writer and state change tasks fail silently, compromising durability.

**Fix**: Apply same pattern as 1.3.1.

---

### 1.3.4 [P1] Replace All Bare `except Exception: pass` Blocks

**Count**: 149 instances across 65+ files

**Critical Files** (prioritize these):
| File | Count | Risk |
|------|-------|------|
| `nodes/manager/server.py` | 5 | Infrastructure |
| `nodes/gate/server.py` | 8 | Infrastructure |
| `nodes/worker/progress.py` | 6 | Data loss |
| `server/server/mercury_sync_base_server.py` | 12 | Networking |
| `encryption/aes_gcm.py` | 4 | **SECURITY** |
| `taskex/task_runner.py` | 5 | Task execution |
| `taskex/run.py` | 5 | Task execution |

**Fix Pattern**: Replace with logging at minimum:
```python
# Before:
except Exception:
    pass

# After:
except Exception as error:
    await self._logger.log(
        ServerError(
            message=f"Operation failed in {context}: {error}",
            error_type=type(error).__name__,
        )
    )
```

**For cleanup paths where we truly want to continue**:
```python
except Exception as error:
    # Intentionally continue cleanup despite error
    await self._logger.log(
        ServerWarning(
            message=f"Cleanup error (continuing): {error}",
        )
    )
```

---

### 1.3.5 [P1] Callback Error Swallowing

**Files** (11 total):
| File | Line |
|------|------|
| `nodes/client/handlers/tcp_job_status_push.py` | 60 |
| `nodes/client/handlers/tcp_windowed_stats.py` | 66 |
| `nodes/client/handlers/tcp_reporter_result.py` | 61 |
| `nodes/client/handlers/tcp_workflow_result.py` | 96 |
| `swim/detection/job_suspicion_manager.py` | 324 |
| `swim/detection/timing_wheel.py` | 373 |
| `swim/health/peer_health_awareness.py` | 209, 215 |
| `swim/gossip/health_gossip_buffer.py` | 263 |
| `swim/gossip/gossip_buffer.py` | 347 |
| `leases/job_lease.py` | 282 |

**Fix**: Log callback errors before continuing:
```python
# Before:
try:
    await callback(data)
except Exception:
    pass

# After:
try:
    await callback(data)
except Exception as error:
    await self._logger.log(
        ServerWarning(
            message=f"Callback error (user code): {error}",
            error_type=type(error).__name__,
        )
    )
```

---

### 1.3.6 [P2] asyncio.gather Without return_exceptions

**Files**:
- `hyperscale/distributed/nodes/client/discovery.py`
- `hyperscale/distributed/nodes/worker/lifecycle.py`
- `hyperscale/distributed/discovery/dns/resolver.py`
- `hyperscale/distributed/taskex/task.py`
- `hyperscale/distributed/taskex/task_runner.py`

**Fix**: Add `return_exceptions=True` to cleanup/parallel operations:
```python
# Before:
results = await asyncio.gather(*tasks)

# After (for cleanup paths):
results = await asyncio.gather(*tasks, return_exceptions=True)
for result in results:
    if isinstance(result, Exception):
        await self._logger.log(ServerWarning(message=f"Parallel task error: {result}"))
```

---

# Part 2: AD Component Integration (P1-P2)

## Section 2.1: Integration Status Matrix

| Component | Gate | Manager | Worker | Status |
|-----------|------|---------|--------|--------|
| **AD-38 WAL** | Optional | Yes | N/A | Partial |
| **AD-38 JobLedger** | Optional | No | N/A | Missing |
| **AD-40 Idempotency** | No | No | N/A | **Missing** |
| **AD-41 Resources** | No | No | No | **Missing** |
| **AD-42 SLO/TDigest** | No | No | No | **Missing** |
| **AD-43 Capacity** | No | No | N/A | **Missing** |
| **AD-44 Retry Budget** | N/A | No | N/A | **Missing** |
| **AD-44 Best-Effort** | No | N/A | N/A | **Missing** |
| **AD-45 Route Learning** | No | N/A | N/A | **Missing** |

---

## Section 2.2: AD-40 Idempotency Integration

### 2.2.1 [P1] Integrate AD-40 Idempotency into Gate Server

**Files to Modify**:
- `hyperscale/distributed/nodes/gate/server.py`
- `hyperscale/distributed/nodes/gate/handlers/tcp_job.py`

**Implementation**:

1. Add to `GateServer.__init__()`:
```python
from hyperscale.distributed.idempotency import GateIdempotencyCache

self._idempotency_cache: GateIdempotencyCache[JobAck] = GateIdempotencyCache(
    max_size=env.IDEMPOTENCY_CACHE_MAX_SIZE,
    ttl_seconds=env.IDEMPOTENCY_CACHE_TTL,
)
```

2. Modify job submission handler to check idempotency:
```python
async def _handle_job_submission(self, submission: JobSubmission, ...) -> JobAck:
    # Check idempotency cache first
    if submission.idempotency_key:
        cached = await self._idempotency_cache.get(submission.idempotency_key)
        if cached and cached.status == IdempotencyStatus.COMMITTED:
            return cached.result
        
        if cached and cached.status == IdempotencyStatus.PENDING:
            # Wait for in-flight request to complete
            return await self._idempotency_cache.wait_for_completion(
                submission.idempotency_key
            )
        
        # Mark as pending
        await self._idempotency_cache.mark_pending(
            submission.idempotency_key,
            job_id=job_id,
            source_gate_id=self._node_id.full,
        )
    
    try:
        result = await self._process_job_submission(submission, ...)
        
        if submission.idempotency_key:
            await self._idempotency_cache.commit(submission.idempotency_key, result)
        
        return result
    except Exception as error:
        if submission.idempotency_key:
            await self._idempotency_cache.reject(
                submission.idempotency_key,
                JobAck(success=False, error=str(error)),
            )
        raise
```

---

## Section 2.3: AD-44 Retry Budgets Integration

### 2.3.1 [P1] Integrate AD-44 Retry Budgets into WorkflowDispatcher

**Files to Modify**:
- `hyperscale/distributed/jobs/workflow_dispatcher.py`
- `hyperscale/distributed/nodes/manager/server.py`

**Implementation**:

1. Add to `WorkflowDispatcher.__init__()`:
```python
from hyperscale.distributed.reliability import RetryBudgetManager, ReliabilityConfig

self._retry_budget_manager = RetryBudgetManager(
    config=ReliabilityConfig.from_env(env),
)
```

2. Check budget before retry:
```python
async def _retry_workflow(self, workflow_id: str, job_id: str, ...) -> bool:
    # Check retry budget before attempting
    if not self._retry_budget_manager.try_consume(job_id):
        await self._logger.log(
            ServerWarning(
                message=f"Retry budget exhausted for job {job_id}, failing workflow {workflow_id}",
            )
        )
        return False
    
    # Proceed with retry
    return await self._dispatch_workflow(...)
```

3. Record outcomes:
```python
async def _handle_workflow_result(self, result: WorkflowResult) -> None:
    if result.success:
        self._retry_budget_manager.record_success(result.job_id)
    else:
        self._retry_budget_manager.record_failure(result.job_id)
```

---

## Section 2.4: AD-41 Resource Guards Integration

### 2.4.1 [P2] Integrate AD-41 Resource Guards into Worker

**Files to Modify**:
- `hyperscale/distributed/nodes/worker/server.py`
- `hyperscale/distributed/nodes/worker/heartbeat.py`

**Implementation**:

1. Add resource monitor to worker:
```python
from hyperscale.distributed.resources import ProcessResourceMonitor

self._resource_monitor = ProcessResourceMonitor(
    smoothing_alpha=0.2,
    process_noise=0.01,
    measurement_noise=0.1,
)
```

2. Include in heartbeat:
```python
async def _build_heartbeat(self) -> WorkerHeartbeat:
    metrics = await self._resource_monitor.sample()
    
    return WorkerHeartbeat(
        worker_id=self._node_id.full,
        # ... existing fields ...
        cpu_percent=metrics.cpu_percent,
        cpu_uncertainty=metrics.cpu_uncertainty,
        memory_percent=metrics.memory_percent,
        memory_uncertainty=metrics.memory_uncertainty,
    )
```

---

## Section 2.5: AD-42 SLO Tracking Integration

### 2.5.1 [P2] Integrate AD-42 SLO Tracking into Manager

**Files to Modify**:
- `hyperscale/distributed/nodes/manager/state.py`
- `hyperscale/distributed/nodes/manager/server.py`

**Implementation**:

1. Add TDigest to manager state:
```python
from hyperscale.distributed.slo import TimeWindowedTDigest, SLOConfig

self._latency_digest = TimeWindowedTDigest(
    config=SLOConfig.from_env(env),
    window_size_seconds=60.0,
)
```

2. Record workflow latencies:
```python
async def _handle_workflow_complete(self, result: WorkflowFinalResult) -> None:
    self._latency_digest.add(result.duration_ms, time.time())
```

3. Include SLO summary in heartbeat:
```python
async def _build_heartbeat(self) -> ManagerHeartbeat:
    slo_summary = self._latency_digest.get_summary()
    
    return ManagerHeartbeat(
        # ... existing fields ...
        slo_p50_ms=slo_summary.p50,
        slo_p95_ms=slo_summary.p95,
        slo_p99_ms=slo_summary.p99,
        slo_compliance=slo_summary.compliance_level,
    )
```

---

## Section 2.6: AD-43 Capacity Spillover Integration

### 2.6.1 [P2] Integrate AD-43 Capacity Spillover into Gate

**Files to Modify**:
- `hyperscale/distributed/nodes/gate/routing.py`
- `hyperscale/distributed/nodes/gate/server.py`

**Implementation**:

1. Add capacity aggregator:
```python
from hyperscale.distributed.capacity import (
    DatacenterCapacityAggregator,
    SpilloverEvaluator,
)

self._capacity_aggregator = DatacenterCapacityAggregator()
self._spillover_evaluator = SpilloverEvaluator.from_env(env)
```

2. Update capacity from manager heartbeats:
```python
async def _handle_manager_heartbeat(self, heartbeat: ManagerHeartbeat) -> None:
    self._capacity_aggregator.update_manager(
        dc_id=heartbeat.dc_id,
        manager_id=heartbeat.manager_id,
        available_cores=heartbeat.available_cores,
        pending_workflows=heartbeat.pending_workflows,
        estimated_wait_ms=heartbeat.estimated_wait_ms,
    )
```

3. Evaluate spillover before routing:
```python
async def _route_job(self, submission: JobSubmission) -> str:
    primary_dc = self._select_primary_dc(submission)
    primary_capacity = self._capacity_aggregator.get_dc_capacity(primary_dc)
    
    decision = self._spillover_evaluator.evaluate(
        primary_capacity=primary_capacity,
        fallback_capacities=self._get_fallback_capacities(primary_dc),
        workflow_count=submission.workflow_count,
    )
    
    if decision.should_spillover:
        return decision.target_dc
    
    return primary_dc
```

---

## Section 2.7: AD-45 Route Learning Integration

### 2.7.1 [P2] Integrate AD-45 Route Learning into Gate

**Files to Modify**:
- `hyperscale/distributed/nodes/gate/server.py`
- `hyperscale/distributed/routing/gate_job_router.py`

**Implementation**:

1. Add observed latency tracker:
```python
from hyperscale.distributed.routing import (
    ObservedLatencyTracker,
    BlendedLatencyScorer,
    DispatchTimeTracker,
)

self._dispatch_time_tracker = DispatchTimeTracker()
self._observed_latency_tracker = ObservedLatencyTracker(
    alpha=env.ROUTE_LEARNING_EWMA_ALPHA,
    min_samples_for_confidence=env.ROUTE_LEARNING_MIN_SAMPLES,
    max_staleness_seconds=env.ROUTE_LEARNING_MAX_STALENESS_SECONDS,
)
self._blended_scorer = BlendedLatencyScorer(self._observed_latency_tracker)
```

2. Record dispatch time:
```python
async def _dispatch_to_dc(self, job_id: str, dc_id: str, ...) -> bool:
    self._dispatch_time_tracker.record_dispatch(job_id, dc_id)
    # ... dispatch logic ...
```

3. Record completion latency:
```python
async def _handle_job_complete(self, job_id: str, dc_id: str) -> None:
    latency_ms = self._dispatch_time_tracker.get_latency(job_id, dc_id)
    if latency_ms is not None:
        self._observed_latency_tracker.record_job_latency(dc_id, latency_ms)
```

4. Use blended scoring in router:
```python
def score_datacenter(self, dc_id: str, rtt_ucb_ms: float) -> float:
    return self._blended_scorer.get_blended_latency(dc_id, rtt_ucb_ms)
```

---

# Part 3: Verification Checklist

After implementing fixes, verify:

## Critical Fixes (P0)
- [ ] Gate server job cleanup removes `_job_reporter_tasks` and `_job_stats_crdt`
- [ ] All counter increment methods in state.py files are async and locked
- [ ] Context.get_value_lock() always acquires creation lock
- [ ] All 19 manager server background tasks have error callbacks
- [ ] All 7 worker server background tasks have error callbacks
- [ ] WAL writer tasks have error callbacks

## High Priority (P1)
- [ ] No bare `except Exception: pass` blocks in critical files
- [ ] Callback error handlers log before continuing
- [ ] AD-40 idempotency prevents duplicate job processing
- [ ] AD-44 retry budgets are checked before dispatch retries

## Medium Priority (P2)
- [ ] Latency sample lists use bounded deques
- [ ] Lock dictionaries have cleanup methods
- [ ] asyncio.gather() uses return_exceptions in cleanup paths
- [ ] AD-41 resource metrics appear in worker heartbeats
- [ ] AD-42 SLO summaries appear in manager heartbeats
- [ ] AD-43 capacity data influences routing decisions
- [ ] AD-45 observed latency is recorded and used for scoring

---

# Appendix A: Files Requiring Most Attention

| Priority | File | Issues |
|----------|------|--------|
| P0 | `nodes/gate/server.py` | Memory leak, 8 silent failures |
| P0 | `nodes/manager/server.py` | 19 unhandled background tasks, 5 silent failures |
| P0 | `nodes/manager/state.py` | 4 race conditions |
| P0 | `nodes/gate/state.py` | 4 race conditions |
| P0 | `nodes/worker/server.py` | 7 unhandled background tasks |
| P0 | `server/context/context.py` | Double-checked locking race |
| P1 | `server/server/mercury_sync_base_server.py` | 12 silent failures |
| P1 | `taskex/task_runner.py` | 5 silent failures |
| P1 | `encryption/aes_gcm.py` | 4 silent failures (**security risk**) |

---

# Appendix B: Original AD Implementation Plan

(Retained from original TODO.md for reference)

## Dependency Analysis

| AD | Title | Dependencies | Blocking For |
|----|-------|--------------|--------------|
| AD-40 | Idempotent Job Submissions | AD-38 (VSR), AD-39 (WAL) | None |
| AD-41 | Resource Guards | None | AD-42 (optional prediction integration) |
| AD-42 | SLO-Aware Health & Routing | AD-41 (for resource prediction) | None |
| AD-43 | Capacity-Aware Spillover | AD-36 (existing) | None |
| AD-44 | Retry Budgets & Best-Effort | None | None |
| AD-45 | Adaptive Route Learning | AD-36 (existing) | None |

## Parallel Execution Tracks

```
TIME ──────────────────────────────────────────────────────────────────►

TRACK A (Idempotency)      TRACK B (Resource Monitoring)   TRACK C (Routing)        TRACK D (Reliability)
─────────────────────      ──────────────────────────────  ──────────────────────   ─────────────────────

┌──────────────────┐       ┌──────────────────────┐       ┌──────────────────┐     ┌──────────────────┐
│    AD-40         │       │      AD-41           │       │    AD-43         │     │    AD-44         │
│  Idempotency     │       │  Resource Guards     │       │   Spillover      │     │  Retry Budgets   │
│  (Gate+Manager)  │       │  (Worker→Manager→    │       │   (Gate)         │     │  (Gate+Manager)  │
│                  │       │   Gate Aggregation)  │       │                  │     │                  │
└──────────────────┘       └──────────┬───────────┘       └──────────────────┘     └──────────────────┘
                                      │
                                      │ resource prediction
                                      ▼
                           ┌──────────────────────┐       ┌──────────────────┐
                           │      AD-42           │       │    AD-45         │
                           │   SLO-Aware Health   │       │ Adaptive Route   │
                           │  (T-Digest, SWIM)    │       │    Learning      │
                           └──────────────────────┘       └──────────────────┘
```

## File Structure Summary

```
hyperscale/distributed/
├── idempotency/                    # AD-40 ✅ IMPLEMENTED
│   ├── __init__.py
│   ├── idempotency_key.py
│   ├── gate_cache.py
│   └── manager_ledger.py
│
├── resources/                      # AD-41 ✅ IMPLEMENTED
│   ├── __init__.py
│   ├── scalar_kalman_filter.py
│   ├── adaptive_kalman_filter.py
│   ├── process_resource_monitor.py
│   ├── manager_cluster_view.py
│   ├── manager_local_view.py
│   ├── manager_resource_gossip.py
│   └── worker_resource_report.py
│
├── slo/                            # AD-42 ✅ IMPLEMENTED
│   ├── __init__.py
│   ├── tdigest.py
│   ├── time_windowed_digest.py
│   ├── slo_config.py
│   ├── slo_summary.py
│   └── resource_aware_predictor.py
│
├── capacity/                       # AD-43 ✅ IMPLEMENTED
│   ├── __init__.py
│   ├── active_dispatch.py
│   ├── execution_time_estimator.py
│   ├── datacenter_capacity.py
│   ├── capacity_aggregator.py
│   ├── spillover_config.py
│   ├── spillover_decision.py
│   └── spillover_evaluator.py
│
├── reliability/                    # AD-44 ✅ IMPLEMENTED
│   ├── __init__.py
│   ├── retry_budget_state.py
│   ├── retry_budget_manager.py
│   ├── best_effort_state.py
│   ├── best_effort_manager.py
│   └── reliability_config.py
│
└── routing/
    ├── observed_latency_state.py           # AD-45 ✅ IMPLEMENTED
    ├── observed_latency_tracker.py         # AD-45 ✅ IMPLEMENTED
    ├── blended_latency_scorer.py           # AD-45 ✅ IMPLEMENTED
    ├── blended_scoring_config.py           # AD-45 ✅ IMPLEMENTED
    ├── dispatch_time_tracker.py            # AD-45 ✅ IMPLEMENTED
    └── datacenter_routing_score_extended.py # AD-45 ✅ IMPLEMENTED
```

**Status**: All AD-38 through AD-45 components are **IMPLEMENTED** as standalone modules. Integration into node servers (Gate, Manager, Worker) is **PENDING** as documented in Part 2 of this TODO.
