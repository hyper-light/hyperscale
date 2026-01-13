# Issues Identified from Scenario Tracing

This document tracks bugs, missing implementations, race conditions, and other issues
discovered during systematic tracing of SCENARIOS.md test scenarios through the codebase.

---

## CRITICAL: Missing Methods in WindowedStatsCollector

### F1: Missing `get_jobs_with_pending_stats()` Method

**Location**: `hyperscale/distributed/jobs/windowed_stats_collector.py`

**Called From**: `hyperscale/distributed/nodes/gate/stats_coordinator.py:188`
```python
pending_jobs = self._windowed_stats.get_jobs_with_pending_stats()
```

**Issue**: Method does not exist in `WindowedStatsCollector`. The class has `get_pending_windows_for_job()` but not `get_jobs_with_pending_stats()`.

**Impact**: `GateStatsCoordinator._batch_stats_loop()` will crash with `AttributeError` at runtime.

**Fix**: Add method to `WindowedStatsCollector`:
```python
def get_jobs_with_pending_stats(self) -> list[str]:
    """Get list of job IDs that have pending stats windows."""
    job_ids: set[str] = set()
    for job_id, _, _ in self._buckets.keys():
        job_ids.add(job_id)
    return list(job_ids)
```

---

### F2: Missing `get_aggregated_stats()` Method

**Location**: `hyperscale/distributed/jobs/windowed_stats_collector.py`

**Called From**: `hyperscale/distributed/nodes/gate/stats_coordinator.py:210`
```python
stats = self._windowed_stats.get_aggregated_stats(job_id)
```

**Issue**: Method does not exist. The class has `flush_job_windows()` but no non-destructive read method.

**Impact**: `GateStatsCoordinator._push_windowed_stats()` will crash with `AttributeError`.

**Fix**: Add method to `WindowedStatsCollector`:
```python
async def get_aggregated_stats(self, job_id: str) -> list[WindowedStatsPush]:
    """
    Get aggregated stats for a job's closed windows without removing them.
    
    This flushes closed windows for the job and returns them.
    Unlike flush_job_windows(), this respects drift tolerance.
    
    Args:
        job_id: The job identifier.
        
    Returns:
        List of WindowedStatsPush for closed windows.
    """
    now = time.time()
    results: list[WindowedStatsPush] = []
    keys_to_remove: list[tuple[str, str, int]] = []

    async with self._lock:
        for key, bucket in self._buckets.items():
            if key[0] != job_id:
                continue
                
            _, _, bucket_num = key
            if self._is_window_closed(bucket_num, now):
                push = self._aggregate_bucket(bucket)
                results.append(push)
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self._buckets[key]

    return results
```

---

### F3: Missing `record()` Method

**Location**: `hyperscale/distributed/jobs/windowed_stats_collector.py`

**Called From**: `hyperscale/distributed/nodes/manager/server.py:2625`
```python
self._windowed_stats.record(progress)
```

**Issue**: Method does not exist. The class has `add_progress(worker_id, progress)` but not `record(progress)`.

**Impact**: Manager server will crash with `AttributeError` when receiving workflow progress.

**Fix**: Add method to `WindowedStatsCollector`:
```python
async def record(self, progress: WorkflowProgress) -> None:
    """
    Record a workflow progress update.
    
    Convenience method that extracts worker_id from progress and calls add_progress().
    
    Args:
        progress: The workflow progress update containing worker_id.
    """
    worker_id = progress.worker_id
    await self.add_progress(worker_id, progress)
```

**Note**: This requires `WorkflowProgress` to have a `worker_id` attribute. If not present, the manager server must be updated to pass worker_id explicitly.

---

## MEDIUM: Race Conditions

### F4: Backpressure Level Race in Stats Coordinator

**Location**: `hyperscale/distributed/nodes/gate/stats_coordinator.py:167-185`

**Issue**: Backpressure level is checked before sleep but used after sleep:
```python
backpressure_level = self._state.get_max_backpressure_level()
# ... adjust interval based on level ...
await asyncio.sleep(interval_seconds)
# Level can change during sleep!
if backpressure_level == BackpressureLevel.REJECT:
    continue
```

**Impact**: Stats may be pushed during REJECT backpressure if level changed during sleep.

**Fix**: Re-check backpressure level after sleep:
```python
backpressure_level = self._state.get_max_backpressure_level()
# ... adjust interval ...
await asyncio.sleep(interval_seconds)

# Re-check after sleep
backpressure_level = self._state.get_max_backpressure_level()
if backpressure_level == BackpressureLevel.REJECT:
    continue
```

---

### F5: Concurrent JobStatsCRDT Merge Race

**Location**: `hyperscale/distributed/models/crdt.py` (JobStatsCRDT.merge_in_place)

**Issue**: `merge_in_place()` performs multiple field updates without atomicity:
```python
def merge_in_place(self, other: JobStatsCRDT) -> None:
    self.completed.merge_in_place(other.completed)
    self.failed.merge_in_place(other.failed)
    self.rates.merge_in_place(other.rates)
    self.statuses.merge_in_place(other.statuses)
```

**Impact**: Concurrent reads during merge may see inconsistent state (some fields merged, others not).

**Scenario**: Peer A merges while Peer B reads `total_completed` - may get stale value while rates are already merged.

**Fix**: Add lock to CRDT or use immutable merge pattern:
```python
# Option 1: Add lock (requires making CRDT stateful)
async def merge_in_place_safe(self, other: JobStatsCRDT, lock: asyncio.Lock) -> None:
    async with lock:
        self.completed.merge_in_place(other.completed)
        self.failed.merge_in_place(other.failed)
        self.rates.merge_in_place(other.rates)
        self.statuses.merge_in_place(other.statuses)

# Option 2: Always use immutable merge() for reads
# Callers should use merge() to create new instance, then atomically replace reference
```

---

### F6: Late-Arriving Stats Race in WindowedStatsCollector

**Location**: `hyperscale/distributed/jobs/windowed_stats_collector.py:131-136`

**Issue**: Stats arriving after window_end + drift_tolerance are silently dropped:
```python
def _is_window_closed(self, bucket_num: int, now: float) -> bool:
    window_end_ms = (bucket_num + 1) * self._window_size_ms
    current_ms = now * 1000
    return current_ms > window_end_ms + self._drift_tolerance_ms
```

**Impact**: If clock skew exceeds `drift_tolerance_ms` (default 50ms), stats are lost.

**Scenario**: Worker sends stats at T=1055ms for window ending at T=1000ms with 50ms drift tolerance. Stats arrive at collector at T=1060ms. Window already flushed, stats dropped.

**Mitigation**: Current 50ms default is conservative. Document that:
1. Systems with high clock skew should increase `drift_tolerance_ms`
2. NTP synchronization is recommended for production deployments
3. Consider adding metric for late-arriving stats to detect clock skew issues

---

## LOW: Potential Issues

### F7: Synchronous Callback in TCP Handler

**Location**: `hyperscale/distributed/nodes/client/handlers/tcp_windowed_stats.py` (line ~66)

**Issue**: User callback invoked synchronously in async handler:
```python
callback(push)  # Blocking call in async context
```

**Impact**: Slow user callbacks block stats processing for other jobs.

**Fix**: Run callback in task runner if synchronous:
```python
callback = self._state._progress_callbacks.get(push.job_id)
if callback:
    try:
        if asyncio.iscoroutinefunction(callback):
            await callback(push)
        else:
            # Run sync callback without blocking event loop
            await asyncio.get_event_loop().run_in_executor(None, callback, push)
    except Exception as callback_error:
        await self._logger.log(ServerWarning(...))
```

---

### F8: No Explicit Duplicate Detection for Workflow Results

**Location**: `hyperscale/distributed/nodes/gate/server.py` (workflow result handling)

**Issue**: Duplicate results from same DC simply overwrite (last-write-wins):
```python
self._workflow_dc_results[push.job_id][push.workflow_id][push.datacenter] = push
```

**Impact**: No way to detect if duplicates are from legitimate retries vs. network issues.

**Current Behavior**: Safe (idempotent), but may mask problems.

**Recommendation**: Add optional logging/metrics for duplicate detection without changing behavior:
```python
if push.datacenter in self._workflow_dc_results[push.job_id][push.workflow_id]:
    # Log duplicate for observability
    await self._logger.log(ServerWarning(
        message=f"Duplicate workflow result from {push.datacenter} for job {push.job_id}",
        context={"workflow_id": push.workflow_id},
    ))
self._workflow_dc_results[push.job_id][push.workflow_id][push.datacenter] = push
```

---

### F9: Missing Concurrency Protection During Result Aggregation

**Location**: `hyperscale/distributed/nodes/gate/server.py` (`_aggregate_and_forward_workflow_result`)

**Issue**: No lock protection when reading/modifying `_workflow_dc_results` during aggregation. New results could arrive mid-aggregation.

**Scenario**:
1. Gate starts aggregating job_id/workflow_id (reads 2 of 3 DC results)
2. Third DC result arrives, modifies `_workflow_dc_results`
3. Aggregation continues with potentially stale view

**Impact**: Unlikely in practice (aggregation is fast), but could cause inconsistent results.

**Fix**: Add lock or use atomic read-then-delete pattern:
```python
async def _aggregate_and_forward_workflow_result(self, job_id: str, workflow_id: str) -> None:
    # Atomic extraction - pop the workflow's results before processing
    async with self._workflow_result_lock:
        if job_id not in self._workflow_dc_results:
            return
        if workflow_id not in self._workflow_dc_results[job_id]:
            return
        dc_results = self._workflow_dc_results[job_id].pop(workflow_id)
        if not self._workflow_dc_results[job_id]:
            del self._workflow_dc_results[job_id]
    
    # Process extracted results (no longer needs lock)
    # ... aggregation logic ...
```

---

## Summary

| ID | Severity | Category | Location | Status |
|----|----------|----------|----------|--------|
| F1 | CRITICAL | Missing Method | windowed_stats_collector.py | TODO |
| F2 | CRITICAL | Missing Method | windowed_stats_collector.py | TODO |
| F3 | CRITICAL | Missing Method | windowed_stats_collector.py | TODO |
| F4 | MEDIUM | Race Condition | stats_coordinator.py | TODO |
| F5 | MEDIUM | Race Condition | crdt.py | TODO |
| F6 | MEDIUM | Race Condition | windowed_stats_collector.py | DOCUMENTED |
| F7 | LOW | Blocking Call | tcp_windowed_stats.py | TODO |
| F8 | LOW | Observability | gate/server.py | OPTIONAL |
| F9 | LOW | Race Condition | gate/server.py | TODO |

---

## Next Steps

1. **Immediate**: Fix F1, F2, F3 - these will cause runtime crashes
2. **Soon**: Fix F4 - violates backpressure contract
3. **Consider**: F5, F7, F9 - edge cases but worth addressing
4. **Optional**: F6, F8 - documentation/observability improvements
