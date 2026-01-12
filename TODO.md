# AD-40 to AD-45 Implementation Execution Plan

This document outlines an optimized execution order for implementing AD-40 through AD-45, maximizing concurrent work across tracks.

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

The work naturally divides into **4 parallel tracks** based on dependencies:

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

---

## Execution Plan

### Phase 1: Foundation (All 4 tracks start simultaneously)

These can all begin immediately with no inter-dependencies:

| Track | Task | AD | Estimated Scope |
|-------|------|----|-----------------| 
| **A** | Idempotency Key & Cache | AD-40 | Gate idempotency cache, key generation |
| **B** | Kalman Filters & Process Monitoring | AD-41 | ScalarKalmanFilter, AdaptiveKalmanFilter, ProcessResourceMonitor |
| **C** | Capacity Aggregation | AD-43 | ActiveDispatch, ExecutionTimeEstimator, DatacenterCapacity |
| **D** | Retry Budget State | AD-44 | RetryBudgetState, BestEffortState models |

### Phase 2: Core Logic (After Phase 1 foundation)

| Track | Task | AD | Dependencies |
|-------|------|----|--------------| 
| **A** | Manager Idempotency Ledger | AD-40 | Phase 1A complete |
| **B** | Manager Resource Gossip | AD-41 | Phase 1B complete |
| **C** | Spillover Evaluator | AD-43 | Phase 1C complete |
| **D** | Retry Budget Enforcement | AD-44 | Phase 1D complete |

### Phase 3: Integration & Extensions

| Track | Task | AD | Dependencies |
|-------|------|----|--------------| 
| **A** | Cross-DC VSR Integration | AD-40 | Phase 2A complete |
| **B** | **AD-42 T-Digest + SLO** | AD-42 | Phase 2B complete (uses AD-41 metrics) |
| **C** | **AD-45 Observed Latency** | AD-45 | Phase 2C complete |
| **D** | Best-Effort Completion | AD-44 | Phase 2D complete |

### Phase 4: Final Integration

| Track | Task | AD | Dependencies |
|-------|------|----|--------------| 
| **A** | Protocol Extensions (JobSubmission) | AD-40 | Phase 3A complete |
| **B** | SLO Health Classification | AD-42 | Phase 3B complete |
| **C** | Blended Latency Scoring | AD-45 | Phase 3C complete |
| **D** | Env Configuration | AD-44 | Phase 3D complete |

---

## Detailed Task Breakdown

### AD-40: Idempotent Job Submissions (Track A)

**Phase 1A - Foundation:**
- [ ] Create `distributed/idempotency/__init__.py`
- [ ] Implement `IdempotencyKey` and `IdempotencyKeyGenerator` 
- [ ] Implement `IdempotencyStatus` enum and `IdempotencyEntry` dataclass
- [ ] Implement `IdempotencyConfig` with Env integration
- [ ] Implement `GateIdempotencyCache` with LRU + TTL

**Phase 2A - Manager Ledger:**
- [ ] Implement `IdempotencyLedgerEntry` with serialization
- [ ] Implement `ManagerIdempotencyLedger` with WAL integration
- [ ] Add cleanup loop and TTL management

**Phase 3A - Cross-DC:**
- [ ] Add `IdempotencyReservedEvent` and `IdempotencyCommittedEvent`
- [ ] Integrate with Per-Job VSR (AD-38) for replication

**Phase 4A - Protocol:**
- [ ] Extend `JobSubmission` with `idempotency_key` field
- [ ] Extend `JobAck` with `was_duplicate`, `original_job_id` fields
- [ ] Add Env configuration variables

---

### AD-41: Resource Guards (Track B)

**Phase 1B - Foundation:**
- [ ] Create `distributed/resources/__init__.py`
- [ ] Implement `ScalarKalmanFilter` for noise reduction
- [ ] Implement `AdaptiveKalmanFilter` with auto-tuning
- [ ] Implement `ResourceMetrics` dataclass
- [ ] Implement `ProcessResourceMonitor` with psutil + process tree

**Phase 2B - Manager Gossip:**
- [ ] Implement `ManagerLocalView` for per-manager state
- [ ] Implement `ManagerClusterResourceView` for aggregated view
- [ ] Implement `ManagerResourceGossip` with peer sync
- [ ] Implement `WorkerResourceReport` for worker→manager reports

**Phase 3B - Health Tracker:**
- [ ] Implement `NodeHealthTracker` generic class
- [ ] Implement `HealthPiggyback` for SWIM embedding
- [ ] Add enforcement thresholds (WARN → THROTTLE → KILL)

---

### AD-42: SLO-Aware Health and Routing (Track B, after AD-41)

**Phase 3B - T-Digest & SLO:**
- [ ] Create `distributed/slo/__init__.py`
- [ ] Implement `TDigest` for streaming percentiles (p50, p95, p99)
- [ ] Implement `LatencySLO` and `LatencyObservation` models
- [ ] Implement `SLOComplianceScore` with compliance levels

**Phase 4B - Health Integration:**
- [ ] Implement `SLOSummary` compact gossip payload
- [ ] Implement `SLOHealthClassifier` for AD-16 integration
- [ ] Implement `ResourceAwareSLOPredictor` (uses AD-41 metrics)
- [ ] Add Env configuration for SLO thresholds

---

### AD-43: Capacity-Aware Spillover (Track C)

**Phase 1C - Foundation:**
- [ ] Create `distributed/capacity/__init__.py`
- [ ] Implement `ActiveDispatch` dataclass with duration tracking
- [ ] Implement `ExecutionTimeEstimator` for wait time prediction
- [ ] Parse `Workflow.duration` using existing `TimeParser`

**Phase 2C - Aggregation:**
- [ ] Implement `DatacenterCapacity` aggregation model
- [ ] Extend `ManagerHeartbeat` with capacity fields:
  - `pending_workflow_count`
  - `pending_duration_seconds`
  - `active_remaining_seconds`
  - `estimated_cores_free_at`
  - `estimated_cores_freeing`

**Phase 3C - Spillover:**
- [ ] Implement `SpilloverDecision` dataclass
- [ ] Implement `SpilloverEvaluator` with decision tree
- [ ] Extend `GateJobRouter.route_job()` to accept `cores_required`

**Phase 4C - Integration:**
- [ ] Wire up `DatacenterCapacityAggregator` in Gate
- [ ] Add Env configuration (`SPILLOVER_*` variables)

---

### AD-44: Retry Budgets and Best-Effort (Track D)

**Phase 1D - Foundation:**
- [ ] Create `distributed/reliability/__init__.py`
- [ ] Implement `RetryBudgetState` with per-workflow tracking
- [ ] Implement `BestEffortState` with DC completion tracking

**Phase 2D - Enforcement:**
- [ ] Implement `RetryBudgetManager` for manager-side enforcement
- [ ] Integrate budget check in `WorkflowDispatcher._dispatch_workflow()`
- [ ] Add budget consumption logging

**Phase 3D - Best-Effort:**
- [ ] Implement `BestEffortManager` for gate-side tracking
- [ ] Implement deadline check loop (periodic task)
- [ ] Handle partial completion with `check_completion()`

**Phase 4D - Protocol:**
- [ ] Extend `JobSubmission` with:
  - `retry_budget`
  - `retry_budget_per_workflow`
  - `best_effort`
  - `best_effort_min_dcs`
  - `best_effort_deadline_seconds`
- [ ] Add Env configuration (`RETRY_BUDGET_*`, `BEST_EFFORT_*`)

---

### AD-45: Adaptive Route Learning (Track C, after AD-43)

**Phase 3C - Observed Latency:**
- [ ] Create `distributed/routing/observed_latency.py`
- [ ] Implement `ObservedLatencyState` with EWMA tracking
- [ ] Implement `ObservedLatencyTracker` with staleness decay

**Phase 4C - Blended Scoring:**
- [ ] Extend `DatacenterRoutingScore` with:
  - `blended_latency_ms`
  - `observed_latency_ms`
  - `observed_confidence`
- [ ] Modify `RoutingScorer` to use `get_blended_latency()`
- [ ] Track dispatch times in `GateJobManager`
- [ ] Add Env configuration (`ADAPTIVE_ROUTING_*`)

---

## File Structure Summary

```
hyperscale/distributed/
├── idempotency/                    # AD-40
│   ├── __init__.py
│   ├── idempotency_key.py
│   ├── gate_cache.py
│   └── manager_ledger.py
│
├── resources/                      # AD-41
│   ├── __init__.py
│   ├── kalman_filter.py
│   ├── process_monitor.py
│   ├── manager_gossip.py
│   └── health_tracker.py
│
├── slo/                            # AD-42
│   ├── __init__.py
│   ├── tdigest.py
│   ├── slo_models.py
│   ├── compliance_scorer.py
│   └── health_classifier.py
│
├── capacity/                       # AD-43
│   ├── __init__.py
│   ├── active_dispatch.py
│   ├── execution_estimator.py
│   ├── datacenter_capacity.py
│   └── capacity_aggregator.py
│
├── reliability/                    # AD-44
│   ├── __init__.py
│   ├── retry_budget.py
│   └── best_effort.py
│
└── routing/
    ├── observed_latency.py         # AD-45
    ├── scoring.py                  # Modified for AD-45
    └── spillover.py                # AD-43
```

---

## Concurrency Summary

| Phase | Track A (AD-40) | Track B (AD-41→42) | Track C (AD-43→45) | Track D (AD-44) |
|-------|-----------------|--------------------|--------------------|-----------------|
| **1** | Key/Cache | Kalman/Monitor | Capacity/Dispatch | Budget State |
| **2** | Manager Ledger | Manager Gossip | Spillover Eval | Enforcement |
| **3** | VSR Integration | T-Digest/SLO | Observed Latency | Best-Effort |
| **4** | Protocol | Health Class | Blended Scoring | Env Config |

**Maximum Parallelism**: 4 concurrent work streams
**Critical Path**: Track B (AD-41 → AD-42) due to resource prediction dependency
**Estimated Total Phases**: 4 sequential phases with full parallelism within each

---

## Notes

1. **AD-41 is foundational for AD-42** - Resource metrics feed SLO prediction
2. **AD-43 and AD-45 share routing infrastructure** - Can share reviewer
3. **AD-40 and AD-44 are fully independent** - Can be developed in isolation
4. **All ADs integrate with Env** - Configuration follows existing patterns
5. **All ADs use existing SWIM hierarchy** - No new transport mechanisms needed
