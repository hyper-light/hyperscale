# Execution Workflow: Concurrent Fix Implementation

Generated: 2026-01-12
Source: `TODO.md`

---

## Dependency Analysis

### Task Dependencies Graph

```
                                    ┌─────────────────────────────────────────────────────────┐
                                    │                      PHASE 0                            │
                                    │              Shared Infrastructure                       │
                                    │                                                         │
                                    │  [0.1] Create _create_background_task() helper         │
                                    │        in HealthAwareServer base class                  │
                                    │        (used by Gate, Manager, Worker)                  │
                                    └─────────────────────────────────────────────────────────┘
                                                              │
                          ┌───────────────────────────────────┼───────────────────────────────────┐
                          │                                   │                                   │
                          ▼                                   ▼                                   ▼
┌─────────────────────────────────────────┐   ┌─────────────────────────────────────────┐   ┌─────────────────────────────────────────┐
│           TRACK A: Gate                 │   │          TRACK B: Manager               │   │          TRACK C: Worker                │
│                                         │   │                                         │   │                                         │
│  [A.1] Fix gate/state.py races (P0)     │   │  [B.1] Fix manager/state.py races (P0)  │   │  [C.1] Fix worker/state.py race (P0)    │
│        - Add _counter_lock              │   │        - Add _counter_lock              │   │        - Add _counter_lock              │
│        - Make 4 methods async           │   │        - Make 4 methods async           │   │        - Make method async              │
│                                         │   │                                         │   │                                         │
│  [A.2] Fix gate/server.py memory (P0)   │   │  [B.2] Fix background tasks (P0)        │   │  [C.2] Fix background tasks (P0)        │
│        - Add job cleanup for            │   │        - Add error callbacks to         │   │        - Add error callbacks to         │
│          _job_reporter_tasks            │   │          19 background tasks            │   │          7 background tasks             │
│          _job_stats_crdt                │   │                                         │   │                                         │
│                                         │   │  [B.3] Fix silent failures (P1)         │   │  [C.3] Fix progress.py failures (P1)    │
│  [A.3] Fix gate/server.py failures (P1) │   │        - 5 except:pass blocks           │   │        - 6 except:pass blocks           │
│        - 8 except:pass blocks           │   │                                         │   │                                         │
│                                         │   │  [B.4] Bounded latency samples (P2)     │   │  [C.4] AD-41 Resource Guards (P2)       │
│  [A.4] AD-40 Idempotency (P1)           │   │        - Use deque(maxlen=1000)         │   │        - Add ProcessResourceMonitor     │
│        - Add cache to __init__          │   │                                         │   │        - Include in heartbeat           │
│        - Modify submission handler      │   │  [B.5] AD-42 SLO Tracking (P2)          │   │                                         │
│                                         │   │        - Add TimeWindowedTDigest        │   │                                         │
│  [A.5] AD-43 Capacity Spillover (P2)    │   │        - Record workflow latencies      │   │                                         │
│        - Add capacity aggregator        │   │        - Include in heartbeat           │   │                                         │
│        - Evaluate before routing        │   │                                         │   │                                         │
│                                         │   │  [B.6] AD-44 Retry Budgets (P1)         │   │                                         │
│  [A.6] AD-45 Route Learning (P2)        │   │        - Add to WorkflowDispatcher      │   │                                         │
│        - Add latency tracker            │   │        - Check before retry             │   │                                         │
│        - Use blended scoring            │   │                                         │   │                                         │
└─────────────────────────────────────────┘   └─────────────────────────────────────────┘   └─────────────────────────────────────────┘
                          │                                   │                                   │
                          └───────────────────────────────────┼───────────────────────────────────┘
                                                              │
                                    ┌─────────────────────────────────────────────────────────┐
                                    │                      TRACK D: Shared                    │
                                    │                                                         │
                                    │  [D.1] Fix context.py race (P0)                        │
                                    │        - Remove unprotected check                       │
                                    │                                                         │
                                    │  [D.2] Fix client/state.py races (P0)                  │
                                    │        - Add _metrics_lock, make async                  │
                                    │                                                         │
                                    │  [D.3] Fix job_manager.py TOCTOU (P1)                  │
                                    │        - Add fence token lock                           │
                                    │                                                         │
                                    │  [D.4] Fix gate_job_manager.py TOCTOU (P1)             │
                                    │        - Add fence token lock                           │
                                    │                                                         │
                                    │  [D.5] Fix connection_pool.py TOCTOU (P2)              │
                                    │        - Re-check limits after creation                 │
                                    │                                                         │
                                    │  [D.6] Fix WAL writer tasks (P0)                       │
                                    │        - Add error callbacks                            │
                                    │                                                         │
                                    │  [D.7] Fix callback swallowing (P1)                    │
                                    │        - 11 files, add logging                          │
                                    │                                                         │
                                    │  [D.8] Fix asyncio.gather (P2)                         │
                                    │        - 5 files, add return_exceptions                 │
                                    │                                                         │
                                    │  [D.9] Fix mercury_sync failures (P1)                  │
                                    │        - 12 except:pass blocks                          │
                                    │                                                         │
                                    │  [D.10] Fix taskex failures (P1)                       │
                                    │         - 10 except:pass blocks                         │
                                    │                                                         │
                                    │  [D.11] Fix encryption failures (P1)                   │
                                    │         - 4 except:pass blocks (SECURITY)              │
                                    │                                                         │
                                    │  [D.12] Fix detector deque (P3)                        │
                                    │         - Use deque(maxlen=N)                           │
                                    │                                                         │
                                    │  [D.13] Fix lock cleanup (P2)                          │
                                    │         - Add remove_*_lock() methods                   │
                                    └─────────────────────────────────────────────────────────┘
```

---

## Execution Phases

### Phase 0: Foundation (Blocking - Must Complete First)

**Duration**: ~15 minutes
**Parallelism**: 1 task

| ID | Task | File | Priority | Dependencies |
|----|------|------|----------|--------------|
| 0.1 | Create `_create_background_task()` helper in base class | `swim/health_aware_server.py` | P0 | None |

**Rationale**: This helper is used by Gate, Manager, and Worker servers. Creating it first avoids duplication.

```python
# Add to HealthAwareServer class:
def _create_background_task(self, coro: Coroutine, name: str) -> asyncio.Task:
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
        self._task_runner.run(
            self._udp_logger.log(
                ServerError(
                    message=f"Background task '{name}' failed: {exc}",
                    node_id=getattr(self, '_node_id', SimpleNamespace(short='unknown')).short,
                    error_type=type(exc).__name__,
                )
            )
        )
```

---

### Phase 1: Critical P0 Fixes (Parallel - 4 Tracks)

**Duration**: ~45 minutes
**Parallelism**: 4 concurrent tracks

#### Track A: Gate Server (P0)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| A.1 | Fix counter races | `nodes/gate/state.py` | 15 min |
| A.2 | Fix memory leak | `nodes/gate/server.py:2768-2777` | 10 min |

#### Track B: Manager Server (P0)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| B.1 | Fix counter races | `nodes/manager/state.py` | 15 min |
| B.2 | Add error callbacks | `nodes/manager/server.py:712-730` | 20 min |

#### Track C: Worker Server (P0)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| C.1 | Fix counter race | `nodes/worker/state.py` | 10 min |
| C.2 | Add error callbacks | `nodes/worker/server.py` | 15 min |

#### Track D: Shared Components (P0)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| D.1 | Fix context race | `server/context/context.py` | 5 min |
| D.2 | Fix client races | `nodes/client/state.py` | 10 min |
| D.6 | Fix WAL writer | `ledger/wal/wal_writer.py` | 10 min |

**Commit Point**: After Phase 1, commit all P0 fixes.

---

### Phase 2: High Priority P1 Fixes (Parallel - 4 Tracks)

**Duration**: ~60 minutes
**Parallelism**: 4 concurrent tracks

#### Track A: Gate Server (P1)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| A.3 | Fix silent failures | `nodes/gate/server.py` (8 blocks) | 20 min |
| A.4 | AD-40 Idempotency | `nodes/gate/server.py`, `handlers/tcp_job.py` | 30 min |

#### Track B: Manager Server (P1)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| B.3 | Fix silent failures | `nodes/manager/server.py` (5 blocks) | 15 min |
| B.6 | AD-44 Retry Budgets | `jobs/workflow_dispatcher.py` | 25 min |

#### Track C: Worker Server (P1)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| C.3 | Fix silent failures | `nodes/worker/progress.py` (6 blocks) | 15 min |

#### Track D: Shared Components (P1)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| D.3 | Fix job_manager TOCTOU | `jobs/job_manager.py` | 10 min |
| D.4 | Fix gate_job_manager TOCTOU | `jobs/gates/gate_job_manager.py` | 10 min |
| D.7 | Fix callback swallowing | 11 files | 30 min |
| D.9 | Fix mercury_sync failures | `server/server/mercury_sync_base_server.py` | 25 min |
| D.10 | Fix taskex failures | `taskex/task_runner.py`, `taskex/run.py` | 20 min |
| D.11 | Fix encryption failures | `encryption/aes_gcm.py` | 10 min |

**Commit Point**: After Phase 2, commit all P1 fixes.

---

### Phase 3: Medium Priority P2 Fixes (Parallel - 4 Tracks)

**Duration**: ~90 minutes
**Parallelism**: 4 concurrent tracks

#### Track A: Gate Server (P2)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| A.5 | AD-43 Capacity Spillover | `nodes/gate/server.py`, `routing.py` | 40 min |
| A.6 | AD-45 Route Learning | `nodes/gate/server.py`, `gate_job_router.py` | 35 min |

#### Track B: Manager Server (P2)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| B.4 | Bounded latency samples | `nodes/manager/state.py` | 15 min |
| B.5 | AD-42 SLO Tracking | `nodes/manager/state.py`, `server.py` | 35 min |

#### Track C: Worker Server (P2)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| C.4 | AD-41 Resource Guards | `nodes/worker/server.py`, `heartbeat.py` | 30 min |

#### Track D: Shared Components (P2)
| ID | Task | File | Est. Time |
|----|------|------|-----------|
| D.5 | Fix connection_pool TOCTOU | `discovery/pool/connection_pool.py` | 15 min |
| D.8 | Fix asyncio.gather | 5 files | 20 min |
| D.13 | Add lock cleanup methods | 4 state.py files | 25 min |

**Commit Point**: After Phase 3, commit all P2 fixes.

---

### Phase 4: Low Priority P3 Fixes (Optional)

**Duration**: ~15 minutes
**Parallelism**: 1 track

| ID | Task | File | Est. Time |
|----|------|------|-----------|
| D.12 | Fix detector deque | `swim/detection/hierarchical_failure_detector.py` | 10 min |

**Commit Point**: After Phase 4, commit P3 fixes.

---

## Optimal Execution Matrix

```
TIME ────────────────────────────────────────────────────────────────────────────────────────────────────►
     │ Phase 0  │        Phase 1 (P0)        │         Phase 2 (P1)          │        Phase 3 (P2)        │ P3 │
     │ 15 min   │          45 min            │           60 min              │          90 min            │15m │
     ├──────────┼────────────────────────────┼───────────────────────────────┼────────────────────────────┼────┤
     │          │                            │                               │                            │    │
  A  │   0.1    │  A.1 ──► A.2               │  A.3 ──────► A.4              │  A.5 ──────► A.6           │    │
     │   │      │  (gate state, memory)      │  (failures, idempotency)      │  (spillover, learning)     │    │
     │   │      │                            │                               │                            │    │
  B  │   │      │  B.1 ──► B.2               │  B.3 ──► B.6                  │  B.4 ──► B.5               │    │
     │   │      │  (manager state, tasks)    │  (failures, retry)            │  (latency, SLO)            │    │
     │   │      │                            │                               │                            │    │
  C  │   │      │  C.1 ──► C.2               │  C.3                          │  C.4                       │    │
     │   │      │  (worker state, tasks)     │  (failures)                   │  (resources)               │    │
     │   │      │                            │                               │                            │    │
  D  │   ▼      │  D.1, D.2, D.6 (parallel)  │  D.3,D.4,D.7,D.9,D.10,D.11   │  D.5, D.8, D.13            │D.12│
     │          │  (context, client, WAL)    │  (TOCTOU, callbacks, etc)     │  (pool, gather, locks)     │    │
     │          │                            │                               │                            │    │
     ├──────────┼────────────────────────────┼───────────────────────────────┼────────────────────────────┼────┤
     │  COMMIT  │         COMMIT             │           COMMIT              │          COMMIT            │ C  │
```

---

## Task Assignments for Parallel Execution

### Recommended Team Distribution

| Track | Focus Area | Files | Task Count |
|-------|------------|-------|------------|
| **A** | Gate Server | gate/server.py, gate/state.py, routing.py | 6 tasks |
| **B** | Manager Server | manager/server.py, manager/state.py, workflow_dispatcher.py | 6 tasks |
| **C** | Worker Server | worker/server.py, worker/state.py, worker/progress.py | 4 tasks |
| **D** | Shared Components | context.py, client/state.py, job_manager.py, etc. | 13 tasks |

---

## Execution Commands

### Phase 0
```bash
# Single task - foundation helper
# File: hyperscale/distributed/swim/health_aware_server.py
```

### Phase 1 (Run in Parallel)
```bash
# Terminal A: Gate
git checkout -b fix/gate-p0

# Terminal B: Manager  
git checkout -b fix/manager-p0

# Terminal C: Worker
git checkout -b fix/worker-p0

# Terminal D: Shared
git checkout -b fix/shared-p0

# After all complete:
git checkout main
git merge fix/gate-p0 fix/manager-p0 fix/worker-p0 fix/shared-p0
git commit -m "fix: P0 critical fixes - races, memory leaks, task errors"
```

### Phase 2 (Run in Parallel)
```bash
# Similar branch pattern for P1 fixes
git checkout -b fix/gate-p1
git checkout -b fix/manager-p1
git checkout -b fix/worker-p1
git checkout -b fix/shared-p1
```

### Phase 3 (Run in Parallel)
```bash
# Similar branch pattern for P2 fixes + AD integration
git checkout -b feat/gate-ad-integration
git checkout -b feat/manager-ad-integration
git checkout -b feat/worker-ad-integration
git checkout -b fix/shared-p2
```

---

## Verification After Each Phase

### Phase 1 Verification
```bash
# Run linting
uv run ruff check hyperscale/distributed/nodes/

# Run type checking
uv run pyright hyperscale/distributed/nodes/

# Verify no regressions (user runs integration tests)
```

### Phase 2 Verification
```bash
# Same as Phase 1, plus:
# Verify idempotency works (manual test)
# Verify retry budgets work (manual test)
```

### Phase 3 Verification
```bash
# Same as Phase 2, plus:
# Verify resource metrics in worker heartbeats
# Verify SLO summaries in manager heartbeats
# Verify capacity influences routing
# Verify observed latency tracking
```

---

## Risk Mitigation

### High-Risk Changes (Require Extra Review)
1. **Counter race fixes (A.1, B.1, C.1, D.2)** - Changes method signatures from sync to async. Callers must be updated.
2. **AD-40 Idempotency (A.4)** - Modifies critical job submission path.
3. **AD-44 Retry Budgets (B.6)** - Modifies workflow dispatch logic.

### Rollback Strategy
Each phase is committed separately. If issues arise:
```bash
# Rollback specific phase
git revert <phase-commit-hash>
```

---

## Summary

| Phase | Priority | Tasks | Tracks | Est. Duration | Commits |
|-------|----------|-------|--------|---------------|---------|
| 0 | Foundation | 1 | 1 | 15 min | - |
| 1 | P0 | 9 | 4 | 45 min | 1 |
| 2 | P1 | 11 | 4 | 60 min | 1 |
| 3 | P2 | 10 | 4 | 90 min | 1 |
| 4 | P3 | 1 | 1 | 15 min | 1 |
| **Total** | | **32** | | **~3.75 hours** | **4** |

**Maximum Parallelism**: 4 concurrent work streams
**Critical Path**: Phase 0 → Phase 1 Track B (Manager has most tasks)
