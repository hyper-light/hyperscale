# AD-29 / AD-30 Compliance Fixes

## AD-29 (Protocol-Level Peer Confirmation) — compliant

Peer confirmation and unconfirmed tracking are wired end-to-end:
- Unconfirmed peers tracked via `add_unconfirmed_peer()` and only activated via confirmation callbacks.
- Confirmation is triggered by SWIM message handlers, and suspicion is gated on confirmation.
- Stale unconfirmed peers are logged during cleanup.

References:
- `hyperscale/distributed_rewrite/swim/health_aware_server.py:273`
- `hyperscale/distributed_rewrite/swim/health_aware_server.py:2709`
- `hyperscale/distributed_rewrite/nodes/manager.py:715`

---

## AD-30 (Hierarchical Failure Detection) — compliant

No fixes required. The global timing wheel and job-layer suspicion manager are implemented and integrated (see `swim/detection/hierarchical_failure_detector.py`, `swim/detection/job_suspicion_manager.py`, and the manager job-responsiveness loop).

---

## AD-31 (Gossip-Informed Callbacks) — compliant

No fixes required. Gossip-informed callbacks are invoked on `dead`/`leave` updates in `HealthAwareServer.process_piggyback_data()` and nodes register `_on_node_dead` handlers.

---

## AD-32 (Hybrid Bounded Execution with Priority Load Shedding) — compliant

No fixes required. Priority-aware in-flight tracking, load shedding, and bounded queues are integrated in `server/mercury_sync_base_server.py` and `server/protocol/in_flight_tracker.py`, with client queue settings in `env/env.py`.

---

## AD-33 (Workflow State Machine + Federated Health Monitoring) — NOT fully compliant

### 1) Rescheduling token handling (worker-failure path) — compliant
`_handle_worker_failure()` separates parent workflow tokens for job lookups and subworkflow tokens for lifecycle transitions.

References:
- `hyperscale/distributed_rewrite/nodes/manager.py:8374`

---

### 2) Dependency discovery for rescheduling — compliant
`_find_dependent_workflows()` reads the dependency graph from `WorkflowDispatcher` and traverses dependents (direct + transitive).

References:
- `hyperscale/distributed_rewrite/nodes/manager.py:11034`

---

### 3) Enforce dependent cancellation before retry
**Problem**: `_handle_worker_failure()` logs and continues if dependent cancellation fails, allowing retries before dependents are fully cancelled.

**Exact changes**:
- Make dependent cancellation a required gate: if cancellation fails or times out, **do not** transition to `FAILED_READY_FOR_RETRY`.
- Persist a retryable “cancel-pending” state and reattempt cancellation until it succeeds or job is cancelled.

**Acceptance**:
- No workflow is re-queued until dependents are confirmed cancelled.

---

### 4) FederatedHealthMonitor integration (AD-33 cross-DC) — NOT fully compliant
**Observed**: Gate initializes `FederatedHealthMonitor` and handles `xprobe/xack`, but DC health classification is still delegated to `DatacenterHealthManager` (manager TCP heartbeats only) in `_classify_datacenter_health()`.

**Exact changes**:
- Incorporate `FederatedHealthMonitor` health signals into DC classification and routing (e.g., feed into `_dc_health_manager` or layer its result in `_classify_datacenter_health()` / `_select_datacenters_with_fallback()`).

**Acceptance**:
- Cross-DC health classification reflects `xprobe/xack` results, not only manager heartbeats.

References:
- `hyperscale/distributed_rewrite/nodes/gate.py:533`
- `hyperscale/distributed_rewrite/nodes/gate.py:1929`

---

## AD-17 to AD-25 Compliance Fixes

### AD-19 (Three-Signal Health Model) — NOT fully compliant
**Problem**: Progress/throughput signals are stubbed (`health_throughput` and `health_expected_throughput` return `0.0`) in gate/manager/worker, so the progress signal is effectively disabled.

**Exact changes**:
- **Worker**: Compute real completions per interval and expected rate, then feed `WorkerHealthState.update_progress()` and SWIM health piggyback.
  - File: `hyperscale/distributed_rewrite/nodes/worker.py` (`get_health_throughput`, `get_health_expected_throughput` lambdas)
- **Manager**: Track workflows dispatched per interval and expected throughput from worker capacity; feed `ManagerHealthState.update_progress()` and SWIM health piggyback.
  - File: `hyperscale/distributed_rewrite/nodes/manager.py` (health embedder lambdas)
- **Gate**: Track jobs forwarded per interval and expected forward rate; feed `GateHealthState.update_progress()` and SWIM health piggyback.
  - File: `hyperscale/distributed_rewrite/nodes/gate.py` (health embedder lambdas)

**Acceptance**:
- Progress state transitions (NORMAL/SLOW/DEGRADED/STUCK) activate based on real rates.
- Health routing/decision logic can evict or drain based on progress signal.

---

### AD-21 (Unified Retry Framework with Jitter) — NOT fully compliant
**Problem**: Multiple custom retry loops with fixed exponential backoff exist instead of the unified `RetryExecutor` with jitter.

**Exact changes**:
- Replace manual retry loops with `RetryExecutor` in:
  - `hyperscale/distributed_rewrite/nodes/gate.py:_try_dispatch_to_manager()`
  - `hyperscale/distributed_rewrite/nodes/manager.py` state sync, peer registration, gate registration, manager registration, worker dispatch paths (all loops using `max_retries` + `base_delay`).
- Standardize retry configs (base delay, max delay, jitter strategy) via shared helper.

**Acceptance**:
- All network operations use the unified retry framework with jitter.
- No bespoke retry loops remain in node code.

---

### AD-23 (Backpressure for Stats Updates) — NOT fully compliant
**Problem**: Workers honor `BackpressureSignal`, but managers do not emit backpressure or maintain tiered stats buffers as specified.

**Exact changes**:
- Implement `StatsBuffer` tiered retention (hot/warm/cold) and compute `BackpressureLevel` based on fill ratio.
  - File: `hyperscale/distributed_rewrite/nodes/manager.py` (stats ingestion + windowed stats processing)
- Emit `BackpressureSignal` to workers when stats buffers cross thresholds (THROTTLE/BATCH/REJECT).
- Ensure worker updates respect backpressure signals (already present in `_handle_backpressure_signal`).

**Acceptance**:
- Managers send backpressure signals during stats overload.
- Workers throttle/batch/drop stats updates accordingly.
