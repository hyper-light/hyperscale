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

## AD-10 to AD-16 Compliance Fixes

### AD-10 (Fencing Tokens from Terms) — NOT fully compliant
**Problem**: AD-10 specifies fencing tokens derived from election terms, but workflow dispatch uses per-job monotonic counters instead of the leader term.

**Exact changes**:
- Align dispatch fencing tokens with leader election terms, or document/justify the divergence if per-job tokens intentionally supersede AD-10.
- Ensure workers validate against term-derived fencing tokens for leader operations.

**Acceptance**:
- Fencing tokens used in `WorkflowDispatch` are derived from election terms (or updated AD-10 rationale explicitly states per-job tokens override term fencing).

References:
- `hyperscale/distributed_rewrite/swim/leadership/leader_state.py:319`
- `hyperscale/distributed_rewrite/jobs/workflow_dispatcher.py:563`

---

### AD-14 (CRDT-Based Cross-DC Statistics) — NOT fully compliant
**Problem**: CRDT data types exist but cross-DC stats aggregation paths do not use them.

**Exact changes**:
- Wire `JobStatsCRDT` into gate/manager cross-DC aggregation to provide CRDT merges for completed/failed counts and rates.
- Replace any ad-hoc cross-DC aggregation with CRDT merges where AD-14 requires eventual consistency without coordination.

**Acceptance**:
- Cross-DC stats aggregation uses `JobStatsCRDT.merge()` / `merge_in_place()` in the data path.

References:
- `hyperscale/distributed_rewrite/models/crdt.py:313`
- `hyperscale/distributed_rewrite/nodes/gate.py:2611`

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

---

## AD-34 to AD-36 Compliance Fixes

### AD-34 (Adaptive Job Timeout with Multi-DC Coordination) — NOT fully compliant
**Problem**: Gate-side tracker is initialized and handlers exist, but it never starts tracking jobs on submission. Manager lacks a handler for gate-issued global timeout decisions.

**Exact changes**:
- **Gate**: Call `GateJobTimeoutTracker.start_tracking_job(job_id, timeout_seconds, target_dcs)` when a job is dispatched to datacenters (after selecting primary + fallback DCs). Stop tracking if dispatch fails before any DC accepts.
  - File: `hyperscale/distributed_rewrite/nodes/gate.py` (job submission/dispatch path)
- **Manager**: Add TCP handler `receive_job_global_timeout` to load `JobGlobalTimeout`, locate the job's timeout strategy, and call `strategy.handle_global_timeout(job_id, reason, fence_token)`. Return `b"ok"` for accepted and `b"error"` for rejected.
  - File: `hyperscale/distributed_rewrite/nodes/manager.py`

**Acceptance**:
- Gate begins tracking every multi-DC job at submission time.
- Managers react to `JobGlobalTimeout` and enforce global timeout decisions.

References:
- `hyperscale/distributed_rewrite/nodes/gate.py:3712`
- `hyperscale/distributed_rewrite/nodes/gate.py:5721`
- `hyperscale/distributed_rewrite/jobs/gates/gate_job_timeout_tracker.py:146`

---

### AD-35 (Vivaldi Network Coordinates with Role-Aware Failure Detection) — NOT fully compliant
**Problem**: Vivaldi coordinates are collected and piggybacked, but there is no RTT UCB estimation, no coordinate quality penalties, and no role-aware confirmation strategy for unconfirmed peers/suspicion timeouts.

**Exact changes**:
- Add `estimate_rtt_ucb_ms()` in `CoordinateTracker`/`NetworkCoordinateEngine` using coordinate error + sample_count (confidence-aware upper bound).
- Persist coordinate quality metrics (error, sample_count, updated_at) and expose them to failure detection.
- Implement role-aware confirmation strategies (Gate/Manager/Worker) and use them in unconfirmed peer cleanup and suspicion timeout calculation.
  - Gate: proactive confirmation with higher base timeout and Vivaldi-adjusted latency multiplier.
  - Manager: moderate confirmation attempts with Vivaldi-adjusted latency multiplier.
  - Worker: passive-only confirmation with higher base timeout, no Vivaldi dependence.
- Use the RTT UCB and role strategy to compute adaptive confirmation timeouts instead of static thresholds.

**Acceptance**:
- Unconfirmed cleanup and suspicion use Vivaldi-aware, role-specific timeouts.
- RTT estimation uses UCB and accounts for coordinate quality.

References:
- `hyperscale/distributed_rewrite/swim/health_aware_server.py:307`
- `hyperscale/distributed_rewrite/swim/coordinates/coordinate_engine.py:35`
- `hyperscale/distributed_rewrite/swim/core/state_embedder.py:185`

---

### AD-36 (Vivaldi-Based Cross-Datacenter Job Routing) — NOT fully compliant
**Problem**: Gate routing only uses health buckets and capacity; no Vivaldi RTT scoring, coordinate quality penalty, or hysteresis/stickiness.

**Exact changes**:
- Track per-DC leader coordinates and quality (from `ManagerHeartbeat.coordinate` and/or FederatedHealthMonitor updates).
- Implement Vivaldi-aware scoring within health buckets:
  - `score = rtt_ucb_ms * load_factor * quality_penalty` (per AD-36).
  - Apply preference multiplier only within the primary bucket.
- Add hysteresis and stickiness:
  - Hold-down window, improvement threshold, cooldown penalty after failover.
- Add coordinate-unaware mode when samples are insufficient (rank by capacity/queue/circuit pressure).
- Build fallback chain in bucket order (HEALTHY → BUSY → DEGRADED) with score ordering inside each bucket.

**Acceptance**:
- Routing preserves AD-17 bucket ordering but ranks candidates using Vivaldi RTT UCB.
- Hysteresis prevents churn and only switches on meaningful improvements.

References:
- `hyperscale/distributed_rewrite/nodes/gate.py:2529`
- `hyperscale/distributed_rewrite/models/coordinates.py:5`
