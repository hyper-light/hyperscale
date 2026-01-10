# AD-29 / AD-30 Compliance Fixes

## AD-29 (Protocol-Level Peer Confirmation) — NOT fully compliant

### 1) Use unconfirmed tracking when adding peers
**Problem**: Nodes add peers to active sets and SWIM probing without calling `add_unconfirmed_peer()`, bypassing the unconfirmed/confirmed state machine.

**Exact changes**:
- **Manager**: In `manager_peer_register()` after `udp_addr` is built, call `self.add_unconfirmed_peer(udp_addr)` and **defer** adding to `_active_manager_peers` / `_active_manager_peer_ids` until confirmation.
  - File: `hyperscale/distributed_rewrite/nodes/manager.py`
  - Method: `manager_peer_register()`
- **Gate**: When discovering managers/gates from registration or discovery, call `add_unconfirmed_peer(udp_addr)` before adding to active peer sets.
  - File: `hyperscale/distributed_rewrite/nodes/gate.py`
  - Methods: registration/discovery paths where `_probe_scheduler.add_member()` is called
- **Worker**: When adding manager UDP addresses to SWIM probing, call `add_unconfirmed_peer(manager_udp_addr)`.
  - File: `hyperscale/distributed_rewrite/nodes/worker.py`
  - Method: manager discovery/registration path where `_probe_scheduler.add_member()` is called

**Acceptance**:
- Unconfirmed peers are not in active peer sets until `confirm_peer()` is invoked by a successful SWIM message.

---

### 2) Wire peer confirmation callback to activate peers
**Problem**: `HealthAwareServer.register_on_peer_confirmed()` exists but no node uses it to move peers into active sets.

**Exact changes**:
- Register a callback in Gate/Manager/Worker that:
  - Adds the confirmed peer to the corresponding active peer sets.
  - Removes it from any pending/unconfirmed tracking used in the node.

**Acceptance**:
- Active peer sets contain only confirmed peers.
- Confirmation occurs on first successful message (ACK/heartbeat/etc.).

---

### 3) Add stale unconfirmed logging/metrics
**Problem**: `_unconfirmed_peer_added_at` is tracked but never used for visibility.

**Exact changes**:
- In `HealthAwareServer._run_cleanup()`, add:
  - A warning log + metric when an unconfirmed peer exceeds a threshold (e.g., 60s).
  - Optional: remove from `_unconfirmed_peers` after a larger TTL if policy allows; logging is the minimum requirement per AD-29 mitigation guidance.

**Acceptance**:
- Long-lived unconfirmed peers are visible in logs/metrics.

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

### 1) Fix rescheduling token mismatch in worker-failure path
**Problem**: `_handle_worker_failure()` builds `workflow_id` from sub-workflow tokens and later looks up `job.workflows[workflow_id]`, but `job.workflows` is keyed by the **parent workflow token** (no worker suffix). This prevents re-queueing and breaks AD-33 reschedule semantics.

**Exact changes**:
- In `hyperscale/distributed_rewrite/nodes/manager.py`, ensure `failed_workflows` uses the **parent workflow token** for lookups and the **subworkflow token** only for lifecycle transitions.
- Update `_requeue_workflows_in_dependency_order()` to accept parent workflow tokens and map them back to subworkflow tokens when applying lifecycle transitions.

**Acceptance**:
- Failed workflows are correctly found in `job.workflows` and re-queued.
- State transitions occur on the correct token type.

---

### 2) Provide dependency information to the rescheduler
**Problem**: `_find_dependent_workflows()` relies on `sub_wf.dependencies`, but `SubWorkflowInfo` has no `dependencies` field; dependencies currently live in `WorkflowDispatcher.PendingWorkflow`.

**Exact changes**:
- Persist dependencies into `SubWorkflowInfo` when constructing sub-workflows, **or**
- In `_find_dependent_workflows()`, consult `WorkflowDispatcher`’s dependency graph instead of `SubWorkflowInfo`.

**Acceptance**:
- Dependent workflows are correctly discovered (direct + transitive).
- AD-33 cancellation-before-retry ordering works.

---

### 3) Enforce dependent cancellation before retry
**Problem**: `_handle_worker_failure()` logs and continues if dependent cancellation fails, allowing retries before dependents are fully cancelled.

**Exact changes**:
- Make dependent cancellation a required gate: if cancellation fails or times out, **do not** transition to `FAILED_READY_FOR_RETRY`.
- Persist a retryable “cancel-pending” state and reattempt cancellation until it succeeds or job is cancelled.

**Acceptance**:
- No workflow is re-queued until dependents are confirmed cancelled.

---

### 4) FederatedHealthMonitor integration (AD-33 cross-DC)
**Problem**: AD-33 specifies `FederatedHealthMonitor` for cross-DC health checks; ensure gate routes through it instead of only local aggregates.

**Exact changes**:
- Verify `Gate` uses `FederatedHealthMonitor` to classify DCs for routing decisions.
- If not wired, integrate `FederatedHealthMonitor` outputs into `_datacenter_status` and `_select_datacenters_with_fallback()`.

**Acceptance**:
- Cross-DC health classification uses `xprobe/xack` signals, not just local SWIM state.

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
