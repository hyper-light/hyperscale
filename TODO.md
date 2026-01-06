# Hyperscale Implementation TODO

This document tracks implementation progress for architectural decisions AD-18 through AD-27.
Items are ordered by implementation priority and dependency.

---

# RULES

Please mark each off in TODO once done. Then proceed linearly down each - do not skip, mark each TODO item as done.


## Completed

### Component 4: Direct DC-to-Job-Leader Routing
- [x] `JobLeaderGateTransfer` message type
- [x] `JobLeaderGateTransferAck` message type
- [x] Gate forwarding logic for results not owned by this gate
- [x] Integration tests for DC-to-Job-Leader routing

### Component 5: Client Reconnection
- [x] `RegisterCallback` message type
- [x] `RegisterCallbackResponse` message type
- [x] Client `reconnect_to_job()` method with retry logic
- [x] Gate `register_callback` handler
- [x] Manager `register_callback` handler
- [x] Integration tests for client reconnection

---

## Phase 0: Critical Bug Fixes

Must be completed before reliability infrastructure.

- [x] Fix `_known_gates` not initialized in gate.py (used but never created)
- [x] Add per-job locking to gate's job state (race condition with concurrent handlers)

---

## Phase 1: Core Infrastructure

These provide the foundation for all other reliability features.

### 1.1 Module Structure Setup

- [x] Create `hyperscale/distributed_rewrite/reliability/` module
- [x] Create `hyperscale/distributed_rewrite/health/` module
- [x] Create `hyperscale/distributed_rewrite/jobs/gates/` module
- [x] Create `hyperscale/distributed_rewrite/datacenters/` module
- [x] Add `__init__.py` files with proper exports

### 1.2 AD-21: Unified Retry Framework with Jitter

Foundation for all network operations.

- [x] Implement `JitterStrategy` enum (FULL, EQUAL, DECORRELATED)
  - [x] FULL: `random(0, min(cap, base * 2^attempt))`
  - [x] EQUAL: `temp/2 + random(0, temp/2)`
  - [x] DECORRELATED: `random(base, previous_delay * 3)`
- [x] Implement `RetryConfig` dataclass
  - [x] `max_attempts: int = 3`
  - [x] `base_delay: float = 0.5`
  - [x] `max_delay: float = 30.0`
  - [x] `jitter: JitterStrategy = JitterStrategy.FULL`
  - [x] `retryable_exceptions: tuple[type[Exception], ...]`
- [x] Implement `RetryExecutor` class
  - [x] `calculate_delay(attempt: int) -> float`
  - [x] `async execute(operation, operation_name) -> T`
- [x] Add integration tests for retry framework

### 1.3 AD-18: Hybrid Overload Detection

Required by load shedding and health models.

- [x] Implement `OverloadConfig` dataclass
  - [x] Delta detection params: `ema_alpha`, `current_window`, `trend_window`
  - [x] Delta thresholds: `(0.2, 0.5, 1.0)` for busy/stressed/overloaded
  - [x] Absolute bounds: `(200.0, 500.0, 2000.0)` ms
  - [x] Resource thresholds for CPU and memory
- [x] Implement `HybridOverloadDetector` class
  - [x] `record_latency(latency_ms: float) -> None`
  - [x] `_calculate_trend() -> float` (linear regression on delta history)
  - [x] `get_state(cpu_percent, memory_percent) -> str`
  - [x] State returns: "healthy" | "busy" | "stressed" | "overloaded"
- [x] Add integration tests for overload detection

---

## Phase 2: Health Model Infrastructure

Three-signal health model for all node types.

### 2.1 AD-19: Worker Health (Manager monitors Workers)

- [x] Implement `WorkerHealthState` dataclass
  - [x] Liveness: `last_liveness_response`, `consecutive_liveness_failures`
  - [x] Readiness: `accepting_work`, `available_capacity`
  - [x] Progress: `workflows_assigned`, `completions_last_interval`, `expected_completion_rate`
- [x] Implement `liveness` property (30s timeout, 3 consecutive failures)
- [x] Implement `readiness` property
- [x] Implement `progress_state` property → "idle" | "normal" | "slow" | "degraded" | "stuck"
- [x] Implement `get_routing_decision()` → "route" | "drain" | "investigate" | "evict"
- [ ] Update manager's worker tracking to use `WorkerHealthState`
- [x] Add integration tests for worker health model

### 2.2 AD-19: Manager Health (Gate monitors Managers)

- [ ] Implement `ManagerHealthState` dataclass
  - [ ] Liveness: `last_liveness_response`, `consecutive_liveness_failures`
  - [ ] Readiness: `has_quorum`, `accepting_jobs`, `active_worker_count`
  - [ ] Progress: `jobs_accepted_last_interval`, `workflows_dispatched_last_interval`, `expected_throughput`
- [ ] Implement `liveness`, `readiness`, `progress_state` properties
- [ ] Implement `get_routing_decision()` method
- [ ] Update gate's manager tracking to use `ManagerHealthState`
- [ ] Integrate with DC Health Classification (AD-16)
  - [ ] ALL managers NOT liveness → DC = UNHEALTHY
  - [ ] MAJORITY managers NOT readiness → DC = DEGRADED
  - [ ] ANY manager progress == "stuck" → DC = DEGRADED
- [ ] Add integration tests for manager health model

### 2.3 AD-19: Gate Health (Gates monitor peer Gates)

- [ ] Implement `GateHealthState` dataclass
  - [ ] Liveness: `last_liveness_response`, `consecutive_liveness_failures`
  - [ ] Readiness: `has_dc_connectivity`, `connected_dc_count`, `overload_state`
  - [ ] Progress: `jobs_forwarded_last_interval`, `stats_aggregated_last_interval`, `expected_forward_rate`
- [ ] Implement `liveness`, `readiness`, `progress_state` properties
- [ ] Implement `get_routing_decision()` method
- [ ] Implement `should_participate_in_election() -> bool`
- [ ] Update gate's peer tracking to use `GateHealthState`
- [ ] Integrate with leader election (unhealthy gates shouldn't lead)
- [ ] Add integration tests for gate health model

### 2.4 AD-19: Generic Health Infrastructure

- [ ] Implement `HealthSignals` Protocol
  - [ ] `liveness: bool`
  - [ ] `readiness: bool`
  - [ ] `progress_state: str`
- [ ] Implement `NodeHealthTracker[T]` generic class
  - [ ] `update_state(node_id, state)`
  - [ ] `get_routing_decision(node_id) -> str`
  - [ ] `get_healthy_nodes() -> list[str]`
  - [ ] `should_evict(node_id) -> tuple[bool, str]` with correlation check
- [ ] Implement `HealthPiggyback` for SWIM integration
  - [ ] `node_id`, `node_type`
  - [ ] `accepting_work`, `capacity`
  - [ ] `throughput`, `expected_throughput`
  - [ ] `overload_state`
- [ ] Add health piggyback to SWIM protocol messages

---

## Phase 3: Load Management

### 3.1 AD-22: Load Shedding with Priority Queues

- [ ] Implement `RequestPriority` enum
  - [ ] CRITICAL = 0 (health checks, cancellation, final results, SWIM)
  - [ ] HIGH = 1 (job submissions, workflow dispatch, state sync)
  - [ ] NORMAL = 2 (progress updates, stats queries, reconnection)
  - [ ] LOW = 3 (detailed stats, debug requests)
- [ ] Implement `LoadShedder` class
  - [ ] Constructor takes `HybridOverloadDetector`
  - [ ] `should_shed(priority: RequestPriority) -> bool`
  - [ ] `classify_request(message_type: str) -> RequestPriority`
  - [ ] Shed thresholds: healthy=none, busy=LOW, stressed=NORMAL+LOW, overloaded=all except CRITICAL
- [ ] Integrate load shedder with gate request handlers
- [ ] Integrate load shedder with manager request handlers
- [ ] Add metrics for shed request counts
- [ ] Add integration tests for load shedding

### 3.2 AD-23: Backpressure for Stats Updates

- [ ] Implement `BackpressureLevel` enum
  - [ ] NONE = 0 (accept all)
  - [ ] THROTTLE = 1 (reduce frequency)
  - [ ] BATCH = 2 (batched only)
  - [ ] REJECT = 3 (reject non-critical)
- [ ] Implement `StatsBuffer` with tiered retention
  - [ ] HOT: 0-60s, full resolution, ring buffer (max 1000 entries)
  - [ ] WARM: 1-60min, 10s aggregates (max 360 entries)
  - [ ] COLD: 1-24h, 1min aggregates (max 1440 entries)
  - [ ] ARCHIVE: final summary only
- [ ] Implement automatic tier promotion (HOT → WARM → COLD)
- [ ] Implement `get_backpressure_level()` based on buffer fill
  - [ ] < 70% → NONE
  - [ ] 70-85% → THROTTLE
  - [ ] 85-95% → BATCH
  - [ ] > 95% → REJECT
- [ ] Add backpressure signaling in stats update responses
- [ ] Update stats senders to respect backpressure signals
- [ ] Add integration tests for backpressure

### 3.3 AD-24: Rate Limiting

- [ ] Implement `TokenBucket` class
  - [ ] `__init__(bucket_size: int, refill_rate: float)`
  - [ ] `async acquire(tokens: int = 1) -> bool`
  - [ ] `_refill()` based on elapsed time
- [ ] Implement `RateLimitConfig` dataclass
  - [ ] Per-operation limits
- [ ] Implement `ServerRateLimiter` class
  - [ ] Per-client token buckets: `dict[str, TokenBucket]`
  - [ ] `check_rate_limit(client_id, operation) -> tuple[bool, float]`
  - [ ] Returns `(allowed, retry_after_seconds)`
- [ ] Integrate rate limiter with gate handlers
- [ ] Add 429 response handling with Retry-After
- [ ] Add client-side cooperative rate limiting
- [ ] Add bucket cleanup for inactive clients (prevent memory leak)
- [ ] Add integration tests for rate limiting

---

## Phase 4: Protocol Extensions

### 4.1 AD-20: Cancellation Propagation

- [ ] Add `JobCancelRequest` message type
  - [ ] `job_id: str`
  - [ ] `requester_id: str`
  - [ ] `timestamp: float`
  - [ ] `fence_token: int`
- [ ] Add `JobCancelResponse` message type
  - [ ] `job_id: str`
  - [ ] `success: bool`
  - [ ] `cancelled_workflow_count: int`
  - [ ] `error: str | None`
- [ ] Implement client `cancel_job(job_id) -> JobCancelResponse`
- [ ] Implement gate `_handle_cancel_job()` handler
  - [ ] Forward to appropriate manager(s)
  - [ ] Aggregate responses from all DCs
- [ ] Implement manager `_handle_cancel_job()` handler
  - [ ] Cancel dispatched workflows on workers
  - [ ] Update job state to CANCELLED
- [ ] Implement worker workflow cancellation
  - [ ] Cancel running workflow tasks
  - [ ] Report cancellation to manager
- [ ] Add idempotency handling (repeated cancel returns success)
- [ ] Add integration tests for cancellation flow

### 4.2 AD-26: Adaptive Healthcheck Extensions

- [ ] Implement `ExtensionTracker` dataclass
  - [ ] `worker_id: str`
  - [ ] `base_deadline: float = 30.0`
  - [ ] `min_grant: float = 1.0`
  - [ ] `max_extensions: int = 5`
  - [ ] `extension_count: int = 0`
  - [ ] `last_progress: float = 0.0`
  - [ ] `total_extended: float = 0.0`
- [ ] Implement `request_extension(reason, current_progress) -> tuple[bool, float]`
  - [ ] Logarithmic grant: `max(min_grant, base / 2^extension_count)`
  - [ ] Deny if no progress since last extension
  - [ ] Deny if max_extensions exceeded
- [ ] Implement `reset()` for tracker cleanup
- [ ] Add `HealthcheckExtensionRequest` message type
  - [ ] `worker_id`, `reason`, `current_progress`, `estimated_completion`, `active_workflow_count`
- [ ] Add `HealthcheckExtensionResponse` message type
  - [ ] `granted`, `extension_seconds`, `new_deadline`, `remaining_extensions`, `denial_reason`
- [ ] Implement `WorkerHealthManager` class
  - [ ] `handle_extension_request()` with tracker management
  - [ ] `on_worker_healthy()` to reset tracker
- [ ] Integrate with manager's worker health tracking
- [ ] Add integration tests for extension protocol

### 4.3 AD-25: Version Skew Handling

- [ ] Implement `ProtocolVersion` dataclass
  - [ ] `major: int`, `minor: int`
  - [ ] `is_compatible_with(other) -> bool` (same major)
  - [ ] `supports_feature(other, feature) -> bool`
- [ ] Define feature version map
  - [ ] `"cancellation": (1, 0)`
  - [ ] `"batched_stats": (1, 1)`
  - [ ] `"client_reconnection": (1, 2)`
  - [ ] `"fence_tokens": (1, 2)`
- [ ] Implement `NodeCapabilities` dataclass
  - [ ] `protocol_version: ProtocolVersion`
  - [ ] `capabilities: set[str]`
  - [ ] `node_version: str`
  - [ ] `negotiate(other) -> set[str]`
- [ ] Add version/capability fields to handshake messages
- [ ] Update message serialization to ignore unknown fields
- [ ] Add protocol version validation on connection
- [ ] Add integration tests for version compatibility

---

## Phase 5: Module Reorganization (AD-27)

Extract classes from monolithic files into focused modules.

### 5.1 Gate Job Management

- [ ] Extract `GateJobManager` class from gate.py
  - [ ] Per-job state with locking
  - [ ] Job lifecycle management
- [ ] Extract `JobForwardingTracker` class from gate.py
  - [ ] Cross-gate job forwarding logic
- [ ] Extract `ConsistentHashRing` class
  - [ ] Per-job gate ownership calculation
- [ ] Update gate.py imports

### 5.2 Datacenter Management

- [ ] Extract `DatacenterHealthManager` class
  - [ ] DC health classification logic
  - [ ] Manager health aggregation
- [ ] Extract `ManagerDispatcher` class
  - [ ] Manager selection and routing
- [ ] Extract `LeaseManager` class (if applicable)
- [ ] Update gate.py imports

### 5.3 Reliability Module

- [ ] Move `RetryExecutor` to `reliability/retry.py`
- [ ] Move `HybridOverloadDetector` to `reliability/overload.py`
- [ ] Move `LoadShedder` to `reliability/load_shedding.py`
- [ ] Move `StatsBuffer` to `reliability/backpressure.py`
- [ ] Move `TokenBucket`, `ServerRateLimiter` to `reliability/rate_limiting.py`
- [ ] Create `reliability/jitter.py` for jitter utilities
- [ ] Add unified exports in `reliability/__init__.py`

### 5.4 Health Module

- [ ] Move `WorkerHealthState` to `health/worker_health.py`
- [ ] Move `ManagerHealthState` to `health/manager_health.py`
- [ ] Move `GateHealthState` to `health/gate_health.py`
- [ ] Move `NodeHealthTracker` to `health/tracker.py`
- [ ] Move `ExtensionTracker` to `health/extension_tracker.py`
- [ ] Add `health/probes.py` for liveness/readiness probe implementations
- [ ] Add unified exports in `health/__init__.py`

---

## Phase 6: SWIM Protocol Extensions

### 6.1 Health State Piggyback

- [ ] Add `HealthPiggyback` to SWIM message embedding
- [ ] Update `StateEmbedder` to include health signals
- [ ] Parse health piggyback in SWIM message handlers

### 6.2 Overload Signaling

- [ ] Piggyback overload state on SWIM messages
- [ ] React to peer overload state (reduce traffic)

### 6.3 Adaptive Timeouts

- [ ] Scale SWIM probe timeouts based on reported load
- [ ] Implement out-of-band health channel for high-priority probes

---

## Phase 7: Remaining Items

### Previously Identified

- [ ] Add `fence_token` field to `JobFinalResult`, `JobProgress`, `JobStatusPush`
- [ ] Implement fence token validation in Gate handlers
- [ ] Write integration test for fencing tokens

### Gate Per-Job Leadership

- [ ] Verify and enhance failover logic for gate leadership transfer
- [ ] Implement cross-DC correlation for eviction decisions
- [ ] Add eviction backoff for repeated failures

---

## Testing Requirements

- Integration tests follow patterns in `tests/integration/`
- **DO NOT run integration tests directly** - user will run and confirm
- Each new class should have corresponding test file
- Test files named `test_<module_name>.py`

---

## Reference

All architectural decisions documented in `docs/architecture.md`:
- AD-18: Hybrid Overload Detection (Delta + Absolute)
- AD-19: Three-Signal Health Model (All Node Types)
- AD-20: Cancellation Propagation
- AD-21: Unified Retry Framework with Jitter
- AD-22: Load Shedding with Priority Queues
- AD-23: Backpressure for Stats Updates
- AD-24: Rate Limiting (Client and Server)
- AD-25: Version Skew Handling
- AD-26: Adaptive Healthcheck Extensions
- AD-27: Gate Module Reorganization
