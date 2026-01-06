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
- [x] Update manager's worker tracking to use `WorkerHealthState`
- [x] Add integration tests for worker health model

### 2.2 AD-19: Manager Health (Gate monitors Managers)

- [x] Implement `ManagerHealthState` dataclass
  - [x] Liveness: `last_liveness_response`, `consecutive_liveness_failures`
  - [x] Readiness: `has_quorum`, `accepting_jobs`, `active_worker_count`
  - [x] Progress: `jobs_accepted_last_interval`, `workflows_dispatched_last_interval`, `expected_throughput`
- [x] Implement `liveness`, `readiness`, `progress_state` properties
- [x] Implement `get_routing_decision()` method
- [x] Update gate's manager tracking to use `ManagerHealthState`
- [x] Integrate with DC Health Classification (AD-16)
  - [x] ALL managers NOT liveness → DC = UNHEALTHY
  - [x] MAJORITY managers NOT readiness → DC = DEGRADED
  - [x] ANY manager progress == "stuck" → DC = DEGRADED
- [x] Add integration tests for manager health model

### 2.3 AD-19: Gate Health (Gates monitor peer Gates)

- [x] Implement `GateHealthState` dataclass
  - [x] Liveness: `last_liveness_response`, `consecutive_liveness_failures`
  - [x] Readiness: `has_dc_connectivity`, `connected_dc_count`, `overload_state`
  - [x] Progress: `jobs_forwarded_last_interval`, `stats_aggregated_last_interval`, `expected_forward_rate`
- [x] Implement `liveness`, `readiness`, `progress_state` properties
- [x] Implement `get_routing_decision()` method
- [x] Implement `should_participate_in_election() -> bool`
- [x] Update gate's peer tracking to use `GateHealthState`
- [x] Integrate with leader election (unhealthy gates shouldn't lead)
- [x] Add integration tests for gate health model

### 2.4 AD-19: Generic Health Infrastructure

- [x] Implement `HealthSignals` Protocol
  - [x] `liveness: bool`
  - [x] `readiness: bool`
  - [x] `progress_state: str`
- [x] Implement `NodeHealthTracker[T]` generic class
  - [x] `update_state(node_id, state)`
  - [x] `get_routing_decision(node_id) -> str`
  - [x] `get_healthy_nodes() -> list[str]`
  - [x] `should_evict(node_id) -> tuple[bool, str]` with correlation check
- [x] Implement `HealthPiggyback` for SWIM integration
  - [x] `node_id`, `node_type`
  - [x] `accepting_work`, `capacity`
  - [x] `throughput`, `expected_throughput`
  - [x] `overload_state`
- [x] Add health piggyback to SWIM protocol messages
  - [x] Add health fields to WorkerHeartbeat, ManagerHeartbeat, GateHeartbeat
  - [x] Update StateEmbedders to populate health fields
  - [x] Add integration tests for health piggyback

---

## Phase 3: Load Management

### 3.1 AD-22: Load Shedding with Priority Queues

- [x] Implement `RequestPriority` enum
  - [x] CRITICAL = 0 (health checks, cancellation, final results, SWIM)
  - [x] HIGH = 1 (job submissions, workflow dispatch, state sync)
  - [x] NORMAL = 2 (progress updates, stats queries, reconnection)
  - [x] LOW = 3 (detailed stats, debug requests)
- [x] Implement `LoadShedder` class
  - [x] Constructor takes `HybridOverloadDetector`
  - [x] `should_shed(priority: RequestPriority) -> bool`
  - [x] `classify_request(message_type: str) -> RequestPriority`
  - [x] Shed thresholds: healthy=none, busy=LOW, stressed=NORMAL+LOW, overloaded=all except CRITICAL
- [x] Integrate load shedder with gate request handlers
- [x] Integrate load shedder with manager request handlers
- [x] Add metrics for shed request counts
- [x] Add integration tests for load shedding

### 3.2 AD-23: Backpressure for Stats Updates

- [x] Implement `BackpressureLevel` enum
  - [x] NONE = 0 (accept all)
  - [x] THROTTLE = 1 (reduce frequency)
  - [x] BATCH = 2 (batched only)
  - [x] REJECT = 3 (reject non-critical)
- [x] Implement `StatsBuffer` with tiered retention
  - [x] HOT: 0-60s, full resolution, ring buffer (max 1000 entries)
  - [x] WARM: 1-60min, 10s aggregates (max 360 entries)
  - [x] COLD: 1-24h, 1min aggregates (max 1440 entries)
  - [x] ARCHIVE: final summary only
- [x] Implement automatic tier promotion (HOT → WARM → COLD)
- [x] Implement `get_backpressure_level()` based on buffer fill
  - [x] < 70% → NONE
  - [x] 70-85% → THROTTLE
  - [x] 85-95% → BATCH
  - [x] > 95% → REJECT
- [x] Add backpressure signaling in stats update responses
- [x] Update stats senders to respect backpressure signals
- [x] Add integration tests for backpressure

### 3.3 AD-24: Rate Limiting

- [x] Implement `TokenBucket` class
  - [x] `__init__(bucket_size: int, refill_rate: float)`
  - [x] `async acquire(tokens: int = 1) -> bool`
  - [x] `_refill()` based on elapsed time
- [x] Implement `RateLimitConfig` dataclass
  - [x] Per-operation limits
- [x] Implement `ServerRateLimiter` class
  - [x] Per-client token buckets: `dict[str, TokenBucket]`
  - [x] `check_rate_limit(client_id, operation) -> tuple[bool, float]`
  - [x] Returns `(allowed, retry_after_seconds)`
- [x] Integrate rate limiter with gate handlers
- [x] Integrate rate limiter with manager handlers
- [x] Add response handling with Retry-After (RateLimitResponse)
- [x] Add client-side cooperative rate limiting
- [x] Add automatic retry-after logic (RateLimitRetryConfig, execute_with_rate_limit_retry)
- [x] Add bucket cleanup for inactive clients (prevent memory leak)
- [x] Add integration tests for rate limiting

---

## Phase 4: Protocol Extensions

### 4.1 AD-20: Cancellation Propagation

- [x] Add `JobCancelRequest` message type
  - [x] `job_id: str`
  - [x] `requester_id: str`
  - [x] `timestamp: float`
  - [x] `fence_token: int`
- [x] Add `JobCancelResponse` message type
  - [x] `job_id: str`
  - [x] `success: bool`
  - [x] `cancelled_workflow_count: int`
  - [x] `error: str | None`
- [x] Add `WorkflowCancelRequest` and `WorkflowCancelResponse` message types
- [x] Implement client `cancel_job(job_id) -> JobCancelResponse`
  - [x] Retry logic with exponential backoff
  - [x] Leader redirect handling
  - [x] Local job state update on cancellation
- [x] Implement gate `_handle_cancel_job()` handler
  - [x] Forward to appropriate manager(s) with retry logic
  - [x] Aggregate responses from all DCs
  - [x] Use exponential backoff for DC communication
  - [x] Validate fence tokens
- [x] Implement manager `_handle_cancel_job()` handler
  - [x] Cancel dispatched workflows on workers
  - [x] Update job state to CANCELLED
  - [x] Send WorkflowCancelRequest to workers
- [x] Implement worker workflow cancellation
  - [x] Cancel running workflow tasks via cancel_workflow handler
  - [x] Report cancellation to manager via WorkflowCancelResponse
  - [x] Idempotency handling for already cancelled/completed workflows
- [x] Add idempotency handling (repeated cancel returns success)
- [x] Add integration tests for cancellation flow
  - [x] Message serialization tests
  - [x] Cancellation propagation scenarios
  - [x] Fence token validation tests
  - [x] Legacy message compatibility tests

### 4.2 AD-26: Adaptive Healthcheck Extensions

- [x] Implement `ExtensionTracker` dataclass
  - [x] `worker_id: str`
  - [x] `base_deadline: float = 30.0`
  - [x] `min_grant: float = 1.0`
  - [x] `max_extensions: int = 5`
  - [x] `extension_count: int = 0`
  - [x] `last_progress: float = 0.0`
  - [x] `total_extended: float = 0.0`
- [x] Implement `request_extension(reason, current_progress) -> tuple[bool, float]`
  - [x] Logarithmic grant: `max(min_grant, base / 2^extension_count)`
  - [x] Deny if no progress since last extension
  - [x] Deny if max_extensions exceeded
- [x] Implement `reset()` for tracker cleanup
- [x] Add `HealthcheckExtensionRequest` message type
  - [x] `worker_id`, `reason`, `current_progress`, `estimated_completion`, `active_workflow_count`
- [x] Add `HealthcheckExtensionResponse` message type
  - [x] `granted`, `extension_seconds`, `new_deadline`, `remaining_extensions`, `denial_reason`
- [x] Implement `WorkerHealthManager` class
  - [x] `handle_extension_request()` with tracker management
  - [x] `on_worker_healthy()` to reset tracker
  - [x] `on_worker_removed()` for cleanup
  - [x] `should_evict_worker()` for eviction decisions
- [x] Integrate with manager's worker health tracking
  - [x] Add WorkerHealthManager to manager initialization
  - [x] Add request_extension TCP handler
  - [x] Add _on_worker_healthy and _on_worker_removed callbacks
  - [x] Track worker deadlines for extension management
- [x] Add integration tests for extension protocol
  - [x] ExtensionTracker logarithmic decay tests
  - [x] Progress requirement tests
  - [x] Message serialization tests
  - [x] WorkerHealthManager handling tests
  - [x] Eviction recommendation tests
  - [x] Realistic scenario tests

### 4.3 AD-25: Version Skew Handling

- [x] Implement `ProtocolVersion` dataclass
  - [x] `major: int`, `minor: int`
  - [x] `is_compatible_with(other) -> bool` (same major)
  - [x] `supports_feature(feature) -> bool`
- [x] Define feature version map
  - [x] `"cancellation": (1, 0)`
  - [x] `"batched_stats": (1, 1)`
  - [x] `"client_reconnection": (1, 2)`
  - [x] `"fence_tokens": (1, 2)`
  - [x] `"rate_limiting": (1, 3)`
  - [x] `"healthcheck_extensions": (1, 4)`
- [x] Implement `NodeCapabilities` dataclass
  - [x] `protocol_version: ProtocolVersion`
  - [x] `capabilities: set[str]`
  - [x] `node_version: str`
  - [x] `negotiate(other) -> set[str]`
- [x] Implement `NegotiatedCapabilities` result class
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

- [ ] Gates accept client job requests (like client -> manager pattern)
  - [ ] Client can submit jobs directly to gates
  - [ ] Gates forward to appropriate DC manager(s)
  - [ ] Gates aggregate results from DCs
- [ ] Gates use retry logic with exponential backoff for DC communication
- [ ] Gates use fencing tokens for all job operations
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
