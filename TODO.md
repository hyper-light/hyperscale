# Hyperscale Implementation TODO

## Previously Identified (Some Completed)

- Add fence_token field to JobFinalResult, JobProgress, JobStatusPush
- Implement fence token validation in Gate handlers
- Write integration test for fencing tokens
- ~~Implement Component 4: Direct DC-to-Job-Leader Routing~~ (DONE)
- ~~Implement Component 5: Client Reconnection~~ (DONE)

---

## Priority 0: Critical Bug Fixes

- [ ] Fix `_known_gates` not initialized in gate.py (used but never created)
- [ ] Add per-job locking to gate's job state (race condition with concurrent handlers)

---

## Priority 1: Reliability Infrastructure (AD-18 to AD-27)

### AD-18: Hybrid Overload Detection
- [ ] Create `hyperscale/distributed_rewrite/reliability/` module
- [ ] Implement `OverloadConfig` dataclass
- [ ] Implement `HybridOverloadDetector` class with:
  - [ ] Delta-based detection (EMA baseline, trend calculation)
  - [ ] Absolute safety bounds
  - [ ] Resource signal integration (CPU, memory)
- [ ] Add integration tests for overload detection

### AD-19: Three-Signal Worker Health Model
- [ ] Create `hyperscale/distributed_rewrite/health/` module
- [ ] Implement `WorkerHealthState` dataclass with:
  - [ ] Liveness signal (ping/pong tracking)
  - [ ] Readiness signal (self-reported + capacity)
  - [ ] Progress signal (completion rate tracking)
- [ ] Implement `get_routing_decision()` method
- [ ] Update manager's worker tracking to use three-signal model
- [ ] Add integration tests for health model

### AD-20: Cancellation Propagation
- [ ] Add `JobCancelRequest` and `JobCancelResponse` message types
- [ ] Implement client `cancel_job()` method
- [ ] Implement gate `_handle_cancel_job()` handler
- [ ] Implement manager `_handle_cancel_job()` handler
- [ ] Implement worker cancellation of running workflows
- [ ] Add idempotency handling for repeated cancellation requests
- [ ] Add integration tests for cancellation flow

### AD-21: Unified Retry Framework with Jitter
- [ ] Implement `JitterStrategy` enum (FULL, EQUAL, DECORRELATED)
- [ ] Implement `RetryConfig` dataclass
- [ ] Implement `RetryExecutor` class
- [ ] Add `calculate_delay()` with all jitter strategies
- [ ] Refactor existing retry code to use RetryExecutor:
  - [ ] State sync retries
  - [ ] Health check retries
  - [ ] Workflow dispatch retries
  - [ ] Reconnection retries
- [ ] Add jitter to heartbeat timing
- [ ] Add jitter to leader election timeouts

### AD-22: Load Shedding with Priority Queues
- [ ] Implement `RequestPriority` enum
- [ ] Implement `LoadShedder` class
- [ ] Add `classify_request()` for message type → priority mapping
- [ ] Integrate load shedder with gate request handlers
- [ ] Integrate load shedder with manager request handlers
- [ ] Add metrics for shed request counts
- [ ] Add integration tests for load shedding

### AD-23: Backpressure for Stats Updates
- [ ] Implement `BackpressureLevel` enum
- [ ] Implement `StatsBuffer` with tiered retention (HOT/WARM/COLD)
- [ ] Add automatic tier promotion (HOT → WARM → COLD)
- [ ] Implement `get_backpressure_level()` based on buffer fill
- [ ] Add backpressure signaling in stats update responses
- [ ] Update stats senders to respect backpressure signals
- [ ] Add integration tests for backpressure

### AD-24: Rate Limiting
- [ ] Implement `TokenBucket` class
- [ ] Implement `ServerRateLimiter` with per-client buckets
- [ ] Add rate limit configuration per operation type
- [ ] Integrate rate limiter with gate handlers
- [ ] Add 429 response handling with Retry-After header
- [ ] Add client-side cooperative rate limiting
- [ ] Add bucket cleanup for inactive clients
- [ ] Add integration tests for rate limiting

### AD-25: Version Skew Handling
- [ ] Implement `ProtocolVersion` dataclass
- [ ] Implement `NodeCapabilities` dataclass
- [ ] Add version/capability fields to handshake messages
- [ ] Implement `is_compatible_with()` check
- [ ] Implement `negotiate()` for capability intersection
- [ ] Update message serialization to ignore unknown fields
- [ ] Add protocol version validation on connection
- [ ] Add integration tests for version compatibility

### AD-26: Adaptive Healthcheck Extensions
- [ ] Implement `ExtensionTracker` dataclass
- [ ] Add `HealthcheckExtensionRequest` message type
- [ ] Add `HealthcheckExtensionResponse` message type
- [ ] Implement logarithmic grant reduction
- [ ] Add progress validation before granting extensions
- [ ] Integrate with manager's worker health tracking
- [ ] Add integration tests for extension protocol

### AD-27: Gate Module Reorganization
- [ ] Create `hyperscale/distributed_rewrite/jobs/gates/` module
- [ ] Extract `GateJobManager` class from gate.py
- [ ] Extract `JobForwardingTracker` class from gate.py
- [ ] Extract `ConsistentHashRing` class from gate.py
- [ ] Create `hyperscale/distributed_rewrite/datacenters/` module
- [ ] Extract `DatacenterHealthManager` class
- [ ] Extract `ManagerDispatcher` class
- [ ] Update gate.py imports to use new modules
- [ ] Add tests for each extracted class

---

## Priority 2: Extended SWIM Integration

- [ ] Extend SWIM protocol for overload signaling (piggyback overload state)
- [ ] Add work-aware health signal to SWIM heartbeats
- [ ] Implement adaptive timeout scaling based on reported load
- [ ] Add out-of-band health channel for high-priority probes

---

## Priority 3: Remaining Gate Per-Job Leadership Components

Reference: See "Gate Per-Job Leadership Architecture" in docs/architecture.md

- [ ] Verify and enhance failover logic for gate leadership transfer
- [ ] Implement cross-DC correlation for eviction decisions
- [ ] Add eviction backoff for repeated failures

---

## Testing Requirements

- Integration tests should follow patterns in `tests/integration/`
- DO NOT run integration tests directly - user will run and confirm
- Each new class should have corresponding test file
