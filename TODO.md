# TODO: Distributed System Architecture Implementation

## Overview

This document tracks the remaining implementation work for AD-34, AD-35, and AD-36 architectural decisions.

---

## 11. AD-34: Adaptive Job Timeout with Multi-DC Coordination

**Status**: Architecture Complete, Implementation Pending

**Overview**: Implement adaptive job timeout tracking that auto-detects single-DC vs multi-DC deployments and uses appropriate timeout strategies. Integrates with AD-26 (healthcheck extensions) and AD-33 (workflow state machine) to prevent resource leaks while respecting legitimate long-running work.

### 11.1 Core Data Structures

**File**: `hyperscale/distributed_rewrite/models/jobs.py`

- [ ] **11.1.1** Add `TimeoutTrackingState` dataclass with all fields:
  - `strategy_type: str` ("local_authority" | "gate_coordinated")
  - `gate_addr: tuple[str, int] | None`
  - `started_at: float` (absolute monotonic timestamp)
  - `last_progress_at: float`
  - `last_report_at: float`
  - `timeout_seconds: float`
  - `stuck_threshold: float = 120.0`
  - `total_extensions_granted: float = 0.0`
  - `max_worker_extension: float = 0.0`
  - `last_extension_at: float = 0.0`
  - `active_workers_with_extensions: set[str]`
  - `locally_timed_out: bool = False`
  - `globally_timed_out: bool = False`
  - `timeout_reason: str = ""`
  - `timeout_fence_token: int = 0`

- [ ] **11.1.2** Add `timeout_tracking: TimeoutTrackingState | None` field to `JobInfo`

### 11.2 Protocol Messages

**File**: `hyperscale/distributed_rewrite/models/distributed.py`

- [ ] **11.2.1** Add `JobProgressReport` message (Manager → Gate):
  - `job_id: str`
  - `datacenter: str`
  - `manager_id: str`
  - `manager_host: str`
  - `manager_port: int`
  - `workflows_total: int`
  - `workflows_completed: int`
  - `workflows_failed: int`
  - `has_recent_progress: bool`
  - `timestamp: float`
  - `fence_token: int`
  - `total_extensions_granted: float = 0.0`
  - `max_worker_extension: float = 0.0`
  - `workers_with_extensions: int = 0`

- [ ] **11.2.2** Add `JobTimeoutReport` message (Manager → Gate):
  - `job_id: str`
  - `datacenter: str`
  - `manager_id: str`
  - `manager_host: str`
  - `manager_port: int`
  - `reason: str`
  - `elapsed_seconds: float`
  - `fence_token: int`

- [ ] **11.2.3** Add `JobGlobalTimeout` message (Gate → Manager):
  - `job_id: str`
  - `reason: str`
  - `timed_out_at: float`
  - `fence_token: int`

- [ ] **11.2.4** Add `JobLeaderTransfer` message (Manager → Gate):
  - `job_id: str`
  - `datacenter: str`
  - `new_leader_id: str`
  - `fence_token: int`

- [ ] **11.2.5** Add `JobFinalStatus` message (Manager → Gate):
  - `job_id: str`
  - `datacenter: str`
  - `manager_id: str`
  - `status: str`
  - `timestamp: float`
  - `fence_token: int`

- [ ] **11.2.6** Add `WorkerExtensionGranted` message (internal):
  - `job_id: str`
  - `worker_id: str`
  - `extension_seconds: float`
  - `total_worker_extensions: float`
  - `worker_progress: float`
  - `timestamp: float`

- [ ] **11.2.7** Add `gate_addr: tuple[str, int] | None` field to `JobSubmission`

- [ ] **11.2.8** Add `target_datacenters: list[str]` field to `JobSubmission`

### 11.3 Timeout Strategy Implementation

**File**: `hyperscale/distributed_rewrite/jobs/timeout_strategy.py` (NEW)

- [ ] **11.3.1** Create `TimeoutStrategy` ABC with methods:
  - `start_tracking(job_id, timeout_seconds, gate_addr) -> None`
  - `resume_tracking(job_id) -> None`
  - `report_progress(job_id, progress_type) -> None`
  - `check_timeout(job_id) -> tuple[bool, str]`
  - `handle_global_timeout(job_id, reason, fence_token) -> bool`
  - `record_worker_extension(job_id, worker_id, extension_seconds, worker_progress) -> None`
  - `stop_tracking(job_id, reason) -> None`
  - `cleanup_worker_extensions(job_id, worker_id) -> None`

- [ ] **11.3.2** Implement `LocalAuthorityTimeout` class:
  - Full state management via `TimeoutTrackingState`
  - Idempotent `check_timeout()` with `locally_timed_out` flag
  - Overall timeout check: `elapsed > timeout_seconds + total_extensions_granted`
  - Stuck detection: `time_since_progress > stuck_threshold`
  - Extension-aware timeout calculation
  - `resume_tracking()` increments fence token
  - No-op `handle_global_timeout()` (returns False)

- [ ] **11.3.3** Implement `GateCoordinatedTimeout` class:
  - All `LocalAuthorityTimeout` features plus:
  - Progress reporting to gate every 10 seconds
  - Timeout reporting to gate (stored in `_pending_reports` until ACK'd)
  - 5-minute fallback if gate unreachable
  - `handle_global_timeout()` validates fence token
  - Leader transfer notification to gate
  - Status correction sending

### 11.4 Manager Integration

**File**: `hyperscale/distributed_rewrite/nodes/manager.py`

- [ ] **11.4.1** Add `_job_timeout_strategies: dict[str, TimeoutStrategy]` field

- [ ] **11.4.2** Implement `_select_timeout_strategy(submission)` method:
  - Check `gate_addr` in submission
  - Return `GateCoordinatedTimeout` if `gate_addr` present
  - Return `LocalAuthorityTimeout` otherwise

- [ ] **11.4.3** Implement `_unified_timeout_loop()` background task:
  - Run every 30 seconds
  - Only check if `ManagerState.ACTIVE`
  - Only check jobs where this manager is leader
  - Call `strategy.check_timeout()` for each job
  - Log timeout events

- [ ] **11.4.4** Update `receive_submit_job()`:
  - Call `_select_timeout_strategy()`
  - Call `strategy.start_tracking()`
  - Store strategy in `_job_timeout_strategies`

- [ ] **11.4.5** Implement `_on_leadership_acquired(job_id)`:
  - Get or create strategy via `_get_or_create_timeout_strategy()`
  - Call `strategy.resume_tracking()`
  - Store in `_job_timeout_strategies`

- [ ] **11.4.6** Implement `_get_or_create_timeout_strategy(job)`:
  - Check `job.timeout_tracking.strategy_type`
  - Return appropriate strategy instance

- [ ] **11.4.7** Implement `_timeout_job(job_id, reason)`:
  - Mark job status as `JobStatus.TIMEOUT`
  - Cancel all workflows via `_cancel_all_workflows_for_job()`
  - Call `strategy.stop_tracking(job_id, "timed_out")`
  - Notify callback (gate or client)
  - Log timeout event

- [ ] **11.4.8** Update `request_extension()` to notify timeout strategies:
  - On successful extension grant, call `_notify_timeout_strategies_of_extension()`

- [ ] **11.4.9** Implement `_notify_timeout_strategies_of_extension(worker_id, extension_seconds, progress)`:
  - Find all jobs this worker is executing
  - Call `strategy.record_worker_extension()` for each

- [ ] **11.4.10** Add cleanup hooks:
  - `receive_cancel_job()` → `strategy.stop_tracking("cancelled")`
  - `_handle_job_completion()` → `strategy.stop_tracking("completed")`
  - `_handle_job_failure()` → `strategy.stop_tracking("failed")`
  - `_handle_worker_failure()` → `strategy.cleanup_worker_extensions()`
  - `_cleanup_job()` → remove strategy from `_job_timeout_strategies`

- [ ] **11.4.11** Add `receive_job_global_timeout()` handler:
  - Load `JobGlobalTimeout` message
  - Call `strategy.handle_global_timeout()`
  - Clean up tracking on acceptance

- [ ] **11.4.12** Add `_setup_timeout_progress_tracking(job_id)`:
  - Connect WorkflowStateMachine progress events to timeout strategy
  - Register callback to call `strategy.report_progress()`

- [ ] **11.4.13** Start `_unified_timeout_loop` in `start()` method

### 11.5 Gate Integration

**File**: `hyperscale/distributed_rewrite/nodes/gate.py`

- [ ] **11.5.1** Add `GateJobTrackingInfo` dataclass:
  - `job_id: str`
  - `submitted_at: float`
  - `timeout_seconds: float`
  - `target_datacenters: list[str]`
  - `dc_status: dict[str, str]`
  - `dc_last_progress: dict[str, float]`
  - `dc_manager_addrs: dict[str, tuple[str, int]]`
  - `dc_total_extensions: dict[str, float]`
  - `dc_max_extension: dict[str, float]`
  - `dc_workers_with_extensions: dict[str, int]`
  - `globally_timed_out: bool = False`
  - `timeout_reason: str = ""`
  - `timeout_fence_token: int = 0`

- [ ] **11.5.2** Implement `GateJobTracker` class:
  - `_tracked_jobs: dict[str, GateJobTrackingInfo]`
  - `_lock: asyncio.Lock`
  - `start_tracking_job(job_id, timeout_seconds, target_dcs)`
  - `record_progress(report: JobProgressReport)` - update dc_last_progress, dc_manager_addrs, extension tracking
  - `record_timeout(report: JobTimeoutReport)` - set dc_status to "timed_out"
  - `check_global_timeouts()` - return list of (job_id, reason)
  - `handle_final_status(report: JobFinalStatus)` - cleanup tracking
  - `get_job(job_id)` - return tracking info

- [ ] **11.5.3** Add `_job_tracker: GateJobTracker` field to `GateServer`

- [ ] **11.5.4** Implement `_global_timeout_loop()` background task:
  - Run every 15 seconds
  - Call `_job_tracker.check_global_timeouts()`
  - Call `_declare_and_broadcast_timeout()` for each timed out job

- [ ] **11.5.5** Implement `_declare_and_broadcast_timeout(job_id, reason)`:
  - Get tracking info from `_job_tracker`
  - Log global timeout event
  - Create `JobGlobalTimeout` message
  - Send to all target DCs via `send_tcp()`

- [ ] **11.5.6** Add `receive_job_progress_report()` handler:
  - Load `JobProgressReport`
  - Call `_job_tracker.record_progress()`

- [ ] **11.5.7** Add `receive_job_timeout_report()` handler:
  - Load `JobTimeoutReport`
  - Call `_job_tracker.record_timeout()`

- [ ] **11.5.8** Add `receive_job_final_status()` handler:
  - Load `JobFinalStatus`
  - Call `_job_tracker.handle_final_status()`

- [ ] **11.5.9** Add `receive_job_leader_transfer()` handler:
  - Update `dc_manager_addrs` for datacenter

- [ ] **11.5.10** Start `_global_timeout_loop` in `start()` method

- [ ] **11.5.11** Update job submission to start tracking:
  - Call `_job_tracker.start_tracking_job()` when submitting to DCs

### 11.6 WorkflowStateMachine Integration (AD-33)

**File**: `hyperscale/distributed_rewrite/workflow/state_machine.py`

- [ ] **11.6.1** Add `_last_progress: dict[str, float]` field

- [ ] **11.6.2** Add `_progress_callbacks: list[Callable]` field

- [ ] **11.6.3** Implement `register_progress_callback(callback)`:
  - Append callback to `_progress_callbacks`

- [ ] **11.6.4** Update `transition()` to notify callbacks:
  - Record `_last_progress[workflow_id] = time.monotonic()`
  - Call all registered callbacks with `(workflow_id, to_state)`

- [ ] **11.6.5** Implement `get_time_since_progress(workflow_id)`:
  - Return `time.monotonic() - _last_progress.get(workflow_id, 0.0)`

- [ ] **11.6.6** Implement `get_stuck_workflows(threshold_seconds)`:
  - Return list of workflow_ids with no progress for threshold

### 11.7 Configuration

**File**: `hyperscale/distributed_rewrite/env/env.py`

- [ ] **11.7.1** Add `JOB_TIMEOUT_CHECK_INTERVAL: float = 30.0`

- [ ] **11.7.2** Add `JOB_STUCK_THRESHOLD: float = 120.0`

- [ ] **11.7.3** Add `GATE_TIMEOUT_CHECK_INTERVAL: float = 15.0`

- [ ] **11.7.4** Add `GATE_TIMEOUT_FALLBACK: float = 300.0`

- [ ] **11.7.5** Add `GATE_ALL_DC_STUCK_THRESHOLD: float = 180.0`

### 11.8 Metrics and Observability

- [ ] **11.8.1** Add metrics:
  - `job_timeout_checks_total{strategy}`
  - `job_timeouts_detected_total{reason}`
  - `job_timeout_reports_sent_total{datacenter}`
  - `job_timeout_reports_failed_total{datacenter}`
  - `gate_global_timeouts_declared_total{reason}`
  - `gate_dc_progress_reports_received_total{datacenter}`
  - `gate_dc_timeout_reports_received_total{datacenter}`
  - `timeout_fence_token_rejections_total{reason}`
  - `timeout_leader_transfers_total`

- [ ] **11.8.2** Add structured logging for:
  - Job timeout detection with reason
  - Gate unresponsive fallback
  - Stale fence token rejections
  - Timeout tracking resume
  - Global timeout declarations

### 11.9 Testing

**File**: `tests/integration/test_job_timeout.py` (NEW)

- [ ] **11.9.1** Test single-DC local authority timeout

- [ ] **11.9.2** Test multi-DC gate coordinated timeout

- [ ] **11.9.3** Test extension-aware timeout (job with extensions)

- [ ] **11.9.4** Test stuck detection (no workflow progress)

- [ ] **11.9.5** Test leader transfer with timeout state

- [ ] **11.9.6** Test fence token rejection

- [ ] **11.9.7** Test cleanup on job completion

- [ ] **11.9.8** Test cleanup on job cancellation

- [ ] **11.9.9** Test worker failure extension cleanup

- [ ] **11.9.10** Test gate failure fallback (5 minute)

- [ ] **11.9.11** Test race condition: job completes during timeout

- [ ] **11.9.12** Test network partition isolation

---

## 12. AD-35: Vivaldi Network Coordinates with Role-Aware Failure Detection

**Status**: Architecture Complete, Implementation Pending

**Overview**: Implement Vivaldi network coordinates for latency-aware failure detection, role-aware confirmation strategies for Gates/Managers/Workers, and an explicit UNCONFIRMED lifecycle state for unconfirmed peers.

### 12.1 Vivaldi Coordinate System

**File**: `hyperscale/distributed_rewrite/swim/vivaldi/coordinate.py` (NEW)

- [ ] **12.1.1** Implement `VivaldiCoordinate` dataclass:
  - `position: list[float]` (4-dimensional)
  - `height: float` (models asymmetric routes)
  - `error: float` (prediction confidence, lower = better)
  - `sample_count: int = 0`
  - `updated_at: float = 0.0`

- [ ] **12.1.2** Implement `vivaldi_distance(coord_a, coord_b) -> float`:
  - Euclidean distance + height components
  - Returns estimated RTT in milliseconds

- [ ] **12.1.3** Implement `coordinate_quality(sample_count, error_ms, staleness_s) -> float`:
  - Combine sample quality, error quality, staleness quality
  - Return 0.0-1.0 quality score

**File**: `hyperscale/distributed_rewrite/swim/vivaldi/vivaldi_system.py` (NEW)

- [ ] **12.1.4** Implement `VivaldiCoordinateSystem` class:
  - `_local_coordinate: VivaldiCoordinate`
  - `_peer_coordinates: dict[NodeAddress, VivaldiCoordinate]`
  - `_config: VivaldiConfig`
  - `_lock: asyncio.Lock`

- [ ] **12.1.5** Implement `update_coordinate(peer, peer_coord, measured_rtt_ms)`:
  - Calculate prediction error
  - Update local position using Vivaldi algorithm
  - Update local error estimate
  - Store peer's coordinate

- [ ] **12.1.6** Implement `estimate_rtt(peer) -> float`:
  - Return `vivaldi_distance(local, peer_coord)`
  - Fall back to default RTT if peer unknown

- [ ] **12.1.7** Implement `estimate_rtt_ucb_ms(peer) -> float`:
  - Upper confidence bound RTT estimate
  - `rtt_hat + K_SIGMA * sigma`
  - Clamp to `[RTT_MIN_MS, RTT_MAX_MS]`

- [ ] **12.1.8** Implement `get_local_coordinate() -> VivaldiCoordinate`

- [ ] **12.1.9** Implement `get_peer_coordinate(peer) -> VivaldiCoordinate | None`

- [ ] **12.1.10** Implement `get_error() -> float`

- [ ] **12.1.11** Implement `is_converged() -> bool`:
  - Return `error < CONVERGENCE_THRESHOLD`

**File**: `hyperscale/distributed_rewrite/swim/vivaldi/config.py` (NEW)

- [ ] **12.1.12** Implement `VivaldiConfig` dataclass:
  - `dimensions: int = 4`
  - `initial_error: float = 1.0`
  - `min_error: float = 0.001`
  - `max_error: float = 1.5`
  - `error_adjustment: float = 0.25`
  - `coordinate_adjustment: float = 0.25`
  - `convergence_threshold: float = 0.15`
  - `rtt_default_ms: float = 100.0`
  - `rtt_min_ms: float = 1.0`
  - `rtt_max_ms: float = 10000.0`
  - `sigma_default_ms: float = 50.0`
  - `sigma_min_ms: float = 5.0`
  - `sigma_max_ms: float = 500.0`
  - `k_sigma: float = 2.0`
  - `min_samples_for_routing: int = 5`
  - `error_good_ms: float = 20.0`
  - `coord_ttl_s: float = 300.0`

### 12.2 SWIM Message Integration

**File**: `hyperscale/distributed_rewrite/models/swim.py`

- [ ] **12.2.1** Add `vivaldi_coord: dict | None` field to ping messages:
  - `position: list[float]`
  - `height: float`
  - `error: float`

- [ ] **12.2.2** Add `vivaldi_coord: dict | None` field to ack messages

- [ ] **12.2.3** Add `rtt_ms: float | None` field to ack messages (measured RTT)

**File**: `hyperscale/distributed_rewrite/swim/health_aware_server.py`

- [ ] **12.2.4** Add `_vivaldi_system: VivaldiCoordinateSystem` field

- [ ] **12.2.5** Initialize `VivaldiCoordinateSystem` in `__init__`

- [ ] **12.2.6** Update ping handler to include local Vivaldi coordinate

- [ ] **12.2.7** Update ack handler to:
  - Include local Vivaldi coordinate
  - Include measured RTT
  - Call `_vivaldi_system.update_coordinate()` with peer coord and RTT

- [ ] **12.2.8** Update ping sender to record send timestamp for RTT measurement

- [ ] **12.2.9** Update ack receiver to calculate RTT and call `update_coordinate()`

### 12.3 UNCONFIRMED Lifecycle State

**File**: `hyperscale/distributed_rewrite/swim/core/incarnation_tracker.py`

- [ ] **12.3.1** Add `UNCONFIRMED = b"UNCONFIRMED"` to `NodeLifecycleState` enum

- [ ] **12.3.2** Update state transition validation:
  - `UNCONFIRMED` → `ALIVE` (on first successful bidirectional communication)
  - `UNCONFIRMED` → Removed (on role-aware timeout)
  - `UNCONFIRMED` cannot transition to `SUSPECT` or `DEAD` (AD-29 compliance)

- [ ] **12.3.3** Add `get_nodes_by_state(state) -> list[NodeAddress]`

- [ ] **12.3.4** Add `get_last_update_time(node) -> float`

- [ ] **12.3.5** Add `remove_node(node)` for unconfirmed peer cleanup

**File**: `hyperscale/distributed_rewrite/swim/health_aware_server.py`

- [ ] **12.3.6** Update `_handle_gossip_discovery()`:
  - Mark new peers as `UNCONFIRMED` instead of `ALIVE`
  - Start role-aware confirmation timer

- [ ] **12.3.7** Update `_on_ack_received()`:
  - Transition peer from `UNCONFIRMED` to `ALIVE` on first ack
  - Cancel confirmation timer
  - Call registered confirmation callbacks

- [ ] **12.3.8** Add `_unconfirmed_peers: dict[NodeAddress, float]` (peer → discovered_at)

- [ ] **12.3.9** Add `_unconfirmed_peer_timers: dict[NodeAddress, str]` (peer → timer_token)

### 12.4 Role Classification

**File**: `hyperscale/distributed_rewrite/swim/roles/peer_role.py` (NEW)

- [ ] **12.4.1** Implement `PeerRole` enum:
  - `GATE = "gate"`
  - `MANAGER = "manager"`
  - `WORKER = "worker"`

- [ ] **12.4.2** Implement `detect_peer_role(node, gossip_data) -> PeerRole`:
  - Check explicit role in gossip data
  - Fall back to port range detection
  - Fall back to hostname pattern detection
  - Default to WORKER

**File**: `hyperscale/distributed_rewrite/swim/roles/confirmation_strategy.py` (NEW)

- [ ] **12.4.3** Implement `RoleBasedConfirmationStrategy` dataclass:
  - `passive_timeout: float`
  - `enable_proactive_confirmation: bool`
  - `confirmation_attempts: int`
  - `attempt_interval: float`
  - `latency_aware: bool`
  - `use_vivaldi: bool`
  - `load_multiplier_max: float`

- [ ] **12.4.4** Define strategy constants:
  - `GATE_STRATEGY`: passive_timeout=120s, proactive=True, attempts=5, vivaldi=True, load_max=3x
  - `MANAGER_STRATEGY`: passive_timeout=90s, proactive=True, attempts=3, vivaldi=True, load_max=5x
  - `WORKER_STRATEGY`: passive_timeout=180s, proactive=False, vivaldi=False, load_max=10x

- [ ] **12.4.5** Implement `get_strategy_for_role(role: PeerRole) -> RoleBasedConfirmationStrategy`

### 12.5 Role-Aware Confirmation Manager

**File**: `hyperscale/distributed_rewrite/swim/roles/confirmation_manager.py` (NEW)

- [ ] **12.5.1** Implement `RoleAwareConfirmationManager` class:
  - `_server: HealthAwareServer`
  - `_vivaldi: VivaldiCoordinateSystem`
  - `_pending_confirmations: dict[NodeAddress, ConfirmationState]`
  - `_lock: asyncio.Lock`
  - `_task_runner: TaskRunner`

- [ ] **12.5.2** Implement `ConfirmationState` dataclass:
  - `peer: NodeAddress`
  - `role: PeerRole`
  - `strategy: RoleBasedConfirmationStrategy`
  - `discovered_at: float`
  - `attempts: int = 0`
  - `last_attempt_at: float = 0.0`
  - `timer_token: str | None = None`

- [ ] **12.5.3** Implement `start_confirmation(peer, role)`:
  - Get strategy for role
  - Create `ConfirmationState`
  - Schedule passive timeout timer
  - If proactive enabled, schedule first probe

- [ ] **12.5.4** Implement `cancel_confirmation(peer)`:
  - Cancel any pending timers
  - Remove from `_pending_confirmations`

- [ ] **12.5.5** Implement `_handle_passive_timeout(peer)`:
  - Check if proactive confirmation enabled for role
  - If yes, start proactive confirmation attempts
  - If no, remove peer from membership

- [ ] **12.5.6** Implement `_attempt_proactive_confirmation(peer)`:
  - Send confirmation ping
  - Wait for ack (timeout = adaptive timeout)
  - If ack received, confirm peer
  - If no ack, increment attempts
  - If attempts exhausted, remove peer

- [ ] **12.5.7** Implement `_remove_unconfirmed_peer(peer)`:
  - Remove from membership (NOT marked as DEAD)
  - Emit metrics
  - Log audit event

- [ ] **12.5.8** Implement `get_adaptive_timeout(peer, base_timeout) -> float`:
  - Get estimated RTT from Vivaldi
  - Calculate latency multiplier
  - Get LHM load multiplier
  - Calculate confidence adjustment
  - Return `base * latency * load * confidence`

### 12.6 Adaptive Timeout Integration

**File**: `hyperscale/distributed_rewrite/swim/health_aware_server.py`

- [ ] **12.6.1** Add `_confirmation_manager: RoleAwareConfirmationManager` field

- [ ] **12.6.2** Initialize confirmation manager in `__init__`

- [ ] **12.6.3** Update `start_suspicion()` to use adaptive timeout:
  - Get peer role
  - Calculate adaptive timeout via Vivaldi
  - Pass adaptive timeout to hierarchical detector

- [ ] **12.6.4** Update probe timeout calculation:
  - Use `_vivaldi_system.estimate_rtt()` for peer-specific timeouts
  - Apply LHM multiplier
  - Apply confidence adjustment

- [ ] **12.6.5** Add method to get adaptive suspicion timeout for peer:
  - Combine Vivaldi RTT, LHM, confidence
  - Respect role-specific limits

### 12.7 HealthAwareServer Integration

**File**: `hyperscale/distributed_rewrite/swim/health_aware_server.py`

- [ ] **12.7.1** Add `get_vivaldi_coordinate() -> VivaldiCoordinate`

- [ ] **12.7.2** Add `get_peer_vivaldi_coordinate(peer) -> VivaldiCoordinate | None`

- [ ] **12.7.3** Add `estimate_peer_rtt(peer) -> float`

- [ ] **12.7.4** Add `estimate_peer_rtt_ucb(peer) -> float`

- [ ] **12.7.5** Add `is_vivaldi_converged() -> bool`

- [ ] **12.7.6** Update `_run_cleanup()` to include:
  - Stale unconfirmed peer warning (> 60s)
  - Metrics for long-lived unconfirmed peers

### 12.8 Metrics and Observability

- [ ] **12.8.1** Add Vivaldi metrics:
  - `vivaldi_coordinate_updates` (counter)
  - `vivaldi_prediction_error` (histogram)
  - `vivaldi_convergence_time` (histogram)
  - `vivaldi_coord_quality{peer}` (gauge)
  - `vivaldi_rtt_ucb_ms{peer}` (gauge)

- [ ] **12.8.2** Add role-aware confirmation metrics:
  - `unconfirmed_peers_removed_gate` (counter)
  - `unconfirmed_peers_removed_manager` (counter)
  - `unconfirmed_peers_removed_worker` (counter)
  - `confirmation_attempts_total{role}` (counter)
  - `confirmation_attempts_success` (counter)
  - `peer_confirmation_attempts_total{role}` (counter)
  - `unconfirmed_cleanup_total{role,reason}` (counter)

- [ ] **12.8.3** Add lifecycle state metrics:
  - `peers_unconfirmed` (gauge)
  - `peers_alive` (gauge)
  - `peers_suspect` (gauge)
  - `peers_dead` (gauge)
  - `transitions_unconfirmed_to_alive` (counter)
  - `transitions_unconfirmed_to_removed` (counter)

- [ ] **12.8.4** Add adaptive timeout metrics:
  - `adaptive_timeout_applied` (histogram)
  - `latency_multiplier` (histogram)
  - `load_multiplier` (histogram)
  - `confidence_adjustment` (histogram)
  - `adaptive_timeout_seconds{role}` (gauge)

- [ ] **12.8.5** Add debug endpoints:
  - `GET /debug/vivaldi/coordinate` - local coordinate info
  - `GET /debug/vivaldi/peers` - peer coordinates and RTT estimates
  - `GET /debug/peers/unconfirmed` - unconfirmed peer status

- [ ] **12.8.6** Add structured logging:
  - `RoleConfirmationAttempt` with role, attempts, outcome
  - `PeerConfirmed` with RTT, error, samples
  - `PeerUnconfirmedCleanup` with reason, elapsed

### 12.9 Configuration

**File**: `hyperscale/distributed_rewrite/env/env.py`

- [ ] **12.9.1** Add Vivaldi configuration:
  - `VIVALDI_DIMENSIONS: int = 4`
  - `VIVALDI_CONVERGENCE_THRESHOLD: float = 0.15`
  - `VIVALDI_K_SIGMA: float = 2.0`
  - `VIVALDI_MIN_SAMPLES_FOR_ROUTING: int = 5`

- [ ] **12.9.2** Add role-aware confirmation configuration:
  - `GATE_PASSIVE_TIMEOUT: float = 120.0`
  - `GATE_CONFIRMATION_ATTEMPTS: int = 5`
  - `MANAGER_PASSIVE_TIMEOUT: float = 90.0`
  - `MANAGER_CONFIRMATION_ATTEMPTS: int = 3`
  - `WORKER_PASSIVE_TIMEOUT: float = 180.0`

- [ ] **12.9.3** Add adaptive timeout configuration:
  - `REFERENCE_RTT_MS: float = 10.0`
  - `MAX_LATENCY_MULTIPLIER: float = 10.0`

### 12.10 Testing

**File**: `tests/integration/test_vivaldi.py` (NEW)

- [ ] **12.10.1** Test Vivaldi coordinate convergence

- [ ] **12.10.2** Test RTT prediction accuracy

- [ ] **12.10.3** Test coordinate update on ping/ack

- [ ] **12.10.4** Test UCB calculation with confidence

**File**: `tests/integration/test_role_aware_confirmation.py` (NEW)

- [ ] **12.10.5** Test gate proactive confirmation (5 attempts)

- [ ] **12.10.6** Test manager proactive confirmation (3 attempts)

- [ ] **12.10.7** Test worker passive-only confirmation

- [ ] **12.10.8** Test UNCONFIRMED → ALIVE transition

- [ ] **12.10.9** Test UNCONFIRMED → Removed transition

- [ ] **12.10.10** Test adaptive timeout calculation

- [ ] **12.10.11** Test role detection from gossip

- [ ] **12.10.12** Test AD-29 compliance (no SUSPECT for unconfirmed)

---

## 13. AD-36: Vivaldi-Based Cross-Datacenter Job Routing

**Status**: Architecture Complete, Implementation Pending

**Overview**: Implement Vivaldi-based multi-factor job routing at gates, maintaining AD-17 health bucket safety while optimizing for latency and load within buckets.

### 13.1 Routing Inputs and State

**File**: `hyperscale/distributed_rewrite/routing/routing_state.py` (NEW)

- [ ] **13.1.1** Implement `DatacenterRoutingState` dataclass:
  - `datacenter_id: str`
  - `health_bucket: HealthBucket` (HEALTHY/BUSY/DEGRADED/UNHEALTHY)
  - `available_cores: int`
  - `total_cores: int`
  - `queue_depth: int`
  - `lhm_multiplier: float`
  - `open_circuit_managers: int`
  - `total_managers: int`
  - `leader_coordinate: VivaldiCoordinate | None`
  - `coordinate_updated_at: float`
  - `heartbeat_updated_at: float`

- [ ] **13.1.2** Implement `ManagerRoutingState` dataclass:
  - `manager_id: str`
  - `host: str`
  - `port: int`
  - `circuit_state: CircuitState`
  - `available_cores: int`
  - `queue_depth: int`
  - `coordinate: VivaldiCoordinate | None`
  - `last_heartbeat: float`

- [ ] **13.1.3** Implement `RoutingDecision` dataclass:
  - `job_id: str`
  - `primary_dcs: list[str]`
  - `fallback_dcs: list[str]`
  - `scores: dict[str, float]`
  - `timestamp: float`

### 13.2 Candidate Filtering

**File**: `hyperscale/distributed_rewrite/routing/candidate_filter.py` (NEW)

- [ ] **13.2.1** Implement `filter_datacenters(dcs) -> list[DatacenterRoutingState]`:
  - Exclude `UNHEALTHY` status
  - Exclude DCs with no registered managers
  - Exclude DCs with all managers circuit-open

- [ ] **13.2.2** Implement `filter_managers(managers) -> list[ManagerRoutingState]`:
  - Exclude circuit-open managers
  - Exclude stale heartbeat managers

- [ ] **13.2.3** Implement `apply_soft_demotions(dcs) -> list[DatacenterRoutingState]`:
  - Stale health → treat as DEGRADED
  - Missing coordinates → keep but apply conservative RTT defaults

### 13.3 Bucket Selection (AD-17 Preserved)

**File**: `hyperscale/distributed_rewrite/routing/bucket_selector.py` (NEW)

- [ ] **13.3.1** Implement `select_primary_bucket(dcs) -> HealthBucket`:
  - Return first non-empty bucket: HEALTHY > BUSY > DEGRADED
  - Never route to UNHEALTHY

- [ ] **13.3.2** Implement `get_dcs_in_bucket(dcs, bucket) -> list[DatacenterRoutingState]`:
  - Filter DCs matching the specified bucket

- [ ] **13.3.3** Ensure health ordering is never violated by RTT scoring

### 13.4 Scoring Function

**File**: `hyperscale/distributed_rewrite/routing/scoring.py` (NEW)

- [ ] **13.4.1** Implement `calculate_rtt_ucb(local_coord, dc_leader_coord) -> float`:
  - Use `estimate_rtt_ucb_ms()` from AD-35
  - Clamp to `[RTT_MIN_MS, RTT_MAX_MS]`

- [ ] **13.4.2** Implement `calculate_load_factor(dc) -> float`:
  - `util = 1.0 - clamp01(available_cores / total_cores)`
  - `queue = queue_depth / (queue_depth + QUEUE_SMOOTHING)`
  - `cb = open_managers / total_managers`
  - `load_factor = 1.0 + A_UTIL * util + A_QUEUE * queue + A_CB * cb`
  - Clamp to `LOAD_FACTOR_MAX`

- [ ] **13.4.3** Implement `calculate_quality_penalty(dc) -> float`:
  - `quality = coordinate_quality(sample_count, error_ms, staleness_s)`
  - `quality_penalty = 1.0 + A_QUALITY * (1.0 - quality)`
  - Clamp to `QUALITY_PENALTY_MAX`

- [ ] **13.4.4** Implement `calculate_score(dc, local_coord) -> float`:
  - `score = rtt_ucb * load_factor * quality_penalty`

- [ ] **13.4.5** Implement `apply_preference_multiplier(score, dc, preferred_dcs) -> float`:
  - If `dc in preferred_dcs`: `score *= PREFERENCE_MULT`
  - Apply within primary bucket only

- [ ] **13.4.6** Define scoring constants:
  - `A_UTIL = 2.0`
  - `A_QUEUE = 1.0`
  - `A_CB = 3.0`
  - `A_QUALITY = 0.5`
  - `QUEUE_SMOOTHING = 10.0`
  - `LOAD_FACTOR_MAX = 5.0`
  - `QUALITY_PENALTY_MAX = 2.0`
  - `PREFERENCE_MULT = 0.8`

### 13.5 Hysteresis and Stickiness

**File**: `hyperscale/distributed_rewrite/routing/hysteresis.py` (NEW)

- [ ] **13.5.1** Implement `HysteresisState` dataclass:
  - `current_primary_dc: str | None`
  - `selected_at: float`
  - `score_at_selection: float`
  - `cooldowns: dict[str, float]` (dc → cooldown_expires_at)

- [ ] **13.5.2** Implement `should_switch_primary(current, new_best, scores) -> bool`:
  - Return False if within hold-down period
  - Return True if current DC dropped bucket or excluded
  - Return True if score degraded by `DEGRADE_RATIO` for `DEGRADE_CONFIRM_S`
  - Return True if new best improves by `IMPROVEMENT_RATIO`
  - Return False otherwise

- [ ] **13.5.3** Implement `apply_cooldown(dc)`:
  - Add DC to cooldowns with expiration time

- [ ] **13.5.4** Implement `is_cooled_down(dc) -> bool`:
  - Check if DC cooldown has expired

- [ ] **13.5.5** Implement `get_cooldown_penalty(dc) -> float`:
  - Return penalty multiplier if in cooldown

- [ ] **13.5.6** Define hysteresis constants:
  - `HOLD_DOWN_S = 30.0`
  - `IMPROVEMENT_RATIO = 0.8` (20% improvement required)
  - `DEGRADE_RATIO = 1.5` (50% degradation)
  - `DEGRADE_CONFIRM_S = 60.0`
  - `COOLDOWN_S = 120.0`

### 13.6 Bootstrapping and Convergence

**File**: `hyperscale/distributed_rewrite/routing/bootstrap.py` (NEW)

- [ ] **13.6.1** Implement `is_coordinate_aware_mode(local_coord) -> bool`:
  - Check `sample_count >= MIN_SAMPLES_FOR_ROUTING`
  - Check `error_ms <= ERROR_MAX_FOR_ROUTING`

- [ ] **13.6.2** Implement `rank_without_coordinates(dcs) -> list[str]`:
  - Rank by capacity (available_cores)
  - Then by queue depth
  - Then by circuit pressure

- [ ] **13.6.3** Implement `get_bootstrap_score(dc) -> float`:
  - Score using only capacity, queue, circuit state
  - No RTT component

### 13.7 Fallback Chain Construction

**File**: `hyperscale/distributed_rewrite/routing/fallback_chain.py` (NEW)

- [ ] **13.7.1** Implement `build_fallback_chain(dcs, scores, primary_bucket) -> list[str]`:
  - Select primary_dcs from primary_bucket by score (with hysteresis)
  - Add remaining DCs from primary_bucket as fallback
  - Append BUSY bucket DCs by score
  - Append DEGRADED bucket DCs by score
  - Return ordered list

- [ ] **13.7.2** Implement `get_next_fallback(chain, failed_dcs) -> str | None`:
  - Return first DC in chain not in failed_dcs

### 13.8 Manager Selection Within Datacenter

**File**: `hyperscale/distributed_rewrite/routing/manager_selection.py` (NEW)

- [ ] **13.8.1** Implement `select_manager(dc, managers, local_coord) -> ManagerRoutingState | None`:
  - Filter out circuit-open and stale managers
  - Score by RTT UCB + manager load + quality penalty
  - Apply per-job stickiness

- [ ] **13.8.2** Implement `get_manager_score(manager, local_coord) -> float`:
  - RTT UCB to manager
  - Load factor from queue_depth and available_cores
  - Quality penalty from coordinate quality

### 13.9 GateJobRouter Implementation

**File**: `hyperscale/distributed_rewrite/routing/gate_job_router.py` (NEW)

- [ ] **13.9.1** Implement `GateJobRouter` class:
  - `_gate: GateServer`
  - `_vivaldi: VivaldiCoordinateSystem`
  - `_dc_states: dict[str, DatacenterRoutingState]`
  - `_hysteresis: dict[str, HysteresisState]` (per job_id or per routing context)
  - `_lock: asyncio.Lock`

- [ ] **13.9.2** Implement `route_job(job_id, preferred_dcs) -> RoutingDecision`:
  - Filter candidates
  - Select primary bucket
  - Score candidates within bucket
  - Apply hysteresis
  - Build fallback chain
  - Return decision

- [ ] **13.9.3** Implement `update_dc_state(dc_id, state)`:
  - Update `_dc_states[dc_id]`
  - Trigger re-evaluation if needed

- [ ] **13.9.4** Implement `record_dispatch_failure(dc_id, job_id)`:
  - Apply cooldown
  - Update metrics

- [ ] **13.9.5** Implement `record_dispatch_success(dc_id, job_id)`:
  - Clear cooldown
  - Update metrics

### 13.10 Gate Integration

**File**: `hyperscale/distributed_rewrite/nodes/gate.py`

- [ ] **13.10.1** Add `_job_router: GateJobRouter` field

- [ ] **13.10.2** Initialize `GateJobRouter` in `__init__`

- [ ] **13.10.3** Update job submission path to use `_job_router.route_job()`

- [ ] **13.10.4** Update manager heartbeat handling to call `_job_router.update_dc_state()`

- [ ] **13.10.5** Update dispatch failure handling to call `_job_router.record_dispatch_failure()`

- [ ] **13.10.6** Update dispatch success handling to call `_job_router.record_dispatch_success()`

### 13.11 Metrics and Observability

- [ ] **13.11.1** Add routing decision metrics:
  - `routing_decisions_total{bucket,reason}` (counter)
  - `routing_score{dc_id}` (gauge)
  - `routing_score_component{dc_id,component}` (gauge)
  - `routing_switch_total{reason}` (counter)
  - `routing_hold_down_blocks_total` (counter)
  - `routing_fallback_used_total{from_dc,to_dc}` (counter)

- [ ] **13.11.2** Add structured logging:
  - `RoutingDecision` with candidate list and score components
  - `RoutingSwitch` with old/new DC and improvement ratio
  - `RoutingCooldown` when DC fails dispatch

### 13.12 Configuration

**File**: `hyperscale/distributed_rewrite/env/env.py`

- [ ] **13.12.1** Add routing configuration:
  - `ROUTING_HOLD_DOWN_S: float = 30.0`
  - `ROUTING_IMPROVEMENT_RATIO: float = 0.8`
  - `ROUTING_DEGRADE_RATIO: float = 1.5`
  - `ROUTING_DEGRADE_CONFIRM_S: float = 60.0`
  - `ROUTING_COOLDOWN_S: float = 120.0`

- [ ] **13.12.2** Add scoring configuration:
  - `ROUTING_A_UTIL: float = 2.0`
  - `ROUTING_A_QUEUE: float = 1.0`
  - `ROUTING_A_CB: float = 3.0`
  - `ROUTING_A_QUALITY: float = 0.5`
  - `ROUTING_QUEUE_SMOOTHING: float = 10.0`
  - `ROUTING_LOAD_FACTOR_MAX: float = 5.0`
  - `ROUTING_QUALITY_PENALTY_MAX: float = 2.0`
  - `ROUTING_PREFERENCE_MULT: float = 0.8`

### 13.13 Testing

**File**: `tests/integration/test_vivaldi_routing.py` (NEW)

- [ ] **13.13.1** Test routing respects AD-17 health buckets

- [ ] **13.13.2** Test RTT UCB scoring within bucket

- [ ] **13.13.3** Test load factor calculation

- [ ] **13.13.4** Test quality penalty for stale coordinates

- [ ] **13.13.5** Test hysteresis prevents oscillation

- [ ] **13.13.6** Test cooldown after dispatch failure

- [ ] **13.13.7** Test bootstrap mode without coordinates

- [ ] **13.13.8** Test fallback chain construction

- [ ] **13.13.9** Test manager selection within DC

- [ ] **13.13.10** Test preferred DC multiplier

---

## Appendix: Dependencies

### AD-34 Dependencies
- AD-26 (Healthcheck Extensions) - extension tracking integration
- AD-33 (Workflow State Machine) - progress tracking integration
- Existing job leadership transfer mechanisms

### AD-35 Dependencies
- AD-29 (Peer Confirmation) - UNCONFIRMED state compliance
- AD-30 (Hierarchical Failure Detection) - adaptive timeout integration
- Existing SWIM protocol implementation

### AD-36 Dependencies
- AD-35 (Vivaldi Coordinates) - RTT estimation
- AD-17 (Datacenter Health Classification) - bucket selection
- AD-33 (Federated Health Monitoring) - DC health signals

---

## Notes

- All changes must be asyncio-safe (use locks where needed)
- Follow existing patterns (TaskRunner for background tasks, structured logging)
- Fencing tokens must be respected to prevent stale operations
- Memory cleanup is critical - track and clean up orphaned state
- Vivaldi coordinates piggyback on existing SWIM messages (50-80 byte overhead)
- Role-aware strategies never probe workers (protect from load)
- Routing decisions never violate AD-17 health bucket ordering
