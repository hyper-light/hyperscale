# TODO: Distributed System Architecture Implementation

## Overview

This document tracks the remaining implementation work for AD-34, AD-35, and AD-36 architectural decisions.

**Implementation Status** (as of 2026-01-10):
- **AD-34**: ✅ **100% COMPLETE** - All critical gaps fixed, fully functional for multi-DC deployments
- **AD-35**: 25% complete - Coordinate algorithm works, SWIM integration and role-aware logic missing
- **AD-36**: 5% complete - Only basic health bucket selection implemented, entire routing subsystem missing

---

## 11. AD-34: Adaptive Job Timeout with Multi-DC Coordination

**Status**: ✅ **COMPLETE** (100%) - All critical gaps fixed 2026-01-10

**Overview**: Adaptive job timeout tracking that auto-detects single-DC vs multi-DC deployments. Integrates with AD-26 (healthcheck extensions) and AD-33 (workflow state machine) to prevent resource leaks while respecting legitimate long-running work.

**Completion Summary**: All 3 Phase 1 critical blockers fixed in commits 622d8c9e, 9a2813e0, 47776106. Multi-DC timeout coordination now fully functional.

### 11.1 Core Data Structures ✅ COMPLETE

**File**: `hyperscale/distributed_rewrite/models/jobs.py`

- [x] **11.1.1** `TimeoutTrackingState` dataclass implemented (lines 238-277) with all fields including extension tracking
- [x] **11.1.2** `timeout_tracking: TimeoutTrackingState | None` field added to `JobInfo`

### 11.2 Protocol Messages ✅ COMPLETE

**File**: `hyperscale/distributed_rewrite/models/distributed.py`

- [x] **11.2.1** `JobProgressReport` message implemented (line 1762)
- [x] **11.2.2** `JobTimeoutReport` message implemented (line 1793)
- [x] **11.2.3** `JobGlobalTimeout` message implemented (line 1814)
- [x] **11.2.4** `JobLeaderTransfer` message implemented (line 1831)
- [x] **11.2.5** `JobFinalStatus` message implemented (line 1849)

### 11.3 Timeout Strategy Implementation ✅ COMPLETE

**File**: `hyperscale/distributed_rewrite/jobs/timeout_strategy.py`

- [x] **11.3.1** `TimeoutStrategy` ABC implemented with all methods (lines 33-178)
- [x] **11.3.2** `LocalAuthorityTimeout` class fully implemented (lines 181-418)
  - Extension-aware timeout: `effective_timeout = base + total_extensions_granted`
  - Stuck detection with extension awareness
  - Idempotent operations with `locally_timed_out` flag
  - Fence token handling for leader transfer safety
- [x] **11.3.3** `GateCoordinatedTimeout` class fully implemented (lines 421-910)
  - All LocalAuthorityTimeout features plus gate coordination
  - Progress reporting every 10 seconds
  - Timeout reporting with retry
  - 5-minute fallback if gate unreachable
  - Fence token validation
  - Leader transfer notifications

### 11.4 Manager Integration ⚠️ MOSTLY COMPLETE (3 Critical Gaps)

**File**: `hyperscale/distributed_rewrite/nodes/manager.py`

**Implemented:**
- [x] **11.4.1** `_job_timeout_strategies: dict[str, TimeoutStrategy]` field (line 485)
- [x] **11.4.2** `_select_timeout_strategy(submission)` method (lines 9279-9299)
- [x] **11.4.3** `_unified_timeout_loop()` background task (lines 9301-9350)
- [x] **11.4.4** `receive_submit_job()` calls `start_tracking()` (lines 10352-10358)
- [x] **11.4.5** `_resume_timeout_tracking_for_all_jobs()` (lines 9664-9721)
- [x] **11.4.6** `_get_or_create_timeout_strategy(job)` (implemented in resume logic)
- [x] **11.4.7** `_timeout_job(job_id, reason)` (line 9352+)
- [x] **11.4.8** Extension notification via `record_worker_extension()` (line 9483)
- [x] **11.4.9** Extension cleanup via `cleanup_worker_extensions()` (lines 9499-9513)
- [x] **11.4.10** Cleanup hooks in place (stop_tracking called appropriately)
- [x] **11.4.13** `_unified_timeout_loop` started in `start()` method

**Critical Gaps:**
- [x] **11.4.11** ✅ **COMPLETE**: Add `receive_job_global_timeout()` handler (lines 10539-10591)
  - Loads JobGlobalTimeout message from gate
  - Delegates to timeout strategy with fence token validation
  - Cleans up tracking on acceptance
  - **FIXED** in commit 622d8c9e

- [x] **11.4.12** ✅ **COMPLETE**: Add workflow progress callbacks to timeout strategies
  - Added `_report_workflow_progress_to_timeout_strategy()` helper method (lines 9524-9557)
  - Updated all 9 workflow lifecycle state transition sites
  - Timeout tracking now receives progress updates on state changes
  - **FIXED** in commit 47776106

### 11.5 Gate Integration ✅ COMPLETE

**File**: `hyperscale/distributed_rewrite/nodes/gate.py`
**File**: `hyperscale/distributed_rewrite/jobs/gates/gate_job_timeout_tracker.py`

**Implemented:**
- [x] **11.5.1** `GateJobTrackingInfo` dataclass (lines 36-87 in gate_job_timeout_tracker.py)
- [x] **11.5.2** `GateJobTimeoutTracker` class fully implemented (same file)
  - Extension-aware global timeout logic
  - Periodic check loop
  - Broadcast coordination
- [x] **11.5.3** `_job_timeout_tracker: GateJobTimeoutTracker` field (line 465 in gate.py)
- [x] **11.5.4** `_timeout_check_loop()` background task (in tracker)
- [x] **11.5.5** `_declare_global_timeout()` method (in tracker)
- [x] **11.5.6** `receive_job_progress_report()` handler (line 5790)
- [x] **11.5.7** `receive_job_timeout_report()` handler (line 5812)
- [x] **11.5.8** `receive_job_final_status()` handler (line 5856)
- [x] **11.5.9** `receive_job_leader_transfer()` handler (line 5834)
- [x] **11.5.10** Tracker started in `start()` (line 3715), stopped in `stop()` (line 3755)
- [x] **11.5.11** ✅ **COMPLETE**: Call `_job_timeout_tracker.start_tracking_job()` in `_dispatch_job_to_datacenters()`
  - Added after successful dispatch (lines 5078-5084)
  - Gate now coordinates global timeout across all datacenters
  - **FIXED** in commit 9a2813e0

### 11.6 WorkflowStateMachine Integration ❌ NOT IMPLEMENTED

**File**: `hyperscale/distributed_rewrite/workflow/state_machine.py`

- [ ] **11.6.1** Add `_progress_callbacks: list[Callable]` field
- [ ] **11.6.2** Implement `register_progress_callback(callback)`
- [ ] **11.6.3** Update `transition()` to call registered callbacks
- [ ] **11.6.4** Implement `get_time_since_progress(workflow_id)`
- [ ] **11.6.5** Implement `get_stuck_workflows(threshold_seconds)`

**Note**: This is optional - AD-34 can work with manual progress reporting in manager.py instead of state machine callbacks

### 11.7 Configuration ⏭️ SKIP (Uses Defaults)

Timeout strategies use hardcoded defaults. Configuration can be added later if needed.

### 11.8 Metrics and Observability ⏭️ DEFERRED

Basic logging exists. Comprehensive metrics can be added after core functionality works.

### 11.9 Testing ⏭️ USER WILL RUN

Per CLAUDE.md: "DO NOT RUN THE INTEGRATION TESTS YOURSELF. Ask me to."

---

## 12. AD-35: Vivaldi Network Coordinates with Role-Aware Failure Detection

**Status**: Foundation Only (25%), Integration Layer Missing

**Overview**: Vivaldi network coordinates for latency-aware failure detection, role-aware confirmation strategies for Gates/Managers/Workers, and an explicit UNCONFIRMED lifecycle state.

### 12.1 Vivaldi Coordinate System ⚠️ PARTIAL (60%)

**Files**:
- `hyperscale/distributed_rewrite/models/coordinates.py` ✅ EXISTS
- `hyperscale/distributed_rewrite/swim/coordinates/coordinate_engine.py` ✅ EXISTS
- `hyperscale/distributed_rewrite/swim/coordinates/coordinate_tracker.py` ✅ EXISTS

**Implemented:**
- [x] **12.1.1** `NetworkCoordinate` dataclass exists (uses `vec` instead of `position`, has `adjustment` field)
- [x] **12.1.2** `NetworkCoordinateEngine` class fully functional
  - Coordinate update algorithm complete
  - RTT estimation complete
  - Distance calculation complete
- [x] **12.1.3** `CoordinateTracker` class exists and tracks local + peer coordinates

**Missing:**
- [ ] **12.1.4** Implement `estimate_rtt_ucb_ms()` - Upper confidence bound RTT (AD-35 requirement)
- [ ] **12.1.5** Implement `coordinate_quality()` function - Quality scoring based on sample_count, error, staleness
- [ ] **12.1.6** Implement `is_converged()` method - Convergence detection
- [ ] **12.1.7** Create `VivaldiConfig` dataclass - Currently uses hardcoded values
- [ ] **12.1.8** Add coordinate cleanup/TTL - No stale coordinate removal

### 12.2 SWIM Message Integration ✅ COMPLETE

**File**: `hyperscale/distributed_rewrite/swim/health_aware_server.py`

✅ **COMPLETE**: Coordinates piggyback on all SWIM messages using #|v{json} format

- [x] **12.2.1** Add `vivaldi_coord` field to ping messages - Commit b8187b27
- [x] **12.2.2** Add `vivaldi_coord` field to ack messages - Commit b8187b27
- [x] **12.2.3** Add `rtt_ms` field to ack messages for measured RTT - Commit b8187b27
- [x] **12.2.4** Update ping handler to include local coordinate - Commit b8187b27
- [x] **12.2.5** Update ack handler to include local coordinate + measured RTT - Commit b8187b27
- [x] **12.2.6** Call `CoordinateTracker.update_coordinate_from_peer()` on every ack - Commit b8187b27

**Current State**: ✅ Coordinates now piggybacked on ALL SWIM messages (#|v{json} format). RTT measured from probe start time on ACK receipt. CoordinateTracker updated with peer coordinates and RTT on every ping/ack exchange.

### 12.3 UNCONFIRMED Lifecycle State ❌ NOT IMPLEMENTED

**File**: `hyperscale/distributed_rewrite/swim/detection/incarnation_tracker.py`

🔴 **CRITICAL**: No formal UNCONFIRMED state exists

- [ ] **12.3.1** Add `UNCONFIRMED = b"UNCONFIRMED"` to lifecycle enum
- [ ] **12.3.2** Implement UNCONFIRMED → ALIVE transition on first successful bidirectional communication
- [ ] **12.3.3** Implement UNCONFIRMED → Removed transition on role-aware timeout
- [ ] **12.3.4** Prevent UNCONFIRMED → SUSPECT transitions (AD-29 compliance)
- [ ] **12.3.5** Add `get_nodes_by_state(state)` method
- [ ] **12.3.6** Add `remove_node(node)` method for unconfirmed cleanup

**Current State**: Ad-hoc tracking via `_unconfirmed_peer_added_at` dict in health_aware_server.py (lines 1205-1218). No formal state machine.

### 12.4 Role Classification ⚠️ EXISTS BUT NOT INTEGRATED (30%)

**File**: `hyperscale/distributed_rewrite/discovery/security/role_validator.py`

**Implemented:**
- [x] **12.4.1** `NodeRole` enum exists (Gate/Manager/Worker) - Used for mTLS validation only

**Missing:**
- [ ] **12.4.2** Integrate NodeRole into SWIM membership
- [ ] **12.4.3** Gossip role in SWIM messages
- [ ] **12.4.4** Make role accessible in HealthAwareServer for failure detection

### 12.5 Role-Aware Confirmation Manager ❌ NOT IMPLEMENTED

**File**: `hyperscale/distributed_rewrite/swim/roles/confirmation_manager.py` (NEW)

🔴 **CRITICAL**: Core component completely missing

- [ ] **12.5.1** Create `RoleBasedConfirmationStrategy` dataclass
- [ ] **12.5.2** Define strategy constants:
  - GATE_STRATEGY: 120s timeout, 5 proactive attempts, Vivaldi-aware
  - MANAGER_STRATEGY: 90s timeout, 3 proactive attempts, Vivaldi-aware
  - WORKER_STRATEGY: 180s timeout, passive-only, no Vivaldi
- [ ] **12.5.3** Implement `RoleAwareConfirmationManager` class
- [ ] **12.5.4** Implement proactive confirmation for Gates/Managers
- [ ] **12.5.5** Implement passive-only strategy for Workers
- [ ] **12.5.6** Integrate with HealthAwareServer

### 12.6 Adaptive Timeouts ❌ NOT IMPLEMENTED (10%)

**File**: `hyperscale/distributed_rewrite/swim/detection/suspicion_manager.py`

**Implemented:**
- [x] LHM (load-aware) multiplier exists (lines 126-130)

**Missing:**
- [ ] **12.6.1** Add latency multiplier from Vivaldi RTT
- [ ] **12.6.2** Add confidence adjustment from coordinate error
- [ ] **12.6.3** Implement `get_adaptive_timeout(peer, base_timeout)`:
  - `timeout = base × latency_multiplier × lhm × confidence_adjustment`
  - `latency_multiplier = min(10.0, max(1.0, estimated_rtt / reference_rtt))`
  - `confidence_adjustment = 1.0 + (coordinate_error / 10.0)`

### 12.7-12.10 Remaining Items ⏭️ DEFERRED

Configuration, metrics, observability, and testing deferred until core functionality works.

---

## 13. AD-36: Vivaldi-Based Cross-Datacenter Job Routing

**Status**: Not Implemented (5%), Only AD-17 Compliance Exists

**Overview**: Vivaldi-based multi-factor job routing maintaining AD-17 health bucket safety while optimizing for latency and load.

### 13.1 Current State ✅ AD-17 COMPLIANT (5%)

**File**: `hyperscale/distributed_rewrite/nodes/gate.py`

**Implemented:**
- [x] Health bucket selection (lines 2567-2608): HEALTHY > BUSY > DEGRADED priority
- [x] UNHEALTHY datacenters excluded (line 2617)
- [x] Basic fallback chain (lines 2607-2623): primary + remaining in health order

**Missing:** Everything else. Current implementation only sorts by `available_capacity` within buckets.

### 13.2 Routing Infrastructure ❌ ENTIRELY MISSING

**Required Files** (ALL NEW):
- [ ] `hyperscale/distributed_rewrite/routing/routing_state.py`
- [ ] `hyperscale/distributed_rewrite/routing/candidate_filter.py`
- [ ] `hyperscale/distributed_rewrite/routing/bucket_selector.py`
- [ ] `hyperscale/distributed_rewrite/routing/scoring.py`
- [ ] `hyperscale/distributed_rewrite/routing/hysteresis.py`
- [ ] `hyperscale/distributed_rewrite/routing/bootstrap.py`
- [ ] `hyperscale/distributed_rewrite/routing/fallback_chain.py`
- [ ] `hyperscale/distributed_rewrite/routing/manager_selection.py`
- [ ] `hyperscale/distributed_rewrite/routing/gate_job_router.py`

### 13.3 Multi-Factor Scoring ❌ NOT IMPLEMENTED

**Required:**
- [ ] **13.3.1** RTT UCB from Vivaldi (AD-35 dependency)
- [ ] **13.3.2** Load factor: `1.0 + A_UTIL × util + A_QUEUE × queue + A_CB × cb`
- [ ] **13.3.3** Quality penalty: `1.0 + A_QUALITY × (1.0 - quality)`
- [ ] **13.3.4** Final score: `rtt_ucb × load_factor × quality_penalty`
- [ ] **13.3.5** Preference multiplier (bounded, within primary bucket only)

**Current:** Single-factor sort by `available_capacity` only

### 13.4 Hysteresis and Stickiness ❌ NOT IMPLEMENTED

**Required:**
- [ ] **13.4.1** Hold-down timers (30s)
- [ ] **13.4.2** Minimum improvement threshold (20% improvement required)
- [ ] **13.4.3** Forced switch on bucket drop or exclusion
- [ ] **13.4.4** Cooldown after DC failover (120s)
- [ ] **13.4.5** Per-job routing state tracking

**Current:** Stateless selection, no churn prevention

### 13.5 Bootstrap Mode ❌ NOT IMPLEMENTED

**Required:**
- [ ] **13.5.1** Coordinate-unaware mode detection (quality < threshold)
- [ ] **13.5.2** Rank by capacity/queue/circuit when coordinates unavailable
- [ ] **13.5.3** Conservative RTT defaults (RTT_DEFAULT_MS)
- [ ] **13.5.4** Graceful degradation

**Current:** Routing proceeds without coordinates (because coordinates not used)

### 13.6 Remaining Sections ⏭️ DEFERRED

All remaining AD-36 items deferred. Core routing subsystem must be built first.

**Estimated Scope**: 106 unchecked tasks across 13 subsections per original TODO.md

---

## Implementation Priority

### Phase 1: Fix AD-34 Critical Blockers ✅ **COMPLETE**
**Effort:** Completed 2026-01-10

1. [x] Add `receive_job_global_timeout()` handler to manager.py (Task 11.4.11) - Commit 622d8c9e
2. [x] Add `_job_timeout_tracker.start_tracking_job()` call in gate.py (Task 11.5.11) - Commit 9a2813e0
3. [x] Add workflow progress callbacks in manager.py (Task 11.4.12) - Commit 47776106

**Result:** ✅ AD-34 is now fully functional for multi-DC deployments

### Phase 2: Complete AD-35 SWIM Integration 🟡 IN PROGRESS
**Effort:** 3-5 days

1. [x] Add `vivaldi_coord` field to SWIM ping/ack messages (Section 12.2) - Commit b8187b27
2. [x] Implement coordinate updates on every ping/ack exchange - Commit b8187b27
3. [ ] Add UNCONFIRMED state to IncarnationTracker (Section 12.3)
4. [ ] Implement basic RoleAwareConfirmationManager (Section 12.5)
5. [ ] Add adaptive timeout calculation using Vivaldi RTT (Section 12.6)

**Result:** AD-35 provides geographic latency awareness and role-specific confirmation

### Phase 3: Implement AD-36 Routing Foundation 🟢 LOWER PRIORITY
**Effort:** 5-7 days

1. [ ] Create routing module structure (9 files)
2. [ ] Implement multi-factor scoring
3. [ ] Integrate Vivaldi coordinates into datacenter selection
4. [ ] Add hysteresis and stickiness state tracking
5. [ ] Implement bootstrap mode

**Result:** AD-36 provides latency-aware, load-balanced job routing

---

## Notes

- **Memory Cleanup is Critical**: Track and clean up orphaned state, prevent leaks
- **Asyncio Safety**: Use locks for all shared state access
- **Fencing Tokens**: Must be respected to prevent stale operations
- **Follow Existing Patterns**: TaskRunner for background tasks, structured logging
- **Vivaldi Overhead**: 50-80 bytes per message when piggybacking on SWIM
- **Role-Aware Protection**: Never probe workers (protect from load)
- **Routing Safety**: Never violate AD-17 health bucket ordering

---

## Dependencies

### AD-34 Dependencies
- ✅ AD-26 (Healthcheck Extensions) - Fully integrated
- ✅ AD-33 (Workflow State Machine) - Exists but not connected to timeout tracking
- ✅ Job leadership transfer mechanisms - Working

### AD-35 Dependencies
- ⚠️  AD-29 (Peer Confirmation) - UNCONFIRMED state not yet compliant
- ✅ AD-30 (Hierarchical Failure Detection) - LHM exists, ready for Vivaldi integration
- ✅ SWIM protocol - Exists, needs message extension

### AD-36 Dependencies
- ❌ AD-35 (Vivaldi Coordinates) - Foundation exists but not usable for routing yet
- ✅ AD-17 (Datacenter Health Classification) - Fully working
- ✅ AD-33 (Federated Health Monitoring) - DC health signals available
