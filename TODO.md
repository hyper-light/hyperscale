# TODO: Job Leadership Transfer and Cancellation Improvements

## Overview

This document tracks the remaining work for robust job leadership transfer and workflow cancellation when managers fail.

---

## 10. AD-34: Adaptive Job Timeout with Multi-DC Coordination

**Status**: 📝 Architecture Complete, Implementation Pending

**Overview**: Implement adaptive job timeout tracking that auto-detects single-DC vs multi-DC deployments and uses appropriate timeout strategies. Integrates with AD-26 (healthcheck extensions) and AD-33 (workflow state machine) to prevent resource leaks while respecting legitimate long-running work.

**Key Features**:
- Auto-detection via `gate_addr` field in JobSubmission
- LocalAuthorityTimeout (single-DC) and GateCoordinatedTimeout (multi-DC)
- Extension-aware timeout calculation: `effective_timeout = base + extensions`
- State persistence across leader transfers
- Comprehensive cleanup on job completion/failure/cancellation

### 10.1 Core Data Structures

#### 10.1.1 TimeoutTrackingState (Add to JobInfo)

**File**: `hyperscale/distributed_rewrite/models/jobs.py`

- [ ] **10.1.1a** Add `TimeoutTrackingState` dataclass
  ```python
  @dataclass
  class TimeoutTrackingState:
      strategy_type: str  # "local_authority" | "gate_coordinated"
      gate_addr: tuple[str, int] | None

      # Timestamps (absolute, monotonic)
      started_at: float
      last_progress_at: float
      last_report_at: float

      # Timeout configuration
      timeout_seconds: float
      stuck_threshold: float = 120.0

      # Extension tracking (AD-26 integration)
      total_extensions_granted: float = 0.0
      max_worker_extension: float = 0.0
      last_extension_at: float = 0.0
      active_workers_with_extensions: set[str] = field(default_factory=set)

      # State flags
      locally_timed_out: bool = False
      globally_timed_out: bool = False
      timeout_reason: str = ""

      # Fencing (prevent stale decisions)
      timeout_fence_token: int = 0
  ```

- [ ] **10.1.1b** Add `timeout_tracking` field to `JobInfo`
  ```python
  class JobInfo:
      # ... existing fields ...
      timeout_tracking: TimeoutTrackingState | None = None
  ```

### 10.2 Protocol Messages

**File**: `hyperscale/distributed_rewrite/models/distributed.py`

- [ ] **10.2.1** Add `JobProgressReport` message (Manager → Gate)
  ```python
  @dataclass(slots=True)
  class JobProgressReport(Message):
      job_id: str
      datacenter: str
      manager_id: str
      manager_host: str
      manager_port: int
      workflows_total: int
      workflows_completed: int
      workflows_failed: int
      has_recent_progress: bool
      timestamp: float
      fence_token: int
      # Extension tracking
      total_extensions_granted: float = 0.0
      max_worker_extension: float = 0.0
      workers_with_extensions: int = 0
  ```

- [ ] **10.2.2** Add `JobTimeoutReport` message (Manager → Gate)
  ```python
  @dataclass(slots=True)
  class JobTimeoutReport(Message):
      job_id: str
      datacenter: str
      manager_id: str
      manager_host: str
      manager_port: int
      reason: str
      elapsed_seconds: float
      fence_token: int
  ```

- [ ] **10.2.3** Add `JobGlobalTimeout` message (Gate → Manager)
  ```python
  @dataclass(slots=True)
  class JobGlobalTimeout(Message):
      job_id: str
      reason: str
      timed_out_at: float
      fence_token: int
  ```

- [ ] **10.2.4** Add `JobLeaderTransfer` message (Manager → Gate)
  ```python
  @dataclass(slots=True)
  class JobLeaderTransfer(Message):
      job_id: str
      datacenter: str
      new_leader_id: str
      fence_token: int
  ```

- [ ] **10.2.5** Add `JobFinalStatus` message (Manager → Gate)
  ```python
  @dataclass(slots=True)
  class JobFinalStatus(Message):
      job_id: str
      datacenter: str
      manager_id: str
      status: str  # JobStatus.COMPLETED/FAILED/CANCELLED/TIMEOUT
      timestamp: float
      fence_token: int
  ```

### 10.3 Timeout Strategy Implementation

**File**: `hyperscale/distributed_rewrite/jobs/timeout_strategy.py` (NEW)

- [ ] **10.3.1** Create `TimeoutStrategy` ABC
  ```python
  class TimeoutStrategy(ABC):
      @abstractmethod
      async def start_tracking(...) -> None: pass

      @abstractmethod
      async def resume_tracking(job_id: str) -> None: pass

      @abstractmethod
      async def report_progress(job_id: str, progress_type: str) -> None: pass

      @abstractmethod
      async def check_timeout(job_id: str) -> tuple[bool, str]: pass

      @abstractmethod
      async def handle_global_timeout(job_id: str, reason: str, fence_token: int) -> bool: pass

      @abstractmethod
      async def record_worker_extension(job_id: str, worker_id: str, extension_seconds: float, worker_progress: float) -> None: pass

      @abstractmethod
      async def stop_tracking(job_id: str, reason: str) -> None: pass

      @abstractmethod
      async def cleanup_worker_extensions(job_id: str, worker_id: str) -> None: pass
  ```

- [ ] **10.3.2** Implement `LocalAuthorityTimeout` (single-DC)
  - Full implementation as documented in AD-34 Part 3
  - Extension-aware timeout calculation
  - Idempotent cleanup

- [ ] **10.3.3** Implement `GateCoordinatedTimeout` (multi-DC)
  - Full implementation as documented in AD-34 Part 4
  - Progress reporting every 10s
  - Timeout reporting on detection
  - 5-minute fallback if gate unreachable

### 10.4 Manager Integration

**File**: `hyperscale/distributed_rewrite/nodes/manager.py`

- [ ] **10.4.1** Add timeout strategy tracking
  ```python
  class ManagerServer:
      def __init__(self, ...):
          self._job_timeout_strategies: dict[str, TimeoutStrategy] = {}
  ```

- [ ] **10.4.2** Add `_select_timeout_strategy()` method
  - Auto-detect via `gate_addr` in JobSubmission
  - Return LocalAuthorityTimeout or GateCoordinatedTimeout

- [ ] **10.4.3** Add `_unified_timeout_loop()` background task
  - Check every 30 seconds
  - Only leader checks
  - Call `strategy.check_timeout()` for each job
  - Handle timeout by calling `_timeout_job()`

- [ ] **10.4.4** Update `receive_submit_job()` to start timeout tracking
  ```python
  strategy = await self._select_timeout_strategy(submission)
  await strategy.start_tracking(job_id, timeout_seconds, gate_addr)
  self._job_timeout_strategies[job_id] = strategy
  ```

- [ ] **10.4.5** Add `_on_leadership_acquired()` integration
  - Call `strategy.resume_tracking(job_id)` when becoming leader
  - Increment fence token

- [ ] **10.4.6** Add `_timeout_job()` method
  - Mark job as TIMEOUT status
  - Cancel all workflows
  - Call `strategy.stop_tracking()`
  - Notify callback (gate or client)

- [ ] **10.4.7** Add extension notification to `request_extension()`
  ```python
  if response.granted:
      await self._notify_timeout_strategies_of_extension(
          worker_id, extension_seconds, worker_progress
      )
  ```

- [ ] **10.4.8** Add `_notify_timeout_strategies_of_extension()` method
  - Find all jobs this worker is executing
  - Call `strategy.record_worker_extension()` for each

- [ ] **10.4.9** Add cleanup hooks
  - `receive_cancel_job()` → `strategy.stop_tracking("cancelled")`
  - `_handle_job_completion()` → `strategy.stop_tracking("completed")`
  - `_handle_job_failure()` → `strategy.stop_tracking("failed")`
  - `_handle_worker_failure()` → `strategy.cleanup_worker_extensions()`
  - `_cleanup_job()` → remove strategy from tracking

- [ ] **10.4.10** Add protocol handlers
  - `receive_job_global_timeout()` → `strategy.handle_global_timeout()`

### 10.5 Gate Integration

**File**: `hyperscale/distributed_rewrite/nodes/gate.py`

- [ ] **10.5.1** Add `GateJobTrackingInfo` dataclass
  ```python
  @dataclass
  class GateJobTrackingInfo:
      job_id: str
      submitted_at: float
      timeout_seconds: float
      target_datacenters: list[str]
      dc_status: dict[str, str]
      dc_last_progress: dict[str, float]
      dc_manager_addrs: dict[str, tuple[str, int]]
      # Extension tracking
      dc_total_extensions: dict[str, float]
      dc_max_extension: dict[str, float]
      dc_workers_with_extensions: dict[str, int]
      # Timeout decision
      globally_timed_out: bool = False
      timeout_reason: str = ""
      timeout_fence_token: int = 0
  ```

- [ ] **10.5.2** Create `GateJobTracker` class
  - `start_tracking_job()` on submission
  - `record_progress()` from JobProgressReport
  - `record_timeout()` from JobTimeoutReport
  - `check_global_timeouts()` logic
  - `handle_final_status()` for cleanup

- [ ] **10.5.3** Add `_global_timeout_loop()` background task
  - Check every 15 seconds
  - Call `tracker.check_global_timeouts()`
  - Call `_declare_and_broadcast_timeout()` for timed out jobs

- [ ] **10.5.4** Add `_declare_and_broadcast_timeout()` method
  - Send `JobGlobalTimeout` to all target DCs
  - Update tracking info

- [ ] **10.5.5** Add protocol handlers
  - `receive_job_progress_report()` → `tracker.record_progress()`
  - `receive_job_timeout_report()` → `tracker.record_timeout()`
  - `receive_job_final_status()` → `tracker.handle_final_status()`

### 10.6 WorkflowStateMachine Integration (AD-33)

**File**: `hyperscale/distributed_rewrite/workflow/state_machine.py`

- [ ] **10.6.1** Add progress tracking fields
  ```python
  class WorkflowStateMachine:
      def __init__(self, ...):
          self._last_progress: dict[str, float] = {}
          self._progress_callbacks: list[Callable] = []
  ```

- [ ] **10.6.2** Add `register_progress_callback()` method
  - Allow timeout strategies to register for state transitions

- [ ] **10.6.3** Update `transition()` to notify callbacks
  - Record `last_progress` timestamp
  - Call all registered callbacks with workflow_id and state

- [ ] **10.6.4** Add `get_time_since_progress()` method
  - Return seconds since last state transition

- [ ] **10.6.5** Add `get_stuck_workflows()` method
  - Return workflows with no progress for threshold_seconds

### 10.7 Testing

**File**: `tests/integration/test_job_timeout.py` (NEW)

- [ ] **10.7.1** Test single-DC local authority timeout
  - Submit job without gate_addr
  - Verify LocalAuthorityTimeout selected
  - Let job exceed timeout
  - Verify job marked as TIMEOUT

- [ ] **10.7.2** Test multi-DC gate coordinated timeout
  - Submit job with gate_addr to multiple DCs
  - Verify GateCoordinatedTimeout selected
  - One DC times out
  - Verify gate declares global timeout
  - Verify all DCs receive cancellation

- [ ] **10.7.3** Test extension-aware timeout
  - Job with 60s timeout
  - Worker requests 30s extension
  - Verify effective timeout = 90s
  - Verify job completes before extended deadline

- [ ] **10.7.4** Test stuck detection
  - Job running but no workflow progress for 2+ minutes
  - Verify timeout triggered despite worker alive

- [ ] **10.7.5** Test leader transfer with timeout state
  - Job leader fails mid-execution
  - New leader takes over
  - Verify timeout tracking continues from same started_at

- [ ] **10.7.6** Test fence token rejection
  - Old leader reports timeout after being replaced
  - New leader receives stale timeout with old fence token
  - Verify rejection

- [ ] **10.7.7** Test cleanup on job completion
  - Job completes successfully
  - Verify strategy removed from tracking
  - Verify no zombie timeout fires

- [ ] **10.7.8** Test cleanup on job cancellation
  - Cancel job mid-execution
  - Verify strategy cleaned up
  - Verify timeout tracking stopped

- [ ] **10.7.9** Test worker failure extension cleanup
  - Worker with extensions fails
  - Verify extensions removed from tracking
  - Verify job doesn't rely on stale extension

- [ ] **10.7.10** Test gate failure fallback
  - Gate becomes unreachable for 5+ minutes
  - Verify manager falls back to local timeout

### 10.8 Configuration

**File**: `hyperscale/distributed_rewrite/env/env.py`

- [ ] **10.8.1** Add timeout configuration
  ```python
  # Job timeout configuration
  JOB_TIMEOUT_CHECK_INTERVAL: float = 30.0  # Manager timeout check interval
  JOB_STUCK_THRESHOLD: float = 120.0        # No progress threshold
  GATE_TIMEOUT_CHECK_INTERVAL: float = 15.0 # Gate timeout check interval
  GATE_TIMEOUT_FALLBACK: float = 300.0      # 5 min fallback if gate unreachable
  ```

### 10.9 Documentation

- [ ] **10.9.1** Update CLAUDE.md with timeout patterns
  - How to configure job timeouts
  - Extension interaction with timeouts
  - Multi-DC timeout coordination

- [ ] **10.9.2** Add timeout observability guide
  - Key metrics to monitor
  - Log patterns for debugging
  - Common timeout scenarios

### Dependencies

- **10.1-10.3**: Core implementation (can be done in parallel)
- **10.4**: Depends on 10.1-10.3 (manager integration)
- **10.5**: Depends on 10.1-10.3 (gate integration)
- **10.6**: Can be done in parallel with 10.4-10.5
- **10.7**: Depends on 10.1-10.6 (testing)
- **10.8-10.9**: Can be done anytime

**Key Integration Points**:
- Integrates with AD-26 (healthcheck extensions) via `record_worker_extension()`
- Integrates with AD-33 (workflow state machine) via progress callbacks
- Integrates with Section 5 (cancellation) via cleanup hooks
- Uses existing job leadership transfer mechanisms from Sections 1-3

---

## Appendix: Key Code Locations

### Cancellation-Related

| Component | File | Key Methods |
|-----------|------|-------------|
| RemoteGraphManager | `hyperscale/core/jobs/graphs/remote_graph_manager.py:1458` | `cancel_workflow()` |
| RemoteGraphController | `hyperscale/core/jobs/graphs/remote_graph_controller.py:428` | `submit_workflow_cancellation()` |
| RemoteGraphController | `hyperscale/core/jobs/graphs/remote_graph_controller.py:941` | `cancel_workflow()` (receive) |
| RemoteGraphController | `hyperscale/core/jobs/graphs/remote_graph_controller.py:1154` | `cancel_workflow_background()` |
| WorkflowRunner | `hyperscale/core/jobs/graphs/workflow_runner.py:55` | `cancel_pending()` |

### Job Leadership-Related

| Component | File | Key Methods |
|-----------|------|-------------|
| Manager | `hyperscale/distributed_rewrite/nodes/manager.py:614` | `_on_manager_become_leader()` |
| Manager | `hyperscale/distributed_rewrite/nodes/manager.py:1078` | `_handle_job_leader_failure()` |
| Manager | `hyperscale/distributed_rewrite/nodes/manager.py:1170` | `_notify_gate_of_leadership_transfer()` |
| Manager | `hyperscale/distributed_rewrite/nodes/manager.py:1257` | `_notify_workers_of_leadership_transfer()` |
| Worker | `hyperscale/distributed_rewrite/nodes/worker.py` | `job_leader_worker_transfer()` handler |

---

## Notes

- All changes must be asyncio-safe (use locks where needed)
- Follow existing patterns in codebase (TaskRunner for background tasks, structured logging)
- Fencing tokens must be respected throughout to prevent stale operations
- Memory cleanup is critical - track and clean up orphaned state
