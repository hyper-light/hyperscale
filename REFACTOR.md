# Refactor Plan: Gate/Manager/Worker Servers

## Goals
- Enforce one-class-per-file across gate/manager/worker server code.
- Group related logic into cohesive submodules with explicit boundaries.
- Ensure all dataclasses use `slots=True` and live in a `models/` submodule.
- Preserve behavior and interfaces; refactor in small, safe moves.
- Prefer list/dict comprehensions, walrus operators, and early returns.

## Constraints
- One class per file (including nested helper classes).
- Dataclasses must be defined in `models/` submodules and declared with `slots=True`.
- Keep async patterns, TaskRunner usage, and logging patterns intact.
- Avoid new architectural behavior changes while splitting files.

## Target Module Layout (Shared Pattern)
```
hyperscale/distributed_rewrite/nodes/<role>/
  __init__.py
  server.py                 # <Role>Server (public entry)
  config.py                 # <Role>Config (env + derived config)
  state.py                  # <Role>State (mutable runtime state)
  registry.py               # Registration + peer tracking
  routing.py                # Routing decisions (DC/manager/gate/worker)
  dispatch.py               # Job/workflow dispatch orchestration
  sync.py                   # State sync and snapshots
  health.py                 # Health integration + embedder plumbing
  leadership.py             # Role-specific leadership hooks
  stats.py                  # Stats aggregation + tiered updates
  cancellation.py           # Cancellation flows
  leases.py                 # Lease/fence/ownership coordination
  discovery.py              # Discovery service integration
  handlers/
    __init__.py
    tcp_*.py                # TCP message handlers (one class each)
    udp_*.py                # UDP message handlers (one class each)
  models/
    __init__.py
    *.py                    # dataclasses with slots=True
```

## Gate Server Refactor (nodes/gate)
### What moves where
- **GateServer** → `nodes/gate/server.py` as the composition root.
  - Responsibilities: lifecycle (`start`, `stop`), wiring dependencies, registering handlers, delegating to modules.
  - No logic beyond orchestration and delegation.
- **Configuration** → `nodes/gate/config.py` as `GateConfig`.
  - Load env settings (timeouts, intervals, thresholds).
  - Derived constants (jitter bounds, retry counts, TTLs).
- **Runtime State** → `nodes/gate/state.py` as `GateState`.
  - Mutable dicts/sets: `_datacenter_manager_status`, `_job_dc_managers`, `_job_lease_manager`, `_gate_peer_info`, `_orphaned_jobs`, etc.
- **Registration + discovery** → `nodes/gate/registry.py` and `nodes/gate/discovery.py`.
  - Gate peer registration, manager registration, discovery maintenance loop.
- **Routing logic** → `nodes/gate/routing.py`.
  - `_select_datacenters_with_fallback`, `_classify_datacenter_health` (if kept gate-local), routing decisions.
- **Dispatch** → `nodes/gate/dispatch.py`.
  - Job submission flow, per-DC dispatch, retry/fallback orchestration.
- **State sync** → `nodes/gate/sync.py`.
  - `_get_state_snapshot`, `_apply_state_snapshot`, sync request/response handling, retry logic.
- **Health** → `nodes/gate/health.py`.
  - SWIM callbacks, federated health monitor integration, DC health change handling.
- **Leadership** → `nodes/gate/leadership.py`.
  - Leader election callbacks, split-brain logic, leadership announcements.
- **Stats** → `nodes/gate/stats.py`.
  - Tiered update classifier, batch loops, windowed stats aggregation and push.
- **Cancellation** → `nodes/gate/cancellation.py`.
  - Job cancel request flow, tracking cancel completions.
- **Leases** → `nodes/gate/leases.py`.
  - Datacenter and job lease coordination, lease transfers.

### Example: move Tiered Updates (Gate)
**Current**: `_classify_update_tier`, `_send_immediate_update`, `_batch_stats_loop` in `nodes/gate.py`.

**New**: `nodes/gate/stats.py`
```python
class GateStatsCoordinator:
    def __init__(self, state: GateState, logger: Logger, task_runner: TaskRunner):
        self._state = state
        self._logger = logger
        self._task_runner = task_runner

    def classify_update_tier(self, job_id: str, old_status: str | None, new_status: str) -> str:
        if new_status in (JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value):
            return UpdateTier.IMMEDIATE.value
        if old_status is None and new_status == JobStatus.RUNNING.value:
            return UpdateTier.IMMEDIATE.value
        if old_status != new_status:
            return UpdateTier.IMMEDIATE.value
        return UpdateTier.PERIODIC.value

    async def send_immediate_update(self, job_id: str, event_type: str, payload: bytes | None = None) -> None:
        if not (job := self._state.job_manager.get_job(job_id)):
            return
        if not (callback := self._state.job_manager.get_callback(job_id)):
            return
        # build JobStatusPush and send
```

### Gate models to relocate
- Any small state containers (e.g., job forwarding state, gate peer state) become dataclasses in `nodes/gate/models/` with `slots=True`.
- Shared message models remain in `distributed_rewrite/models/`.

## Manager Server Refactor (nodes/manager)
### What moves where
- **ManagerServer** → `nodes/manager/server.py`.
- **Configuration** → `nodes/manager/config.py`.
- **Runtime State** → `nodes/manager/state.py`.
  - Worker pools, job registries, peer tracking, state clocks.
- **Registry** → `nodes/manager/registry.py`.
  - Worker/gate registration, peer manager registration.
- **Dispatch** → `nodes/manager/dispatch.py`.
  - Workflow dispatch orchestration, worker allocation.
- **State sync** → `nodes/manager/sync.py`.
  - Worker and peer manager sync, retry logic, snapshot handling.
- **Health** → `nodes/manager/health.py`.
  - Worker health manager integration, SWIM callbacks.
- **Leadership** → `nodes/manager/leadership.py`.
  - Leader election callbacks, split-brain handling.
- **Stats** → `nodes/manager/stats.py`.
  - Windowed stats aggregation, backpressure hooks.
- **Cancellation** → `nodes/manager/cancellation.py`.
  - Job and workflow cancellation flows, workflow cancellation propagation.
- **Leases** → `nodes/manager/leases.py`.
  - Fencing tokens, leadership leases, ownership updates.
- **Discovery** → `nodes/manager/discovery.py`.
  - Discovery service and maintenance loop.
- **Workflow Lifecycle** → `nodes/manager/workflow_lifecycle.py`.
  - AD-33 transitions, dependency resolution, reschedule handling.

### Example: move state sync (Manager)
**Current**: `_request_worker_state`, `_request_manager_peer_state`, `_sync_state_from_workers` in `nodes/manager.py`.

**New**: `nodes/manager/sync.py`
```python
class ManagerStateSync:
    def __init__(self, state: ManagerState, logger: Logger, task_runner: TaskRunner):
        self._state = state
        self._logger = logger
        self._task_runner = task_runner

    async def request_worker_state(self, worker_addr: tuple[str, int], request: StateSyncRequest, max_retries: int, base_delay: float) -> WorkerStateSnapshot | None:
        last_error = None
        for attempt in range(max_retries):
            try:
                response, _ = await self._state.send_tcp(worker_addr, "state_sync_request", request.dump(), timeout=5.0)
                if response and not isinstance(response, Exception):
                    if (sync_response := StateSyncResponse.load(response)).worker_state:
                        return await self._process_worker_state_response(sync_response.worker_state)
                last_error = "Empty or invalid response"
            except Exception as exc:
                last_error = str(exc)
            if attempt < max_retries - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))
        await self._logger.log(ServerError(...))
        return None
```

### Manager models to relocate
- `PeerState`, `WorkerSyncState`, `JobSyncState`, `CancellationState` as dataclasses in `nodes/manager/models/` with `slots=True`.

## Worker Server Refactor (nodes/worker)
### What moves where
- **WorkerServer** → `nodes/worker/server.py`.
- **Configuration** → `nodes/worker/config.py`.
- **Runtime State** → `nodes/worker/state.py`.
  - Active workflows, core allocator, manager tracking, circuits.
- **Registry** → `nodes/worker/registry.py`.
  - Manager registration, health tracking.
- **Execution** → `nodes/worker/execution.py`.
  - Workflow execution, progress reporting, cleanup.
- **Health** → `nodes/worker/health.py`.
  - SWIM callbacks, embedding, health signals.
- **State sync** → `nodes/worker/sync.py`.
  - Sync request handling, snapshot generation.
- **Cancellation** → `nodes/worker/cancellation.py`.
  - Workflow cancel requests, completion notifications.
- **Discovery** → `nodes/worker/discovery.py`.
  - Discovery service management.
- **Backpressure** → `nodes/worker/backpressure.py`.
  - Backpressure signals, overload detection.

### Example: move execution (Worker)
**Current**: workflow dispatch handling in `nodes/worker.py`.

**New**: `nodes/worker/execution.py`
```python
class WorkerExecutor:
    def __init__(self, state: WorkerState, logger: Logger, task_runner: TaskRunner):
        self._state = state
        self._logger = logger
        self._task_runner = task_runner

    async def handle_dispatch(self, dispatch: WorkflowDispatch) -> WorkflowDispatchAck:
        if (current := self._state.workflow_fence_tokens.get(dispatch.workflow_id)) and dispatch.fence_token <= current:
            return WorkflowDispatchAck(...)
        self._state.workflow_fence_tokens[dispatch.workflow_id] = dispatch.fence_token
        # allocate cores, run workflow, track progress
```

### Worker models to relocate
- `ManagerPeerState`, `WorkflowRuntimeState`, `CancelState` in `nodes/worker/models/` with `slots=True`.

## Handler Modules (Examples)
### Gate TCP handler example
`nodes/gate/handlers/tcp_job_submission.py`
```python
class GateJobSubmissionHandler:
    def __init__(self, server: GateServer, dispatcher: GateDispatcher):
        self._server = server
        self._dispatcher = dispatcher

    async def handle(self, submission: JobSubmission) -> JobAck:
        return await self._dispatcher.submit_job(submission)
```

### Manager UDP handler example
`nodes/manager/handlers/udp_manager_swim.py`
```python
class ManagerSwimHandler:
    def __init__(self, health: ManagerHealthIntegration):
        self._health = health

    def handle_heartbeat(self, heartbeat: ManagerHeartbeat, source_addr: tuple[str, int]) -> None:
        self._health.handle_peer_heartbeat(heartbeat, source_addr)
```

### Worker TCP handler example
`nodes/worker/handlers/tcp_dispatch.py`
```python
class WorkerDispatchHandler:
    def __init__(self, executor: WorkerExecutor):
        self._executor = executor

    async def handle(self, dispatch: WorkflowDispatch) -> WorkflowDispatchAck:
        return await self._executor.handle_dispatch(dispatch)
```

## Dataclass Placement + Slots Guidance
- Any data container introduced during split becomes a dataclass in `models/` with `slots=True`.
- Avoid inline dataclasses in server modules.
- Keep shared protocol message dataclasses in `distributed_rewrite/models/`.

Example:
```python
@dataclass(slots=True)
class GatePeerState:
    udp_addr: tuple[str, int]
    tcp_addr: tuple[str, int]
    last_seen: float
```

## Style Refactor Guidance
- **Comprehensions**: replace loop-based list/dict builds where possible.
  - Example: `result = {dc: self._classify_datacenter_health(dc) for dc in dcs}`
- **Early returns**: reduce nested control flow.
  - Example: `if not payload: return None`
- **Walrus operator**: use to avoid repeated lookups.
  - Example: `if not (job := self._state.job_manager.get_job(job_id)):
      return`

## Migration Steps (Detailed)
1. **Create new module tree** (`nodes/gate`, `nodes/manager`, `nodes/worker`) with `__init__.py` exports.
2. **Move state containers** into `state.py`, update imports.
3. **Move model dataclasses** into `models/` with `slots=True`.
4. **Extract handlers** (TCP/UDP) first, wire from server.
5. **Extract state sync + registry + discovery** modules.
6. **Extract dispatch + cancellation + stats + leases** modules.
7. **Collapse server** to orchestration + dependency injection.
8. **Tighten style** (comprehensions, early returns, walrus) per module.
9. **Remove dead imports** and resolve cycles with dependency inversion.

## Verification Strategy
- Run LSP diagnostics on touched files.
- No integration tests (per repo guidance).
- Ensure all public protocol messages and network actions are unchanged.

## Open Decisions
- Whether to keep shared base classes for handlers.
- Whether to centralize shared models at `distributed_rewrite/models/` vs node-local `models/`.
