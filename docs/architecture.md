# Hyperscale Distributed Architecture

A high-performance, fault-tolerant distributed workflow execution system designed for multi-datacenter deployments with high CPU and memory utilization per node.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [Node Types](#node-types)
  - [Communication Protocols](#communication-protocols)
  - [Leadership Election](#leadership-election)
- [Component Diagrams](#component-diagrams)
- [State Machines](#state-machines)
  - [SWIM Node States](#swim-node-states)
  - [Worker States](#worker-states)
  - [Job Lifecycle](#job-lifecycle)
  - [Workflow Lifecycle](#workflow-lifecycle)
  - [Leadership States](#leadership-states)
- [Data Flow](#data-flow)
- [Timing Diagrams](#timing-diagrams)
  - [SWIM Probe Cycle](#swim-probe-cycle)
  - [Quorum Confirmation](#quorum-confirmation)
  - [Leader Election Sequence](#leader-election-sequence)
- [Failure Handling](#failure-handling)
  - [Failure Recovery Flows](#failure-recovery-flows)
  - [Network Partition Handling](#network-partition-handling)
  - [Cascading Failure Protection](#cascading-failure-protection)
- [Zombie Job Prevention & Detection](#zombie-job-prevention--detection)
  - [Zombie Job Lifecycle Diagram](#zombie-job-lifecycle-diagram)
  - [Detection Mechanisms](#detection-mechanisms)
  - [Prevention Mechanisms](#prevention-mechanisms)
  - [Cleanup Mechanisms](#cleanup-mechanisms)
  - [Cancellation Flow](#cancellation-flow-killing-zombie-jobs)
  - [Complete Zombie Prevention State Machine](#complete-zombie-prevention-state-machine)
  - [Known Gaps and Future Improvements](#known-gaps-and-future-improvements)
- [Backpressure & Degradation](#backpressure--degradation)
- [Scaling Operations](#scaling-operations)
- [State Management](#state-management)
- [Security](#security)
- [Message Protocol Reference](#message-protocol-reference)
- [Module Structure](#module-structure)
- [Bootstrap & Service Discovery](#bootstrap--service-discovery)
  - [Design Goals](#design-goals)
  - [Architecture Decision](#architecture-decision)
  - [Discovery Approaches Evaluated](#discovery-approaches-evaluated)
  - [Chosen Solution: DNS + Seeds with Parallel Probing](#chosen-solution-dns--seeds-with-parallel-probing)
  - [Bootstrap Protocol](#bootstrap-protocol)
  - [DNS Resolution](#dns-resolution)
  - [Peer Probing](#peer-probing)
  - [Health-Aware Peer Cache](#health-aware-peer-cache)
  - [Failure Scenarios](#failure-scenarios)
  - [Configuration](#configuration)
  - [Module Structure](#bootstrap-module-structure)
  - [Example Implementations](#example-implementations)

---

## Overview

The distributed system implements a three-tier architecture optimized for executing load testing workflows across multiple datacenters:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT                                          │
│                         (Job Submission)                                     │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           GATE CLUSTER                                       │
│                    (Optional, Cross-DC Coordination)                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                                      │
│  │  Gate   │◄─┤  Gate   │◄─┤  Gate   │  ◄── Leader elected via SWIM        │
│  │(Leader) │  │(Follower│  │(Follower│                                      │
│  └────┬────┘  └─────────┘  └─────────┘                                      │
└───────┼─────────────────────────────────────────────────────────────────────┘
        │
        ├──────────────────┬──────────────────┐
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│ DATACENTER A  │  │ DATACENTER B  │  │ DATACENTER C  │
│               │  │               │  │               │
│ ┌───────────┐ │  │ ┌───────────┐ │  │ ┌───────────┐ │
│ │  Manager  │ │  │ │  Manager  │ │  │ │  Manager  │ │
│ │  Cluster  │ │  │ │  Cluster  │ │  │ │  Cluster  │ │
│ └─────┬─────┘ │  │ └─────┬─────┘ │  │ └─────┬─────┘ │
│       │       │  │       │       │  │       │       │
│ ┌─────┴─────┐ │  │ ┌─────┴─────┐ │  │ ┌─────┴─────┐ │
│ │  Workers  │ │  │ │  Workers  │ │  │ │  Workers  │ │
│ │ (N cores) │ │  │ │ (N cores) │ │  │ │ (N cores) │ │
│ └───────────┘ │  │ └───────────┘ │  │ └───────────┘ │
└───────────────┘  └───────────────┘  └───────────────┘
```

### Detailed Single-Datacenter View

```
                          ┌─────────────────────────────────────────────┐
                          │             GATE CLUSTER                    │
                          │          (Gossip Protocol)                  │
                          │    ┌────┐   ┌────┐   ┌────┐                 │
                          │    │ G1 │◄─►│ G2 │◄─►│ G3 │  ← Job submit   │
                          │    └──┬─┘   └──┬─┘   └──┬─┘    from users   │
                          │       │        │        │                   │
                          └───────┼────────┼────────┼───────────────────┘
                                  │        │        │
                           TCP (job submission) + UDP (health checks)
                                  │        │        │
           ┌──────────────────────┼────────┼────────┼──────────────────────┐
           │                      ▼        ▼        ▼                      │
           │  ┌────────────────────────────────────────────────────────┐   │
           │  │               MANAGER CLUSTER (DC-A)                   │   │
           │  │            (Gossip + Leader Election)                  │   │
           │  │    ┌────┐       ┌────┐       ┌────┐                    │   │
           │  │    │ M1 │◄─────►│ M2 │◄─────►│ M3 │                    │   │
           │  │    │    │       │ ★  │       │    │   ★ = Leader       │   │
           │  │    └──┬─┘       └──┬─┘       └──┬─┘                    │   │
           │  │       │    TCP    │    TCP    │    (Full state sync)   │   │
           │  │       └───────────┼───────────┘                        │   │
           │  └───────────────────┼────────────────────────────────────┘   │
           │                      │                                        │
           │             UDP/TCP (workflow dispatch + status reports)      │
           │                      │                                        │
           │  ┌───────────────────┼────────────────────────────────────┐   │
           │  │            WORKER POOL (DC-A)                          │   │
           │  │                   │                                    │   │
           │  │    ┌──────────────┼──────────────┐                     │   │
           │  │    │              │              │                     │   │
           │  │    ▼              ▼              ▼                     │   │
           │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐              │   │
           │  │  │ Worker1  │  │ Worker2  │  │ Worker3  │              │   │
           │  │  │ 8 cores  │  │ 8 cores  │  │ 8 cores  │              │   │
           │  │  │[■■■■□□□□]│  │[■■□□□□□□]│  │[□□□□□□□□]│              │   │
           │  │  │ 4 in use │  │ 2 in use │  │ 0 idle   │              │   │
           │  │  └──────────┘  └──────────┘  └──────────┘              │   │
           │  │                                                        │   │
           │  │  ■ = core running workflow    □ = core available       │   │
           │  └────────────────────────────────────────────────────────┘   │
           │                       DATACENTER A                            │
           └───────────────────────────────────────────────────────────────┘
```

### Key Design Principles

1. **Workers are the source of truth** - Workers maintain authoritative state for their own workflows
2. **Passive state discovery** - Serf-style heartbeat embedding in SWIM messages
3. **Quorum-based provisioning** - Manager decisions require quorum confirmation
4. **Lease-based execution** - Gates use leases for at-most-once DC semantics
5. **Graceful degradation** - Load shedding under pressure, LHM-aware timeouts
6. **Composition over inheritance** - All extensibility via callbacks, not method overriding
7. **TaskRunner for lifecycle management** - All background tasks managed via TaskRunner
8. **Quorum uses configured size** - Prevents split-brain in partitions (see below)

---

## Architectural Decisions

This section documents key architectural decisions made during development.

### AD-1: Composition Over Inheritance

**Decision**: All extensibility is via callbacks and composition, never method overriding.

**Rationale**: 
- Prevents fragile base class problems
- Makes dependencies explicit
- Easier to test individual components
- Allows runtime reconfiguration

**Implementation**:
- `StateEmbedder` protocol for heartbeat embedding
- Leadership callbacks: `register_on_become_leader()`, `register_on_lose_leadership()`
- Node status callbacks: `register_on_node_dead()`, `register_on_node_join()`
- All node types (Worker, Manager, Gate) use these instead of overriding UDPServer methods

### AD-2: TaskRunner for All Background Tasks

**Decision**: All background/async tasks must be managed through TaskRunner, not raw `asyncio.create_task()`.

**Rationale**:
- Prevents orphaned tasks on shutdown
- Provides cancellation via tokens
- Enables task lifecycle monitoring
- Centralizes cleanup logic

**Implementation**:
- `self._task_runner.run(coro, *args)` returns a token
- `self._task_runner.cancel(token)` for cancellation
- Cleanup loops, state sync, progress reporting all use TaskRunner

### AD-3: Quorum Uses Configured Cluster Size

**Decision**: Quorum calculation uses the **configured** cluster size, not the **active** member count.

**Rationale**:
- Prevents split-brain in network partitions
- A partition with 1 of 3 managers won't think it has quorum
- Standard Raft/Paxos behavior

**Implementation**:
```python
def _quorum_size(self) -> int:
    """Uses CONFIGURED peer count."""
    total_managers = len(self._manager_peers) + 1  # Include self
    return (total_managers // 2) + 1

def _has_quorum_available(self) -> bool:
    """Uses ACTIVE peer count for monitoring only."""
    active_count = len(self._active_manager_peers) + 1
    return active_count >= self._quorum_size()
```

### AD-4: Workers Are Source of Truth

**Decision**: Workers maintain authoritative state for their workflows. Managers rebuild state from workers on leader election.

**Rationale**:
- Workers have the actual running processes
- Eliminates single point of failure for state
- New leader can recover without distributed log

**Implementation**:
- `_on_manager_become_leader()` triggers `_sync_state_from_workers()`
- Workers respond with `WorkerStateSnapshot` containing `active_workflows`
- Manager rebuilds `_workflow_assignments` from worker responses

### AD-5: Pre-Voting for Split-Brain Prevention

**Decision**: Leader election uses a pre-vote phase before the actual election.

**Rationale**:
- Pre-vote doesn't increment term (prevents term explosion)
- Candidate checks if it would win before disrupting cluster
- Nodes only grant pre-vote if no healthy leader exists

**Implementation**:
- `_run_pre_vote()` gathers pre-votes without changing state
- Only proceeds to real election if pre-vote majority achieved
- If pre-vote fails, election is aborted

### AD-6: Manager Peer Failure Detection

**Decision**: Managers track peer liveness and quorum availability separately.

**Rationale**:
- Need to know if quorum operations will succeed
- Leadership re-election is automatic via lease expiry
- Logging quorum status aids debugging

**Implementation**:
- `_manager_udp_to_tcp`: Maps UDP addresses to TCP addresses
- `_active_manager_peers`: Set of currently live peers
- `_on_node_dead()` checks both workers AND manager peers
- `_handle_manager_peer_failure()` updates active set

### AD-7: Worker Manager Failover

**Decision**: Workers detect manager failure via SWIM and automatically failover to backup managers.

**Rationale**:
- Workers must continue operating during manager transitions
- Active workflows shouldn't be lost on manager failure
- New manager needs to know about in-flight work

**Implementation**:
- Worker registers `_handle_manager_failure` as `on_node_dead` callback
- On manager death: clear current manager, try alternatives
- On successful failover: call `_report_active_workflows_to_manager()`

### AD-8: Cores Completed for Faster Provisioning

**Decision**: Workers report `cores_completed` in progress updates; managers optimistically update available cores.

**Rationale**:
- Don't wait for entire workflow to complete before provisioning
- Enables pipelining of workflow execution
- Better utilization of worker capacity

**Implementation**:
- `WorkflowProgress.cores_completed` field
- Manager's `_update_worker_cores_from_progress()` calculates freed cores
- Optimistic update may be superseded by next heartbeat (acceptable)

### AD-9: Retry Data Preserved at Dispatch

**Decision**: Original `WorkflowDispatch` bytes are stored when workflow is first dispatched, not reconstructed on retry.

**Rationale**:
- Ensures retry has exact same parameters (VUs, timeout, context)
- Avoids serialization round-trip errors
- Simplifies retry logic

**Implementation**:
- `_workflow_retries[workflow_id] = (count, original_dispatch_bytes, failed_workers)`
- On retry: deserialize original, create new dispatch with updated fence_token
- `failed_workers` set prevents re-dispatching to same worker

### AD-10: Fencing Tokens from Terms

**Decision**: Fencing tokens are derived from election terms.

**Rationale**:
- Monotonically increasing
- Tied to leadership changes
- Workers can reject stale leader operations

**Implementation**:
- `get_fencing_token()` returns current term
- `is_fencing_token_valid(token)` checks `token >= current_term`
- Included in `WorkflowDispatch`, checked by workers

### AD-11: State Sync Retries with Exponential Backoff

**Decision**: State sync operations use retries with exponential backoff.

**Rationale**:
- Network partitions are often transient
- Single-attempt sync may miss temporarily unavailable workers
- Exponential backoff prevents thundering herd on recovery

**Implementation**:
- `_request_worker_state(max_retries=3, base_delay=0.5)` retries with backoff
- `_request_manager_peer_state(max_retries=3, base_delay=0.5)` similarly
- Delay formula: `base_delay * (2 ** attempt)`
- After exhausting retries, error is logged but sync continues with other peers

### AD-12: Manager Peer State Sync on Leadership

**Decision**: New leaders sync from both workers AND peer managers.

**Rationale**:
- Workers are source of truth for workflow execution state
- Peer managers have job-level metadata (retry counts, completion status)
- Both are needed for complete state recovery

**Implementation**:
- `_on_manager_become_leader()` calls both sync methods
- `_sync_state_from_workers()` - gets workflow execution state
- `_sync_state_from_manager_peers()` - gets job metadata
- Both use retry logic (AD-11)

### AD-13: Gate Split-Brain Prevention

**Decision**: Gates use the same split-brain prevention as managers.

**Rationale**:
- Gates coordinate across datacenters - split-brain would cause duplicate jobs
- Same SWIM-based detection works for gate clusters
- Consistent patterns reduce complexity

**Implementation**:
- `_gate_udp_to_tcp` maps UDP addresses to TCP for peer tracking
- `_active_gate_peers` tracks currently reachable peers
- `_on_node_dead` / `_on_node_join` handle peer failure/recovery
- Leadership re-election via `LocalLeaderElection` (same as managers)
- Pre-voting and term-based resolution prevent split-brain

### AD-14: CRDT-Based Cross-DC Statistics

**Decision**: Use Conflict-free Replicated Data Types (CRDTs) for cross-datacenter job statistics.

**Rationale**:
- Cross-DC coordination is expensive (10-100ms+ RTT)
- Stats like `completed_count` and `failed_count` are monotonic and perfect for G-Counters
- CRDTs allow coordination-free updates with guaranteed eventual consistency
- Merge is always safe - gates can combine stats from any subset of DCs

**Implementation**:
```python
class GCounter:
    """Grow-only counter - each DC has its own slot."""
    counts: dict[str, int]  # dc_id -> count
    
    def increment(self, dc_id: str, amount: int = 1) -> None
    def merge(self, other: "GCounter") -> "GCounter"  # commutative, associative, idempotent
    @property
    def value(self) -> int  # sum of all slots

class JobStatsCRDT:
    """CRDT-based job statistics."""
    completed: GCounter  # Monotonic - perfect for G-Counter
    failed: GCounter     # Monotonic - perfect for G-Counter
    rates: dict[str, tuple[float, int]]  # dc -> (rate, lamport_timestamp) - LWW register
```

### AD-15: Tiered Update Strategy for Cross-DC Stats

**Decision**: Use tiered update frequency based on stat criticality.

**Rationale**:
- Not all stats need real-time updates
- Critical events (completion, failure) need immediate notification
- Aggregate stats can be batched for efficiency
- Detailed stats should be pull-based to avoid overhead

**Tiers**:
| Tier | Stats | Frequency | Transport |
|------|-------|-----------|-----------|
| Immediate | Job completion, failure, critical alerts | Event-driven | TCP push |
| Periodic | Workflow progress, aggregate rates | Every 1-5s | TCP batch |
| On-Demand | Step-level stats, historical data | Client request | TCP pull |

**Implementation**:
- `_send_immediate_update()` for tier 1 events
- `_batch_stats_loop()` aggregates tier 2 stats periodically
- `receive_job_status_request()` fetches tier 3 on demand

### AD-16: Datacenter Health Classification

**Decision**: Classify datacenter health into four distinct states to enable intelligent routing.

**Rationale**:
- BUSY ≠ UNHEALTHY (critical distinction)
- BUSY = transient, will clear when workflows complete
- DEGRADED = structural problem, reduced capacity but operational
- UNHEALTHY = severe problem, requires intervention
- Routing should actively seek healthier DCs before accepting degraded states

**States** (evaluated in order):

| State | Definition | Condition |
|-------|------------|-----------|
| UNHEALTHY | No managers responding OR no workers registered | `alive_managers == 0` OR `worker_count == 0` |
| DEGRADED | Majority of workers unhealthy OR majority of managers unhealthy | `healthy_workers < worker_count // 2 + 1` OR `alive_managers < total_managers // 2 + 1` |
| BUSY | Not degraded AND no available capacity | NOT degraded AND `available_cores == 0` |
| HEALTHY | Not degraded AND capacity available | NOT degraded AND `available_cores > 0` |

**Key Metrics from ManagerHeartbeat**:
- `worker_count`: Total registered workers
- `healthy_worker_count`: Workers responding to SWIM probes
- `available_cores`: Available cores from healthy workers only
- `total_cores`: Total cores across all registered workers

**Implementation**:
```python
class DatacenterHealth(Enum):
    HEALTHY = "healthy"      # Capacity available, all systems operational
    BUSY = "busy"            # No capacity but structurally healthy (transient)
    DEGRADED = "degraded"    # Majority of workers/managers unhealthy
    UNHEALTHY = "unhealthy"  # No managers OR no workers

def _classify_datacenter_health(self, dc_id: str) -> DatacenterStatus:
    # 1. Check manager liveness via SWIM
    # 2. If alive_managers == 0 → UNHEALTHY
    # 3. If no workers registered → UNHEALTHY
    # 4. Check majority health:
    #    - healthy_workers < worker_quorum → DEGRADED
    #    - alive_managers < manager_quorum → DEGRADED
    # 5. If not degraded and available_cores == 0 → BUSY
    # 6. If not degraded and available_cores > 0 → HEALTHY
```

### AD-17: Smart Dispatch with Fallback Chain

**Decision**: Implement cascading fallback for job dispatch across datacenters.

**Rationale**:
- Single DC failure shouldn't fail entire job
- Automatic recovery without client involvement
- Actively seek healthier DCs before accepting degraded states
- Preserve user's datacenter preferences while enabling fallback

**Routing Rules** (in order of preference):

| Current DC State | Action |
|------------------|--------|
| HEALTHY | Enqueue job (preferred) |
| BUSY | Fallback to HEALTHY DC if available, else queue |
| DEGRADED | Fallback to HEALTHY or BUSY DC if available, else queue with warning |
| UNHEALTHY | Fallback to any non-UNHEALTHY DC, else **fail job with error** |

**Selection Priority**: HEALTHY > BUSY > DEGRADED (UNHEALTHY excluded)

**Flow**:
1. Classify all DCs by health
2. Bucket DCs: HEALTHY (sorted by capacity), BUSY, DEGRADED
3. Determine `worst_health` we must accept
4. Select primary DCs from best available bucket
5. Build fallback list from remaining usable DCs
6. Dispatch with appropriate logging:
   - If `worst_health == "unhealthy"` → **fail job immediately**
   - If `worst_health == "degraded"` → log warning, then queue
   - If `worst_health == "busy"` → log info, then queue
   - If `worst_health == "healthy"` → queue normally

**Implementation**:
```python
def _select_datacenters_with_fallback(
    self,
    count: int,
    preferred: list[str] | None = None,
) -> tuple[list[str], list[str], str]:  # (primary_dcs, fallback_dcs, worst_health)
    # worst_health: "healthy" | "busy" | "degraded" | "unhealthy"

async def _dispatch_job_to_datacenters(
    self,
    submission: JobSubmission,
    target_dcs: list[str],
) -> None:
    primary_dcs, fallback_dcs, worst_health = self._select_datacenters_with_fallback(...)
    
    if worst_health == "unhealthy":
        # Fail job - no usable DCs
        job.status = JobStatus.FAILED
        return
    
    if worst_health == "degraded":
        log_warning("Routing to DEGRADED DCs")
    elif worst_health == "busy":
        log_info("Routing to BUSY DCs")
    
    # Dispatch with fallback support
    await self._dispatch_job_with_fallback(submission, primary_dcs, fallback_dcs)
```

### AD-18: Hybrid Overload Detection (Delta + Absolute)

**Decision**: Use delta-based detection with absolute safety bounds for overload detection.

**Rationale**:
- Fixed thresholds cause flapping and require per-workload tuning
- Delta-based detection (rate of change) is self-calibrating
- Pure delta misses absolute capacity limits and suffers baseline drift
- Hybrid approach combines benefits of both

**Detection Model**:
```
┌─────────────────────────────────────────────────────────────────┐
│                    Hybrid Overload Detection                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Primary: Delta-based (% above EMA baseline + trend slope)     │
│  ├─ Tracks latency/queue depth relative to baseline            │
│  ├─ Uses Exponential Moving Average for baseline               │
│  ├─ Calculates trend via linear regression on delta history    │
│  └─ Self-calibrates to workload characteristics                │
│                                                                 │
│  Secondary: Absolute safety bounds (hard limits)               │
│  ├─ Prevents baseline drift masking real problems              │
│  ├─ Catches "stable but maxed out" scenarios                   │
│  └─ Example: latency > 5000ms = overloaded regardless          │
│                                                                 │
│  Tertiary: Resource signals (CPU, memory, queue depth)         │
│  ├─ Provides capacity awareness                                │
│  └─ Catches "about to fail" before latency spikes              │
│                                                                 │
│  Final State = max(delta_state, absolute_state, resource_state)│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**State Levels**:
| State | Delta Threshold | Absolute Bound | Action |
|-------|-----------------|----------------|--------|
| healthy | < 20% above baseline | < 200ms | Normal operation |
| busy | 20-50% above baseline | 200-500ms | Reduce new work |
| stressed | 50-100% above baseline | 500-2000ms | Shed low-priority |
| overloaded | > 100% above baseline OR rising trend | > 2000ms | Emergency shed |

**Implementation**:
```python
@dataclass
class OverloadConfig:
    """Configuration for hybrid overload detection."""
    # Delta detection
    ema_alpha: float = 0.1  # Smoothing factor for baseline
    current_window: int = 10  # Samples for current average
    trend_window: int = 20  # Samples for trend calculation
    delta_thresholds: tuple[float, float, float] = (0.2, 0.5, 1.0)  # busy/stressed/overloaded

    # Absolute bounds (safety rails)
    absolute_bounds: tuple[float, float, float] = (200.0, 500.0, 2000.0)

    # Resource signals
    cpu_thresholds: tuple[float, float, float] = (0.7, 0.85, 0.95)
    memory_thresholds: tuple[float, float, float] = (0.7, 0.85, 0.95)

class HybridOverloadDetector:
    """Combines delta-based and absolute detection."""

    def __init__(self, config: OverloadConfig | None = None):
        self._config = config or OverloadConfig()
        self._baseline_ema: float = 0.0
        self._recent: deque[float] = deque(maxlen=self._config.current_window)
        self._delta_history: deque[float] = deque(maxlen=self._config.trend_window)

    def record_latency(self, latency_ms: float) -> None:
        """Record a latency sample and update state."""
        # Update baseline EMA
        if self._baseline_ema == 0.0:
            self._baseline_ema = latency_ms
        else:
            alpha = self._config.ema_alpha
            self._baseline_ema = alpha * latency_ms + (1 - alpha) * self._baseline_ema

        self._recent.append(latency_ms)

        # Calculate delta (% above baseline)
        if self._baseline_ema > 0:
            current_avg = sum(self._recent) / len(self._recent)
            delta = (current_avg - self._baseline_ema) / self._baseline_ema
            self._delta_history.append(delta)

    def get_state(self, cpu_percent: float = 0.0, memory_percent: float = 0.0) -> str:
        """Get current overload state using hybrid detection."""
        states = []

        # Delta-based state
        if len(self._recent) >= 3:
            current_avg = sum(self._recent) / len(self._recent)
            delta = (current_avg - self._baseline_ema) / max(self._baseline_ema, 1.0)
            trend = self._calculate_trend()

            if delta > self._config.delta_thresholds[2] or trend > 0.1:
                states.append("overloaded")
            elif delta > self._config.delta_thresholds[1]:
                states.append("stressed")
            elif delta > self._config.delta_thresholds[0]:
                states.append("busy")
            else:
                states.append("healthy")

        # Absolute bound state
        if self._recent:
            current_avg = sum(self._recent) / len(self._recent)
            if current_avg > self._config.absolute_bounds[2]:
                states.append("overloaded")
            elif current_avg > self._config.absolute_bounds[1]:
                states.append("stressed")
            elif current_avg > self._config.absolute_bounds[0]:
                states.append("busy")

        # Resource state
        cpu = cpu_percent / 100.0
        if cpu > self._config.cpu_thresholds[2]:
            states.append("overloaded")
        elif cpu > self._config.cpu_thresholds[1]:
            states.append("stressed")
        elif cpu > self._config.cpu_thresholds[0]:
            states.append("busy")

        # Return worst state
        state_order = {"healthy": 0, "busy": 1, "stressed": 2, "overloaded": 3}
        return max(states, key=lambda s: state_order.get(s, 0)) if states else "healthy"
```

**Advantages**:
- Self-calibrating: adapts to workload characteristics
- Less configuration: works across different deployments
- Catches both gradual degradation AND absolute limits
- Trend detection provides early warning

**Disadvantages**:
- Warm-up period required (mitigated by absolute bounds)
- More complex than simple thresholds
- Baseline drift possible over long periods (mitigated by absolute bounds)

### AD-19: Three-Signal Health Model (All Node Types)

**Decision**: Separate node health into three independent signals: Liveness, Readiness, and Progress. Apply this model uniformly to Workers, Managers, and Gates.

**Rationale**:
- All node types run demanding workloads in a distributed system
- Conflating "can't accept work" with "dead" causes premature eviction
- Resource metrics alone are meaningless for heavy workloads
- Progress (throughput) is ground truth for all node types
- Uniform model simplifies reasoning and implementation

**Health Model**:
```
┌─────────────────────────────────────────────────────────────────┐
│                 Three-Signal Worker Health Model                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │  LIVENESS   │  │  READINESS  │  │  PROGRESS   │             │
│  │             │  │             │  │             │             │
│  │ Can respond │  │ Can accept  │  │ Completing  │             │
│  │ to probes?  │  │ new work?   │  │ workflows?  │             │
│  │             │  │             │  │             │             │
│  │ Binary:     │  │ Binary:     │  │ Rate-based: │             │
│  │ yes/no      │  │ yes/no      │  │ completions │             │
│  │             │  │             │  │ per interval│             │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘             │
│         │                │                │                     │
│         ▼                ▼                ▼                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   Decision Matrix                        │   │
│  ├─────────────────────────────────────────────────────────┤   │
│  │ Liveness  Readiness  Progress   →  Action               │   │
│  │ ────────  ─────────  ────────      ──────────────────── │   │
│  │ YES       YES        NORMAL     →  HEALTHY (route work) │   │
│  │ YES       NO         NORMAL     →  BUSY (drain only)    │   │
│  │ YES       YES        LOW        →  SLOW (investigate)   │   │
│  │ YES       NO         LOW        →  DEGRADED (drain)     │   │
│  │ YES       *          ZERO       →  STUCK (drain+timer)  │   │
│  │ NO        *          *          →  SUSPECT (begin evict)│   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Signal Definitions**:

| Signal | Question | Measurement | Failure Threshold |
|--------|----------|-------------|-------------------|
| Liveness | Is process alive? | Ping/pong response | 3 consecutive misses, 30s timeout |
| Readiness | Can accept work? | Self-reported + capacity | `accepting_work=false` OR `capacity=0` |
| Progress | Is work completing? | Completions per interval | `actual_rate < expected_rate * 0.3` |

**Implementation**:
```python
@dataclass
class WorkerHealthState:
    """Unified health state combining all three signals."""
    worker_id: str

    # Signal 1: Liveness
    last_liveness_response: float  # timestamp
    consecutive_liveness_failures: int

    # Signal 2: Readiness
    accepting_work: bool  # reported by worker
    available_capacity: int

    # Signal 3: Progress
    workflows_assigned: int
    completions_last_interval: int
    expected_completion_rate: float

    @property
    def liveness(self) -> bool:
        """Is the worker process alive and responsive?"""
        time_since_response = time.monotonic() - self.last_liveness_response
        return (
            time_since_response < 30.0
            and self.consecutive_liveness_failures < 3
        )

    @property
    def readiness(self) -> bool:
        """Can the worker accept new work?"""
        return self.accepting_work and self.available_capacity > 0

    @property
    def progress_state(self) -> str:
        """Is work completing at expected rate?"""
        if self.workflows_assigned == 0:
            return "idle"

        actual_rate = self.completions_last_interval / max(self.workflows_assigned, 1)

        if actual_rate >= self.expected_completion_rate * 0.8:
            return "normal"
        elif actual_rate >= self.expected_completion_rate * 0.3:
            return "slow"
        elif actual_rate > 0:
            return "degraded"
        else:
            return "stuck"

    def get_routing_decision(self) -> str:
        """Determine action: route, drain, investigate, or evict."""
        if not self.liveness:
            return "evict"

        progress = self.progress_state

        if progress == "stuck" and self.workflows_assigned > 0:
            return "evict"

        if progress in ("slow", "degraded"):
            return "investigate"

        if not self.readiness:
            return "drain"

        return "route"
```

**Why This Model Is Correct**:
| Alternative | Problem |
|-------------|---------|
| Single health score | Conflates independent failure modes |
| Resource thresholds | Doesn't account for expected heavy usage |
| Timeout-only | Can't distinguish slow from stuck |
| Heartbeat-only | Process can heartbeat while frozen |

#### Manager Health (Gate monitors Managers)

Gates monitor manager health to make intelligent DC routing decisions.

**Signal Definitions for Managers**:
| Signal | Question | Measurement | Failure Threshold |
|--------|----------|-------------|-------------------|
| Liveness | Is manager responding? | SWIM probe response | 3 consecutive misses |
| Readiness | Can accept jobs? | Has quorum + accepting jobs | `has_quorum=false` OR `accepting_jobs=false` |
| Progress | Is work flowing? | Job throughput + dispatch rate | `dispatch_rate < expected * 0.3` |

```python
@dataclass
class ManagerHealthState:
    """Three-signal health state for managers (monitored by gates)."""
    manager_id: str
    datacenter_id: str

    # Signal 1: Liveness
    last_liveness_response: float
    consecutive_liveness_failures: int

    # Signal 2: Readiness
    has_quorum: bool  # Can make authoritative decisions
    accepting_jobs: bool  # Self-reported
    active_worker_count: int  # Workers available for dispatch

    # Signal 3: Progress
    jobs_accepted_last_interval: int
    workflows_dispatched_last_interval: int
    expected_throughput: float  # Based on worker capacity

    @property
    def liveness(self) -> bool:
        time_since_response = time.monotonic() - self.last_liveness_response
        return (
            time_since_response < 30.0
            and self.consecutive_liveness_failures < 3
        )

    @property
    def readiness(self) -> bool:
        return (
            self.has_quorum
            and self.accepting_jobs
            and self.active_worker_count > 0
        )

    @property
    def progress_state(self) -> str:
        if self.jobs_accepted_last_interval == 0:
            return "idle"

        actual_rate = self.workflows_dispatched_last_interval
        if actual_rate >= self.expected_throughput * 0.8:
            return "normal"
        elif actual_rate >= self.expected_throughput * 0.3:
            return "slow"
        elif actual_rate > 0:
            return "degraded"
        else:
            return "stuck"

    def get_routing_decision(self) -> str:
        """Determine whether gate should route jobs to this manager."""
        if not self.liveness:
            return "evict"  # Remove from DC's active managers

        progress = self.progress_state

        if progress == "stuck" and self.jobs_accepted_last_interval > 0:
            return "evict"

        if progress in ("slow", "degraded"):
            return "investigate"

        if not self.readiness:
            return "drain"  # Don't send new jobs, let existing complete

        return "route"
```

**Integration with DC Health Classification (AD-16)**:
```
DC Health = f(manager_health_states)

If ALL managers NOT liveness → DC = UNHEALTHY
If MAJORITY managers NOT readiness → DC = DEGRADED
If ANY manager progress == "stuck" → DC = DEGRADED
If ALL managers readiness but NO capacity → DC = BUSY
Otherwise → DC = HEALTHY
```

#### Gate Health (Gates monitor peer Gates)

Gates monitor peer gate health for leader election and job forwarding decisions.

**Signal Definitions for Gates**:
| Signal | Question | Measurement | Failure Threshold |
|--------|----------|-------------|-------------------|
| Liveness | Is gate responding? | SWIM probe response | 3 consecutive misses |
| Readiness | Can handle jobs? | Has DC connectivity + not overloaded | `dc_connectivity=false` OR `overloaded=true` |
| Progress | Is work flowing? | Job forwarding rate + stats aggregation | `forward_rate < expected * 0.3` |

```python
@dataclass
class GateHealthState:
    """Three-signal health state for gates (monitored by peer gates)."""
    gate_id: str

    # Signal 1: Liveness
    last_liveness_response: float
    consecutive_liveness_failures: int

    # Signal 2: Readiness
    has_dc_connectivity: bool  # Can reach at least one DC
    connected_dc_count: int
    overload_state: str  # From HybridOverloadDetector

    # Signal 3: Progress
    jobs_forwarded_last_interval: int
    stats_aggregated_last_interval: int
    expected_forward_rate: float

    @property
    def liveness(self) -> bool:
        time_since_response = time.monotonic() - self.last_liveness_response
        return (
            time_since_response < 30.0
            and self.consecutive_liveness_failures < 3
        )

    @property
    def readiness(self) -> bool:
        return (
            self.has_dc_connectivity
            and self.connected_dc_count > 0
            and self.overload_state not in ("stressed", "overloaded")
        )

    @property
    def progress_state(self) -> str:
        if self.jobs_forwarded_last_interval == 0:
            return "idle"

        actual_rate = self.jobs_forwarded_last_interval
        if actual_rate >= self.expected_forward_rate * 0.8:
            return "normal"
        elif actual_rate >= self.expected_forward_rate * 0.3:
            return "slow"
        elif actual_rate > 0:
            return "degraded"
        else:
            return "stuck"

    def get_routing_decision(self) -> str:
        """Determine whether to forward jobs to this gate."""
        if not self.liveness:
            return "evict"  # Remove from peer list

        progress = self.progress_state

        if progress == "stuck" and self.jobs_forwarded_last_interval > 0:
            return "evict"

        if progress in ("slow", "degraded"):
            return "investigate"

        if not self.readiness:
            return "drain"

        return "route"

    def should_participate_in_election(self) -> bool:
        """Gates with poor health shouldn't become leaders."""
        return (
            self.liveness
            and self.readiness
            and self.progress_state in ("idle", "normal")
        )
```

#### Generic Node Health Infrastructure

```python
from typing import Generic, TypeVar, Protocol

class HealthSignals(Protocol):
    """Protocol for health signal providers."""
    @property
    def liveness(self) -> bool: ...
    @property
    def readiness(self) -> bool: ...
    @property
    def progress_state(self) -> str: ...

T = TypeVar("T", bound=HealthSignals)

class NodeHealthTracker(Generic[T]):
    """Generic health tracker for any node type."""

    def __init__(self, node_type: str):
        self._node_type = node_type
        self._states: dict[str, T] = {}
        self._history: dict[str, deque[str]] = {}  # node_id -> recent decisions

    def update_state(self, node_id: str, state: T) -> None:
        self._states[node_id] = state

    def get_routing_decision(self, node_id: str) -> str:
        if node_id not in self._states:
            return "unknown"
        return self._states[node_id].get_routing_decision()

    def get_healthy_nodes(self) -> list[str]:
        return [
            node_id for node_id, state in self._states.items()
            if state.liveness and state.readiness
        ]

    def should_evict(self, node_id: str) -> tuple[bool, str]:
        """
        Determine if node should be evicted with correlation check.
        Returns (should_evict, reason).
        """
        if node_id not in self._states:
            return False, "unknown node"

        state = self._states[node_id]
        decision = state.get_routing_decision()

        if decision != "evict":
            return False, "healthy"

        # Correlation check: are many nodes failing?
        total = len(self._states)
        failing = sum(
            1 for s in self._states.values()
            if s.get_routing_decision() == "evict"
        )

        if failing > total * 0.5:
            # More than half failing - likely systemic issue
            return False, "systemic failure detected, holding eviction"

        return True, "eviction criteria met"
```

#### SWIM Piggyback for Health State

Health signals are piggybacked on SWIM protocol messages for protocol efficiency:

```python
@dataclass
class HealthPiggyback:
    """Health state embedded in SWIM messages."""
    node_id: str
    node_type: str  # "worker" | "manager" | "gate"

    # Readiness signal
    accepting_work: bool
    capacity: int  # Available slots/cores

    # Progress signal (last interval)
    throughput: int  # Completions/dispatches/forwards
    expected_throughput: int

    # Overload signal (from AD-18)
    overload_state: str  # "healthy" | "busy" | "stressed" | "overloaded"
```

### AD-20: Cancellation Propagation

**Decision**: Implement four-phase cancellation: Client → Gate → Manager → Worker.

**Rationale**:
- Users need ability to stop long-running jobs
- Resources should be freed promptly
- Cancellation must be idempotent and handle partial failures
- Each layer confirms cancellation before propagating

**Cancellation Flow**:
```
┌─────────────────────────────────────────────────────────────────┐
│                    Cancellation Propagation                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Client                Gate                Manager      Worker  │
│    │                    │                    │            │     │
│    │─ CancelJob(id) ───►│                    │            │     │
│    │                    │─ CancelJob(id) ───►│            │     │
│    │                    │                    │─ Cancel ──►│     │
│    │                    │                    │◄── Ack ────│     │
│    │                    │◄─── Ack ───────────│            │     │
│    │◄─── Ack ───────────│                    │            │     │
│    │                    │                    │            │     │
│  Phase 1: Request    Phase 2: Forward     Phase 3: Execute     │
│                      Phase 4: Confirm (reverse direction)       │
│                                                                 │
│  Timeout behavior:                                              │
│  - If Worker doesn't ACK: Manager retries, then marks failed   │
│  - If Manager doesn't ACK: Gate retries, then best-effort      │
│  - Client receives "cancellation requested" immediately        │
│  - Final status pushed when all DCs confirm                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Message Types**:
```python
@dataclass
class JobCancelRequest:
    job_id: str
    requester_id: str  # For audit trail
    timestamp: float
    fence_token: int  # Must match current job epoch

@dataclass
class JobCancelResponse:
    job_id: str
    success: bool
    cancelled_workflow_count: int
    error: str | None = None
```

**Idempotency**: Cancellation requests are idempotent - repeated requests return success if job is already cancelled or cancelling.

### AD-21: Unified Retry Framework with Jitter

**Decision**: Implement a unified retry framework with exponential backoff and jitter for all network operations.

**Rationale**:
- Scattered retry implementations lead to inconsistency
- Without jitter, retries cause thundering herd
- Different jitter strategies suit different scenarios
- Framework enables consistent timeout and backoff across codebase

**Jitter Strategies**:
```
┌─────────────────────────────────────────────────────────────────┐
│                       Jitter Strategies                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Full Jitter (default for most operations):                    │
│  ├─ delay = random(0, min(cap, base * 2^attempt))              │
│  ├─ Best for independent clients                               │
│  └─ Maximum spread, minimum correlation                        │
│                                                                 │
│  Equal Jitter (for operations needing minimum delay):          │
│  ├─ temp = min(cap, base * 2^attempt)                          │
│  ├─ delay = temp/2 + random(0, temp/2)                         │
│  └─ Guarantees minimum delay while spreading                   │
│                                                                 │
│  Decorrelated Jitter (for AWS-style retries):                  │
│  ├─ delay = random(base, previous_delay * 3)                   │
│  ├─ Each retry depends on previous                             │
│  └─ Good spread with bounded growth                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Implementation**:
```python
class JitterStrategy(Enum):
    FULL = "full"
    EQUAL = "equal"
    DECORRELATED = "decorrelated"

@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 0.5  # seconds
    max_delay: float = 30.0  # cap
    jitter: JitterStrategy = JitterStrategy.FULL
    retryable_exceptions: tuple[type[Exception], ...] = (
        ConnectionError,
        TimeoutError,
        OSError,
    )

class RetryExecutor:
    """Unified retry execution with jitter."""

    def __init__(self, config: RetryConfig | None = None):
        self._config = config or RetryConfig()
        self._previous_delay: float = self._config.base_delay

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay with jitter for given attempt."""
        base = self._config.base_delay
        cap = self._config.max_delay

        if self._config.jitter == JitterStrategy.FULL:
            temp = min(cap, base * (2 ** attempt))
            return random.uniform(0, temp)

        elif self._config.jitter == JitterStrategy.EQUAL:
            temp = min(cap, base * (2 ** attempt))
            return temp / 2 + random.uniform(0, temp / 2)

        elif self._config.jitter == JitterStrategy.DECORRELATED:
            delay = random.uniform(base, self._previous_delay * 3)
            delay = min(cap, delay)
            self._previous_delay = delay
            return delay

        return base * (2 ** attempt)  # fallback: no jitter

    async def execute(
        self,
        operation: Callable[[], Awaitable[T]],
        operation_name: str = "operation",
    ) -> T:
        """Execute operation with retry and jitter."""
        last_exception: Exception | None = None

        for attempt in range(self._config.max_attempts):
            try:
                return await operation()
            except self._config.retryable_exceptions as exc:
                last_exception = exc
                if attempt < self._config.max_attempts - 1:
                    delay = self.calculate_delay(attempt)
                    await asyncio.sleep(delay)

        raise last_exception or RuntimeError(f"{operation_name} failed")
```

**Where Jitter Is Applied**:
- Health check intervals
- Retry delays
- Heartbeat timing
- State sync intervals
- Leader election timeouts
- Reconnection attempts

### AD-22: Load Shedding with Priority Queues

**Decision**: Implement load shedding using priority-based request classification.

**Rationale**:
- Under overload, processing all requests degrades all users
- Shedding low-priority work protects critical operations
- Priority should be explicit, not implicit
- Graceful degradation is better than complete failure

**Priority Levels**:
```
┌─────────────────────────────────────────────────────────────────┐
│                    Load Shedding Priority                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Priority 0 (CRITICAL) - Never shed:                           │
│  ├─ Health checks / liveness probes                            │
│  ├─ Cancellation requests                                      │
│  ├─ Final result delivery                                      │
│  └─ Cluster membership (SWIM)                                  │
│                                                                 │
│  Priority 1 (HIGH) - Shed under severe overload:               │
│  ├─ Job submissions                                            │
│  ├─ Workflow dispatch                                          │
│  └─ State sync requests                                        │
│                                                                 │
│  Priority 2 (NORMAL) - Shed under moderate overload:           │
│  ├─ Progress updates                                           │
│  ├─ Stats queries                                              │
│  └─ Reconnection requests                                      │
│                                                                 │
│  Priority 3 (LOW) - Shed first:                                │
│  ├─ Detailed stats                                             │
│  ├─ Debug/diagnostic requests                                  │
│  └─ Non-essential sync                                         │
│                                                                 │
│  Shedding Thresholds (based on overload state):                │
│  ├─ healthy: shed nothing                                      │
│  ├─ busy: shed Priority 3                                      │
│  ├─ stressed: shed Priority 2-3                                │
│  └─ overloaded: shed Priority 1-3 (only CRITICAL processed)   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Implementation**:
```python
class RequestPriority(Enum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3

class LoadShedder:
    """Determines whether to shed requests based on priority and load."""

    def __init__(self, overload_detector: HybridOverloadDetector):
        self._detector = overload_detector

        # Map overload state to minimum priority processed
        self._shed_thresholds: dict[str, int] = {
            "healthy": 4,    # Process all (nothing shed)
            "busy": 3,       # Shed LOW
            "stressed": 2,   # Shed NORMAL and LOW
            "overloaded": 1, # Only CRITICAL (shed HIGH, NORMAL, LOW)
        }

    def should_shed(self, priority: RequestPriority) -> bool:
        """Return True if request should be shed."""
        state = self._detector.get_state()
        min_priority = self._shed_thresholds.get(state, 4)
        return priority.value >= min_priority

    def classify_request(self, message_type: str) -> RequestPriority:
        """Classify request by message type."""
        critical_types = {"ping", "cancel_job", "final_result", "swim_*"}
        high_types = {"job_submit", "workflow_dispatch", "state_sync"}
        normal_types = {"progress_update", "stats_query", "register_callback"}

        if message_type in critical_types:
            return RequestPriority.CRITICAL
        elif message_type in high_types:
            return RequestPriority.HIGH
        elif message_type in normal_types:
            return RequestPriority.NORMAL
        else:
            return RequestPriority.LOW
```

### AD-23: Backpressure for Stats Updates

**Decision**: Implement tiered stats retention with backpressure signaling.

**Rationale**:
- Unbounded stats history causes memory exhaustion
- Different retention needs for different data freshness
- Upstream should slow down when downstream is overwhelmed
- Explicit backpressure prevents silent data loss

**Tiered Retention**:
```
┌─────────────────────────────────────────────────────────────────┐
│                  Tiered Stats Retention                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  HOT (0-60 seconds):                                           │
│  ├─ Full resolution (every update)                             │
│  ├─ In-memory ring buffer                                      │
│  └─ Used for real-time dashboards                              │
│                                                                 │
│  WARM (1-60 minutes):                                          │
│  ├─ 10-second aggregates                                       │
│  ├─ Compressed in-memory                                       │
│  └─ Used for recent history                                    │
│                                                                 │
│  COLD (1-24 hours):                                            │
│  ├─ 1-minute aggregates                                        │
│  ├─ Spill to disk if needed                                    │
│  └─ Used for job post-mortems                                  │
│                                                                 │
│  ARCHIVE (> 24 hours):                                         │
│  ├─ Final summary only                                         │
│  └─ Persisted with job completion                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Backpressure Levels**:
```python
class BackpressureLevel(Enum):
    NONE = 0       # Accept all updates
    THROTTLE = 1   # Reduce update frequency
    BATCH = 2      # Only accept batched updates
    REJECT = 3     # Reject non-critical updates

@dataclass
class StatsBuffer:
    """Bounded stats buffer with backpressure."""
    max_hot_entries: int = 1000
    max_warm_entries: int = 360  # 1 hour at 10s intervals
    max_cold_entries: int = 1440  # 24 hours at 1m intervals

    hot: deque[StatsEntry]
    warm: deque[AggregatedStats]
    cold: deque[AggregatedStats]

    def get_backpressure_level(self) -> BackpressureLevel:
        """Determine backpressure based on buffer fill."""
        hot_fill = len(self.hot) / self.max_hot_entries

        if hot_fill < 0.7:
            return BackpressureLevel.NONE
        elif hot_fill < 0.85:
            return BackpressureLevel.THROTTLE
        elif hot_fill < 0.95:
            return BackpressureLevel.BATCH
        else:
            return BackpressureLevel.REJECT
```

### AD-24: Rate Limiting (Client and Server)

**Decision**: Implement token bucket rate limiting at both client and server sides.

**Rationale**:
- Prevents any single client from overwhelming the system
- Server-side is authoritative; client-side is cooperative
- Token bucket allows bursts while enforcing average rate
- Per-client tracking enables fair sharing

**Implementation**:
```
┌─────────────────────────────────────────────────────────────────┐
│                    Rate Limiting Architecture                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Client-Side (cooperative):                                     │
│  ├─ Pre-flight check before sending                            │
│  ├─ Respects server's rate limit headers                       │
│  └─ Delays requests when approaching limit                     │
│                                                                 │
│  Server-Side (authoritative):                                   │
│  ├─ Per-client token buckets                                   │
│  ├─ Returns 429 with Retry-After when exceeded                 │
│  └─ Different limits for different operation types             │
│                                                                 │
│  Token Bucket Parameters:                                       │
│  ├─ bucket_size: Maximum burst capacity                        │
│  ├─ refill_rate: Tokens added per second                       │
│  └─ current_tokens: Available tokens                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```python
class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, bucket_size: int, refill_rate: float):
        self._bucket_size = bucket_size
        self._refill_rate = refill_rate
        self._tokens = float(bucket_size)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens. Returns False if rate limited."""
        async with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self._bucket_size,
            self._tokens + elapsed * self._refill_rate
        )
        self._last_refill = now

class ServerRateLimiter:
    """Server-side rate limiter with per-client buckets."""

    def __init__(self, default_config: RateLimitConfig):
        self._config = default_config
        self._buckets: dict[str, TokenBucket] = {}

    def check_rate_limit(self, client_id: str, operation: str) -> tuple[bool, float]:
        """Check if request is allowed. Returns (allowed, retry_after)."""
        bucket = self._get_or_create_bucket(client_id, operation)
        if bucket.acquire(1):
            return True, 0.0
        else:
            retry_after = 1.0 / bucket._refill_rate
            return False, retry_after
```

### AD-25: Version Skew Handling

**Decision**: Support rolling upgrades via protocol versioning and capability negotiation.

**Rationale**:
- Zero-downtime upgrades require version compatibility
- Nodes must handle messages from older/newer versions
- Unknown fields should be ignored, not rejected
- Capability advertisement enables gradual feature rollout

**Protocol Versioning**:
```
┌─────────────────────────────────────────────────────────────────┐
│                    Version Skew Handling                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Version Format: MAJOR.MINOR                                    │
│  ├─ MAJOR: Breaking changes (must match)                       │
│  └─ MINOR: Additive changes (newer can talk to older)          │
│                                                                 │
│  Handshake includes:                                            │
│  ├─ protocol_version: "1.2"                                    │
│  ├─ capabilities: ["cancellation", "batched_stats", ...]       │
│  └─ node_version: "hyperscale-0.5.0" (informational)           │
│                                                                 │
│  Compatibility Rules:                                           │
│  ├─ Same MAJOR: compatible                                     │
│  ├─ Different MAJOR: reject connection                         │
│  ├─ Newer MINOR → older: use older's feature set               │
│  └─ Older MINOR → newer: newer ignores unknown capabilities    │
│                                                                 │
│  Message Handling:                                              │
│  ├─ Unknown fields: ignore (forward compatibility)             │
│  ├─ Missing optional fields: use defaults                      │
│  └─ Missing required fields: reject with clear error           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Implementation**:
```python
@dataclass
class ProtocolVersion:
    major: int
    minor: int

    def is_compatible_with(self, other: "ProtocolVersion") -> bool:
        return self.major == other.major

    def supports_feature(self, other: "ProtocolVersion", feature: str) -> bool:
        """Check if feature is supported by both versions."""
        # Feature was added in version X.Y
        feature_versions = {
            "cancellation": (1, 0),
            "batched_stats": (1, 1),
            "client_reconnection": (1, 2),
            "fence_tokens": (1, 2),
        }
        required = feature_versions.get(feature, (999, 999))
        return (
            (self.major, self.minor) >= required
            and (other.major, other.minor) >= required
        )

@dataclass
class NodeCapabilities:
    protocol_version: ProtocolVersion
    capabilities: set[str]
    node_version: str  # Informational

    def negotiate(self, other: "NodeCapabilities") -> set[str]:
        """Return capabilities supported by both nodes."""
        return self.capabilities & other.capabilities
```

### AD-26: Adaptive Healthcheck Extensions

**Decision**: Allow healthcheck deadline extensions with logarithmic grant reduction.

**Rationale**:
- Long-running operations may legitimately need more time
- Unlimited extensions enable abuse
- Logarithmic reduction discourages repeated requests
- Extensions require active negotiation (not automatic)

**Extension Protocol**:
```
┌─────────────────────────────────────────────────────────────────┐
│              Adaptive Healthcheck Extensions                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Base deadline: 30 seconds                                      │
│                                                                 │
│  Extension grants (logarithmic reduction):                      │
│  ├─ 1st extension: +30s (100% of base)                         │
│  ├─ 2nd extension: +15s (50% of base)                          │
│  ├─ 3rd extension: +7.5s (25% of base)                         │
│  ├─ 4th extension: +3.75s (12.5% of base)                      │
│  └─ ...converges to minimum (1s)                               │
│                                                                 │
│  Formula: grant = max(min_grant, base / (2^extension_count))   │
│                                                                 │
│  Extension request must include:                                │
│  ├─ reason: "long_workflow" | "gc_pause" | "resource_contention"│
│  ├─ estimated_completion: timestamp                            │
│  └─ current_progress: 0.0-1.0                                  │
│                                                                 │
│  Extension denied if:                                           │
│  ├─ No progress since last extension                           │
│  ├─ Total extensions exceed max (e.g., 5)                      │
│  └─ Node is already marked suspect                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Implementation**:
```python
@dataclass
class ExtensionTracker:
    """Tracks healthcheck extensions for a worker."""
    worker_id: str
    base_deadline: float = 30.0
    min_grant: float = 1.0
    max_extensions: int = 5

    extension_count: int = 0
    last_progress: float = 0.0
    total_extended: float = 0.0

    def request_extension(
        self,
        reason: str,
        current_progress: float,
    ) -> tuple[bool, float]:
        """
        Request deadline extension.
        Returns (granted, extension_seconds).
        """
        # Deny if too many extensions
        if self.extension_count >= self.max_extensions:
            return False, 0.0

        # Deny if no progress
        if current_progress <= self.last_progress and self.extension_count > 0:
            return False, 0.0

        # Calculate grant with logarithmic reduction
        grant = max(
            self.min_grant,
            self.base_deadline / (2 ** self.extension_count)
        )

        self.extension_count += 1
        self.last_progress = current_progress
        self.total_extended += grant

        return True, grant

    def reset(self) -> None:
        """Reset tracker when worker completes operation or recovers."""
        self.extension_count = 0
        self.last_progress = 0.0
        self.total_extended = 0.0
```

**Message Types**:
```python
@dataclass
class HealthcheckExtensionRequest:
    """Worker requests more time before being marked unhealthy."""
    worker_id: str
    reason: str  # "long_workflow" | "gc_pause" | "resource_contention"
    current_progress: float  # 0.0 to 1.0
    estimated_completion: float  # Unix timestamp
    active_workflow_count: int

@dataclass
class HealthcheckExtensionResponse:
    """Manager response to extension request."""
    granted: bool
    extension_seconds: float  # 0.0 if not granted
    new_deadline: float  # Unix timestamp of new deadline
    remaining_extensions: int  # How many more can be requested
    denial_reason: str | None = None  # If not granted
```

**Complete Protocol Flow Example**:
```
┌─────────────────────────────────────────────────────────────────┐
│           Healthcheck Extension Protocol Flow                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Worker                                    Manager              │
│    │                                          │                 │
│    │◄──── Healthcheck probe ─────────────────│ (deadline: 30s) │
│    │                                          │                 │
│    │  [Running long workflow, needs more time]│                 │
│    │                                          │                 │
│    │─── ExtensionRequest(progress=0.3) ─────►│                 │
│    │                                          │                 │
│    │    [Manager: extension_count=0]          │                 │
│    │    [Grant: 30s / 2^0 = 30s]              │                 │
│    │                                          │                 │
│    │◄── ExtensionResponse(granted=True, 30s)─│ (deadline: 60s) │
│    │                                          │                 │
│    │  [Still working...]                      │                 │
│    │                                          │                 │
│    │─── ExtensionRequest(progress=0.6) ─────►│                 │
│    │                                          │                 │
│    │    [Manager: extension_count=1]          │                 │
│    │    [Grant: 30s / 2^1 = 15s]              │                 │
│    │                                          │                 │
│    │◄── ExtensionResponse(granted=True, 15s)─│ (deadline: 75s) │
│    │                                          │                 │
│    │─── ExtensionRequest(progress=0.6) ─────►│ [NO PROGRESS!]  │
│    │                                          │                 │
│    │◄── ExtensionResponse(granted=False) ────│ (denied)        │
│    │                                          │                 │
│    │  [Worker marked SUSPECT after deadline] │                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Manager-Side Integration**:
```python
class WorkerHealthManager:
    """Manages worker health with extension support."""

    def __init__(self):
        self._extension_trackers: dict[str, ExtensionTracker] = {}
        self._worker_deadlines: dict[str, float] = {}

    def handle_extension_request(
        self,
        request: HealthcheckExtensionRequest,
    ) -> HealthcheckExtensionResponse:
        """Process extension request from worker."""
        tracker = self._extension_trackers.setdefault(
            request.worker_id,
            ExtensionTracker(worker_id=request.worker_id)
        )

        granted, extension_seconds = tracker.request_extension(
            reason=request.reason,
            current_progress=request.current_progress,
        )

        if granted:
            current_deadline = self._worker_deadlines.get(
                request.worker_id,
                time.monotonic() + 30.0
            )
            new_deadline = current_deadline + extension_seconds
            self._worker_deadlines[request.worker_id] = new_deadline

            return HealthcheckExtensionResponse(
                granted=True,
                extension_seconds=extension_seconds,
                new_deadline=new_deadline,
                remaining_extensions=tracker.max_extensions - tracker.extension_count,
            )
        else:
            denial_reason = self._get_denial_reason(tracker, request)
            return HealthcheckExtensionResponse(
                granted=False,
                extension_seconds=0.0,
                new_deadline=self._worker_deadlines.get(request.worker_id, 0.0),
                remaining_extensions=max(0, tracker.max_extensions - tracker.extension_count),
                denial_reason=denial_reason,
            )

    def _get_denial_reason(
        self,
        tracker: ExtensionTracker,
        request: HealthcheckExtensionRequest,
    ) -> str:
        if tracker.extension_count >= tracker.max_extensions:
            return f"Maximum extensions ({tracker.max_extensions}) exceeded"
        if request.current_progress <= tracker.last_progress:
            return f"No progress since last extension (was {tracker.last_progress}, now {request.current_progress})"
        return "Extension denied"

    def on_worker_healthy(self, worker_id: str) -> None:
        """Reset extension tracker when worker completes successfully."""
        if worker_id in self._extension_trackers:
            self._extension_trackers[worker_id].reset()
```

**Grant Reduction Table**:
| Extension # | Formula | Grant (base=30s) | Cumulative |
|-------------|---------|------------------|------------|
| 1 | 30 / 2^0 | 30.0s | 30.0s |
| 2 | 30 / 2^1 | 15.0s | 45.0s |
| 3 | 30 / 2^2 | 7.5s | 52.5s |
| 4 | 30 / 2^3 | 3.75s | 56.25s |
| 5 | 30 / 2^4 | 1.875s → 1.0s (min) | 57.25s |
| 6+ | — | denied | — |

**Key Properties**:
- **Converging**: Total extension converges (geometric series)
- **Progress-gated**: Must show forward progress to get more time
- **Bounded**: Hard limit on extension count prevents indefinite delays
- **Self-limiting**: Diminishing returns discourage dependency on extensions

### AD-27: Gate Module Reorganization

**Decision**: Reorganize gate-related code into focused modules following manager patterns.

**Rationale**:
- Current gate.py is monolithic and hard to maintain
- Similar to manager refactoring already completed
- One class per file improves testability
- Clear module boundaries reduce coupling

**Proposed Structure**:
```
hyperscale/distributed_rewrite/
├── jobs/
│   ├── gates/                    # Gate-side job management
│   │   ├── __init__.py
│   │   ├── gate_job_manager.py   # Per-job state and locking
│   │   ├── job_forwarding.py     # Cross-gate job forwarding
│   │   └── consistent_hash.py    # Per-job gate ownership
│   │
│   ├── managers/                 # Manager-side (existing)
│   │   ├── __init__.py
│   │   ├── job_manager.py
│   │   ├── worker_pool.py
│   │   └── workflow_dispatcher.py
│   │
│   └── __init__.py
│
├── datacenters/                  # DC-level coordination
│   ├── __init__.py
│   ├── datacenter_health.py      # DatacenterHealthManager
│   ├── manager_dispatcher.py     # ManagerDispatcher
│   └── lease_manager.py          # DC lease management
│
├── reliability/                  # Cross-cutting reliability
│   ├── __init__.py
│   ├── retry.py                  # RetryExecutor
│   ├── circuit_breaker.py        # CircuitBreaker
│   ├── load_shedding.py          # LoadShedder
│   ├── backpressure.py           # BackpressureController
│   ├── rate_limiting.py          # TokenBucket, RateLimiter
│   ├── overload.py               # HybridOverloadDetector
│   └── jitter.py                 # Jitter utilities
│
├── health/                       # Health checking
│   ├── __init__.py
│   ├── worker_health.py          # WorkerHealthState, three-signal model
│   ├── extension_tracker.py      # Adaptive extensions
│   └── probes.py                 # Liveness/Readiness probe implementations
│
└── swim/
    └── gates/                    # Gate SWIM extensions
        ├── __init__.py
        └── peer_topology.py      # GatePeerTopology
```

**Migration Plan**:
1. Create new module directories
2. Extract classes one at a time (preserve behavior)
3. Update imports in gate.py incrementally
4. Add tests for each extracted class
5. Final cleanup of gate.py

---

### AD-28: Enhanced DNS Discovery with Peer Selection

**Decision**: Implement a robust, locality-aware peer discovery and selection system using Weighted Rendezvous Hashing combined with Adaptive EWMA-based selection, bounded connection pools, and comprehensive security validation.

**Rationale**:
- Current static seed approach doesn't scale for globally distributed deployments
- Need to prevent accidental cross-cluster and cross-environment joins
- Role-based security prevents workers from directly contacting gates or vice versa
- Locality awareness reduces latency by preferring same-DC peers
- Adaptive selection handles heterogeneous peer performance gracefully
- Sticky connections reduce connection churn while allowing health-based eviction

**Problem Statement**:
In a globally distributed performance testing framework, peers can:
1. Be in different datacenters with varying latencies (1ms same-DC vs 200ms cross-region)
2. Experience temporary overload during test execution
3. Crash and restart with different IPs (Kubernetes pod replacement)
4. Be misconfigured to accidentally join wrong cluster/environment
5. Attempt unauthorized role-based connections (worker→gate should be blocked)

#### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                     ENHANCED DNS DISCOVERY ARCHITECTURE                               │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐ │
│  │                           LAYER 1: DNS RESOLUTION                                │ │
│  │                                                                                   │ │
│  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐     │ │
│  │  │   Static     │   │     DNS      │   │   Negative   │   │   Positive   │     │ │
│  │  │   Seeds      │   │   Resolver   │   │    Cache     │   │    Cache     │     │ │
│  │  │              │   │              │   │              │   │              │     │ │
│  │  │ 10.0.1.5:9000│   │ SRV records  │   │ Failed hosts │   │ Resolved IPs │     │ │
│  │  │ 10.0.1.6:9000│   │ + A records  │   │ (30s TTL)    │   │ (DNS TTL)    │     │ │
│  │  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘     │ │
│  │         │                  │                  │                  │              │ │
│  │         └──────────────────┴──────────────────┴──────────────────┘              │ │
│  │                                   │                                              │ │
│  │                                   ▼                                              │ │
│  │                        ┌─────────────────────┐                                  │ │
│  │                        │  Candidate Set      │                                  │ │
│  │                        │  (all discovered)   │                                  │ │
│  │                        └──────────┬──────────┘                                  │ │
│  └───────────────────────────────────┼──────────────────────────────────────────────┘ │
│                                      │                                                │
│  ┌───────────────────────────────────┼──────────────────────────────────────────────┐ │
│  │                           LAYER 2: SECURITY VALIDATION                           │ │
│  │                                   │                                              │ │
│  │                                   ▼                                              │ │
│  │                        ┌─────────────────────┐                                  │ │
│  │                        │  Cluster ID Check   │ ─── Reject if cluster_id ≠ ours  │ │
│  │                        └──────────┬──────────┘                                  │ │
│  │                                   │                                              │ │
│  │                                   ▼                                              │ │
│  │                        ┌─────────────────────┐                                  │ │
│  │                        │ Environment Check   │ ─── Reject if env_id ≠ ours      │ │
│  │                        └──────────┬──────────┘                                  │ │
│  │                                   │                                              │ │
│  │                                   ▼                                              │ │
│  │                        ┌─────────────────────┐                                  │ │
│  │                        │  Role Validation    │ ─── Check mTLS cert claims       │ │
│  │                        └──────────┬──────────┘                                  │ │
│  └───────────────────────────────────┼──────────────────────────────────────────────┘ │
│                                      │                                                │
│  ┌───────────────────────────────────┼──────────────────────────────────────────────┐ │
│  │                           LAYER 3: LOCALITY FILTER                               │ │
│  │                                   │                                              │ │
│  │                                   ▼                                              │ │
│  │     ┌─────────────────────────────────────────────────────────────────────┐    │ │
│  │     │                      LOCALITY TIERS                                   │    │ │
│  │     │                                                                       │    │ │
│  │     │   Tier 0 (preferred): Same datacenter         (latency < 2ms)        │    │ │
│  │     │   Tier 1 (fallback):  Same region             (latency < 50ms)       │    │ │
│  │     │   Tier 2 (emergency): Global (any DC)         (latency varies)       │    │ │
│  │     │                                                                       │    │ │
│  │     │   Selection: Try Tier 0 first. If < min_peers, add Tier 1, etc.      │    │ │
│  │     │                                                                       │    │ │
│  │     └─────────────────────────────────────────────────────────────────────┘    │ │
│  │                                   │                                              │ │
│  │                                   ▼                                              │ │
│  │                        ┌─────────────────────┐                                  │ │
│  │                        │  Locality-Filtered  │                                  │ │
│  │                        │   Candidate Set     │                                  │ │
│  │                        └──────────┬──────────┘                                  │ │
│  └───────────────────────────────────┼──────────────────────────────────────────────┘ │
│                                      │                                                │
│  ┌───────────────────────────────────┼──────────────────────────────────────────────┐ │
│  │                           LAYER 4: PEER SELECTION                                │ │
│  │                                   │                                              │ │
│  │                                   ▼                                              │ │
│  │     ┌─────────────────────────────────────────────────────────────────────┐    │ │
│  │     │           WEIGHTED RENDEZVOUS HASH + POWER OF TWO CHOICES            │    │ │
│  │     │                                                                       │    │ │
│  │     │  Step 1: Rendezvous Hash produces deterministic candidate ranking    │    │ │
│  │     │          score = hash(peer_id || selector_id || role) * health_weight│    │ │
│  │     │          → Top K candidates (K=8)                                    │    │ │
│  │     │                                                                       │    │ │
│  │     │  Step 2: Power of Two Choices for load balancing                     │    │ │
│  │     │          From K candidates, randomly sample 2                        │    │ │
│  │     │          Compare their EWMA latency scores                           │    │ │
│  │     │          Choose the one with lower latency                           │    │ │
│  │     │                                                                       │    │ │
│  │     │  Step 3: Maintain sticky primary (K=3) and backup (K=2) connections  │    │ │
│  │     │          Only switch when health degrades significantly              │    │ │
│  │     │                                                                       │    │ │
│  │     └─────────────────────────────────────────────────────────────────────┘    │ │
│  │                                   │                                              │ │
│  │                                   ▼                                              │ │
│  │                        ┌─────────────────────┐                                  │ │
│  │                        │   Selected Peers    │                                  │ │
│  │                        │   (3 primary +      │                                  │ │
│  │                        │    2 backup)        │                                  │ │
│  │                        └──────────┬──────────┘                                  │ │
│  └───────────────────────────────────┼──────────────────────────────────────────────┘ │
│                                      │                                                │
│  ┌───────────────────────────────────┼──────────────────────────────────────────────┐ │
│  │                           LAYER 5: CONNECTION POOL                               │ │
│  │                                   │                                              │ │
│  │                                   ▼                                              │ │
│  │     ┌─────────────────────────────────────────────────────────────────────┐    │ │
│  │     │                    STICKY CONNECTION POOL                             │    │ │
│  │     │                                                                       │    │ │
│  │     │  Primary Connections (3):                                            │    │ │
│  │     │  ┌─────────┐  ┌─────────┐  ┌─────────┐                              │    │ │
│  │     │  │ Peer A  │  │ Peer B  │  │ Peer C  │   Active connections        │    │ │
│  │     │  │ EWMA:2ms│  │ EWMA:3ms│  │ EWMA:5ms│   Round-robin for requests  │    │ │
│  │     │  └─────────┘  └─────────┘  └─────────┘                              │    │ │
│  │     │                                                                       │    │ │
│  │     │  Backup Connections (2):                                             │    │ │
│  │     │  ┌─────────┐  ┌─────────┐                                           │    │ │
│  │     │  │ Peer D  │  │ Peer E  │   Ready to promote on primary failure     │    │ │
│  │     │  │ EWMA:8ms│  │EWMA:10ms│                                           │    │ │
│  │     │  └─────────┘  └─────────┘                                           │    │ │
│  │     │                                                                       │    │ │
│  │     │  Eviction Policy:                                                    │    │ │
│  │     │  - error_rate > 5%  OR                                               │    │ │
│  │     │  - consecutive_failures > 3  OR                                      │    │ │
│  │     │  - latency > p99_baseline * 3                                        │    │ │
│  │     │                                                                       │    │ │
│  │     │  On eviction: Promote backup → primary, replenish from candidates    │    │ │
│  │     │                                                                       │    │ │
│  │     └─────────────────────────────────────────────────────────────────────┘    │ │
│  └──────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

#### Security: Cluster ID and Environment ID

Prevents accidental cross-cluster and cross-environment joins:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                     CLUSTER/ENVIRONMENT ISOLATION                                    │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  Problem: Misconfigured node in staging tries to join production cluster             │
│                                                                                       │
│  ┌────────────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                                  │ │
│  │    STAGING NODE                        PRODUCTION CLUSTER                       │ │
│  │    cluster_id: "hyperscale-staging"    cluster_id: "hyperscale-prod"           │ │
│  │    env_id: "staging"                   env_id: "production"                    │ │
│  │                                                                                  │ │
│  │         │                                       │                               │ │
│  │         │──── Registration Request ────────────▶│                               │ │
│  │         │     cluster_id: "hyperscale-staging" │                               │ │
│  │         │                                       │                               │ │
│  │         │◀─── REJECT: cluster_id mismatch ─────│                               │ │
│  │         │     expected: "hyperscale-prod"       │                               │ │
│  │         │                                       │                               │ │
│  └────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                       │
│  Configuration:                                                                      │
│  ```python                                                                           │
│  @dataclass(slots=True)                                                             │
│  class DiscoveryConfig:                                                             │
│      cluster_id: str         # Required - unique cluster identifier                 │
│      environment_id: str     # Required - prod/staging/dev                          │
│      ...                                                                            │
│  ```                                                                                │
│                                                                                       │
│  Wire Protocol Addition:                                                            │
│  - All registration messages include cluster_id and environment_id                  │
│  - Receiver validates BEFORE processing any other fields                            │
│  - Mismatch results in immediate rejection with clear error message                 │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

#### Security: Role-Based Connection Matrix

mTLS certificate claims enforce which node types can communicate:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        ROLE-BASED CONNECTION MATRIX                                  │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  Certificate Claim Format:                                                           │
│  ┌────────────────────────────────────────────────────────────────────────────────┐ │
│  │  Subject Alternative Name (SAN):                                                │ │
│  │    URI: hyperscale://role/{worker|manager|gate|client}                         │ │
│  │    URI: hyperscale://cluster/{cluster_id}                                      │ │
│  │    URI: hyperscale://env/{environment_id}                                      │ │
│  │    URI: hyperscale://dc/{datacenter_id}                                        │ │
│  └────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                       │
│  Connection Matrix:                                                                  │
│  ┌────────────┬─────────────────────────────────────────────────────────────────┐  │
│  │  Initiator │                        Can Connect To                            │  │
│  ├────────────┼──────────┬──────────┬──────────┬──────────────────────────────────┤  │
│  │            │  Worker  │  Manager │   Gate   │   Client                       │  │
│  ├────────────┼──────────┼──────────┼──────────┼──────────────────────────────────┤  │
│  │  Client    │    ❌    │    ❌    │    ✅    │      ❌                        │  │
│  │            │          │          │ (submit) │                                │  │
│  ├────────────┼──────────┼──────────┼──────────┼──────────────────────────────────┤  │
│  │  Gate      │    ❌    │    ✅    │    ✅    │      ✅ (push)                 │  │
│  │            │          │ (forward)│ (peer)   │                                │  │
│  ├────────────┼──────────┼──────────┼──────────┼──────────────────────────────────┤  │
│  │  Manager   │    ✅    │    ✅    │    ✅    │      ✅ (push)                 │  │
│  │            │(dispatch)│  (peer)  │ (report) │                                │  │
│  ├────────────┼──────────┼──────────┼──────────┼──────────────────────────────────┤  │
│  │  Worker    │    ❌    │    ✅    │    ❌    │      ❌                        │  │
│  │            │          │(progress)│          │                                │  │
│  └────────────┴──────────┴──────────┴──────────┴──────────────────────────────────┘  │
│                                                                                       │
│  Example Rejection:                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────────┐ │
│  │  Worker (role=worker) attempts to connect to Gate (role=gate)                  │ │
│  │                                                                                  │ │
│  │  Gate extracts initiator role from mTLS cert: "worker"                         │ │
│  │  Gate checks: is "worker" in allowed_initiators? NO                            │ │
│  │  Gate rejects: "Connection denied: role 'worker' cannot connect to 'gate'"     │ │
│  └────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

#### Peer Selection Algorithm: Weighted Rendezvous Hash + Power of Two Choices

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    PEER SELECTION ALGORITHM                                          │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  STEP 1: WEIGHTED RENDEZVOUS HASH (for deterministic candidate ranking)             │
│  ─────────────────────────────────────────────────────────────────────────────────  │
│                                                                                       │
│  For each peer P in the locality-filtered candidate set:                            │
│                                                                                       │
│    base_score = hash(peer_id || selector_id || role)                                │
│    health_weight = 1.0 - (error_rate * 2) - (latency_factor * 0.5)                 │
│    weighted_score = base_score * max(0.1, health_weight)                            │
│                                                                                       │
│  Sort by weighted_score descending → Top K candidates (K=8)                         │
│                                                                                       │
│  Why Rendezvous Hash?                                                               │
│  - Deterministic: same inputs always produce same ranking (debuggable)              │
│  - Minimal disruption: adding/removing peer only affects that peer's connections    │
│  - No central coordination needed                                                    │
│                                                                                       │
│  ─────────────────────────────────────────────────────────────────────────────────  │
│  STEP 2: POWER OF TWO CHOICES (for load balancing among candidates)                 │
│  ─────────────────────────────────────────────────────────────────────────────────  │
│                                                                                       │
│  From K candidates, to select one connection:                                       │
│                                                                                       │
│    candidate_a = random.choice(candidates)                                          │
│    candidate_b = random.choice(candidates - {candidate_a})                          │
│    chosen = candidate_a if ewma_latency[a] < ewma_latency[b] else candidate_b       │
│                                                                                       │
│  Why Power of Two?                                                                   │
│  - Avoids thundering herd (not everyone picks the "best")                           │
│  - Automatically load balances across peers                                         │
│  - O(1) selection vs O(n) for finding global minimum                                │
│                                                                                       │
│  ─────────────────────────────────────────────────────────────────────────────────  │
│  STEP 3: ADAPTIVE EWMA LATENCY TRACKING                                             │
│  ─────────────────────────────────────────────────────────────────────────────────  │
│                                                                                       │
│  For each request to peer P:                                                        │
│                                                                                       │
│    measured_latency = response_time - request_time                                  │
│    ewma[P] = α * measured_latency + (1 - α) * ewma[P]                              │
│                                                                                       │
│  Where α = 0.2 (balance between responsiveness and stability)                       │
│                                                                                       │
│  Benefits:                                                                           │
│  - Smooths transient spikes (one slow request doesn't cause failover)               │
│  - Adapts to persistent degradation                                                 │
│  - Simple to compute and store                                                       │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

#### Sticky Connections with Health-Based Eviction

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    STICKY CONNECTION LIFECYCLE                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  Initial State:                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  PRIMARY (3)          BACKUP (2)           CANDIDATE POOL (K=8)            │   │
│  │  [A, B, C]            [D, E]               [A, B, C, D, E, F, G, H]         │   │
│  │  (active)             (warm standby)        (from rendezvous hash)          │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                       │
│  Request Routing:                                                                    │
│  - Round-robin across PRIMARY connections                                           │
│  - Track latency per request for EWMA                                               │
│  - Track errors per connection                                                       │
│                                                                                       │
│  Health Monitoring (per connection):                                                │
│  ┌────────────────────────────────────────────────────────────────────────────┐    │
│  │  Metric               │  Threshold        │  Action                        │    │
│  ├───────────────────────┼───────────────────┼─────────────────────────────────┤    │
│  │  error_rate           │  > 5%             │  Mark DEGRADED                 │    │
│  │  consecutive_failures │  > 3              │  Mark UNHEALTHY → evict        │    │
│  │  ewma_latency         │  > p99 * 3        │  Mark SLOW → evict             │    │
│  │  connection_age       │  > 1 hour         │  Consider refresh              │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
│  Eviction Sequence:                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                              │   │
│  │  t=0   PRIMARY: [A, B, C]    BACKUP: [D, E]                                 │   │
│  │        Peer B: consecutive_failures = 4 (threshold = 3)                     │   │
│  │                                                                              │   │
│  │  t=1   Evict B from PRIMARY                                                 │   │
│  │        PRIMARY: [A, _, C]    BACKUP: [D, E]                                 │   │
│  │                                                                              │   │
│  │  t=2   Promote D to PRIMARY                                                 │   │
│  │        PRIMARY: [A, D, C]    BACKUP: [_, E]                                 │   │
│  │                                                                              │   │
│  │  t=3   Replenish BACKUP from candidate pool (with jitter: 100-500ms)        │   │
│  │        Select F using Power of Two Choices                                  │   │
│  │        PRIMARY: [A, D, C]    BACKUP: [F, E]                                 │   │
│  │                                                                              │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

#### Discovery Timing and Jitter

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    TIMING CONFIGURATION                                              │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  DNS Resolution:                                                                     │
│  ┌────────────────────────────────────────────────────────────────────────────┐    │
│  │  dns_timeout:           2.0 seconds                                        │    │
│  │  dns_cache_ttl:         Respect DNS TTL (or default 30s)                   │    │
│  │  negative_cache_ttl:    30 seconds (don't hammer failed lookups)           │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
│  Peer Probing:                                                                       │
│  ┌────────────────────────────────────────────────────────────────────────────┐    │
│  │  probe_timeout:         500ms per probe                                    │    │
│  │  max_concurrent_probes: 10 (prevent socket exhaustion)                     │    │
│  │  probe_jitter:          0-100ms (prevent synchronized probing)             │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
│  Backoff (when all probes fail):                                                    │
│  ┌────────────────────────────────────────────────────────────────────────────┐    │
│  │  initial_backoff:       500ms                                              │    │
│  │  max_backoff:           15 seconds                                         │    │
│  │  backoff_multiplier:    2.0                                                │    │
│  │  jitter_factor:         0.25 (25% randomization)                           │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
│  Discovery Refresh:                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐    │
│  │  refresh_interval:      60 seconds (re-evaluate candidate set)             │    │
│  │  refresh_jitter:        0-5 seconds (prevent synchronized refresh)         │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
│  Connection Pool:                                                                    │
│  ┌────────────────────────────────────────────────────────────────────────────┐    │
│  │  promotion_jitter:      100-500ms (prevent synchronized recovery)          │    │
│  │  connection_max_age:    3600 seconds (1 hour, then consider refresh)       │    │
│  │  ewma_alpha:            0.2 (balance responsiveness vs stability)          │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

#### Metrics and Observability

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    DISCOVERY METRICS                                                 │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  DNS Metrics:                                                                        │
│  ┌────────────────────────────────────────────────────────────────────────────┐    │
│  │  discovery_dns_lookups_total{datacenter, result}                           │    │
│  │    - result: "success" | "timeout" | "error" | "negative_cached"           │    │
│  │                                                                              │    │
│  │  discovery_dns_cache_hits_total{type}                                      │    │
│  │    - type: "positive" | "negative"                                         │    │
│  │                                                                              │    │
│  │  discovery_dns_resolution_duration_ms{datacenter}                          │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
│  Selection Metrics:                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐    │
│  │  discovery_candidate_set_size{role, datacenter}                            │    │
│  │  discovery_candidate_set_changes_total{reason}                             │    │
│  │    - reason: "dns_update" | "health_change" | "peer_added" | "peer_removed"│    │
│  │                                                                              │    │
│  │  discovery_locality_tier_selected_total{tier}                              │    │
│  │    - tier: "same_dc" | "same_region" | "global"                            │    │
│  │                                                                              │    │
│  │  discovery_selection_duration_ms                                           │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
│  Connection Pool Metrics:                                                            │
│  ┌────────────────────────────────────────────────────────────────────────────┐    │
│  │  discovery_pool_connections{state, role}                                   │    │
│  │    - state: "primary" | "backup"                                           │    │
│  │                                                                              │    │
│  │  discovery_pool_promotions_total{from_state, to_state}                     │    │
│  │  discovery_pool_evictions_total{reason}                                    │    │
│  │    - reason: "error_rate" | "consecutive_failures" | "latency" | "stale"   │    │
│  │                                                                              │    │
│  │  discovery_peer_ewma_latency_ms{peer_id, datacenter}                       │    │
│  │  discovery_peer_error_rate{peer_id}                                        │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
│  Security Metrics:                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────────┐    │
│  │  discovery_cluster_id_rejections_total{expected, received}                 │    │
│  │  discovery_environment_id_rejections_total{expected, received}             │    │
│  │  discovery_role_rejections_total{initiator_role, target_role}              │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                       │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

#### Configuration

```python
@dataclass(slots=True)
class DiscoveryConfig:
    """Configuration for enhanced peer discovery."""

    # ===== Security (Required) =====
    cluster_id: str              # Unique cluster identifier (e.g., "hyperscale-prod")
    environment_id: str          # Environment (e.g., "production", "staging")

    # ===== DNS Configuration =====
    dns_names: list[str] = field(default_factory=list)  # SRV/A records to resolve
    static_seeds: list[str] = field(default_factory=list)  # Fallback addresses
    dns_timeout: float = 2.0
    dns_cache_ttl: float = 30.0  # Override if DNS doesn't provide TTL
    negative_cache_ttl: float = 30.0  # Don't re-resolve failed names

    # ===== Locality =====
    datacenter_id: str = ""      # This node's datacenter
    region_id: str = ""          # This node's region (group of DCs)
    prefer_same_dc: bool = True
    prefer_same_region: bool = True
    min_peers_per_tier: int = 3  # Minimum before falling back to next tier

    # ===== Peer Selection =====
    candidate_set_size: int = 8           # K for rendezvous hash
    primary_connections: int = 3          # Active connections
    backup_connections: int = 2           # Warm standby
    ewma_alpha: float = 0.2               # Latency smoothing factor

    # ===== Health Thresholds =====
    error_rate_threshold: float = 0.05    # 5% errors → concern
    consecutive_failure_limit: int = 3    # Hard failures → evict
    latency_multiplier_threshold: float = 3.0  # 3x baseline → evict

    # ===== Timing =====
    probe_timeout: float = 0.5            # 500ms per probe
    max_concurrent_probes: int = 10
    initial_backoff: float = 0.5          # 500ms
    max_backoff: float = 15.0             # 15 seconds
    backoff_multiplier: float = 2.0
    jitter_factor: float = 0.25           # 25% randomization
    refresh_interval: float = 60.0        # Re-evaluate candidates
    promotion_jitter: tuple[float, float] = (0.1, 0.5)  # 100-500ms
```

#### Module Structure

```
hyperscale/distributed_rewrite/discovery/
├── __init__.py                    # Public exports
├── discovery_service.py           # Main DiscoveryService orchestrator
│
├── dns/
│   ├── __init__.py
│   ├── resolver.py                # AsyncDNSResolver with caching
│   └── negative_cache.py          # NegativeCache for failed lookups
│
├── locality/
│   ├── __init__.py
│   ├── locality_filter.py         # LocalityFilter (DC/region preference)
│   └── locality_info.py           # LocalityInfo dataclass
│
├── selection/
│   ├── __init__.py
│   ├── rendezvous_hash.py         # WeightedRendezvousHash
│   ├── power_of_two.py            # PowerOfTwoSelector
│   └── ewma_tracker.py            # EWMALatencyTracker
│
├── pool/
│   ├── __init__.py
│   ├── connection_pool.py         # ConnectionPool with sticky connections
│   ├── peer_health.py             # PeerHealthTracker
│   └── promotion.py               # PromotionManager
│
├── security/
│   ├── __init__.py
│   ├── cluster_validator.py       # ClusterValidator (cluster_id/env_id)
│   └── role_validator.py          # RoleValidator (mTLS cert claims)
│
├── metrics/
│   ├── __init__.py
│   └── discovery_metrics.py       # DiscoveryMetrics
│
└── models/
    ├── __init__.py
    ├── discovery_config.py        # DiscoveryConfig dataclass
    ├── peer_info.py               # PeerInfo with health data
    ├── candidate_set.py           # CandidateSet dataclass
    └── connection_state.py        # ConnectionState enum
```

**Trade-offs**:
- (+) Deterministic peer selection via rendezvous hash (debuggable)
- (+) Load balancing via Power of Two Choices (avoids thundering herd)
- (+) Locality awareness reduces cross-DC traffic
- (+) Strong security boundaries prevent misconfiguration
- (+) Sticky connections reduce churn overhead
- (-) More complex than simple round-robin
- (-) Requires certificate infrastructure for role validation
- (-) EWMA requires per-peer state tracking

**Alternatives Considered**:
- Simple round-robin: Too naive, no health awareness
- Consistent hashing: Good but disrupts more on topology changes
- Central load balancer: Single point of failure, external dependency
- Random selection: No locality awareness, unpredictable behavior

---

## Architecture

### Node Types

#### Gate Nodes (Optional)

Cross-datacenter coordinators that manage global job state and DC-level retries.

```
┌─────────────────────────────────────────────────────────────────┐
│                         GATE NODE                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │   SWIM UDP       │    │   TCP Protocol   │                   │
│  │   (Healthcheck)  │    │   (Job/Status)   │                   │
│  │                  │    │                  │                   │
│  │ • Probe/Ack      │    │ • Job Submission │                   │
│  │ • Suspicion      │    │ • Status Relay   │                   │
│  │ • Leadership     │    │ • State Sync     │                   │
│  │ • State Embed    │    │ • Lease Transfer │                   │
│  └──────────────────┘    └──────────────────┘                   │
│           │                      │                               │
│           ▼                      ▼                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    Gate State                            │    │
│  │  • _jobs: GlobalJobStatus per job                       │    │
│  │  • _leases: DatacenterLease per job:dc                  │    │
│  │  • _datacenter_status: ManagerHeartbeat per DC          │    │
│  │  • _versioned_clock: Per-entity Lamport timestamps      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Responsibilities:                                               │
│  • Accept job submissions from clients                          │
│  • Select target datacenters for job execution                  │
│  • Create leases for at-most-once semantics                     │
│  • Aggregate status from managers across DCs                    │
│  • Handle DC-level failure and retry (lease-based)              │
│  • Leader election among gates                                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### Manager Nodes

Orchestrate workflow execution within a datacenter.

```
┌─────────────────────────────────────────────────────────────────┐
│                       MANAGER NODE                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │   SWIM UDP       │    │   TCP Protocol   │                   │
│  │   (Healthcheck)  │    │   (Workflows)    │                   │
│  │                  │    │                  │                   │
│  │ • Probe Workers  │    │ • Job Dispatch   │                   │
│  │ • Probe Managers │    │ • Quorum Confirm │                   │
│  │ • Worker HB Recv │    │ • State Sync     │                   │
│  │ • Manager HB Send│    │ • Progress Recv  │                   │
│  └──────────────────┘    └──────────────────┘                   │
│           │                      │                               │
│           ▼                      ▼                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   Manager State                          │    │
│  │  • _workers: WorkerRegistration per node_id             │    │
│  │  • _worker_status: WorkerHeartbeat per node_id          │    │
│  │  • _worker_addr_to_id: (host,port) → node_id reverse    │    │
│  │  • _jobs: JobProgress per job_id                        │    │
│  │  • _workflow_assignments: workflow_id → worker_node_id  │    │
│  │  • _workflow_retries: Retry tracking with dispatch data │    │
│  │  • _versioned_clock: Per-entity Lamport timestamps      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Responsibilities:                                               │
│  • Register workers and track their capacity                    │
│  • Select workers for workflow dispatch (crypto-random)         │
│  • Request quorum confirmation before provisioning              │
│  • Retry failed workflows on different workers                  │
│  • Aggregate progress from workers                              │
│  • Report status to gates (via SWIM heartbeat embedding)        │
│  • State sync on leader election                                │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### Worker Nodes

Execute actual workflow code on CPU cores.

```
┌─────────────────────────────────────────────────────────────────┐
│                        WORKER NODE                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │   SWIM UDP       │    │   TCP Protocol   │                   │
│  │   (Healthcheck)  │    │   (Workflows)    │                   │
│  │                  │    │                  │                   │
│  │ • Respond Probes │    │ • Recv Dispatch  │                   │
│  │ • Worker HB Send │    │ • Send Progress  │                   │
│  │ • State Embed    │    │ • State Sync     │                   │
│  └──────────────────┘    └──────────────────┘                   │
│           │                      │                               │
│           ▼                      ▼                               │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    Worker State                          │    │
│  │  • _total_cores / _available_cores: Core capacity       │    │
│  │  • _core_assignments: core_idx → workflow_id            │    │
│  │  • _workflow_cores: workflow_id → [core_idx, ...]       │    │
│  │  • _active_workflows: workflow_id → WorkflowProgress    │    │
│  │  • _workflow_tokens: workflow_id → TaskRunner token     │    │
│  │  • _workflow_cancel_events: workflow_id → asyncio.Event │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Responsibilities:                                               │
│  • Track per-core workflow assignments                          │
│  • Execute workflows via TaskRunner                             │
│  • Send throttled progress updates to manager                   │
│  • Respond to cancellation requests                             │
│  • Report state via SWIM heartbeat embedding                    │
│  • Provide state snapshots for manager sync                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Communication Protocols

```
┌─────────────────────────────────────────────────────────────────┐
│                    PROTOCOL SEPARATION                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│                         UDP (SWIM)                               │
│                    ┌─────────────────┐                          │
│                    │   HEALTHCHECK   │                          │
│                    │   ONLY          │                          │
│                    └─────────────────┘                          │
│                            │                                     │
│     ┌──────────────────────┼──────────────────────┐             │
│     │                      │                      │             │
│     ▼                      ▼                      ▼             │
│  ┌──────┐              ┌──────┐              ┌──────┐           │
│  │Probe │              │ Ack  │              │Gossip│           │
│  │      │              │      │              │      │           │
│  │+ HB  │◄────────────►│+ HB  │              │      │           │
│  │embed │              │embed │              │      │           │
│  └──────┘              └──────┘              └──────┘           │
│                                                                  │
│    Serf-style: Heartbeat data embedded in probe/ack responses   │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│                         TCP (Data)                               │
│                    ┌─────────────────┐                          │
│                    │   STATE SYNC    │                          │
│                    │   JOB SUBMIT    │                          │
│                    │   PROGRESS      │                          │
│                    └─────────────────┘                          │
│                            │                                     │
│     ┌──────────────────────┼──────────────────────┐             │
│     │                      │                      │             │
│     ▼                      ▼                      ▼             │
│  ┌────────┐          ┌──────────┐          ┌──────────┐         │
│  │Workflow│          │ Quorum   │          │  State   │         │
│  │Dispatch│          │ Confirm  │          │   Sync   │         │
│  └────────┘          └──────────┘          └──────────┘         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### TCP Length-Prefixed Framing

TCP is a stream protocol, not a message protocol. Data can arrive fragmented across multiple `data_received` callbacks, especially for large payloads like cloudpickled workflow classes. To ensure reliable message delivery, all TCP messages use **length-prefixed framing**:

```
┌─────────────────────────────────────────────────────────────────┐
│                    TCP MESSAGE FRAMING                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Wire Format:                                                    │
│  ┌──────────────┬────────────────────────────────────────────┐  │
│  │ Length (4B)  │              Payload (N bytes)              │  │
│  │  big-endian  │  [encrypted(compressed(addr<action<data))]  │  │
│  └──────────────┴────────────────────────────────────────────┘  │
│                                                                  │
│  Processing Flow:                                                │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                                                             │ │
│  │  1. Buffer incoming data in ReceiveBuffer                  │ │
│  │  2. Read 4-byte length prefix (big-endian uint32)          │ │
│  │  3. Wait for complete message (length prefix + payload)    │ │
│  │  4. Extract and process complete message                   │ │
│  │  5. Repeat for any remaining buffered data                 │ │
│  │                                                             │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Design Rationale:                                               │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                                                             │ │
│  │  • 4-byte prefix supports messages up to ~4GB              │ │
│  │  • Handles arbitrary-sized cloudpickled classes            │ │
│  │  • Prevents pickle truncation on large payloads            │ │
│  │  • Applied after compression/encryption (framing is outer) │ │
│  │                                                             │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Leadership Election

Hierarchical lease-based leadership with LHM (Local Health Multiplier) eligibility:

```
┌─────────────────────────────────────────────────────────────────┐
│                    LEADERSHIP ELECTION                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    Eligibility Check                        │ │
│  │                                                             │ │
│  │  1. LHM Score ≤ max_leader_lhm (default: 4.0)              │ │
│  │  2. Node is ALIVE in SWIM cluster                          │ │
│  │  3. Priority factor (configurable per node)                │ │
│  └────────────────────────────────────────────────────────────┘ │
│                            │                                     │
│                            ▼                                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    Pre-Voting Phase                         │ │
│  │                                                             │ │
│  │  • Candidate requests pre-votes from all members           │ │
│  │  • Members compare candidate eligibility vs current leader │ │
│  │  • Prevents split-brain from network partitions            │ │
│  └────────────────────────────────────────────────────────────┘ │
│                            │                                     │
│                            ▼                                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    Term-Based Resolution                    │ │
│  │                                                             │ │
│  │  • Each election increments term number                    │ │
│  │  • Higher term always wins conflicts                       │ │
│  │  • Fencing tokens derived from term for at-most-once       │ │
│  └────────────────────────────────────────────────────────────┘ │
│                            │                                     │
│                            ▼                                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    Flapping Detection                       │ │
│  │                                                             │ │
│  │  • Track leadership changes in sliding window              │ │
│  │  • Cooldown period if too many changes detected            │ │
│  │  • Prevents oscillation under unstable conditions          │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Component Diagrams

### SWIM Protocol Implementation

```
┌─────────────────────────────────────────────────────────────────┐
│                      SWIM + LIFEGUARD                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                  Local Health Multiplier                 │    │
│  │  ┌─────────────────────────────────────────────────────┐│    │
│  │  │ score = 0 (healthy) → 8 (degraded)                  ││    │
│  │  │ timeout_multiplier = 1 + (score × factor)           ││    │
│  │  │ Incremented on: failed probes, event loop lag       ││    │
│  │  │ Decremented on: successful probes, recovery         ││    │
│  │  └─────────────────────────────────────────────────────┘│    │
│  └─────────────────────────────────────────────────────────┘    │
│                            │                                     │
│           ┌────────────────┼────────────────┐                   │
│           ▼                ▼                ▼                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │   Direct    │  │  Indirect   │  │  Suspicion  │              │
│  │   Probe     │  │   Probe     │  │  Protocol   │              │
│  │             │  │  (Ping-Req) │  │             │              │
│  │ timeout =   │  │             │  │ timeout =   │              │
│  │ base × LHM  │  │ via random  │  │ fn(n, LHM)  │              │
│  │             │  │ proxy node  │  │             │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│           │                │                │                    │
│           └────────────────┼────────────────┘                   │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                  Incarnation Tracker                     │    │
│  │  • Per-node incarnation numbers                         │    │
│  │  • Higher incarnation = fresher state                   │    │
│  │  • Refutation: increment own incarnation to clear       │    │
│  │    suspicion                                            │    │
│  └─────────────────────────────────────────────────────────┘    │
│                            │                                     │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    Gossip Buffer                         │    │
│  │  • Piggybacked membership updates                       │    │
│  │  • Priority: JOIN > LEAVE > ALIVE > SUSPECT > DEAD      │    │
│  │  • Bounded size with overflow callback                  │    │
│  │  • Efficient encoding within UDP MTU                    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### State Embedder (Serf-Style Heartbeats)

```
┌─────────────────────────────────────────────────────────────────┐
│                    STATE EMBEDDER PATTERN                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Protocol (Composition over Inheritance):                        │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  class StateEmbedder(Protocol):                          │    │
│  │      def get_state(self) -> bytes | None                 │    │
│  │      def process_state(self, data: bytes, addr) -> None  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                            │                                     │
│        ┌───────────────────┼───────────────────┐                │
│        ▼                   ▼                   ▼                │
│  ┌───────────┐      ┌───────────┐      ┌───────────┐            │
│  │  Worker   │      │  Manager  │      │   Gate    │            │
│  │  Embedder │      │  Embedder │      │  Embedder │            │
│  └───────────┘      └───────────┘      └───────────┘            │
│        │                   │                   │                 │
│        ▼                   ▼                   ▼                 │
│  ┌───────────┐      ┌───────────┐      ┌───────────┐            │
│  │ Worker    │      │ Manager   │      │  (none)   │            │
│  │ Heartbeat │      │ Heartbeat │      │           │            │
│  │ • cores   │      │ • DC      │      │ Gates are │            │
│  │ • queue   │      │ • workers │      │ receivers │            │
│  │ • cpu %   │      │ • jobs    │      │ only      │            │
│  │ • mem %   │      │ • leader? │      │           │            │
│  └───────────┘      └───────────┘      └───────────┘            │
│                                                                  │
│  Flow:                                                           │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                                                           │   │
│  │   Worker ──probe─→ Manager                               │   │
│  │   Worker ←─ack+WorkerHeartbeat── Manager                 │   │
│  │                                                           │   │
│  │   Manager ──probe─→ Gate                                 │   │
│  │   Manager ←─ack+ManagerHeartbeat── Gate                  │   │
│  │                                                           │   │
│  │   (State learned passively via SWIM protocol)            │   │
│  │                                                           │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Worker Core Allocation & Execution Cycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      WORKER NODE - CORE ALLOCATION                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Physical/Virtual Cores:                                                     │
│  ┌───┬───┬───┬───┬───┬───┬───┬───┐                                          │
│  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │  (8-core worker example)                 │
│  └───┴───┴───┴───┴───┴───┴───┴───┘                                          │
│    │   │   │       │   │   │                                                 │
│    │   └───┴───────┘   └───┴──────► wf-456 (3 cores: 1,2,5,6)               │
│    │                                                                         │
│    └──────────────────────────────► wf-123 (1 core: 0)                      │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                     _core_assignments                                  │  │
│  │  {0: "wf-123", 1: "wf-456", 2: "wf-456", 3: None,                    │  │
│  │   4: None, 5: "wf-456", 6: "wf-456", 7: None}                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                     _workflow_cores                                    │  │
│  │  {"wf-123": [0], "wf-456": [1, 2, 5, 6]}                             │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  Allocation Algorithm (_allocate_cores):                                     │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  1. Scan _core_assignments for cores where value is None              │  │
│  │  2. Take first N available cores (requested vus)                      │  │
│  │  3. Mark cores as assigned to workflow_id                             │  │
│  │  4. Add to _workflow_cores mapping                                    │  │
│  │  5. Return list of allocated core indices                             │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  Deallocation (_free_cores):                                                 │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  1. Look up cores from _workflow_cores[workflow_id]                   │  │
│  │  2. Set each core to None in _core_assignments                        │  │
│  │  3. Remove workflow_id from _workflow_cores                           │  │
│  │  4. Cancel running task via TaskRunner token                          │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Worker Execution Cycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      WORKER REQUEST/EXECUTION CYCLE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  INBOUND: receive_workflow_dispatch (TCP)                            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1. Deserialize WorkflowDispatch                                     │    │
│  │  2. Check capacity: available_cores >= vus                           │    │
│  │  3. If insufficient → return WorkflowDispatchAck(accepted=False)     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  4. _allocate_cores(workflow_id, vus) → [core_indices]               │    │
│  │  5. Deserialize Workflow class from cloudpickle                      │    │
│  │  6. Create WorkflowProgress tracker                                  │    │
│  │  7. Store in _active_workflows[workflow_id]                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  8. Submit to TaskRunner:                                            │    │
│  │     token = _task_runner.run(_execute_workflow, workflow, ...)       │    │
│  │  9. Store token: _workflow_tokens[workflow_id] = token               │    │
│  │ 10. Return WorkflowDispatchAck(accepted=True, cores_assigned=N)      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    WORKFLOW EXECUTION LOOP                           │    │
│  │  ┌─────────────────────────────────────────────────────────────┐    │    │
│  │  │  while not cancel_event.is_set():                           │    │    │
│  │  │      execute_action()                                        │    │    │
│  │  │      update_progress()                                       │    │    │
│  │  │                                                              │    │    │
│  │  │      # Throttled TCP progress updates (every 100ms)         │    │    │
│  │  │      if int(elapsed * 10) % 10 == 0:                        │    │    │
│  │  │          send_progress_to_manager()                         │    │    │
│  │  └─────────────────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                    ┌─────────┴─────────┐                                    │
│                    ▼                   ▼                                    │
│  ┌─────────────────────┐   ┌─────────────────────┐                          │
│  │  COMPLETION         │   │  CANCELLATION       │                          │
│  │  ───────────        │   │  ────────────       │                          │
│  │  1. Update status   │   │  1. cancel_event    │                          │
│  │  2. Send final      │   │     .set()          │                          │
│  │     progress        │   │  2. TaskRunner      │                          │
│  │  3. _free_cores()   │   │     .cancel(token)  │                          │
│  │  4. Cleanup maps    │   │  3. _free_cores()   │                          │
│  └─────────────────────┘   └─────────────────────┘                          │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  PARALLEL: SWIM UDP Probe Response                                   │    │
│  │  • Embed WorkerHeartbeat in ack (via StateEmbedder)                 │    │
│  │  • Fields: node_id, state, available_cores, queue_depth,            │    │
│  │           cpu_percent, memory_percent, version, active_workflows    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Manager Request Cycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MANAGER REQUEST CYCLE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ INBOUND: receive_job_submission (TCP from Gate or Client)              │ │
│  │          JobSubmission { job_id, workflows (pickled), vus, timeout }   │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                     │                                        │
│                                     ▼                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 1. Leader Check: if not self.is_leader() → forward to leader          │ │
│  │ 2. Deserialize workflows list from cloudpickle                        │ │
│  │ 3. Create JobProgress tracker for job_id                              │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                     │                                        │
│           ┌─────────────────────────┴─────────────────────────┐             │
│           │            FOR EACH WORKFLOW IN JOB:              │             │
│           ▼                                                    │             │
│  ┌─────────────────────────────────────────────────────────┐  │             │
│  │  WORKER SELECTION (crypto-random for security)          │  │             │
│  │  ───────────────────────────────────────────────────────│  │             │
│  │  1. Get all registered workers from _workers            │  │             │
│  │  2. Filter by health: HEALTHY or DEGRADED (not DRAINING)│  │             │
│  │  3. Filter by capacity: available_cores >= vus          │  │             │
│  │  4. Apply backpressure: queue_depth < soft_limit        │  │             │
│  │  5. Use secrets.SystemRandom().choice() for selection   │  │             │
│  └─────────────────────────────────────────────────────────┘  │             │
│                         │                                      │             │
│                         ▼                                      │             │
│  ┌─────────────────────────────────────────────────────────┐  │             │
│  │  QUORUM CONFIRMATION (if manager cluster size > 1)      │  │             │
│  │  ───────────────────────────────────────────────────────│  │             │
│  │  1. Create ProvisionRequest { workflow_id, worker, ... }│  │             │
│  │  2. Send to all peer managers                           │  │             │
│  │  3. Wait for quorum: (n // 2) + 1 confirmations         │  │             │
│  │  4. Timeout → reject provisioning                       │  │             │
│  │  5. Quorum achieved → proceed to commit                 │  │             │
│  └─────────────────────────────────────────────────────────┘  │             │
│                         │                                      │             │
│                         ▼                                      │             │
│  ┌─────────────────────────────────────────────────────────┐  │             │
│  │  DISPATCH TO WORKER (TCP)                               │  │             │
│  │  ───────────────────────────────────────────────────────│  │             │
│  │  1. Create WorkflowDispatch { fence_token, ... }        │  │             │
│  │  2. Store in _workflow_assignments[workflow_id]         │  │             │
│  │  3. Store pickled bytes in _workflow_retries for retry  │  │             │
│  │  4. Send via send_tcp(worker_addr, "dispatch", data)    │  │             │
│  │  5. Wait for WorkflowDispatchAck                        │  │             │
│  └─────────────────────────────────────────────────────────┘  │             │
│                         │                                      │             │
│           └─────────────┴──────────────────────────────────────┘             │
│                         │                                                    │
│                         ▼                                                    │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ OUTBOUND: JobAck { job_id, accepted, workflows_dispatched }            │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════════│
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ INBOUND: receive_workflow_progress (TCP from Worker)                   │ │
│  │          WorkflowProgress { job_id, workflow_id, status, stats... }    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                     │                                        │
│                                     ▼                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 1. Stale Check: _versioned_clock.is_entity_stale()                    │ │
│  │ 2. Update _jobs[job_id] with workflow progress                        │ │
│  │ 3. Check status:                                                       │ │
│  │    • COMPLETED → _cleanup_workflow(), cleanup retry info              │ │
│  │    • FAILED    → _handle_workflow_failure() (retry or mark failed)    │ │
│  │ 4. Aggregate job-level stats                                          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════════│
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ PARALLEL: SWIM UDP Operations                                          │ │
│  │                                                                         │ │
│  │ 1. Receive WorkerHeartbeat (via StateEmbedder from worker probes)     │ │
│  │    → Update _worker_status[node_id]                                    │ │
│  │    → Passive capacity/health monitoring                                │ │
│  │                                                                         │ │
│  │ 2. Embed ManagerHeartbeat in probe acks (to Gates)                    │ │
│  │    → Fields: node_id, datacenter, is_leader, term, job/workflow counts│ │
│  │                                                                         │ │
│  │ 3. Node death callback → _on_node_dead(worker_addr)                   │ │
│  │    → Trigger workflow retry on different workers                       │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Gate Request Cycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GATE REQUEST CYCLE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ INBOUND: receive_job_submission (TCP from Client)                      │ │
│  │          JobSubmission { job_id, workflows, vus, datacenter_count }    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                     │                                        │
│                                     ▼                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 1. Leader Check: if not self.is_leader() → forward to leader          │ │
│  │ 2. Create GlobalJobStatus tracker                                      │ │
│  │ 3. Select target datacenters:                                          │ │
│  │    • If datacenters specified → use those                              │ │
│  │    • Else → select N available DCs with healthy managers              │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                     │                                        │
│           ┌─────────────────────────┴─────────────────────────┐             │
│           │            FOR EACH TARGET DATACENTER:            │             │
│           ▼                                                    │             │
│  ┌─────────────────────────────────────────────────────────┐  │             │
│  │  LEASE CREATION (at-most-once semantics)                │  │             │
│  │  ───────────────────────────────────────────────────────│  │             │
│  │  1. Generate fence_token (monotonic, derived from term) │  │             │
│  │  2. Create DatacenterLease {                            │  │             │
│  │       job_id, datacenter, lease_holder: self.node_id,  │  │             │
│  │       fence_token, expires_at: now + timeout           │  │             │
│  │     }                                                   │  │             │
│  │  3. Store in _leases[(job_id, datacenter)]             │  │             │
│  └─────────────────────────────────────────────────────────┘  │             │
│                         │                                      │             │
│                         ▼                                      │             │
│  ┌─────────────────────────────────────────────────────────┐  │             │
│  │  DISPATCH TO MANAGER (TCP)                              │  │             │
│  │  ───────────────────────────────────────────────────────│  │             │
│  │  1. Find leader manager for datacenter                  │  │             │
│  │     (from _datacenter_status ManagerHeartbeats)         │  │             │
│  │  2. Send JobSubmission with fence_token                 │  │             │
│  │  3. Wait for JobAck                                     │  │             │
│  │  4. If failed → mark DC as failed, continue to others   │  │             │
│  └─────────────────────────────────────────────────────────┘  │             │
│                         │                                      │             │
│           └─────────────┴──────────────────────────────────────┘             │
│                         │                                                    │
│                         ▼                                                    │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ OUTBOUND: JobAck { job_id, accepted, datacenters_dispatched }          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════════│
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ PARALLEL: Status Aggregation                                           │ │
│  │                                                                         │ │
│  │ 1. Receive ManagerHeartbeat (via StateEmbedder from SWIM probes)      │ │
│  │    → Update _datacenter_status[datacenter]                             │ │
│  │    → Passive monitoring of DC health                                   │ │
│  │                                                                         │ │
│  │ 2. Receive JobProgress (TCP from Managers)                            │ │
│  │    → Update _jobs[job_id].datacenters[dc]                              │ │
│  │    → Aggregate totals: completed, failed, rate                         │ │
│  │                                                                         │ │
│  │ 3. Lease Management (_lease_cleanup_loop via TaskRunner)              │ │
│  │    → Check expired leases every cleanup_interval                       │ │
│  │    → Expired lease → mark DC as FAILED for that job                   │ │
│  │    → No retry (explicit failure to client)                             │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════════│
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ CLIENT STATUS QUERY: get_job_status(job_id) → GlobalJobStatus         │ │
│  │                                                                         │ │
│  │ GlobalJobStatus {                                                       │ │
│  │   job_id: "job-123"                                                    │ │
│  │   status: RUNNING                                                       │ │
│  │   datacenters: [                                                        │ │
│  │     JobProgress { dc: "us-east-1", completed: 10000, rate: 5000/s },  │ │
│  │     JobProgress { dc: "eu-west-1", completed: 8500, rate: 4200/s },   │ │
│  │   ]                                                                     │ │
│  │   total_completed: 18500                                                │ │
│  │   overall_rate: 9200/s                                                  │ │
│  │   elapsed_seconds: 42.5                                                 │ │
│  │   completed_datacenters: 0                                              │ │
│  │   failed_datacenters: 0                                                 │ │
│  │ }                                                                       │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Complete Request Flow (End-to-End)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        END-TO-END JOB EXECUTION                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  CLIENT                                                                      │
│    │                                                                         │
│    │ ① JobSubmission (workflows, vus, dc_count)                             │
│    ▼                                                                         │
│  GATE (Leader)                                                               │
│    │                                                                         │
│    ├─► Create leases for target DCs                                          │
│    │                                                                         │
│    │ ② JobSubmission + fence_token (per DC)                                 │
│    ├──────────────────┬──────────────────┐                                  │
│    ▼                  ▼                  ▼                                  │
│  MANAGER-A          MANAGER-B          MANAGER-C     (DC leaders)           │
│    │                  │                  │                                   │
│    ├─► Quorum        ├─► Quorum        ├─► Quorum                          │
│    │   confirm       │   confirm       │   confirm                          │
│    │                  │                  │                                   │
│    │ ③ WorkflowDispatch (per workflow)                                      │
│    ├───┬───┬───┐     ├───┬───┬───┐     ├───┬───┬───┐                       │
│    ▼   ▼   ▼   ▼     ▼   ▼   ▼   ▼     ▼   ▼   ▼   ▼                       │
│   W1  W2  W3  W4    W5  W6  W7  W8    W9 W10 W11 W12  (Workers)             │
│    │   │   │   │     │   │   │   │     │   │   │   │                        │
│    │   │   │   │     │   │   │   │     │   │   │   │                        │
│    ├───┴───┴───┘     ├───┴───┴───┘     ├───┴───┴───┘                        │
│    │                  │                  │                                   │
│    │ ④ WorkflowProgress (throttled TCP, every 100ms)                        │
│    ▼                  ▼                  ▼                                  │
│  MANAGER-A          MANAGER-B          MANAGER-C                            │
│    │                  │                  │                                   │
│    │ ⑤ JobProgress (aggregated)                                             │
│    ├──────────────────┴──────────────────┘                                  │
│    ▼                                                                         │
│  GATE (Leader)                                                               │
│    │                                                                         │
│    │ ⑥ GlobalJobStatus (aggregated across DCs)                              │
│    ▼                                                                         │
│  CLIENT                                                                      │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════════│
│                                                                              │
│  PARALLEL SWIM UDP FLOW (Healthcheck + Passive Discovery):                  │
│                                                                              │
│      Workers ◄──probe──► Managers ◄──probe──► Gates                         │
│              └─ack+HB─┘            └─ack+HB─┘                               │
│                                                                              │
│      WorkerHeartbeat               ManagerHeartbeat                          │
│      • available_cores             • datacenter                              │
│      • queue_depth                 • is_leader                               │
│      • cpu/mem percent             • job/workflow counts                     │
│      • active_workflows            • worker_count                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## State Machines

### SWIM Node States

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SWIM NODE STATE MACHINE                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                              ┌─────────┐                                     │
│                              │ UNKNOWN │                                     │
│                              └────┬────┘                                     │
│                                   │                                          │
│                          join / probe response                               │
│                                   │                                          │
│                                   ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                         │ │
│  │   ┌───────────────────────────────────────────────────────────────┐    │ │
│  │   │                         ALIVE                                  │    │ │
│  │   │                                                                │    │ │
│  │   │  • Responds to probes                                         │    │ │
│  │   │  • Participates in gossip                                     │    │ │
│  │   │  • Eligible for work dispatch                                 │    │ │
│  │   └───────────────────────────────┬───────────────────────────────┘    │ │
│  │                                   │                                     │ │
│  │                    probe timeout / suspect message                      │ │
│  │                    (incarnation ≥ current)                              │ │
│  │                                   │                                     │ │
│  │                                   ▼                                     │ │
│  │   ┌───────────────────────────────────────────────────────────────┐    │ │
│  │   │                        SUSPECT                                 │    │ │
│  │   │                                                                │    │ │
│  │   │  • Suspicion timer started: T = k × log(n) × LHM              │    │ │
│  │   │  • Can be refuted with higher incarnation                     │    │ │
│  │   │  • Confirmations accelerate timeout                           │    │ │
│  │   └──────────┬─────────────────────────────────┬──────────────────┘    │ │
│  │              │                                 │                        │ │
│  │   refutation (higher incarnation)     suspicion timeout expired         │ │
│  │   or alive message                    (no refutation received)          │ │
│  │              │                                 │                        │ │
│  │              ▼                                 ▼                        │ │
│  │   ┌─────────────────┐               ┌─────────────────┐                │ │
│  │   │     ALIVE       │               │      DEAD       │                │ │
│  │   │   (restored)    │               │                 │                │ │
│  │   └─────────────────┘               │  • Removed from │                │ │
│  │                                     │    membership   │                │ │
│  │                                     │  • Gossip DEAD  │                │ │
│  │                                     │    propagated   │                │ │
│  │                                     └────────┬────────┘                │ │
│  │                                              │                          │ │
│  └──────────────────────────────────────────────┼──────────────────────────┘ │
│                                                 │                            │
│                                      cleanup after TTL                       │
│                                                 │                            │
│                                                 ▼                            │
│                                          ┌───────────┐                       │
│                                          │  REMOVED  │                       │
│                                          │ (garbage  │                       │
│                                          │ collected)│                       │
│                                          └───────────┘                       │
│                                                                              │
│  Transitions:                                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │  UNKNOWN → ALIVE    : First probe response or join acknowledgment      │ │
│  │  ALIVE   → SUSPECT  : Probe timeout OR suspect gossip with inc ≥ curr  │ │
│  │  SUSPECT → ALIVE    : Refutation with incarnation > current            │ │
│  │  SUSPECT → DEAD     : Suspicion timer expires without refutation       │ │
│  │  DEAD    → REMOVED  : Cleanup task removes after TTL                   │ │
│  │  DEAD    → ALIVE    : Rejoin with higher incarnation (rare)            │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Worker States

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         WORKER STATE MACHINE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                           ┌──────────────┐                                   │
│                           │  REGISTERING │                                   │
│                           └──────┬───────┘                                   │
│                                  │                                           │
│                      manager acknowledges registration                       │
│                                  │                                           │
│                                  ▼                                           │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                         │ │
│  │   ┌───────────────────────────────────────────────────────────────┐    │ │
│  │   │                        HEALTHY                                 │    │ │
│  │   │                                                                │    │ │
│  │   │  Conditions:                                                   │    │ │
│  │   │  • CPU < 80%                                                  │    │ │
│  │   │  • Memory < 85%                                               │    │ │
│  │   │  • Queue depth < soft_limit                                   │    │ │
│  │   │  • LHM score < 4                                              │    │ │
│  │   │                                                                │    │ │
│  │   │  Behavior: Accepts new workflows normally                     │    │ │
│  │   └────────────────────────────┬──────────────────────────────────┘    │ │
│  │                                │                                        │ │
│  │              resource pressure increases                                │ │
│  │              (CPU ≥ 80% OR memory ≥ 85% OR queue ≥ soft_limit)         │ │
│  │                                │                                        │ │
│  │                                ▼                                        │ │
│  │   ┌───────────────────────────────────────────────────────────────┐    │ │
│  │   │                       DEGRADED                                 │    │ │
│  │   │                                                                │    │ │
│  │   │  Conditions:                                                   │    │ │
│  │   │  • CPU 80-95% OR Memory 85-95% OR Queue at soft_limit         │    │ │
│  │   │  • LHM score 4-6                                              │    │ │
│  │   │                                                                │    │ │
│  │   │  Behavior:                                                     │    │ │
│  │   │  • Accepts work with backpressure signaling                   │    │ │
│  │   │  • Manager deprioritizes in worker selection                  │    │ │
│  │   │  • Extended timeouts via LHM                                  │    │ │
│  │   └──────────┬─────────────────────────────────┬──────────────────┘    │ │
│  │              │                                 │                        │ │
│  │    pressure relieved                  pressure critical                 │ │
│  │    (metrics return to normal)         (CPU > 95% OR OOM risk)          │ │
│  │              │                                 │                        │ │
│  │              ▼                                 ▼                        │ │
│  │   ┌─────────────────┐               ┌─────────────────┐                │ │
│  │   │     HEALTHY     │               │    DRAINING     │                │ │
│  │   │   (restored)    │               │                 │                │ │
│  │   └─────────────────┘               │  • No new work  │                │ │
│  │                                     │  • Complete     │                │ │
│  │          ▲                          │    existing     │                │ │
│  │          │                          │  • Report drain │                │ │
│  │   all work completed                │    to manager   │                │ │
│  │   AND healthy metrics               └────────┬────────┘                │ │
│  │          │                                   │                          │ │
│  │          │                        shutdown requested OR                 │ │
│  │          │                        unrecoverable error                   │ │
│  │          │                                   │                          │ │
│  │          │                                   ▼                          │ │
│  │          │                          ┌─────────────────┐                │ │
│  │          └──────────────────────────│    OFFLINE      │                │ │
│  │                                     │                 │                │ │
│  │                                     │  • Not in SWIM  │                │ │
│  │                                     │  • Cleanup done │                │ │
│  │                                     └─────────────────┘                │ │
│  │                                                                         │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  State reported in WorkerHeartbeat.state for manager visibility             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Job Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           JOB STATE MACHINE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Client submits JobSubmission                                                │
│            │                                                                 │
│            ▼                                                                 │
│  ┌─────────────────┐                                                        │
│  │    SUBMITTED    │  Job received by Gate/Manager                          │
│  └────────┬────────┘                                                        │
│           │                                                                  │
│           │ validate & queue                                                 │
│           ▼                                                                  │
│  ┌─────────────────┐                                                        │
│  │     QUEUED      │  Waiting for resources                                 │
│  └────────┬────────┘                                                        │
│           │                                                                  │
│           │ resources available, begin dispatch                              │
│           ▼                                                                  │
│  ┌─────────────────┐                                                        │
│  │   DISPATCHING   │  Workflows being sent to workers                       │
│  │                 │  (quorum confirmation in progress)                     │
│  └────────┬────────┘                                                        │
│           │                                                                  │
│           │ all workflows dispatched                                         │
│           ▼                                                                  │
│  ┌─────────────────┐                                                        │
│  │     RUNNING     │  Workflows executing on workers                        │
│  │                 │  Progress updates flowing                              │
│  └────────┬────────┘                                                        │
│           │                                                                  │
│           ├─────────────────────────────────────────┐                       │
│           │                                         │                       │
│           │ all workflows complete                  │ user cancellation     │
│           ▼                                         ▼                       │
│  ┌─────────────────┐                       ┌─────────────────┐              │
│  │   COMPLETING    │                       │   CANCELLING    │              │
│  │                 │                       │                 │              │
│  │ Aggregating     │                       │ Sending cancel  │              │
│  │ final results   │                       │ to all workers  │              │
│  └────────┬────────┘                       └────────┬────────┘              │
│           │                                         │                       │
│           │ results aggregated                      │ all cancelled         │
│           ▼                                         ▼                       │
│  ┌─────────────────┐                       ┌─────────────────┐              │
│  │    COMPLETED    │                       │    CANCELLED    │              │
│  │                 │                       │                 │              │
│  │ Success!        │                       │ User stopped    │              │
│  │ Results ready   │                       │                 │              │
│  └─────────────────┘                       └─────────────────┘              │
│                                                                              │
│           │ (alternate paths from RUNNING)                                  │
│           │                                                                  │
│           ├─────────────────────────────────────────┐                       │
│           │                                         │                       │
│           │ unrecoverable errors                    │ timeout exceeded      │
│           ▼                                         ▼                       │
│  ┌─────────────────┐                       ┌─────────────────┐              │
│  │     FAILED      │                       │     TIMEOUT     │              │
│  │                 │                       │                 │              │
│  │ Max retries     │                       │ Exceeded        │              │
│  │ exhausted       │                       │ timeout_seconds │              │
│  └─────────────────┘                       └─────────────────┘              │
│                                                                              │
│  Terminal states: COMPLETED, CANCELLED, FAILED, TIMEOUT                     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Workflow Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        WORKFLOW STATE MACHINE                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Part of Job dispatching                                                     │
│            │                                                                 │
│            ▼                                                                 │
│  ┌─────────────────┐                                                        │
│  │     PENDING     │  Workflow created, not yet dispatched                  │
│  └────────┬────────┘                                                        │
│           │                                                                  │
│           │ worker selected, dispatch sent                                   │
│           ▼                                                                  │
│  ┌─────────────────┐                                                        │
│  │    ASSIGNED     │  Sent to worker, awaiting ack                          │
│  └────────┬────────┘                                                        │
│           │                                                                  │
│           ├─────────────────────────────────────────┐                       │
│           │                                         │                       │
│           │ worker accepts (cores allocated)        │ worker rejects        │
│           ▼                                         ▼                       │
│  ┌─────────────────┐                       ┌─────────────────┐              │
│  │     RUNNING     │                       │   RE-DISPATCH   │              │
│  │                 │                       │                 │              │
│  │ Executing on    │                       │ Select another  │──┐           │
│  │ allocated cores │                       │ worker          │  │           │
│  │                 │                       └─────────────────┘  │           │
│  │ Progress:       │                              ▲              │           │
│  │ • completed_cnt │                              │              │           │
│  │ • failed_cnt    │                              │              │           │
│  │ • rate/second   │                              │              │           │
│  │ • step_stats[]  │                              │ retry < max  │           │
│  └────────┬────────┘                              │              │           │
│           │                                       │              │           │
│           ├─────────────────────────────────┬─────┘              │           │
│           │                                 │                    │           │
│           │ all actions complete            │ worker fails       │           │
│           │ successfully                    │ (SWIM DEAD)        │           │
│           ▼                                 ▼                    │           │
│  ┌─────────────────┐               ┌─────────────────┐          │           │
│  │    COMPLETED    │               │  WORKER_FAILED  │──────────┘           │
│  │                 │               │                 │                      │
│  │ Success!        │               │ Retry on        │                      │
│  │ Results in      │               │ different       │                      │
│  │ WorkflowProgress│               │ worker          │                      │
│  └─────────────────┘               └────────┬────────┘                      │
│                                             │                                │
│                                             │ retry >= max                   │
│                                             ▼                                │
│                                    ┌─────────────────┐                      │
│                                    │     FAILED      │                      │
│                                    │                 │                      │
│                                    │ Max retries     │                      │
│                                    │ exhausted       │                      │
│                                    └─────────────────┘                      │
│                                                                              │
│  Also from RUNNING:                                                          │
│  ┌─────────────────┐                                                        │
│  │    CANCELLED    │  ← Cancel request received                             │
│  └─────────────────┘                                                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Leadership States

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      LEADERSHIP STATE MACHINE                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                              ┌──────────────┐                                │
│                              │   INITIAL    │                                │
│                              └──────┬───────┘                                │
│                                     │                                        │
│                          join cluster / startup                              │
│                                     │                                        │
│                                     ▼                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                         │ │
│  │   ┌───────────────────────────────────────────────────────────────┐    │ │
│  │   │                        FOLLOWER                                │    │ │
│  │   │                                                                │    │ │
│  │   │  • Accepts leader heartbeats                                  │    │ │
│  │   │  • Forwards requests to leader                                │    │ │
│  │   │  • Responds to pre-vote requests                              │    │ │
│  │   │  • Monitors leader liveness                                   │    │ │
│  │   └────────────────────────────┬──────────────────────────────────┘    │ │
│  │                                │                                        │ │
│  │              leader timeout expired AND                                 │ │
│  │              self is eligible (LHM ≤ max_leader_lhm)                   │ │
│  │                                │                                        │ │
│  │                                ▼                                        │ │
│  │   ┌───────────────────────────────────────────────────────────────┐    │ │
│  │   │                      PRE_CANDIDATE                             │    │ │
│  │   │                                                                │    │ │
│  │   │  • Sends pre-vote requests to all members                     │    │ │
│  │   │  • Collects pre-vote responses                                │    │ │
│  │   │  • Does NOT increment term yet (prevents disruption)          │    │ │
│  │   │  • Timeout: pre_vote_timeout                                  │    │ │
│  │   └──────────┬─────────────────────────────────┬──────────────────┘    │ │
│  │              │                                 │                        │ │
│  │   pre-vote majority granted              pre-vote denied OR             │ │
│  │   (> n/2 nodes agree)                    timeout OR higher term         │ │
│  │              │                                 │                        │ │
│  │              ▼                                 ▼                        │ │
│  │   ┌─────────────────┐               ┌─────────────────┐                │ │
│  │   │    CANDIDATE    │               │    FOLLOWER     │                │ │
│  │   │                 │               │   (step down)   │                │ │
│  │   │ • Increment term│               └─────────────────┘                │ │
│  │   │ • Vote for self │                                                   │ │
│  │   │ • Request votes │                                                   │ │
│  │   │   from peers    │                                                   │ │
│  │   └────────┬────────┘                                                   │ │
│  │            │                                                            │ │
│  │            ├─────────────────────────────────────────┐                 │ │
│  │            │                                         │                 │ │
│  │   vote majority granted                     vote denied OR             │ │
│  │   (> n/2 votes for self)                    higher term seen           │ │
│  │            │                                         │                 │ │
│  │            ▼                                         ▼                 │ │
│  │   ┌─────────────────┐                       ┌─────────────────┐        │ │
│  │   │     LEADER      │                       │    FOLLOWER     │        │ │
│  │   │                 │                       │   (step down)   │        │ │
│  │   │ • Broadcast win │                       └─────────────────┘        │ │
│  │   │ • Send heartbeat│                                                   │ │
│  │   │ • Handle requests                                                   │ │
│  │   │ • State sync    │                                                   │ │
│  │   └────────┬────────┘                                                   │ │
│  │            │                                                            │ │
│  │   ┌────────┴────────────────────────────────────────────┐              │ │
│  │   │                                                      │              │ │
│  │   │ LHM exceeds threshold     higher term         network partition    │ │
│  │   │ (unhealthy leader)        discovered          (loses majority)     │ │
│  │   │                                                      │              │ │
│  │   ▼                           ▼                          ▼              │ │
│  │   ┌──────────────────────────────────────────────────────────────┐     │ │
│  │   │                        FOLLOWER                               │     │ │
│  │   │                       (step down)                             │     │ │
│  │   └──────────────────────────────────────────────────────────────┘     │ │
│  │                                                                         │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Flapping Protection:                                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │  If leadership changes > threshold in window → cooldown period         │ │
│  │  During cooldown: no new elections initiated                           │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Job Submission Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    JOB SUBMISSION FLOW                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Client                                                          │
│    │                                                             │
│    │ TCP: JobSubmission                                          │
│    ▼                                                             │
│  Gate (Leader)                                                   │
│    │                                                             │
│    ├──► Create DatacenterLease (fence_token)                    │
│    │                                                             │
│    │ TCP: JobSubmission (with lease)                            │
│    ▼                                                             │
│  Manager (Leader)                                                │
│    │                                                             │
│    ├──► Deserialize workflows                                   │
│    │                                                             │
│    │    For each workflow:                                      │
│    │    ┌────────────────────────────────────────────────┐      │
│    │    │ 1. Select eligible worker (crypto-random)      │      │
│    │    │ 2. Create ProvisionRequest (fence_token)       │      │
│    │    │ 3. Request quorum confirmation from peers      │      │
│    │    │ 4. On quorum: commit and dispatch              │      │
│    │    └────────────────────────────────────────────────┘      │
│    │                                                             │
│    │ TCP: WorkflowDispatch                                      │
│    ▼                                                             │
│  Worker                                                          │
│    │                                                             │
│    ├──► Allocate cores via _allocate_cores()                    │
│    ├──► Create WorkflowProgress tracker                         │
│    ├──► Execute via TaskRunner                                  │
│    │                                                             │
│    │ TCP: WorkflowDispatchAck                                   │
│    ▼                                                             │
│  Manager                                                         │
│    │                                                             │
│    │ TCP: JobAck                                                │
│    ▼                                                             │
│  Gate → Client                                                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Progress Update Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                   PROGRESS UPDATE FLOW                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Two parallel flows:                                             │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 1. ACTIVE UPDATES (TCP, throttled to 1/sec)               │  │
│  │                                                            │  │
│  │    Worker ──WorkflowProgress──► Manager                   │  │
│  │             (TCP, explicit)                                │  │
│  │                                                            │  │
│  │    • completed_count, failed_count                        │  │
│  │    • rate_per_second, elapsed_seconds                     │  │
│  │    • per-step stats                                       │  │
│  │    • assigned_cores list                                  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 2. PASSIVE DISCOVERY (UDP, via SWIM heartbeats)           │  │
│  │                                                            │  │
│  │    Worker ←─probe/ack─► Manager                           │  │
│  │    (WorkerHeartbeat embedded)                             │  │
│  │                                                            │  │
│  │    Manager ←─probe/ack─► Gate                             │  │
│  │    (ManagerHeartbeat embedded)                            │  │
│  │                                                            │  │
│  │    • Capacity, queue depth, resource utilization          │  │
│  │    • Active job/workflow counts                           │  │
│  │    • Leadership status                                    │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Aggregation:                                                    │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                                                            │  │
│  │   Worker Progress → Manager JobProgress → Gate GlobalJob  │  │
│  │                                                            │  │
│  │   GlobalJobStatus {                                       │  │
│  │     job_id, status                                        │  │
│  │     datacenters: [JobProgress, ...]                       │  │
│  │     total_completed, total_failed                         │  │
│  │     overall_rate, elapsed_seconds                         │  │
│  │     completed_datacenters, failed_datacenters             │  │
│  │   }                                                       │  │
│  │                                                            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Timing Diagrams

### SWIM Probe Cycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SWIM PROBE CYCLE TIMING                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Time ─────────────────────────────────────────────────────────────────────► │
│                                                                              │
│  Node A          Node B          Node C (proxy)         Node D               │
│    │                │                  │                   │                 │
│    │ ① probe        │                  │                   │                 │
│    │───────────────►│                  │                   │                 │
│    │                │                  │                   │                 │
│    │                │                  │                   │                 │
│    │ ② ack + HB     │                  │                   │                 │
│    │◄───────────────│                  │                   │                 │
│    │                │                  │                   │                 │
│  ──┴────────────────┴──────────────────┴───────────────────┴──────────────── │
│                                                                              │
│    SUCCESSFUL PROBE: base_timeout × LHM_multiplier                          │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════════ │
│                                                                              │
│  Time ─────────────────────────────────────────────────────────────────────► │
│                                                                              │
│  Node A          Node B (slow)       Node C (proxy)         Node D           │
│    │                │                     │                   │              │
│    │ ① probe        │                     │                   │              │
│    │───────────────►│                     │                   │              │
│    │                │                     │                   │              │
│    │      ┌─────────┼─────────────────────┼───────────────────┼────┐         │
│    │      │ TIMEOUT │ (no response)       │                   │    │         │
│    │      └─────────┼─────────────────────┼───────────────────┼────┘         │
│    │                │                     │                   │              │
│    │ ② ping-req (indirect probe)         │                   │              │
│    │─────────────────────────────────────►│                   │              │
│    │                │                     │                   │              │
│    │                │    ③ probe          │                   │              │
│    │                │◄────────────────────│                   │              │
│    │                │                     │                   │              │
│    │                │    ④ ack            │                   │              │
│    │                │────────────────────►│                   │              │
│    │                │                     │                   │              │
│    │ ⑤ ack (indirect)                     │                   │              │
│    │◄─────────────────────────────────────│                   │              │
│    │                │                     │                   │              │
│  ──┴────────────────┴─────────────────────┴───────────────────┴───────────── │
│                                                                              │
│    INDIRECT PROBE SUCCESS: Node B is alive but slow                          │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════════ │
│                                                                              │
│  Time ─────────────────────────────────────────────────────────────────────► │
│                                                                              │
│  Node A          Node B (dead)       Node C (proxy)         Node D           │
│    │                ╳                     │                   │              │
│    │ ① probe        ╳                     │                   │              │
│    │───────────────►╳                     │                   │              │
│    │                ╳                     │                   │              │
│    │      ┌─────────┼─────────────────────┼────┐              │              │
│    │      │ TIMEOUT │                     │    │              │              │
│    │      └─────────┼─────────────────────┼────┘              │              │
│    │                ╳                     │                   │              │
│    │ ② ping-req     ╳                     │                   │              │
│    │─────────────────────────────────────►│                   │              │
│    │                ╳                     │                   │              │
│    │                ╳    ③ probe          │                   │              │
│    │                ╳◄────────────────────│                   │              │
│    │                ╳                     │                   │              │
│    │                ╳     ┌───────────────┼────┐              │              │
│    │                ╳     │ TIMEOUT       │    │              │              │
│    │                ╳     └───────────────┼────┘              │              │
│    │                ╳                     │                   │              │
│    │ ④ nack (indirect failed)             │                   │              │
│    │◄─────────────────────────────────────│                   │              │
│    │                ╳                     │                   │              │
│    │ ⑤ START SUSPICION                    │                   │              │
│    │ broadcast suspect msg                │                   │              │
│    │─────────────────────────────────────►│──────────────────►│              │
│    │                ╳                     │                   │              │
│    │      ┌─────────┼─────────────────────┼───────────────────┼────┐         │
│    │      │ SUSPICION TIMEOUT             │                   │    │         │
│    │      │ T = k × log(n) × LHM          │                   │    │         │
│    │      └─────────┼─────────────────────┼───────────────────┼────┘         │
│    │                ╳                     │                   │              │
│    │ ⑥ MARK DEAD    ╳                     │                   │              │
│    │ broadcast dead msg                   │                   │              │
│    │─────────────────────────────────────►│──────────────────►│              │
│    │                ╳                     │                   │              │
│  ──┴────────────────╳─────────────────────┴───────────────────┴───────────── │
│                                                                              │
│    FAILURE DETECTION: Direct → Indirect → Suspicion → Dead                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Quorum Confirmation

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      QUORUM CONFIRMATION TIMING                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Time ─────────────────────────────────────────────────────────────────────► │
│                                                                              │
│  Manager 1         Manager 2 (★)        Manager 3           Worker           │
│  (follower)        (leader)             (follower)                           │
│     │                  │                    │                   │            │
│     │                  │ ① Job received     │                   │            │
│     │                  │◄═══════════════════│                   │            │
│     │                  │                    │                   │            │
│     │                  │ Select worker      │                   │            │
│     │                  │ Create provision   │                   │            │
│     │                  │                    │                   │            │
│     │ ② ProvisionReq   │                    │                   │            │
│     │◄─────────────────│                    │                   │            │
│     │                  │ ② ProvisionReq     │                   │            │
│     │                  │───────────────────►│                   │            │
│     │                  │                    │                   │            │
│     │ Validate:        │                    │ Validate:         │            │
│     │ • Worker alive?  │                    │ • Worker alive?   │            │
│     │ • Version fresh? │                    │ • Version fresh?  │            │
│     │ • Capacity ok?   │                    │ • Capacity ok?    │            │
│     │                  │                    │                   │            │
│     │ ③ ProvisionConf  │                    │                   │            │
│     │─────────────────►│                    │                   │            │
│     │                  │ ③ ProvisionConf    │                   │            │
│     │                  │◄───────────────────│                   │            │
│     │                  │                    │                   │            │
│     │                  │ QUORUM ACHIEVED    │                   │            │
│     │                  │ (2/3 = majority)   │                   │            │
│     │                  │                    │                   │            │
│     │ ④ ProvisionCommit│                    │                   │            │
│     │◄─────────────────│                    │                   │            │
│     │                  │ ④ ProvisionCommit  │                   │            │
│     │                  │───────────────────►│                   │            │
│     │                  │                    │                   │            │
│     │                  │ ⑤ WorkflowDispatch │                   │            │
│     │                  │────────────────────────────────────────►            │
│     │                  │                    │                   │            │
│     │                  │ ⑥ DispatchAck      │                   │            │
│     │                  │◄────────────────────────────────────────            │
│     │                  │                    │                   │            │
│  ───┴──────────────────┴────────────────────┴───────────────────┴─────────── │
│                                                                              │
│    SUCCESS: Quorum (n/2 + 1) confirmations → commit → dispatch               │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════════ │
│                                                                              │
│  TIMEOUT SCENARIO:                                                           │
│                                                                              │
│  Manager 1         Manager 2 (★)        Manager 3 (slow)      Worker         │
│     │                  │                    │                   │            │
│     │ ② ProvisionReq   │                    │                   │            │
│     │◄─────────────────│                    │                   │            │
│     │                  │ ② ProvisionReq     │                   │            │
│     │                  │───────────────────►│                   │            │
│     │                  │                    │                   │            │
│     │ ③ ProvisionConf  │                    │                   │            │
│     │─────────────────►│                    │ (processing...)   │            │
│     │                  │                    │                   │            │
│     │                  │      ┌─────────────┼────┐              │            │
│     │                  │      │ TIMEOUT     │    │              │            │
│     │                  │      └─────────────┼────┘              │            │
│     │                  │                    │                   │            │
│     │                  │ Only 1/3 confirm   │                   │            │
│     │                  │ (no quorum)        │                   │            │
│     │                  │                    │                   │            │
│     │ ④ ProvisionAbort │                    │                   │            │
│     │◄─────────────────│                    │                   │            │
│     │                  │                    │                   │            │
│     │                  │ Retry with         │                   │            │
│     │                  │ different worker   │                   │            │
│     │                  │                    │                   │            │
│  ───┴──────────────────┴────────────────────┴───────────────────┴─────────── │
│                                                                              │
│    FAILURE: Quorum timeout → abort → retry (different worker if available)  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Leader Election Sequence

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      LEADER ELECTION SEQUENCE                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Time ─────────────────────────────────────────────────────────────────────► │
│                                                                              │
│  TERM: 5           Node A (★ old)      Node B           Node C               │
│                       │                   │                │                 │
│                       ╳ CRASH             │                │                 │
│                       ╳                   │                │                 │
│                       ╳                   │                │                 │
│                       ╳     ┌─────────────┼────────────────┼────┐            │
│                       ╳     │ LEADER      │                │    │            │
│                       ╳     │ TIMEOUT     │                │    │            │
│                       ╳     └─────────────┼────────────────┼────┘            │
│                       ╳                   │                │                 │
│  ─────────────────────╳───────────────────┴────────────────┴──────────────── │
│                       ╳                                                      │
│  PRE-VOTE PHASE       ╳                                                      │
│                       ╳                                                      │
│  TERM: 5 (unchanged)  ╳    Node B             Node C                         │
│                       ╳       │                  │                           │
│                       ╳       │ Check eligibility│                           │
│                       ╳       │ (LHM ≤ 4.0 ✓)    │                           │
│                       ╳       │                  │                           │
│                       ╳       │ ① pre-vote-req (term=5)                      │
│                       ╳       │─────────────────►│                           │
│                       ╳       │                  │                           │
│                       ╳       │                  │ Compare:                  │
│                       ╳       │                  │ • No current leader       │
│                       ╳       │                  │ • B is eligible           │
│                       ╳       │                  │                           │
│                       ╳       │ ② pre-vote-grant │                           │
│                       ╳       │◄─────────────────│                           │
│                       ╳       │                  │                           │
│                       ╳       │ Pre-vote majority│                           │
│                       ╳       │ (2/2 = 100%)     │                           │
│                       ╳       │                  │                           │
│  ─────────────────────╳───────┴──────────────────┴────────────────────────── │
│                       ╳                                                      │
│  VOTE PHASE           ╳                                                      │
│                       ╳                                                      │
│  TERM: 6 (incremented)╳    Node B             Node C                         │
│                       ╳       │                  │                           │
│                       ╳       │ Increment term   │                           │
│                       ╳       │ Vote for self    │                           │
│                       ╳       │                  │                           │
│                       ╳       │ ③ vote-req (term=6)                          │
│                       ╳       │─────────────────►│                           │
│                       ╳       │                  │                           │
│                       ╳       │                  │ Term 6 > my term 5        │
│                       ╳       │                  │ Grant vote                │
│                       ╳       │                  │                           │
│                       ╳       │ ④ vote-grant     │                           │
│                       ╳       │◄─────────────────│                           │
│                       ╳       │                  │                           │
│                       ╳       │ Vote majority    │                           │
│                       ╳       │ (2/2 = 100%)     │                           │
│                       ╳       │                  │                           │
│  ─────────────────────╳───────┴──────────────────┴────────────────────────── │
│                       ╳                                                      │
│  LEADER ANNOUNCEMENT  ╳                                                      │
│                       ╳                                                      │
│  TERM: 6              ╳    Node B (★ new)     Node C                         │
│                       ╳       │                  │                           │
│                       ╳       │ ⑤ leader-announce│                           │
│                       ╳       │─────────────────►│                           │
│                       ╳       │                  │                           │
│                       ╳       │ Trigger:         │                           │
│                       ╳       │ _on_become_leader│                           │
│                       ╳       │                  │ Trigger:                  │
│                       ╳       │                  │ _on_leader_change         │
│                       ╳       │                  │                           │
│                       ╳       │ Begin state sync │                           │
│                       ╳       │ from workers     │                           │
│                       ╳       │                  │                           │
│  ─────────────────────╳───────┴──────────────────┴────────────────────────── │
│                                                                              │
│  SPLIT-BRAIN PREVENTION:                                                     │
│  • Pre-vote phase doesn't increment term (prevents term explosion)           │
│  • Candidate must get pre-vote majority before real election                │
│  • Nodes only grant pre-vote if no current leader OR candidate is better    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Failure Handling

### Worker Failure

```
┌─────────────────────────────────────────────────────────────────┐
│                    WORKER FAILURE HANDLING                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Detection (SWIM UDP):                                           │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 1. Direct probe times out (LHM-adjusted timeout)          │  │
│  │ 2. Indirect probe via random proxy                        │  │
│  │ 3. Suspicion timer starts (confirmation-based)            │  │
│  │ 4. No refutation → Node marked DEAD                       │  │
│  └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  Manager._on_node_dead() callback:                               │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 1. O(1) lookup via _worker_addr_to_id                     │  │
│  │ 2. Clean up: _workers, _worker_status, _worker_last_status│  │
│  │ 3. Find workflows assigned to failed worker               │  │
│  │ 4. For each workflow:                                     │  │
│  │    • Get/create retry info (_workflow_retries)            │  │
│  │    • Add failed worker to exclusion set                   │  │
│  │    • If retries < max: select new worker, re-dispatch     │  │
│  │    • If retries >= max: mark workflow FAILED              │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Retry Logic:                                                    │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ _workflow_retries: {                                      │  │
│  │   workflow_id: (                                          │  │
│  │     retry_count: int,                                     │  │
│  │     original_dispatch_bytes: bytes,  # preserved          │  │
│  │     failed_workers: set[str],        # exclusion list     │  │
│  │   )                                                       │  │
│  │ }                                                         │  │
│  │                                                            │  │
│  │ New dispatch:                                             │  │
│  │ • Deserialize original WorkflowDispatch                   │  │
│  │ • Create new dispatch with new fence_token                │  │
│  │ • Select worker excluding failed_workers set              │  │
│  │ • Increment retry_count                                   │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Manager Failure

```
┌─────────────────────────────────────────────────────────────────┐
│                   MANAGER FAILURE HANDLING                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Detection: SWIM cluster among managers                          │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ PEER TRACKING (each manager maintains):                   │  │
│  │                                                            │  │
│  │ _manager_udp_to_tcp: dict[(host,port) → (host,port)]     │  │
│  │   Maps SWIM UDP addresses to TCP addresses                │  │
│  │                                                            │  │
│  │ _active_manager_peers: set[(host,port)]                   │  │
│  │   Currently live peer managers (updated via callbacks)    │  │
│  │                                                            │  │
│  │ _on_node_dead() checks BOTH:                              │  │
│  │   • _worker_addr_to_id (for worker failure)               │  │
│  │   • _manager_udp_to_tcp (for peer manager failure)        │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  New Leader Election:                                            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 1. Leader failure detected via SWIM                       │  │
│  │ 2. Leader's heartbeats stop → lease expires on followers  │  │
│  │ 3. Pre-voting phase among eligible managers               │  │
│  │ 4. Candidate with lowest LHM + highest priority wins      │  │
│  │ 5. New leader announces with new term number              │  │
│  │                                                            │  │
│  │ Note: Leadership re-election is AUTOMATIC via lease       │  │
│  │ expiry in LocalLeaderElection - no manual intervention    │  │
│  └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  Peer Manager Failure:                                           │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ _handle_manager_peer_failure():                           │  │
│  │                                                            │  │
│  │ 1. Remove from _active_manager_peers                      │  │
│  │ 2. Check if dead peer was the leader                      │  │
│  │ 3. Log quorum status for monitoring                       │  │
│  │                                                            │  │
│  │ Quorum calculation:                                        │  │
│  │ • Uses CONFIGURED peer count (prevents split-brain)       │  │
│  │ • _has_quorum_available() checks ACTIVE vs required       │  │
│  └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  Peer Manager Recovery:                                          │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ _handle_manager_peer_recovery() (via on_node_join):       │  │
│  │                                                            │  │
│  │ 1. Add back to _active_manager_peers                      │  │
│  │ 2. Log recovery and quorum status                         │  │
│  │ 3. Quorum capacity restored                               │  │
│  └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  State Synchronization (new leader only):                        │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ _on_manager_become_leader() callback:                     │  │
│  │                                                            │  │
│  │ 1. Request StateSyncRequest from all registered workers   │  │
│  │ 2. Workers respond with WorkerStateSnapshot               │  │
│  │    • active_workflows: dict[workflow_id → progress]       │  │
│  │    • Core allocations, version                            │  │
│  │ 3. New leader rebuilds authoritative state from workers   │  │
│  │    (Workers are source of truth)                          │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  In-Flight Work:                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ • Pending provisions: timeout and client retries          │  │
│  │ • Running workflows: continue on workers (unaffected)     │  │
│  │ • Progress updates: resume after new leader sync          │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Worker Manager Failover

```
┌─────────────────────────────────────────────────────────────────┐
│                 WORKER MANAGER FAILOVER                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  When a worker detects its assigned manager has failed:          │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ _handle_manager_failure() (via on_node_dead callback):    │  │
│  │                                                            │  │
│  │ 1. Check if dead node is current manager                  │  │
│  │ 2. Clear _current_manager reference                       │  │
│  │ 3. Iterate through _manager_addrs backup list             │  │
│  │ 4. Skip the failed manager                                │  │
│  │ 5. Attempt registration with each alternative             │  │
│  │ 6. On success: set _current_manager, report workflows     │  │
│  └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  Report Active Workflows:                                        │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ _report_active_workflows_to_manager():                    │  │
│  │                                                            │  │
│  │ For each workflow in _active_workflows:                   │  │
│  │   • Send WorkflowProgress to new manager                  │  │
│  │   • Ensures new manager is aware of in-flight work        │  │
│  │   • No workflow interruption during failover              │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Timeline:                                                       │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                                                            │  │
│  │  Manager A dies                                            │  │
│  │       │                                                    │  │
│  │       ▼                                                    │  │
│  │  SWIM detects (probe → indirect → suspicion → DEAD)       │  │
│  │       │                                                    │  │
│  │       ▼                                                    │  │
│  │  Worker._on_node_dead(Manager A addr)                     │  │
│  │       │                                                    │  │
│  │       ▼                                                    │  │
│  │  _handle_manager_failure() runs                           │  │
│  │       │                                                    │  │
│  │       ▼                                                    │  │
│  │  Try Manager B from _manager_addrs                        │  │
│  │       │                                                    │  │
│  │       ▼                                                    │  │
│  │  Registration succeeds → _current_manager = B             │  │
│  │       │                                                    │  │
│  │       ▼                                                    │  │
│  │  _report_active_workflows_to_manager()                    │  │
│  │       │                                                    │  │
│  │       ▼                                                    │  │
│  │  Normal operation resumes with Manager B                  │  │
│  │                                                            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Datacenter Failure

```
┌─────────────────────────────────────────────────────────────────┐
│                  DATACENTER FAILURE HANDLING                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Detection (at Gate):                                            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ • No ManagerHeartbeat received (SWIM timeout)             │  │
│  │ • All managers in DC marked DEAD                          │  │
│  │ • DC marked unavailable                                   │  │
│  └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  Gate Handling:                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Lease-based at-most-once:                                 │  │
│  │                                                            │  │
│  │ • If lease expired → Job marked FAILED for that DC        │  │
│  │ • If lease valid → Wait for recovery or timeout           │  │
│  │                                                            │  │
│  │ User-facing: Gate returns job failure to client           │  │
│  │ (No automatic cross-DC retry - explicit decision)         │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Failure Recovery Flows

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       FAILURE RECOVERY MATRIX                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────┬─────────────────────┬──────────────────────────────────┐│
│  │ FAILURE TYPE   │ DETECTION           │ RECOVERY ACTION                  ││
│  ├────────────────┼─────────────────────┼──────────────────────────────────┤│
│  │                │                     │                                  ││
│  │ Worker crash   │ SWIM probe timeout  │ Retry workflow on another worker ││
│  │                │ + indirect probe    │ Exclude failed worker from retry ││
│  │                │ + suspicion expiry  │ Mark workflow FAILED if max retry││
│  │                │                     │                                  ││
│  ├────────────────┼─────────────────────┼──────────────────────────────────┤│
│  │                │                     │                                  ││
│  │ Worker         │ WorkerHeartbeat     │ Deprioritize in worker selection ││
│  │ overloaded     │ state = DEGRADED    │ Apply backpressure signaling     ││
│  │                │ OR queue_depth high │ Extend timeouts via LHM          ││
│  │                │                     │                                  ││
│  ├────────────────┼─────────────────────┼──────────────────────────────────┤│
│  │                │                     │                                  ││
│  │ Manager        │ SWIM detects DEAD   │ Pre-vote → elect new leader      ││
│  │ leader crash   │ among manager peers │ New leader syncs state from      ││
│  │                │                     │ all workers (source of truth)    ││
│  │                │                     │                                  ││
│  ├────────────────┼─────────────────────┼──────────────────────────────────┤│
│  │                │                     │                                  ││
│  │ Manager        │ Quorum timeout      │ Retry with original quorum       ││
│  │ follower crash │ for confirmation    │ If quorum impossible → abort job ││
│  │                │                     │ New manager syncs when joins     ││
│  │                │                     │                                  ││
│  ├────────────────┼─────────────────────┼──────────────────────────────────┤│
│  │                │                     │                                  ││
│  │ Gate leader    │ SWIM among gates    │ New gate leader elected          ││
│  │ crash          │                     │ Lease transfer to new leader     ││
│  │                │                     │ Jobs continue with new gate      ││
│  │                │                     │                                  ││
│  ├────────────────┼─────────────────────┼──────────────────────────────────┤│
│  │                │                     │                                  ││
│  │ Datacenter     │ All managers DEAD   │ Gate marks DC as failed          ││
│  │ total failure  │ No ManagerHeartbeat │ Lease expires → job FAILED       ││
│  │                │                     │ Return failure to client         ││
│  │                │                     │                                  ││
│  ├────────────────┼─────────────────────┼──────────────────────────────────┤│
│  │                │                     │                                  ││
│  │ Network        │ Partial SWIM        │ Pre-vote prevents split-brain    ││
│  │ partition      │ connectivity        │ Minority partition steps down    ││
│  │                │                     │ Majority continues operation     ││
│  │                │                     │                                  ││
│  └────────────────┴─────────────────────┴──────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Network Partition Handling

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     NETWORK PARTITION SCENARIOS                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  SCENARIO 1: Manager Cluster Partition (2+1)                                 │
│  ════════════════════════════════════════════                                │
│                                                                              │
│     ┌─────────────────────────┐       ║      ┌─────────────────┐            │
│     │      PARTITION A        │       ║      │   PARTITION B   │            │
│     │   (majority: 2 nodes)   │       ║      │ (minority: 1)   │            │
│     │                         │       ║      │                 │            │
│     │   ┌────┐     ┌────┐    │       ║      │     ┌────┐      │            │
│     │   │ M1 │◄───►│ M2 │    │       ║      │     │ M3 │      │            │
│     │   │ ★  │     │    │    │       ║      │     │    │      │            │
│     │   └────┘     └────┘    │       ║      │     └────┘      │            │
│     │                         │       ║      │                 │            │
│     │   Maintains leadership  │       ║      │  Steps down     │            │
│     │   Continues operation   │       ║      │  (no majority)  │            │
│     │                         │       ║      │                 │            │
│     └─────────────────────────┘       ║      └─────────────────┘            │
│                                       ║                                      │
│                              NETWORK PARTITION                               │
│                                                                              │
│  Behavior:                                                                   │
│  • M3 cannot reach M1/M2, loses leader heartbeats                           │
│  • M3 starts pre-vote, but cannot get majority (only self)                  │
│  • M3 remains follower, does not disrupt cluster                            │
│  • M1 (leader) continues with M2 (2/3 = quorum for confirmations)           │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════════ │
│                                                                              │
│  SCENARIO 2: Worker Isolation                                                │
│  ════════════════════════════════                                            │
│                                                                              │
│     ┌─────────────────────────┐       ║      ┌─────────────────┐            │
│     │      MANAGER SIDE       │       ║      │  ISOLATED       │            │
│     │                         │       ║      │  WORKER         │            │
│     │   ┌────┐     ┌────┐    │       ║      │                 │            │
│     │   │ M1 │     │ M2 │    │       ║      │     ┌────┐      │            │
│     │   │ ★  │     │    │    │       ║      │     │ W3 │      │            │
│     │   └──┬─┘     └────┘    │       ║      │     │    │      │            │
│     │      │                  │       ║      │     └────┘      │            │
│     │      ▼                  │       ║      │                 │            │
│     │   ┌────┐     ┌────┐    │       ║      │  Continues      │            │
│     │   │ W1 │     │ W2 │    │       ║      │  executing      │            │
│     │   └────┘     └────┘    │       ║      │  (timeout will  │            │
│     │                         │       ║      │  eventually     │            │
│     │   Reschedule W3 work   │       ║      │  cancel)        │            │
│     │   on W1 or W2          │       ║      │                 │            │
│     └─────────────────────────┘       ║      └─────────────────┘            │
│                                       ║                                      │
│                                                                              │
│  Behavior:                                                                   │
│  • Manager probes W3 → timeout → indirect probe → suspicion → DEAD          │
│  • Manager triggers _on_node_dead callback                                  │
│  • Workflows on W3 are retried on W1/W2 (excluding W3)                      │
│  • If partition heals before W3 timeout, W3 may complete redundantly        │
│  • Fence tokens prevent duplicate commits                                   │
│                                                                              │
│  ═══════════════════════════════════════════════════════════════════════════ │
│                                                                              │
│  SCENARIO 3: Gate-to-DC Partition                                            │
│  ════════════════════════════════════                                        │
│                                                                              │
│     ┌─────────────────┐               ║      ┌─────────────────┐            │
│     │   GATE CLUSTER  │               ║      │  DATACENTER A   │            │
│     │                 │               ║      │                 │            │
│     │   ┌────┐       │               ║      │   ┌────┐        │            │
│     │   │ G1 │        │               ║      │   │ M1 │        │            │
│     │   │ ★  │        │               ║      │   │ ★  │        │            │
│     │   └────┘        │               ║      │   └──┬─┘        │            │
│     │                 │               ║      │      ▼          │            │
│     │   Jobs for DC-A │               ║      │   ┌────┐        │            │
│     │   marked FAILED │               ║      │   │ W1 │        │            │
│     │   (lease expiry)│               ║      │   └────┘        │            │
│     │                 │               ║      │                 │            │
│     └─────────────────┘               ║      │ DC continues    │            │
│                                       ║      │ until timeout   │            │
│                                       ║      └─────────────────┘            │
│                                                                              │
│  Behavior:                                                                   │
│  • Gate stops receiving ManagerHeartbeat from DC-A                          │
│  • Gate marks DC-A managers as DEAD via SWIM                                │
│  • Lease for DC-A jobs expires                                              │
│  • Gate returns job failure to client (no cross-DC retry)                   │
│  • DC-A workflows eventually timeout or complete (ignored by gate)          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Cascading Failure Protection

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     CASCADING FAILURE PROTECTION                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  PROTECTION MECHANISMS:                                                      │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 1. LOCAL HEALTH MULTIPLIER (LHM)                                       │ │
│  │                                                                         │ │
│  │    ┌──────────────────────────────────────────────────────────────┐    │ │
│  │    │                                                               │    │ │
│  │    │   Probe fails ──► LHM increases ──► Timeouts extend           │    │ │
│  │    │        ▲                                    │                 │    │ │
│  │    │        │                                    ▼                 │    │ │
│  │    │        └────────── Prevents ◄─── False positives reduced      │    │ │
│  │    │                    cascade                                    │    │ │
│  │    │                                                               │    │ │
│  │    └──────────────────────────────────────────────────────────────┘    │ │
│  │                                                                         │ │
│  │    If one node is slow, we don't mark it dead prematurely              │ │
│  │    → Prevents triggering retry storm on healthy workers                │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 2. GRACEFUL DEGRADATION                                                │ │
│  │                                                                         │ │
│  │    Load Level     │ Action                                             │ │
│  │    ───────────────┼─────────────────────────────────────────────────── │ │
│  │    NORMAL         │ Full operation                                     │ │
│  │    ELEVATED       │ Reduce gossip frequency                            │ │
│  │    HIGH           │ Skip non-essential probes                          │ │
│  │    SEVERE         │ Leader considers stepping down                     │ │
│  │    CRITICAL       │ Reject new work, focus on completing existing      │ │
│  │                                                                         │ │
│  │    Prevents: Overloaded node being marked dead due to slow responses   │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 3. BACKPRESSURE SIGNALING                                              │ │
│  │                                                                         │ │
│  │    Worker queue_depth ──► Embedded in WorkerHeartbeat                  │ │
│  │                                    │                                    │ │
│  │                                    ▼                                    │ │
│  │                           Manager respects soft_limit                   │ │
│  │                                    │                                    │ │
│  │                                    ▼                                    │ │
│  │                           New work → other workers                      │ │
│  │                                                                         │ │
│  │    Prevents: Overloading already-stressed workers                      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 4. RETRY LIMITS & EXCLUSION                                            │ │
│  │                                                                         │ │
│  │    Workflow fails on Worker A                                          │ │
│  │         │                                                               │ │
│  │         ▼                                                               │ │
│  │    Retry 1: Select from {B, C, D} (A excluded)                         │ │
│  │         │                                                               │ │
│  │    Fails on Worker B                                                    │ │
│  │         │                                                               │ │
│  │         ▼                                                               │ │
│  │    Retry 2: Select from {C, D} (A, B excluded)                         │ │
│  │         │                                                               │ │
│  │    Fails on Worker C                                                    │ │
│  │         │                                                               │ │
│  │         ▼                                                               │ │
│  │    max_retries reached → FAILED (no more attempts)                      │ │
│  │                                                                         │ │
│  │    Prevents: Infinite retry loops, same worker repeated failure        │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 5. CIRCUIT BREAKERS                                                    │ │
│  │                                                                         │ │
│  │    ErrorHandler tracks errors by category:                              │ │
│  │                                                                         │ │
│  │    NETWORK errors ──► threshold exceeded ──► circuit OPEN              │ │
│  │                                                   │                     │ │
│  │                                                   ▼                     │ │
│  │                                           Fail fast (no retry)          │ │
│  │                                                   │                     │ │
│  │                                           cooldown period               │ │
│  │                                                   │                     │ │
│  │                                                   ▼                     │ │
│  │                                           circuit HALF-OPEN             │ │
│  │                                                   │                     │ │
│  │                                           test request                  │ │
│  │                                                   │                     │ │
│  │                                    success ──► CLOSED   failure ──► OPEN│ │
│  │                                                                         │ │
│  │    Prevents: Repeated attempts to failing resources                    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 6. FLAPPING DETECTION                                                  │ │
│  │                                                                         │ │
│  │    Leadership changes in sliding window:                                │ │
│  │                                                                         │ │
│  │    Time: ─────────[change]───[change]───[change]───[change]─────►       │ │
│  │                                                          │              │ │
│  │                                            4 changes in 60s             │ │
│  │                                                          │              │ │
│  │                                                          ▼              │ │
│  │                                               COOLDOWN ACTIVATED        │ │
│  │                                               (no new elections)        │ │
│  │                                                          │              │ │
│  │                                               cooldown expires          │ │
│  │                                                          │              │ │
│  │                                                          ▼              │ │
│  │                                               Normal operation          │ │
│  │                                                                         │ │
│  │    Prevents: Leadership oscillation under unstable conditions          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Zombie Job Prevention & Detection

This section documents the mechanisms for detecting, preventing, and cleaning up "zombie" jobs - jobs that become stuck, orphaned, or fail to complete properly.

### Zombie Job Lifecycle Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ZOMBIE JOB LIFECYCLE & PREVENTION                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  What is a "Zombie Job"?                                                     │
│  ───────────────────────                                                     │
│  A job that:                                                                 │
│  • Consumes resources without making progress                                │
│  • Has no live owner/manager tracking it                                     │
│  • Cannot be cancelled via normal means                                      │
│  • Prevents completion of parent job                                         │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                    ZOMBIE CREATION SCENARIOS                           │ │
│  │                                                                        │ │
│  │  Scenario 1: Worker Dies Mid-Workflow                                  │ │
│  │  ─────────────────────────────────────────                             │ │
│  │  Worker ──[executing workflow]──► CRASH! ──► Workflow state lost       │ │
│  │                                                                        │ │
│  │  Scenario 2: Manager Dies After Dispatch                               │ │
│  │  ─────────────────────────────────────────                             │ │
│  │  Manager ──[dispatch]──► Worker ──► Manager CRASH ──► No result recv   │ │
│  │                                                                        │ │
│  │  Scenario 3: Network Partition                                         │ │
│  │  ─────────────────────────────────────────                             │ │
│  │  Manager ◄──X──► Worker   (both think workflow is running)             │ │
│  │                                                                        │ │
│  │  Scenario 4: Workflow Execution Hang                                   │ │
│  │  ─────────────────────────────────────────                             │ │
│  │  Worker ──[workflow.execute() hangs indefinitely]──► Never completes   │ │
│  │                                                                        │ │
│  │  Scenario 5: Result Delivery Failure                                   │ │
│  │  ─────────────────────────────────────────                             │ │
│  │  Worker ──► Result ──X──► Manager   (result lost, no retry)            │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Detection Mechanisms

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ZOMBIE DETECTION MECHANISMS                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 1. WORKFLOW TIMEOUT DETECTION (WorkflowDispatcher)                     │ │
│  │                                                                        │ │
│  │    Location: hyperscale/distributed_rewrite/jobs/workflow_dispatcher.py│ │
│  │                                                                        │ │
│  │    ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │    │                                                                  │ │ │
│  │    │    WorkflowDispatcher.check_timeouts()                          │ │ │
│  │    │           │                                                      │ │ │
│  │    │           ▼                                                      │ │ │
│  │    │    for pending in self._pending:                                │ │ │
│  │    │        age = now - pending.registered_at                        │ │ │
│  │    │        │                                                         │ │ │
│  │    │        ├── if age > pending.timeout_seconds:                    │ │ │
│  │    │        │       └── EVICT (reason: "timeout")                    │ │ │
│  │    │        │                                                         │ │ │
│  │    │        └── if pending.dispatch_attempts > max_attempts:         │ │ │
│  │    │                └── EVICT (reason: "max_dispatch_attempts")       │ │ │
│  │    │                                                                  │ │ │
│  │    │    Default timeout_seconds: 300 (5 minutes)                     │ │ │
│  │    │    Default max_dispatch_attempts: 5                             │ │ │
│  │    │    Check interval: 30 seconds (via _job_cleanup_loop)            │ │ │
│  │    │                                                                  │ │ │
│  │    └─────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                        │ │
│  │    Callbacks Invoked:                                                  │ │
│  │    • on_workflow_evicted(job_id, workflow_id, reason)                 │ │
│  │    • on_dispatch_failed(job_id, workflow_id)                          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 2. DEAD WORKER DETECTION (SWIM Protocol + Callbacks)                   │ │
│  │                                                                        │ │
│  │    Detection Flow:                                                     │ │
│  │                                                                        │ │
│  │    SWIM Probe ──► Timeout ──► Indirect Probe ──► Timeout               │ │
│  │                                    │                                   │ │
│  │                                    ▼                                   │ │
│  │                           Enter SUSPECT state                          │ │
│  │                                    │                                   │ │
│  │                           No refutation (30s)                          │ │
│  │                                    │                                   │ │
│  │                                    ▼                                   │ │
│  │                           Mark DEAD ──► _on_node_dead() callback       │ │
│  │                                    │                                   │ │
│  │                                    ▼                                   │ │
│  │    Manager identifies all workflows assigned to dead worker            │ │
│  │    │                                                                   │ │
│  │    ├── Retry count < max: Re-dispatch to new worker                   │ │
│  │    │       └── Failed worker added to exclusion set                   │ │
│  │    │                                                                   │ │
│  │    └── Retry count >= max: Mark workflow FAILED                       │ │
│  │                                                                        │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 3. PROGRESS-BASED HEALTH DETECTION (AD-19 Three-Signal Model)          │ │
│  │                                                                        │ │
│  │    Location: hyperscale/distributed_rewrite/health/                    │ │
│  │                                                                        │ │
│  │    ProgressState Assessment:                                           │ │
│  │    ┌─────────────────────────────────────────────────────────────────┐ │ │
│  │    │ State     │ Criteria                  │ Implication             │ │ │
│  │    │───────────┼───────────────────────────┼─────────────────────────│ │ │
│  │    │ IDLE      │ No active workflows       │ Normal - no work        │ │ │
│  │    │ NORMAL    │ completion_rate >= expected │ Healthy operation     │ │ │
│  │    │ SLOW      │ completion_rate < 50%     │ Possible contention     │ │ │
│  │    │ DEGRADED  │ completion_rate < 25%     │ Significant slowdown    │ │ │
│  │    │ STUCK     │ No progress for threshold │ Potential zombie        │ │ │
│  │    └─────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                        │ │
│  │    Routing Decision Based on Health:                                   │ │
│  │    • ROUTE: Send new work                                              │ │
│  │    • DRAIN: Stop sending work, let existing complete                   │ │
│  │    • INVESTIGATE: Suspect issue, check more signals                    │ │
│  │    • EVICT: Remove from routing, assume dead/zombie                    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 4. LEASE EXPIRY DETECTION (Gate Layer)                                 │ │
│  │                                                                        │ │
│  │    Location: hyperscale/distributed_rewrite/leases/job_lease.py        │ │
│  │                                                                        │ │
│  │    Job Lease Lifecycle:                                                │ │
│  │                                                                        │ │
│  │    Gate-1 acquires lease ──► lease.expires_at = now + 30s              │ │
│  │          │                                                             │ │
│  │          ├── Renew: lease.expires_at += renewal_period                 │ │
│  │          │                                                             │ │
│  │          └── Fail to renew (crash/partition):                          │ │
│  │                  │                                                     │ │
│  │                  ▼                                                     │ │
│  │          Lease expires ──► Gate-2 can claim ──► fence_token++          │ │
│  │                                   │                                    │ │
│  │                                   ▼                                    │ │
│  │          Old results with stale fence_token are REJECTED               │ │
│  │                                                                        │ │
│  │    Default lease_timeout: 30 seconds                                   │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Prevention Mechanisms

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ZOMBIE PREVENTION MECHANISMS                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 1. FENCE TOKENS (At-Most-Once Dispatch Semantics)                      │ │
│  │                                                                        │ │
│  │    Location: Worker._workflow_fence_tokens                             │ │
│  │                                                                        │ │
│  │    Purpose: Prevent duplicate/stale dispatches from creating zombies   │ │
│  │                                                                        │ │
│  │    ┌────────────────────────────────────────────────────────────────┐  │ │
│  │    │                                                                 │  │ │
│  │    │  Worker receives WorkflowDispatch(workflow_id, fence_token=5)  │  │ │
│  │    │              │                                                  │  │ │
│  │    │              ▼                                                  │  │ │
│  │    │  current = _workflow_fence_tokens.get(workflow_id, -1)         │  │ │
│  │    │              │                                                  │  │ │
│  │    │   ┌──────────┴──────────┐                                       │  │ │
│  │    │   │                     │                                       │  │ │
│  │    │   ▼                     ▼                                       │  │ │
│  │    │ fence_token <= current  fence_token > current                  │  │ │
│  │    │   │                     │                                       │  │ │
│  │    │   ▼                     ▼                                       │  │ │
│  │    │ REJECT (stale)        ACCEPT                                   │  │ │
│  │    │ Return NACK            │                                       │  │ │
│  │    │                        ▼                                       │  │ │
│  │    │              _workflow_fence_tokens[workflow_id] = fence_token │  │ │
│  │    │              Execute workflow                                  │  │ │
│  │    │                                                                 │  │ │
│  │    └────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                        │ │
│  │    Prevents:                                                           │ │
│  │    • Duplicate execution from retry storms                            │ │
│  │    • Stale dispatches from recovered old manager                      │ │
│  │    • Split-brain double execution                                      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 2. VERSIONED STATE CLOCK (Stale Update Rejection)                      │ │
│  │                                                                        │ │
│  │    Location: hyperscale/distributed_rewrite/swim/versioned_clock.py    │ │
│  │                                                                        │ │
│  │    Purpose: Reject out-of-order updates that could create             │ │
│  │             inconsistent state                                         │ │
│  │                                                                        │ │
│  │    VersionedStateClock {                                               │ │
│  │        _entity_versions: dict[str, (version, timestamp)]              │ │
│  │                                                                        │ │
│  │        is_entity_stale(entity_id, incoming_version) -> bool           │ │
│  │        check_and_update(entity_id, incoming_version) -> bool          │ │
│  │        cleanup_old_entities(max_age) -> None                          │ │
│  │    }                                                                   │ │
│  │                                                                        │ │
│  │    Used at:                                                            │ │
│  │    • Manager receiving WorkerHeartbeat                                │ │
│  │    • Manager receiving WorkflowProgress                               │ │
│  │    • Gate receiving ManagerHeartbeat                                  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 3. CANCELLATION POLLING (Fallback When Push Fails)                     │ │
│  │                                                                        │ │
│  │    Location: Worker._cancellation_poll_loop()                          │ │
│  │                                                                        │ │
│  │    Problem: Cancellation push from manager might not reach worker      │ │
│  │    Solution: Worker periodically polls manager for cancellation status │ │
│  │                                                                        │ │
│  │    ┌────────────────────────────────────────────────────────────────┐  │ │
│  │    │                                                                 │  │ │
│  │    │  while running:                                                 │  │ │
│  │    │      await sleep(poll_interval)  # Default: 5-10s              │  │ │
│  │    │                                                                 │  │ │
│  │    │      for workflow_id in active_workflows:                      │  │ │
│  │    │          │                                                      │  │ │
│  │    │          ▼                                                      │  │ │
│  │    │      Send WorkflowCancellationQuery to manager                 │  │ │
│  │    │          │                                                      │  │ │
│  │    │          ▼                                                      │  │ │
│  │    │      if response.is_cancelled:                                 │  │ │
│  │    │          _cancel_workflow(workflow_id, "poll_detected")        │  │ │
│  │    │                                                                 │  │ │
│  │    └────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                        │ │
│  │    Ensures: Cancellations are never "lost" due to network issues      │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 4. ADAPTIVE HEALTHCHECK EXTENSIONS (AD-26)                             │ │
│  │                                                                        │ │
│  │    Location: hyperscale/distributed_rewrite/health/extension_tracker.py│ │
│  │                                                                        │ │
│  │    Problem: Long-running workflows might be killed as "stuck"         │ │
│  │    Solution: Allow legitimate slow workers to request deadline extensions│
│  │                                                                        │ │
│  │    Extension Request Flow:                                             │ │
│  │                                                                        │ │
│  │    Worker ──► Heartbeat with extension_requested=True ──► Manager      │ │
│  │                    │                                                   │ │
│  │                    ▼                                                   │ │
│  │    ExtensionTracker.request_extension(reason, current_progress)        │ │
│  │                    │                                                   │ │
│  │        ┌───────────┴───────────┐                                       │ │
│  │        │                       │                                       │ │
│  │        ▼                       ▼                                       │ │
│  │    GRANTED                  DENIED                                     │ │
│  │    (extension_seconds)      (denial_reason)                            │ │
│  │                                                                        │ │
│  │    Grant Decay (Logarithmic):                                          │ │
│  │    ┌────────────────────────────────────────────────────────────────┐  │ │
│  │    │ Grant # │ Formula        │ Example (base=30s) │                │  │ │
│  │    │─────────┼────────────────┼────────────────────│                │  │ │
│  │    │ 1       │ base / 2       │ 15s                │                │  │ │
│  │    │ 2       │ base / 4       │ 7.5s               │                │  │ │
│  │    │ 3       │ base / 8       │ 3.75s              │                │  │ │
│  │    │ 4       │ base / 16      │ 1.875s             │                │  │ │
│  │    │ 5       │ min_grant      │ 1s (capped)        │                │  │ │
│  │    └────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                        │ │
│  │    Denial Reasons:                                                     │ │
│  │    • "max_extensions_exceeded" - Already used all extensions          │ │
│  │    • "no_progress" - Progress same as last request (stuck)            │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Cleanup Mechanisms

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ZOMBIE CLEANUP MECHANISMS                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 1. MANAGER JOB CLEANUP LOOP                                            │ │
│  │                                                                        │ │
│  │    Location: Manager._job_cleanup_loop() (manager.py:6225)             │ │
│  │                                                                        │ │
│  │    Interval: MERCURY_SYNC_CLEANUP_INTERVAL (default: 30s)              │ │
│  │                                                                        │ │
│  │    ┌────────────────────────────────────────────────────────────────┐  │ │
│  │    │                                                                 │  │ │
│  │    │  while running:                                                 │  │ │
│  │    │      await sleep(cleanup_interval)                             │  │ │
│  │    │                                                                 │  │ │
│  │    │      # 1. Check workflow timeouts via dispatcher               │  │ │
│  │    │      evicted = await _workflow_dispatcher.check_timeouts()     │  │ │
│  │    │      for (job_id, workflow_id, reason) in evicted:             │  │ │
│  │    │          mark_workflow_failed(job_id, workflow_id, reason)     │  │ │
│  │    │                                                                 │  │ │
│  │    │      # 2. Clean completed jobs after retention period          │  │ │
│  │    │      for job_id, job in _jobs.items():                         │  │ │
│  │    │          if job.status == COMPLETED:                           │  │ │
│  │    │              if age > _completed_job_max_age:  # ~30 min       │  │ │
│  │    │                  cleanup_job(job_id)                           │  │ │
│  │    │                                                                 │  │ │
│  │    │      # 3. Clean failed/cancelled/timeout jobs                  │  │ │
│  │    │      for job_id, job in _jobs.items():                         │  │ │
│  │    │          if job.status in [FAILED, CANCELLED, TIMEOUT]:        │  │ │
│  │    │              if age > _failed_job_max_age:  # longer retention │  │ │
│  │    │                  cleanup_job(job_id)                           │  │ │
│  │    │                                                                 │  │ │
│  │    └────────────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 2. DEAD NODE REAP LOOP                                                 │ │
│  │                                                                        │ │
│  │    Location: Manager._dead_node_reap_loop() (manager.py:6380)          │ │
│  │                                                                        │ │
│  │    ┌────────────────────────────────────────────────────────────────┐  │ │
│  │    │                                                                 │  │ │
│  │    │  Reap Intervals:                                                │  │ │
│  │    │  ├── Dead workers: MANAGER_DEAD_WORKER_REAP_INTERVAL (~24h)    │  │ │
│  │    │  ├── Dead peers:   MANAGER_DEAD_PEER_REAP_INTERVAL   (~24h)    │  │ │
│  │    │  └── Dead gates:   MANAGER_DEAD_GATE_REAP_INTERVAL   (~24h)    │  │ │
│  │    │                                                                 │  │ │
│  │    │  For each dead node past reap interval:                        │  │ │
│  │    │  ├── Remove from _dead_workers / _dead_peers / _dead_gates     │  │ │
│  │    │  ├── Remove from all tracking structures                       │  │ │
│  │    │  └── Free any resources/leases associated                      │  │ │
│  │    │                                                                 │  │ │
│  │    └────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                        │ │
│  │    Note: 24h is conservative for debugging. In production,            │ │
│  │    consider reducing to 1-2h via environment variables.               │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 3. WORKER WORKFLOW CLEANUP (finally block)                             │ │
│  │                                                                        │ │
│  │    Location: Worker._execute_workflow() finally block                  │ │
│  │                                                                        │ │
│  │    ┌────────────────────────────────────────────────────────────────┐  │ │
│  │    │                                                                 │  │ │
│  │    │  async def _execute_workflow(...):                             │  │ │
│  │    │      try:                                                       │  │ │
│  │    │          # Execute workflow                                    │  │ │
│  │    │          result = await remote_manager.execute(...)            │  │ │
│  │    │                                                                 │  │ │
│  │    │      except CancelledError:                                    │  │ │
│  │    │          # Handle cancellation                                 │  │ │
│  │    │                                                                 │  │ │
│  │    │      except Exception:                                         │  │ │
│  │    │          # Handle failure                                      │  │ │
│  │    │                                                                 │  │ │
│  │    │      finally:                                                   │  │ │
│  │    │          # ALWAYS cleanup - prevents resource leaks            │  │ │
│  │    │          await _core_allocator.free(workflow_id)  ◄── Free CPU │  │ │
│  │    │          _workflow_tokens.pop(workflow_id)        ◄── Remove   │  │ │
│  │    │          _workflow_cancel_events.pop(workflow_id) ◄── tracking │  │ │
│  │    │          _active_workflows.pop(workflow_id)       ◄── state    │  │ │
│  │    │          _workflow_fence_tokens.pop(workflow_id)  ◄── data     │  │ │
│  │    │          _remote_manger.start_server_cleanup()    ◄── Cleanup  │  │ │
│  │    │                                                                 │  │ │
│  │    └────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                        │ │
│  │    Guarantees: Workflow resources are ALWAYS freed, regardless of     │ │
│  │    success, failure, or cancellation.                                 │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ 4. GATE LEASE CLEANUP LOOP                                             │ │
│  │                                                                        │ │
│  │    Location: Gate._lease_cleanup_loop()                                │ │
│  │                                                                        │ │
│  │    ┌────────────────────────────────────────────────────────────────┐  │ │
│  │    │                                                                 │  │ │
│  │    │  while running:                                                 │  │ │
│  │    │      await sleep(cleanup_interval)                             │  │ │
│  │    │                                                                 │  │ │
│  │    │      for lease_key, lease in _leases.items():                  │  │ │
│  │    │          if time.monotonic() > lease.expires_at:               │  │ │
│  │    │              │                                                  │  │ │
│  │    │              ▼                                                  │  │ │
│  │    │          Mark job's DC as FAILED                               │  │ │
│  │    │          │                                                      │  │ │
│  │    │          ▼                                                      │  │ │
│  │    │          Remove expired lease                                  │  │ │
│  │    │          │                                                      │  │ │
│  │    │          ▼                                                      │  │ │
│  │    │          Notify client of partial failure                      │  │ │
│  │    │                                                                 │  │ │
│  │    └────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                        │ │
│  │    Ensures: Jobs with dead datacenters don't hang forever             │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Cancellation Flow (Killing Zombie Jobs)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CANCELLATION PROPAGATION FLOW                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  User Request: client.cancel_job(job_id)                                     │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                          │ │
│  │  CLIENT                                                                  │ │
│  │    │                                                                     │ │
│  │    │ JobCancelRequest(job_id, fence_token, reason)                      │ │
│  │    │                                                                     │ │
│  │    ▼                                                                     │ │
│  │  GATE                                                                    │ │
│  │    │                                                                     │ │
│  │    ├── Validate fence_token (reject stale)                              │ │
│  │    ├── Check lease ownership (am I responsible?)                        │ │
│  │    │                                                                     │ │
│  │    │ FOR EACH datacenter with active workflows:                         │ │
│  │    │    │                                                                │ │
│  │    │    │ WorkflowCancelRequest(job_id, workflow_ids)                   │ │
│  │    │    │                                                                │ │
│  │    │    ▼                                                                │ │
│  │  MANAGER                                                                 │ │
│  │    │                                                                     │ │
│  │    ├── Update job status to CANCELLING                                  │ │
│  │    ├── Update workflow status to CANCELLED                              │ │
│  │    │                                                                     │ │
│  │    │ FOR EACH worker with workflow:                                     │ │
│  │    │    │                                                                │ │
│  │    │    │ WorkflowCancelRequest(workflow_id, fence_token)               │ │
│  │    │    │                                                                │ │
│  │    │    ▼                                                                │ │
│  │  WORKER                                                                  │ │
│  │    │                                                                     │ │
│  │    ├── Set _workflow_cancel_events[workflow_id]                         │ │
│  │    ├── TaskRunner.cancel(workflow_token)                                │ │
│  │    ├── RemoteGraphManager.cancel_workflow(run_id)                       │ │
│  │    │                                                                     │ │
│  │    │ RESPONSE PROPAGATION (reverse):                                    │ │
│  │    │                                                                     │ │
│  │    ▼                                                                     │ │
│  │  WorkflowCancelResponse(success=True, cancelled_count=N)                │ │
│  │    │                                                                     │ │
│  │    ▼                                                                     │ │
│  │  JobCancelResponse(success=True, cancelled_workflow_count=M)            │ │
│  │    │                                                                     │ │
│  │    ▼                                                                     │ │
│  │  CLIENT receives confirmation                                            │ │
│  │                                                                          │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Fallback Mechanism (if push fails):                                         │ │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                                                                          │ │
│  │  Worker._cancellation_poll_loop():                                       │ │
│  │                                                                          │ │
│  │    Every 5-10 seconds:                                                   │ │
│  │    ├── For each active workflow                                          │ │
│  │    │    │                                                                │ │
│  │    │    │ WorkflowCancellationQuery(workflow_id)                        │ │
│  │    │    │                                                                │ │
│  │    │    ▼                                                                │ │
│  │    │    Manager checks if cancelled ──► Response                        │ │
│  │    │                                        │                            │ │
│  │    │    ┌───────────────────────────────────┘                            │ │
│  │    │    │                                                                │ │
│  │    │    ├── is_cancelled=True → _cancel_workflow()                      │ │
│  │    │    └── is_cancelled=False → continue execution                     │ │
│  │    │                                                                     │ │
│  │    Ensures: Even if manager→worker push is lost, worker will            │ │
│  │    discover cancellation within poll_interval seconds                   │ │
│  │                                                                          │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Complete Zombie Prevention State Machine

```
┌─────────────────────────────────────────────────────────────────────────────┐
│               ZOMBIE PREVENTION STATE MACHINE (per workflow)                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                                                                              │
│                           ┌──────────────┐                                  │
│                           │   PENDING    │                                  │
│                           │   (queued)   │                                  │
│                           └──────┬───────┘                                  │
│                                  │                                          │
│                    ┌─────────────┼─────────────┐                            │
│                    │             │             │                            │
│                    ▼             ▼             ▼                            │
│           ┌────────────┐ ┌────────────┐ ┌────────────┐                      │
│           │  TIMEOUT   │ │ DISPATCHED │ │ MAX_RETRY  │                      │
│           │ (evicted)  │ │            │ │ (evicted)  │                      │
│           └─────┬──────┘ └──────┬─────┘ └──────┬─────┘                      │
│                 │               │              │                            │
│                 │               ▼              │                            │
│                 │        ┌────────────┐        │                            │
│                 │        │  RUNNING   │        │                            │
│                 │        │ (on worker)│        │                            │
│                 │        └──────┬─────┘        │                            │
│                 │               │              │                            │
│        ┌────────┼───────────────┼──────────────┼────────┐                   │
│        │        │               │              │        │                   │
│        ▼        ▼               ▼              ▼        ▼                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐          │
│  │ COMPLETED│ │  FAILED  │ │CANCELLED │ │ TIMEOUT  │ │WORKER_DIE│          │
│  │          │ │(internal)│ │ (user)   │ │(runtime) │ │(detected)│          │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘          │
│       │            │            │            │            │                 │
│       │            │            │            │            │                 │
│       │            │            │            │      ┌─────┴─────┐           │
│       │            │            │            │      │           │           │
│       │            │            │            │      ▼           ▼           │
│       │            │            │            │  RETRY #N   MAX_RETRY        │
│       │            │            │            │  (redispatch) (failed)       │
│       │            │            │            │      │           │           │
│       │            │            │            │      │           │           │
│       ▼            ▼            ▼            ▼      ▼           ▼           │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        CLEANUP (always)                              │   │
│  │  • Free cores: _core_allocator.free(workflow_id)                    │   │
│  │  • Remove tracking: _workflow_tokens, _active_workflows, etc.       │   │
│  │  • Send result/status to manager                                    │   │
│  │  • RemoteGraphManager.start_server_cleanup()                        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Legend:                                                                     │
│  ───────                                                                     │
│  • Timeout paths prevent indefinite waiting                                 │
│  • Worker death triggers immediate retry or failure                         │
│  • All paths lead to CLEANUP (no resource leaks)                           │
│  • Fence tokens prevent duplicate execution on retry                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Mechanism Summary Table

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ZOMBIE PREVENTION MECHANISM SUMMARY                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────┬───────────────┬──────────────────────────────────┐│
│  │ Mechanism            │ Location      │ Protects Against                 ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ Workflow Timeout     │ Dispatcher    │ Hung pending workflows           ││
│  │ (check_timeouts)     │               │ (default: 300s)                  ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ SWIM Dead Detection  │ All nodes     │ Dead workers/managers/gates      ││
│  │ (_on_node_dead)      │               │ (suspicion: ~30s)                ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ Progress Health      │ Manager       │ Stuck workers without progress   ││
│  │ (AD-19)              │               │ (STUCK state detection)          ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ Lease Expiry         │ Gate          │ Jobs orphaned by gate failure    ││
│  │ (job_lease)          │               │ (default: 30s)                   ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ Fence Tokens         │ Worker        │ Duplicate/stale dispatches       ││
│  │                      │               │ (at-most-once semantics)         ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ Versioned Clock      │ Manager/Gate  │ Out-of-order state updates       ││
│  │                      │               │ (stale update rejection)         ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ Cancel Polling       │ Worker        │ Lost cancellation messages       ││
│  │                      │               │ (poll interval: 5-10s)           ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ Extension Tracking   │ Manager       │ Legitimate slow work killed      ││
│  │ (AD-26)              │               │ (max 5 extensions, decay)        ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ Job Cleanup Loop     │ Manager       │ Resource accumulation            ││
│  │                      │               │ (interval: 30s)                  ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ Dead Node Reaping    │ Manager       │ Stale dead node tracking         ││
│  │                      │               │ (interval: ~24h)                 ││
│  ├──────────────────────┼───────────────┼──────────────────────────────────┤│
│  │ finally Cleanup      │ Worker        │ Resource leaks on any exit       ││
│  │ (_execute_workflow)  │               │ (always runs)                    ││
│  └──────────────────────┴───────────────┴──────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Known Gaps and Future Improvements

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    KNOWN GAPS & FUTURE IMPROVEMENTS                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ GAP 1: NO RUNTIME EXECUTION TIMEOUT                                    │ │
│  │                                                                        │ │
│  │ Current: timeout_seconds only affects dispatch eligibility             │ │
│  │ Problem: Workflow can run indefinitely if execution hangs             │ │
│  │                                                                        │ │
│  │ Recommendation: Add execution_timeout at RemoteGraphManager level     │ │
│  │   • asyncio.wait_for() wrapper with hard timeout                      │ │
│  │   • Separate from dispatch timeout (dispatch_timeout vs exec_timeout) │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ GAP 2: LONG DEAD NODE REAP INTERVAL                                    │ │
│  │                                                                        │ │
│  │ Current: 24h default for dead node reaping                            │ │
│  │ Problem: Dead worker tracking accumulates memory                       │ │
│  │                                                                        │ │
│  │ Recommendation: Reduce to 1-2h in production                          │ │
│  │   • Configure via MANAGER_DEAD_WORKER_REAP_INTERVAL                   │ │
│  │   • Keep 24h for debugging/development only                           │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ GAP 3: NO HARD KILL SIGNAL                                             │ │
│  │                                                                        │ │
│  │ Current: Cancellation relies on workflow respecting cancel event       │ │
│  │ Problem: Misbehaving workflow can ignore cancellation                  │ │
│  │                                                                        │ │
│  │ Recommendation: Add process-level kill capability                      │ │
│  │   • Track workflow PID at execution start                             │ │
│  │   • SIGKILL after grace period if cancel not acknowledged             │ │
│  │   • May require process isolation (subprocess vs thread)              │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ GAP 4: NO ORPHAN JOB SCANNER                                           │ │
│  │                                                                        │ │
│  │ Current: Rely on timeout and heartbeat for detection                   │ │
│  │ Problem: Jobs can be orphaned if all tracking state lost              │ │
│  │                                                                        │ │
│  │ Recommendation: Add periodic reconciliation scan                       │ │
│  │   • Manager queries all workers for active workflow list              │ │
│  │   • Compare with manager's tracking → find orphans                    │ │
│  │   • Clean up or re-adopt orphaned workflows                           │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ GAP 5: EXTENSION EXHAUSTION HARD CUTOFF                                │ │
│  │                                                                        │ │
│  │ Current: After max extensions, no more time granted                    │ │
│  │ Problem: Legitimate slow work killed abruptly                          │ │
│  │                                                                        │ │
│  │ Recommendation: Graceful degradation                                   │ │
│  │   • Notify workflow of impending timeout                              │ │
│  │   • Allow checkpoint/save before kill                                 │ │
│  │   • Configurable behavior (kill vs pause vs notify)                   │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Configuration Reference

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ZOMBIE PREVENTION CONFIGURATION                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Environment Variables:                                                      │
│                                                                              │
│  ┌────────────────────────────────────┬──────────┬────────────────────────┐ │
│  │ Variable                           │ Default  │ Description            │ │
│  ├────────────────────────────────────┼──────────┼────────────────────────┤ │
│  │ MERCURY_SYNC_CLEANUP_INTERVAL      │ 30s      │ Job cleanup loop freq  │ │
│  │ MANAGER_DEAD_WORKER_REAP_INTERVAL  │ 86400s   │ Dead worker reap (24h) │ │
│  │ MANAGER_DEAD_PEER_REAP_INTERVAL    │ 86400s   │ Dead peer reap (24h)   │ │
│  │ MANAGER_DEAD_GATE_REAP_INTERVAL    │ 86400s   │ Dead gate reap (24h)   │ │
│  │ WORKER_CANCELLATION_POLL_INTERVAL  │ 5s       │ Cancel poll frequency  │ │
│  │ SWIM_SUSPICION_TIMEOUT             │ 30s      │ Time before DEAD       │ │
│  └────────────────────────────────────┴──────────┴────────────────────────┘ │
│                                                                              │
│  Per-Job Configuration:                                                      │
│                                                                              │
│  ┌────────────────────────────────────┬──────────┬────────────────────────┐ │
│  │ Parameter                          │ Default  │ Description            │ │
│  ├────────────────────────────────────┼──────────┼────────────────────────┤ │
│  │ timeout_seconds                    │ 300s     │ Workflow dispatch time │ │
│  │ max_dispatch_attempts              │ 5        │ Retries before fail    │ │
│  │ max_extensions                     │ 5        │ Deadline extensions    │ │
│  │ lease_timeout                      │ 30s      │ Gate job lease duration│ │
│  └────────────────────────────────────┴──────────┴────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Backpressure & Degradation

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BACKPRESSURE & GRACEFUL DEGRADATION                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  DEGRADATION LEVELS:                                                         │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                                                                          ││
│  │  Level      │ LHM   │ Event Loop │ Actions                              ││
│  │             │ Score │ Lag Ratio  │                                       ││
│  │  ───────────┼───────┼────────────┼────────────────────────────────────── ││
│  │  NORMAL     │ 0-2   │ < 0.5      │ Full operation                       ││
│  │             │       │            │                                       ││
│  │  ELEVATED   │ 2-4   │ 0.5-1.0    │ • Extend timeouts by 1.25x           ││
│  │             │       │            │ • Reduce gossip rate                 ││
│  │             │       │            │                                       ││
│  │  HIGH       │ 4-6   │ 1.0-2.0    │ • Extend timeouts by 1.5x            ││
│  │             │       │            │ • Skip 25% of probes                 ││
│  │             │       │            │ • Reduce piggyback size              ││
│  │             │       │            │                                       ││
│  │  SEVERE     │ 6-7   │ 2.0-4.0    │ • Extend timeouts by 2x              ││
│  │             │       │            │ • Skip 50% of probes                 ││
│  │             │       │            │ • Consider leadership stepdown       ││
│  │             │       │            │                                       ││
│  │  CRITICAL   │ 7-8   │ > 4.0      │ • Extend timeouts by 3x              ││
│  │             │       │            │ • Skip all non-essential probes      ││
│  │             │       │            │ • Force leadership stepdown          ││
│  │             │       │            │ • Reject new work                    ││
│  │                                                                          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  BACKPRESSURE FLOW:                                                          │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                                                                          ││
│  │   Worker              Manager              Gate                Client   ││
│  │     │                    │                   │                   │      ││
│  │     │ WorkerHeartbeat    │                   │                   │      ││
│  │     │ {queue_depth: 45}  │                   │                   │      ││
│  │     │───────────────────►│                   │                   │      ││
│  │     │                    │                   │                   │      ││
│  │     │                    │ Check soft_limit  │                   │      ││
│  │     │                    │ (e.g., 50)        │                   │      ││
│  │     │                    │                   │                   │      ││
│  │     │                    │ Worker approaching│                   │      ││
│  │     │                    │ limit - depriori- │                   │      ││
│  │     │                    │ tize in selection │                   │      ││
│  │     │                    │                   │                   │      ││
│  │     │                    │◄──────────────────│ New job           │      ││
│  │     │                    │                   │                   │      ││
│  │     │                    │ Select different  │                   │      ││
│  │     │                    │ worker with lower │                   │      ││
│  │     │                    │ queue_depth       │                   │      ││
│  │     │                    │                   │                   │      ││
│  │  ───┴────────────────────┴───────────────────┴───────────────────┴───── ││
│  │                                                                          ││
│  │   If ALL workers at capacity:                                            ││
│  │                                                                          ││
│  │   Worker 1            Worker 2            Worker 3                       ││
│  │   queue: 50           queue: 48           queue: 50                      ││
│  │   (at limit)          (near limit)        (at limit)                     ││
│  │       │                   │                   │                          ││
│  │       └───────────────────┼───────────────────┘                          ││
│  │                           │                                              ││
│  │                           ▼                                              ││
│  │                    Manager rejects                                        ││
│  │                    new workflow with                                      ││
│  │                    backpressure error                                     ││
│  │                           │                                              ││
│  │                           ▼                                              ││
│  │                    Gate/Client receives                                   ││
│  │                    "capacity exceeded"                                    ││
│  │                           │                                              ││
│  │                           ▼                                              ││
│  │                    Client implements                                      ││
│  │                    exponential backoff                                    ││
│  │                                                                          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  LHM ADJUSTMENT FLOW:                                                        │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                                                                          ││
│  │   Event                        │ LHM Change                             ││
│  │   ─────────────────────────────┼──────────────────────────────────────  ││
│  │   Probe success                │ Decrement by 1 (min 0)                 ││
│  │   Probe failure                │ Increment by 1                         ││
│  │   Indirect probe required      │ Increment by 1                         ││
│  │   Event loop lag detected      │ Increment by 1-2                       ││
│  │   Event loop recovered         │ Decrement by 1                         ││
│  │   Suspicion started            │ Increment by 1                         ││
│  │   Refutation successful        │ Decrement by 1                         ││
│  │                                                                          ││
│  │   Timeout Calculation:                                                   ││
│  │   effective_timeout = base_timeout × (1 + LHM_score × 0.25)             ││
│  │                                                                          ││
│  │   Example (base_timeout = 500ms):                                        ││
│  │   • LHM 0 → 500ms                                                        ││
│  │   • LHM 2 → 750ms                                                        ││
│  │   • LHM 4 → 1000ms                                                       ││
│  │   • LHM 8 → 1500ms                                                       ││
│  │                                                                          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Scaling Operations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SCALING OPERATIONS                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ADDING A WORKER:                                                            │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                                                                          ││
│  │   New Worker              Manager (Leader)                               ││
│  │      │                          │                                        ││
│  │      │ ① TCP: WorkerRegistration│                                        ││
│  │      │ {node, total_cores, ...} │                                        ││
│  │      │─────────────────────────►│                                        ││
│  │      │                          │                                        ││
│  │      │                          │ Add to _workers                        ││
│  │      │                          │ Add to probe_scheduler                 ││
│  │      │                          │                                        ││
│  │      │ ② TCP: RegistrationAck   │                                        ││
│  │      │◄─────────────────────────│                                        ││
│  │      │                          │                                        ││
│  │      │ ③ UDP: Join SWIM cluster │                                        ││
│  │      │─────────────────────────►│                                        ││
│  │      │                          │                                        ││
│  │      │ ④ UDP: Ack + member list │                                        ││
│  │      │◄─────────────────────────│                                        ││
│  │      │                          │                                        ││
│  │      │      ════════════════════│═══════════════════                     ││
│  │      │         Worker now ACTIVE and receiving work                      ││
│  │      │                          │                                        ││
│  │                                                                          ││
│  │   Time: ~1-2 seconds from registration to first workflow                ││
│  │                                                                          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  REMOVING A WORKER (GRACEFUL):                                               │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                                                                          ││
│  │   Worker                    Manager (Leader)                             ││
│  │      │                          │                                        ││
│  │      │ ① Set state = DRAINING   │                                        ││
│  │      │                          │                                        ││
│  │      │ ② UDP: WorkerHeartbeat   │                                        ││
│  │      │ {state: DRAINING}        │                                        ││
│  │      │─────────────────────────►│                                        ││
│  │      │                          │                                        ││
│  │      │                          │ Stop sending new work                  ││
│  │      │                          │                                        ││
│  │      │ ③ Complete existing workflows                                    ││
│  │      │                          │                                        ││
│  │      │ ④ TCP: All workflows done│                                        ││
│  │      │─────────────────────────►│                                        ││
│  │      │                          │                                        ││
│  │      │ ⑤ UDP: Leave message     │                                        ││
│  │      │─────────────────────────►│                                        ││
│  │      │                          │                                        ││
│  │      │                          │ Remove from _workers                   ││
│  │      │                          │ Gossip leave to cluster                ││
│  │      │                          │                                        ││
│  │      ╳ Shutdown                 │                                        ││
│  │                                                                          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  ADDING A MANAGER:                                                           │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                                                                          ││
│  │   New Manager           Existing Managers                                ││
│  │      │                       │                                           ││
│  │      │ ① UDP: Join SWIM cluster                                         ││
│  │      │──────────────────────►│                                           ││
│  │      │                       │                                           ││
│  │      │ ② UDP: Ack + members  │                                           ││
│  │      │◄──────────────────────│                                           ││
│  │      │                       │                                           ││
│  │      │ ★ CURRENT: Immediately joins quorum                              ││
│  │      │ ★ FUTURE: STATE: SYNCING (not in quorum until sync done)        ││
│  │      │                       │                                           ││
│  │      │ ③ TCP: StateSyncRequest (NOT YET IMPLEMENTED)                    ││
│  │      │──────────────────────►│ (to leader, should get manager state)    ││
│  │      │                       │                                           ││
│  │      │ ④ TCP: ManagerStateSnapshot (NOT YET IMPLEMENTED)                ││
│  │      │◄──────────────────────│                                           ││
│  │      │                       │                                           ││
│  │      │ Apply state snapshot  │                                           ││
│  │      │ Verify consistency    │                                           ││
│  │      │                       │                                           ││
│  │      │ STATE: ACTIVE         │                                           ││
│  │      │ (counted in quorum)   │                                           ││
│  │      │                       │                                           ││
│  │      │      ════════════════════════════════════                         ││
│  │      │         New manager now participates in quorum                    ││
│  │      │         (n/2 + 1 threshold recalculated)                         ││
│  │      │                       │                                           ││
│  │                                                                          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  REMOVING A MANAGER (GRACEFUL):                                              │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                                                                          ││
│  │   Leaving Manager           Other Managers                               ││
│  │      │                          │                                        ││
│  │      │ ① STATE: LEAVING         │                                        ││
│  │      │                          │                                        ││
│  │      │ If leader:               │                                        ││
│  │      │ ② Trigger pre-vote for   │                                        ││
│  │      │    new leader            │                                        ││
│  │      │──────────────────────────►│                                        ││
│  │      │                          │                                        ││
│  │      │ ③ Wait for new leader    │                                        ││
│  │      │◄──────────────────────────│                                        ││
│  │      │                          │                                        ││
│  │      │ ④ Confirm pending work   │                                        ││
│  │      │    completes or transfers│                                        ││
│  │      │                          │                                        ││
│  │      │ ⑤ UDP: Leave message     │                                        ││
│  │      │──────────────────────────►│                                        ││
│  │      │                          │                                        ││
│  │      │                          │ Recalculate quorum                     ││
│  │      │                          │ (new work uses new quorum)             ││
│  │      │                          │                                        ││
│  │      ╳ Shutdown                 │                                        ││
│  │                                                                          ││
│  │   Note: In-flight work uses original quorum until completion            ││
│  │                                                                          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  ADDING A GATE:                                                              │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                                                                          ││
│  │   New Gate              Existing Gates                                   ││
│  │      │                       │                                           ││
│  │      │ ① UDP: Join SWIM cluster                                         ││
│  │      │──────────────────────►│                                           ││
│  │      │                       │                                           ││
│  │      │ ② TCP: StateSyncRequest                                          ││
│  │      │──────────────────────►│ (to leader)                               ││
│  │      │                       │                                           ││
│  │      │ ③ TCP: GlobalJobStatus[]│                                         ││
│  │      │◄──────────────────────│ + DatacenterLease[]                       ││
│  │      │                       │                                           ││
│  │      │ Apply state           │                                           ││
│  │      │                       │                                           ││
│  │      │ STATE: ACTIVE         │                                           ││
│  │      │ (can become leader)   │                                           ││
│  │      │                       │                                           ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  REMOVING A GATE (GRACEFUL):                                                 │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                                                                          ││
│  │   Leaving Gate (★)          Other Gates                                  ││
│  │      │                          │                                        ││
│  │      │ ① Transfer leases        │                                        ││
│  │      │    to new leader         │                                        ││
│  │      │──────────────────────────►│                                        ││
│  │      │                          │                                        ││
│  │      │ ② LeaseTransfer ack      │                                        ││
│  │      │◄──────────────────────────│                                        ││
│  │      │                          │                                        ││
│  │      │ ③ Update registry        │                                        ││
│  │      │    (clients should       │                                        ││
│  │      │    reconnect to new gate)│                                        ││
│  │      │                          │                                        ││
│  │      │ ④ UDP: Leave message     │                                        ││
│  │      │──────────────────────────►│                                        ││
│  │      │                          │                                        ││
│  │      ╳ Shutdown                 │                                        ││
│  │                                                                          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## State Management

### Versioned Lamport Clock

```
┌─────────────────────────────────────────────────────────────────┐
│                  VERSIONED STATE CLOCK                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Purpose: Reject stale updates from workers/managers             │
│                                                                  │
│  VersionedStateClock {                                           │
│    _entity_versions: dict[str, tuple[int, float]]               │
│    # entity_id → (last_version, last_update_time)               │
│  }                                                               │
│                                                                  │
│  Operations:                                                     │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ is_entity_stale(entity_id, incoming_version) -> bool      │  │
│  │   • True if incoming_version <= tracked version           │  │
│  │   • False if incoming_version > tracked version           │  │
│  │                                                            │  │
│  │ update_entity(entity_id, new_version) -> None             │  │
│  │   • Updates tracked version if new > current              │  │
│  │   • Records update timestamp                              │  │
│  │                                                            │  │
│  │ should_accept_update(entity_id, version) -> bool          │  │
│  │   • Combined check + update in one atomic operation       │  │
│  │                                                            │  │
│  │ cleanup_old_entities(max_age: float) -> None              │  │
│  │   • Remove entities not updated for > max_age seconds     │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Usage:                                                          │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ # In Manager, receiving WorkerHeartbeat:                  │  │
│  │ if self._versioned_clock.is_entity_stale(                 │  │
│  │     heartbeat.node_id, heartbeat.version                  │  │
│  │ ):                                                        │  │
│  │     return  # Discard stale update                        │  │
│  │                                                            │  │
│  │ # Accept update                                           │  │
│  │ self._worker_status[heartbeat.node_id] = heartbeat        │  │
│  │ self._versioned_clock.update_entity(                      │  │
│  │     heartbeat.node_id, heartbeat.version                  │  │
│  │ )                                                         │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Per-Core Workflow Assignment

```
┌─────────────────────────────────────────────────────────────────┐
│                  PER-CORE WORKFLOW TRACKING                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Worker State:                                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ _total_cores: int = os.cpu_count()                        │  │
│  │ _available_cores: int (computed)                          │  │
│  │                                                            │  │
│  │ _core_assignments: dict[int, str | None]                  │  │
│  │   # core_index → workflow_id (or None if free)            │  │
│  │   {0: None, 1: "wf-123", 2: "wf-123", 3: None, ...}       │  │
│  │                                                            │  │
│  │ _workflow_cores: dict[str, list[int]]                     │  │
│  │   # workflow_id → [core_indices]                          │  │
│  │   {"wf-123": [1, 2], "wf-456": [5, 6, 7]}                │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Operations:                                                     │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ _allocate_cores(workflow_id, num_cores) -> list[int]      │  │
│  │   • Find num_cores free cores                             │  │
│  │   • Update _core_assignments                              │  │
│  │   • Update _workflow_cores                                │  │
│  │   • Return allocated core indices                         │  │
│  │                                                            │  │
│  │ _free_cores(workflow_id) -> None                          │  │
│  │   • Look up cores in _workflow_cores                      │  │
│  │   • Mark all as None in _core_assignments                 │  │
│  │   • Remove from _workflow_cores                           │  │
│  │                                                            │  │
│  │ stop_workflows_on_cores(core_indices) -> list[str]        │  │
│  │   • Hierarchical stop for specific cores                  │  │
│  │   • Returns workflow_ids that were cancelled              │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Reported in WorkflowProgress.assigned_cores for visibility     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Security

### Encryption & Authentication

```
┌─────────────────────────────────────────────────────────────────┐
│                   SECURITY ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  AES-256-GCM Encryption:                                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ • HKDF key derivation from shared secret                  │  │
│  │ • Per-message salt (never reuse nonces)                   │  │
│  │ • Key rotation via MERCURY_SYNC_AUTH_SECRET_PREVIOUS      │  │
│  │ • Weak secret detection and rejection                     │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Replay Protection:                                              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ • Snowflake IDs with embedded timestamps                  │  │
│  │ • Sliding window detection (configurable)                 │  │
│  │ • Rejects duplicate and stale messages                    │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Rate Limiting:                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ • Token bucket per source address                         │  │
│  │ • Configurable tokens and refill rate                     │  │
│  │ • Prevents DoS from flooding                              │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Message Size Limits:                                            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ • MAX_MESSAGE_SIZE: 1MB (compressed)                      │  │
│  │ • MAX_DECOMPRESSED_SIZE: 50MB                             │  │
│  │ • Compression bomb detection (max ratio: 100x)            │  │
│  │ • Large enough for cloudpickled workflow classes          │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Serialization Security:                                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ • RestrictedUnpickler with explicit allowlist             │  │
│  │ • Blocks dangerous modules (os, subprocess, sys)          │  │
│  │ • Allows hyperscale.*, cloudpickle, and dependencies      │  │
│  │ • Sanitized error responses (no stack traces)             │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│  TLS Configuration:                                              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ • MERCURY_SYNC_TLS_VERIFY_HOSTNAME: true/false            │  │
│  │ • Certificate-based authentication available              │  │
│  │ • Configurable for local vs production environments       │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Module Structure

```
hyperscale/distributed_rewrite/
├── README.md                 # This documentation
│
├── nodes/                    # Node implementations
│   ├── worker.py            # WorkerServer
│   ├── manager.py           # ManagerServer
│   └── gate.py              # GateServer
│
├── models/                   # Data models
│   ├── distributed.py       # Distributed message types
│   ├── message.py           # Base Message class
│   ├── restricted_unpickler.py  # Security: allowlist unpickler
│   └── ...
│
├── swim/                     # SWIM + Lifeguard protocol
│   ├── udp_server.py        # Base SWIM server
│   ├── core/                # Core types and utilities
│   │   ├── state_embedder.py   # Serf-style heartbeat embedding
│   │   ├── node_id.py          # Node identification
│   │   ├── errors.py           # Error hierarchy
│   │   ├── error_handler.py    # Circuit breakers, recovery
│   │   ├── metrics.py          # Protocol metrics
│   │   ├── audit.py            # Membership audit log
│   │   └── ...
│   ├── detection/           # Failure detection
│   │   ├── incarnation_tracker.py
│   │   ├── suspicion_manager.py
│   │   ├── indirect_probe_manager.py
│   │   └── probe_scheduler.py
│   ├── gossip/              # Gossip protocol
│   │   ├── gossip_buffer.py
│   │   └── piggyback_update.py
│   ├── health/              # Health monitoring
│   │   ├── local_health_multiplier.py
│   │   ├── health_monitor.py
│   │   └── graceful_degradation.py
│   └── leadership/          # Leader election
│       ├── local_leader_election.py
│       ├── leader_eligibility.py
│       ├── leader_state.py
│       └── flapping_detector.py
│
├── server/                   # Base server infrastructure
│   ├── server/
│   │   ├── mercury_sync_base_server.py
│   │   └── mercury_sync_server.py
│   ├── protocol/            # Network protocols
│   │   ├── mercury_sync_tcp_protocol.py
│   │   ├── mercury_sync_udp_protocol.py
│   │   └── security.py      # ReplayGuard, RateLimiter
│   ├── hooks/               # Decorators for TCP/UDP
│   │   ├── tcp/
│   │   └── udp/
│   ├── events/              # Logical clocks
│   │   ├── lamport_clock.py
│   │   └── versioned_state_clock.py
│   └── context/
│
├── taskex/                   # Task execution
│   ├── task_runner.py       # Async task management
│   ├── task.py
│   └── snowflake/           # ID generation
│
├── encryption/               # Cryptography
│   └── aes_gcm.py           # AESGCMFernet with key rotation
│
└── env/                      # Configuration
    └── env.py               # Environment variables
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MERCURY_SYNC_AUTH_SECRET` | (required) | Shared secret for encryption (min 16 chars) |
| `MERCURY_SYNC_AUTH_SECRET_PREVIOUS` | None | Previous secret for key rotation |
| `MERCURY_SYNC_TLS_VERIFY_HOSTNAME` | `true` | TLS hostname verification |
| `MERCURY_SYNC_CLEANUP_INTERVAL` | `30s` | Background cleanup interval |
| `MERCURY_SYNC_TASK_RUNNER_MAX_THREADS` | 4 | TaskRunner thread pool size |

### Node Configuration

```python
# Worker example
worker = WorkerServer(
    host="0.0.0.0",
    tcp_port=8001,
    udp_port=8002,
    env=Env(),
    dc_id="us-east-1",
    manager_addrs=[("manager1.local", 9001)],
)

# Manager example
manager = ManagerServer(
    host="0.0.0.0",
    tcp_port=9001,
    udp_port=9002,
    env=Env(),
    dc_id="us-east-1",
    gate_addrs=[("gate1.local", 10001)],
    manager_peers=[("manager2.local", 9001)],
    quorum_timeout=5.0,
    max_workflow_retries=3,
)

# Gate example
gate = GateServer(
    host="0.0.0.0",
    tcp_port=10001,
    udp_port=10002,
    env=Env(),
    dc_id="global",
    datacenter_managers={
        "us-east-1": [("manager1.us-east.local", 9001)],
        "eu-west-1": [("manager1.eu-west.local", 9001)],
    },
)
```

---

## Message Protocol Reference

### TCP Messages (Data Transfer)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TCP MESSAGE TYPES                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ JOB LIFECYCLE MESSAGES                                                 │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  JobSubmission                                                          │ │
│  │  ├─ job_id: str                    # Unique job identifier              │ │
│  │  ├─ workflows: bytes               # Cloudpickled Workflow classes      │ │
│  │  ├─ vus: int                       # Cores per workflow                 │ │
│  │  ├─ timeout_seconds: float         # Max execution time                 │ │
│  │  ├─ datacenter_count: int = 1      # Target DC count (gates only)       │ │
│  │  └─ datacenters: list[str] = []    # Specific DCs (empty = auto)        │ │
│  │                                                                         │ │
│  │  JobAck                                                                  │ │
│  │  ├─ job_id: str                    # Job identifier                     │ │
│  │  ├─ accepted: bool                 # Whether accepted                   │ │
│  │  ├─ error: str | None = None       # Error if rejected                  │ │
│  │  └─ queued_position: int = 0       # Queue position                     │ │
│  │                                                                         │ │
│  │  CancelJob                                                               │ │
│  │  ├─ job_id: str                    # Job to cancel                      │ │
│  │  ├─ reason: str = ""               # Cancellation reason                │ │
│  │  └─ fence_token: int = 0           # Fencing token                      │ │
│  │                                                                         │ │
│  │  CancelAck                                                               │ │
│  │  ├─ job_id: str                    # Job identifier                     │ │
│  │  ├─ cancelled: bool                # Success                            │ │
│  │  ├─ workflows_cancelled: int = 0   # Count stopped                      │ │
│  │  └─ error: str | None = None       # Error if failed                    │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ WORKFLOW DISPATCH MESSAGES                                             │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  WorkflowDispatch                                                        │ │
│  │  ├─ job_id: str                    # Parent job                         │ │
│  │  ├─ workflow_id: str               # Unique workflow instance           │ │
│  │  ├─ workflow: bytes                # Cloudpickled Workflow class        │ │
│  │  ├─ context: bytes                 # Cloudpickled context dict          │ │
│  │  ├─ vus: int                       # Cores to use                       │ │
│  │  ├─ timeout_seconds: float         # Execution timeout                  │ │
│  │  └─ fence_token: int               # At-most-once fencing               │ │
│  │                                                                         │ │
│  │  WorkflowDispatchAck                                                     │ │
│  │  ├─ workflow_id: str               # Workflow identifier                │ │
│  │  ├─ accepted: bool                 # Whether accepted                   │ │
│  │  ├─ error: str | None = None       # Error if rejected                  │ │
│  │  └─ cores_assigned: int = 0        # Actual cores                       │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ PROGRESS & STATUS MESSAGES                                             │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  StepStats                                                               │ │
│  │  ├─ step_name: str                 # Step method name                   │ │
│  │  ├─ completed_count: int = 0       # Successful executions              │ │
│  │  ├─ failed_count: int = 0          # Failed executions                  │ │
│  │  └─ total_count: int = 0           # Total attempts                     │ │
│  │                                                                         │ │
│  │  WorkflowProgress                                                        │ │
│  │  ├─ job_id: str                    # Parent job                         │ │
│  │  ├─ workflow_id: str               # Workflow instance                  │ │
│  │  ├─ workflow_name: str             # Workflow class name                │ │
│  │  ├─ status: str                    # WorkflowStatus value               │ │
│  │  ├─ completed_count: int           # Actions completed                  │ │
│  │  ├─ failed_count: int              # Actions failed                     │ │
│  │  ├─ rate_per_second: float         # Current rate                       │ │
│  │  ├─ elapsed_seconds: float         # Time since start                   │ │
│  │  ├─ step_stats: list[StepStats]    # Per-step breakdown                 │ │
│  │  ├─ timestamp: float = 0.0         # Monotonic timestamp                │ │
│  │  └─ assigned_cores: list[int] = [] # Core indices                       │ │
│  │                                                                         │ │
│  │  JobProgress                                                             │ │
│  │  ├─ job_id: str                    # Job identifier                     │ │
│  │  ├─ datacenter: str                # Reporting DC                       │ │
│  │  ├─ status: str                    # JobStatus value                    │ │
│  │  ├─ workflows: list[WorkflowProgress]  # Per-workflow                   │ │
│  │  ├─ total_completed: int = 0       # Total actions                      │ │
│  │  ├─ total_failed: int = 0          # Total failed                       │ │
│  │  ├─ overall_rate: float = 0.0      # Aggregate rate                     │ │
│  │  ├─ elapsed_seconds: float = 0.0   # Job runtime                        │ │
│  │  └─ timestamp: float = 0.0         # Monotonic timestamp                │ │
│  │                                                                         │ │
│  │  GlobalJobStatus                                                         │ │
│  │  ├─ job_id: str                    # Job identifier                     │ │
│  │  ├─ status: str                    # JobStatus value                    │ │
│  │  ├─ datacenters: list[JobProgress] # Per-DC progress                    │ │
│  │  ├─ total_completed: int = 0       # Global total                       │ │
│  │  ├─ total_failed: int = 0          # Global failed                      │ │
│  │  ├─ overall_rate: float = 0.0      # Global rate                        │ │
│  │  ├─ elapsed_seconds: float = 0.0   # Since submission                   │ │
│  │  ├─ completed_datacenters: int = 0 # DCs finished                       │ │
│  │  └─ failed_datacenters: int = 0    # DCs failed                         │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ QUORUM & PROVISIONING MESSAGES                                         │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  ProvisionRequest                                                        │ │
│  │  ├─ job_id: str                    # Job identifier                     │ │
│  │  ├─ workflow_id: str               # Workflow to provision              │ │
│  │  ├─ target_worker: str             # Selected worker node_id            │ │
│  │  ├─ cores_required: int            # Cores needed                       │ │
│  │  ├─ fence_token: int               # Fencing token                      │ │
│  │  └─ version: int                   # State version                      │ │
│  │                                                                         │ │
│  │  ProvisionConfirm                                                        │ │
│  │  ├─ job_id: str                    # Job identifier                     │ │
│  │  ├─ workflow_id: str               # Workflow                           │ │
│  │  ├─ confirming_node: str           # Confirming manager                 │ │
│  │  ├─ confirmed: bool                # Whether confirmed                  │ │
│  │  ├─ version: int                   # Node's version                     │ │
│  │  └─ error: str | None = None       # Error if not confirmed             │ │
│  │                                                                         │ │
│  │  ProvisionCommit                                                         │ │
│  │  ├─ job_id: str                    # Job identifier                     │ │
│  │  ├─ workflow_id: str               # Workflow                           │ │
│  │  ├─ target_worker: str             # Final worker                       │ │
│  │  ├─ cores_assigned: int            # Cores allocated                    │ │
│  │  ├─ fence_token: int               # Fencing token                      │ │
│  │  └─ committed_version: int         # Version at commit                  │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ STATE SYNC MESSAGES                                                    │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  StateSyncRequest                                                        │ │
│  │  ├─ requester_id: str              # Requesting node                    │ │
│  │  ├─ requester_role: str            # NodeRole value                     │ │
│  │  └─ since_version: int = 0         # Only updates after this            │ │
│  │                                                                         │ │
│  │  StateSyncResponse                                                       │ │
│  │  ├─ responder_id: str              # Responding node                    │ │
│  │  ├─ current_version: int           # Current state version              │ │
│  │  ├─ worker_state: WorkerStateSnapshot | None  # If worker               │ │
│  │  └─ manager_state: ManagerStateSnapshot | None # If manager             │ │
│  │                                                                         │ │
│  │  WorkerStateSnapshot                                                     │ │
│  │  ├─ node_id: str                   # Worker identifier                  │ │
│  │  ├─ state: str                     # WorkerState value                  │ │
│  │  ├─ total_cores: int               # Total cores                        │ │
│  │  ├─ available_cores: int           # Free cores                         │ │
│  │  ├─ version: int                   # State version                      │ │
│  │  └─ active_workflows: dict[str, WorkflowProgress]                       │ │
│  │                                                                         │ │
│  │  ManagerStateSnapshot                                                    │ │
│  │  ├─ node_id: str                   # Manager identifier                 │ │
│  │  ├─ datacenter: str                # Datacenter                         │ │
│  │  ├─ is_leader: bool                # Leadership status                  │ │
│  │  ├─ term: int                      # Current term                       │ │
│  │  ├─ version: int                   # State version                      │ │
│  │  ├─ workers: list[WorkerStateSnapshot]  # Registered workers            │ │
│  │  └─ jobs: dict[str, JobProgress]   # Active jobs                        │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ LEASE MESSAGES (Gates only)                                            │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  DatacenterLease                                                         │ │
│  │  ├─ job_id: str                    # Job identifier                     │ │
│  │  ├─ datacenter: str                # Datacenter holding lease           │ │
│  │  ├─ lease_holder: str              # Gate node_id                       │ │
│  │  ├─ fence_token: int               # Fencing token                      │ │
│  │  ├─ expires_at: float              # Monotonic expiration               │ │
│  │  └─ version: int                   # Lease version                      │ │
│  │                                                                         │ │
│  │  LeaseTransfer                                                           │ │
│  │  ├─ job_id: str                    # Job identifier                     │ │
│  │  ├─ datacenter: str                # Datacenter                         │ │
│  │  ├─ from_gate: str                 # Current holder                     │ │
│  │  ├─ to_gate: str                   # New holder                         │ │
│  │  ├─ new_fence_token: int           # New fencing token                  │ │
│  │  └─ version: int                   # Transfer version                   │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### UDP Messages (SWIM Protocol)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         UDP MESSAGE TYPES                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ PROBE MESSAGES                                                         │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  Format: message_type>target_host:target_port[#base64_state]            │ │
│  │                                                                         │ │
│  │  probe>192.168.1.10:8001                                                │ │
│  │  └───┬─┘└────────────────┘                                              │ │
│  │      │        │                                                         │ │
│  │      │        └─ Target address                                         │ │
│  │      └─ Message type                                                    │ │
│  │                                                                         │ │
│  │  ack>192.168.1.5:8000#eyJub2RlX2lkIjoiLi4uIn0=                          │ │
│  │  └─┬┘└──────────────┘ └────────────────────────┘                        │ │
│  │    │        │                    │                                      │ │
│  │    │        │                    └─ Base64-encoded embedded state       │ │
│  │    │        └─ Sender address                                           │ │
│  │    └─ Message type                                                      │ │
│  │                                                                         │ │
│  │  ping-req>192.168.1.15:8002                                             │ │
│  │  └──────┘ (indirect probe via proxy node)                               │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ MEMBERSHIP MESSAGES                                                    │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  join>192.168.1.5:8000                                                  │ │
│  │  └──┘ (request to join cluster)                                         │ │
│  │                                                                         │ │
│  │  leave>192.168.1.5:8000                                                 │ │
│  │  └───┘ (graceful departure)                                             │ │
│  │                                                                         │ │
│  │  alive:5>192.168.1.5:8000                                               │ │
│  │  └───┘ │ (refutation with incarnation 5)                                │ │
│  │        │                                                                │ │
│  │        └─ Incarnation number                                            │ │
│  │                                                                         │ │
│  │  suspect:3>192.168.1.10:8001                                            │ │
│  │  └─────┘ │ (suspicion with incarnation 3)                               │ │
│  │          │                                                              │ │
│  │          └─ Target node's last known incarnation                        │ │
│  │                                                                         │ │
│  │  dead:3>192.168.1.10:8001                                               │ │
│  │  └──┘ (node marked dead after suspicion expired)                        │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ LEADERSHIP MESSAGES                                                    │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  pre-vote:5>192.168.1.5:8000                                            │ │
│  │  └──────┘ │ (pre-vote request for term 5)                               │ │
│  │           │                                                             │ │
│  │           └─ Proposed term                                              │ │
│  │                                                                         │ │
│  │  pre-vote-response:5:true>192.168.1.10:8001                             │ │
│  │                    │  │                                                 │ │
│  │                    │  └─ Granted (true/false)                           │ │
│  │                    └─ Term                                              │ │
│  │                                                                         │ │
│  │  vote-req:6>192.168.1.5:8000                                            │ │
│  │  └──────┘ (vote request for term 6)                                     │ │
│  │                                                                         │ │
│  │  vote-response:6:true>192.168.1.10:8001                                 │ │
│  │  (vote granted for term 6)                                              │ │
│  │                                                                         │ │
│  │  leader:6>192.168.1.5:8000                                              │ │
│  │  └────┘ (leader announcement for term 6)                                │ │
│  │                                                                         │ │
│  │  heartbeat:6>192.168.1.5:8000                                           │ │
│  │  └───────┘ (leader heartbeat for term 6)                                │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ GOSSIP PIGGYBACK FORMAT                                                │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  Piggybacked updates are appended to messages:                          │ │
│  │                                                                         │ │
│  │  ack>192.168.1.5:8000|J:192.168.1.20:8003:0|A:192.168.1.10:8001:5       │ │
│  │                     └─────────────────────────────────────────────┘     │ │
│  │                                         │                               │ │
│  │                           Piggybacked gossip updates                    │ │
│  │                                                                         │ │
│  │  Update format: TYPE:HOST:PORT:INCARNATION                              │ │
│  │                                                                         │ │
│  │  Types:                                                                  │ │
│  │  • J = JOIN (highest priority)                                          │ │
│  │  • L = LEAVE                                                            │ │
│  │  • A = ALIVE                                                            │ │
│  │  • S = SUSPECT                                                          │ │
│  │  • D = DEAD (lowest priority)                                           │ │
│  │                                                                         │ │
│  │  Priority ensures important updates propagate first when space limited  │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ EMBEDDED STATE (Serf-style Heartbeats)                                 │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  State embedded in ack responses after '#' separator:                   │ │
│  │                                                                         │ │
│  │  ack>192.168.1.5:8000#eyJub2RlX2lkIjogIndvcmtlci0xIiwgLi4ufQ==          │ │
│  │                      └────────────────────────────────────────┘         │ │
│  │                                   Base64(cloudpickle(Heartbeat))        │ │
│  │                                                                         │ │
│  │  WorkerHeartbeat (embedded by workers):                                 │ │
│  │  ├─ node_id: str                                                        │ │
│  │  ├─ state: str                   # HEALTHY|DEGRADED|DRAINING|OFFLINE    │ │
│  │  ├─ available_cores: int                                                │ │
│  │  ├─ queue_depth: int                                                    │ │
│  │  ├─ cpu_percent: float                                                  │ │
│  │  ├─ memory_percent: float                                               │ │
│  │  ├─ version: int                                                        │ │
│  │  └─ active_workflows: dict[str, str]  # workflow_id → status            │ │
│  │                                                                         │ │
│  │  ManagerHeartbeat (embedded by managers):                               │ │
│  │  ├─ node_id: str                                                        │ │
│  │  ├─ datacenter: str                                                     │ │
│  │  ├─ is_leader: bool                                                     │ │
│  │  ├─ term: int                                                           │ │
│  │  ├─ version: int                                                        │ │
│  │  ├─ active_jobs: int                                                    │ │
│  │  ├─ active_workflows: int                                               │ │
│  │  ├─ worker_count: int                                                   │ │
│  │  └─ available_cores: int                                                │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Enums Reference

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ENUM VALUES                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  NodeRole           │ Description                                           │
│  ───────────────────┼────────────────────────────────────────────────────── │
│  GATE               │ Cross-DC coordination node                            │
│  MANAGER            │ Datacenter workflow orchestrator                      │
│  WORKER             │ Workflow execution node                               │
│                                                                              │
│  ───────────────────────────────────────────────────────────────────────────│
│                                                                              │
│  JobStatus          │ Description                                           │
│  ───────────────────┼────────────────────────────────────────────────────── │
│  SUBMITTED          │ Job received, not yet dispatched                      │
│  QUEUED             │ Waiting for resources                                 │
│  DISPATCHING        │ Workflows being sent to workers                       │
│  RUNNING            │ Active execution                                      │
│  COMPLETING         │ Gathering final results                               │
│  COMPLETED          │ Successfully finished                                 │
│  FAILED             │ Failed (max retries exhausted)                        │
│  CANCELLED          │ User cancelled                                        │
│  TIMEOUT            │ Exceeded timeout_seconds                              │
│                                                                              │
│  ───────────────────────────────────────────────────────────────────────────│
│                                                                              │
│  WorkflowStatus     │ Description                                           │
│  ───────────────────┼────────────────────────────────────────────────────── │
│  PENDING            │ Not yet started                                       │
│  ASSIGNED           │ Sent to worker, awaiting ack                          │
│  RUNNING            │ Executing on worker                                   │
│  COMPLETED          │ Finished successfully                                 │
│  FAILED             │ Failed                                                │
│  CANCELLED          │ Cancelled                                             │
│                                                                              │
│  ───────────────────────────────────────────────────────────────────────────│
│                                                                              │
│  WorkerState        │ Description                                           │
│  ───────────────────┼────────────────────────────────────────────────────── │
│  HEALTHY            │ Normal operation, accepts work                        │
│  DEGRADED           │ High load, accepts with backpressure                  │
│  DRAINING           │ Not accepting new work                                │
│  OFFLINE            │ Not responding / shutdown                             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Known Limitations & Future Work

### New Manager Join Process (✅ Implemented)

New managers join the cluster in a SYNCING state before becoming ACTIVE:

**Implementation**:
1. New manager joins SWIM cluster → State = SYNCING
2. SYNCING managers are NOT counted in quorum (`_has_quorum_available()` returns false)
3. Manager starts leader election
4. If leader: immediately transitions to ACTIVE (syncs state via `_on_manager_become_leader`)
5. If not leader: requests state sync from current leader via `_complete_startup_sync()`
6. After sync completes (or times out): State = ACTIVE → now counted in quorum

**Key Components**:
- `ManagerState` enum: SYNCING, ACTIVE, DRAINING
- `_manager_state` field tracks current state
- `ManagerHeartbeat.state` field broadcasts state to peers
- `_complete_startup_sync()` handles non-leader state sync on startup
- `_has_quorum_available()` excludes SYNCING managers from quorum count

### Quorum Timeout Handling (✅ Implemented)

When quorum cannot be achieved (e.g., too many managers down), operations fail fast with clear errors.

**Implementation**:
- Circuit breaker pattern prevents cascading failures during degraded cluster state
- Three specific quorum error types provide clear diagnostics:
  - `QuorumUnavailableError`: Not enough active managers (structural issue)
  - `QuorumTimeoutError`: Managers available but didn't respond in time
  - `QuorumCircuitOpenError`: Too many recent failures, failing fast
- Circuit breaker settings: Opens after 3 failures in 30s window, recovers after 10s
- `get_quorum_status()` method provides observability into circuit state

**Error Flow**:
1. Check circuit breaker first → `QuorumCircuitOpenError` if OPEN
2. Check if quorum possible → `QuorumUnavailableError` if insufficient managers
3. Attempt quorum → `QuorumTimeoutError` if timeout without enough confirmations
4. Record success/failure for circuit breaker state transitions

### New Gate Join Process (✅ Implemented)

Same pattern as managers - gates join in SYNCING state before becoming ACTIVE:

**Implementation**:
1. New gate joins SWIM cluster → State = SYNCING
2. SYNCING gates are NOT counted in quorum (`_has_quorum_available()` returns false)
3. Gate starts leader election
4. If leader: immediately transitions to ACTIVE
5. If not leader: requests state sync from current leader via `_complete_startup_sync()`
6. After sync completes (or times out): State = ACTIVE → now counted in quorum

**Key Components**:
- `GateState` enum: SYNCING, ACTIVE, DRAINING
- `_gate_state` field tracks current state
- `GateHeartbeat.state` field broadcasts state to peers
- `_complete_startup_sync()` handles non-leader state sync on startup
- `_has_quorum_available()` excludes SYNCING gates from quorum count

### Gate Quorum Timeout Handling (✅ Implemented)

Gates use the same circuit breaker pattern as managers for fail-fast behavior.

**Implementation**:
- `_quorum_circuit` ErrorStats instance tracks failures
- `_quorum_size()` calculates required quorum (majority of gates)
- `_has_quorum_available()` checks gate state and active peer count
- `get_quorum_status()` returns circuit state and gate metrics
- `receive_job_submission()` checks circuit breaker before accepting jobs
- `_dispatch_job_to_datacenters()` records success/failure for circuit breaker

**Job Submission Flow**:
1. Check if leader (only leader accepts jobs)
2. Check circuit breaker state → reject if OPEN
3. Check quorum availability → reject if insufficient active gates
4. Select datacenters and dispatch job
5. Record success/failure for circuit breaker transitions

### Worker ↔ Manager Communication Resilience (✅ Implemented)

All Worker ↔ Manager communication now uses retries with exponential backoff and circuit breakers.

#### Worker → Manager Communication

**Circuit Breaker**:
- `_manager_circuit`: ErrorStats tracking failures to managers
- `_is_manager_circuit_open()`: Check if circuit is open (fail-fast mode)
- `get_manager_circuit_status()`: Observability endpoint
- Settings: Opens after 3 failures in 30s, recovers after 10s

**Registration with Retries**:
```python
_register_with_manager(manager_addr, max_retries=3, base_delay=0.5)
# Delays: 0.5s → 1.0s → 2.0s
# Checks circuit breaker before attempting
# Records success/error for circuit state
```

**Progress Updates with Retries**:
```python
_send_progress_update(progress, max_retries=2, base_delay=0.2)
# Delays: 0.2s → 0.4s (shorter for frequent updates)
# Checks circuit breaker before attempting
# Records success/error for circuit state
```

#### Manager → Worker Communication

**Per-Worker Circuit Breakers**:
- `_worker_circuits: dict[str, ErrorStats]`: One circuit per worker
- `_get_worker_circuit()`: Get or create circuit for a worker
- `_is_worker_circuit_open()`: Check if worker's circuit is open
- `get_worker_circuit_status()`: Status for specific worker
- `get_all_worker_circuit_status()`: Status for all workers

**Worker Selection**:
- `_select_worker_for_workflow()`: Skips workers with open circuits
- `_select_worker_for_workflow_excluding()`: Skips workers with open circuits

**Workflow Dispatch with Retries**:
```python
_dispatch_workflow_to_worker(worker_id, dispatch, max_retries=2, base_delay=0.3)
# Delays: 0.3s → 0.6s
# Checks per-worker circuit before attempting
# Records success/error for per-worker circuit
# Worker rejection (not accepted) does NOT trigger retry
```

**Benefits**:
- Transient network failures are retried automatically
- Persistent failures trigger circuit breaker (fail-fast)
- Per-worker circuits prevent one bad worker from affecting others
- Exponential backoff prevents thundering herd on recovery

### Manager ↔ Gate Communication Resilience (✅ Implemented)

All Manager ↔ Gate communication now uses retries with exponential backoff and circuit breakers.

#### Manager → Gate Communication

**Circuit Breaker**:
- `_gate_circuit`: ErrorStats tracking failures to gates
- `_is_gate_circuit_open()`: Check if circuit is open (fail-fast mode)
- `get_gate_circuit_status()`: Observability endpoint
- Settings: Opens after 3 failures in 30s, recovers after 10s

**Registration with Retries**:
```python
_try_register_with_gate(gate_addr, max_retries=3, base_delay=0.5)
# Delays: 0.5s → 1.0s → 2.0s
# Checks circuit breaker before attempting
# Records success/error for circuit state
# Gate rejection (not accepted) does NOT trigger retry
```

**Job Progress with Retries**:
```python
_send_job_progress_to_gate(job, max_retries=2, base_delay=0.2)
# Delays: 0.2s → 0.4s (shorter for frequent updates)
# Checks circuit breaker before attempting
# Records success/error for circuit state
```

#### Gate → Manager Communication

**Per-Manager Circuit Breakers**:
- `_manager_circuits: dict[tuple[str, int], ErrorStats]`: One circuit per manager
- `_get_manager_circuit()`: Get or create circuit for a manager
- `_is_manager_circuit_open()`: Check if manager's circuit is open
- `get_manager_circuit_status()`: Status for specific manager
- `get_all_manager_circuit_status()`: Status for all managers

**Dispatch with Retries**:
```python
_try_dispatch_to_manager(manager_addr, submission, max_retries=2, base_delay=0.3)
# Delays: 0.3s → 0.6s
# Checks per-manager circuit before attempting
# Records success/error for per-manager circuit
# Manager rejection (not accepted, not busy) does NOT trigger retry
# BUSY response treated as success (job will be queued)
```

**DC-Level Dispatch**:
- `_try_dispatch_to_dc()`: Iterates managers, uses `_try_dispatch_to_manager`
- `_dispatch_job_with_fallback()`: Handles DC-level fallback chain
- Per-manager failures don't affect other managers in same DC
- If all managers in DC fail, tries fallback DCs

**Benefits**:
- Transient network failures retried automatically
- Per-manager circuits prevent one bad manager from affecting others
- DC-level fallback ensures jobs reach healthy DCs
- Exponential backoff prevents thundering herd on recovery

### Client Push Notifications (Implemented)

Client push notifications allow Gates and Managers to push job status updates directly to clients, eliminating the need for polling.

**Architecture**:

```
┌──────────────────────────────────────────────────────────────┐
│                   Push Notification Flow                     │
├──────────────────────────────────────────────────────────────┤
│  1. Client starts a TCP listener                             │
│  2. Client → Gate/Manager: JobSubmission(callback_addr=...)  │
│  3. Gate/Manager stores callback in _job_callbacks           │
│  4. On Tier 1 events (completion/failure):                   │
│     Gate/Manager → Client: JobStatusPush                     │
│  5. On Tier 2 interval (every 2s):                           │
│     Gate/Manager → Client: JobBatchPush                      │
└──────────────────────────────────────────────────────────────┘
```

**Message Types**:

- `JobStatusPush`: Tier 1 immediate updates for critical events (started, completed, failed)
  - `job_id`, `status`, `message`, `total_completed`, `total_failed`, `overall_rate`, `elapsed_seconds`, `is_final`
- `JobBatchPush`: Tier 2 periodic updates with aggregated stats
  - `job_id`, `status`, `step_stats[]`, `total_completed`, `total_failed`, `overall_rate`, `elapsed_seconds`

**JobSubmission Extension**:

```python
@dataclass
class JobSubmission(Message):
    job_id: str
    workflows: bytes
    # ... other fields ...
    callback_addr: tuple[str, int] | None = None  # Optional push callback
```

**Gate Implementation** (`GateServer`):

- `_job_callbacks: dict[str, tuple[str, int]]` - Stores callbacks by job_id
- `_send_immediate_update()` - Pushes `JobStatusPush` on critical events
- `_batch_stats_update()` - Pushes `JobBatchPush` to all callbacks for running jobs

**Manager Implementation** (`ManagerServer`):

- `_job_callbacks: dict[str, tuple[str, int]]` - Stores callbacks by job_id
- `_push_job_status_to_client()` - Pushes `JobStatusPush` on critical events
- `_push_batch_stats_to_clients()` - Pushes `JobBatchPush` periodically
- `_client_batch_push_loop()` - Background loop for Tier 2 updates (only when no gates)
- `_check_job_completion()` - Detects job completion and triggers push

**Client Implementation**:

Clients that want push notifications must implement TCP receivers:

```python
class JobStatusClient(MercurySyncBaseServer):
    @tcp.receive()
    async def receive_job_status_push(self, addr, data, clock_time):
        status = JobStatusPush.load(data)
        # Handle immediate status update
        return b'ok'
    
    @tcp.receive()
    async def receive_job_batch_push(self, addr, data, clock_time):
        batch = JobBatchPush.load(data)
        # Handle batched progress update
        return b'ok'
```

**Behavior**:

- Gate mode: Gates push to clients, managers forward to gates
- Direct mode: Managers push directly to clients (when no gates configured)
- Callbacks are automatically cleaned up when jobs reach final state

---

## Testing

Run the test suite:

```bash
python examples/test_distributed_rewrite.py
```

Current test coverage: 254+ tests covering:
- SWIM protocol (probing, suspicion, gossip)
- Leadership election (pre-voting, flapping)
- State embedding (heartbeat serialization)
- Distributed messages (all message types)
- Worker/Manager/Gate functionality
- State sync with retry mechanisms
- Per-core workflow assignment
- Worker/Manager failure handling
- Manager peer failure/recovery
- Gate split-brain prevention
- CRDTs (GCounter, LWWRegister, LWWMap, JobStatsCRDT)
- Datacenter health classification (HEALTHY/BUSY/DEGRADED/UNHEALTHY)
- Smart dispatch with fallback chain
- Tiered update strategy
- Client push notifications (JobStatusPush, JobBatchPush)
- Gate state management (SYNCING/ACTIVE/DRAINING)
- Gate quorum circuit breaker
- Worker circuit breaker for manager communication
- Worker retries with exponential backoff (registration, progress)
- Manager per-worker circuit breakers
- Manager retries with exponential backoff (workflow dispatch)
- Manager circuit breaker for gate communication
- Manager retries with exponential backoff (gate registration, job progress)
- Gate per-manager circuit breakers
- Gate retries with exponential backoff (manager dispatch)

---

## Manager Workflow Execution Architecture

This section documents how Managers handle workflow execution, mirroring the `RemoteGraphManager` architecture for distributed execution.

### Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     MANAGER WORKFLOW EXECUTION FLOW                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  JobSubmission                                                               │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 1. WORKFLOW CLASSIFICATION                                           │    │
│  │    • Detect test workflows (have HookType.TEST hooks)               │    │
│  │    • Build dependency graph (DependentWorkflow relationships)       │    │
│  │    • Determine execution order (BFS traversal)                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 2. PRIORITY-BASED THREAD ALLOCATION                                  │    │
│  │    • Calculate thread range from TOTAL pool (not available)         │    │
│  │    • Use StagePriority.get_worker_allocation_range()                │    │
│  │    • Provisioner.partion_by_priority() returns batches              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 3. VU PROVISIONING                                                   │    │
│  │    • vus_per_thread = workflow.vus / threads                        │    │
│  │    • Distribute remainder to last thread                            │    │
│  │    • Store workflow_vus: dict[workflow_name, list[int]]            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 4. CAPACITY CHECK & WORKER SELECTION                                 │    │
│  │    • Check if workers have enough AVAILABLE cores for threads      │    │
│  │    • Select workers via crypto-random (avoid bias)                  │    │
│  │    • If insufficient capacity → queue job (BUSY) or fail (DEGRADED)│    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 5. QUORUM CONFIRMATION & DISPATCH                                    │    │
│  │    • Request quorum confirmation from peer managers                 │    │
│  │    • On quorum: commit provisioning                                 │    │
│  │    • Dispatch WorkflowDispatch to selected workers                  │    │
│  │    • Include context for dependent workflows                        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 6. EXECUTION & CONTEXT SYNCHRONIZATION                               │    │
│  │    • Workers execute workflows                                      │    │
│  │    • Workers send WorkflowProgress with context updates             │    │
│  │    • Manager syncs context updates to peers                         │    │
│  │    • Dependent workflows receive context from predecessors          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 1: Workflow Classification

A workflow is classified as a **test workflow** if it has at least one hook with `HookType.TEST`. This classification is **critical** because it determines how many CPU cores the workflow receives:

- **Test workflows**: Get cores based on priority (can use up to 100% of pool)
- **Non-test workflows**: Always get 1 core (they don't parallelize load testing)

---

#### How HookType.TEST is Determined

A hook's type is set automatically by the `Hook` class based on the **return type annotation** of the decorated method.

**The Hook Type Decision Tree** (from `hook.py` lines 161-189):

```python
# Simplified logic from Hook.__init__()
if is_test and self.return_type in CallResult.__subclasses__():
    self.hook_type = HookType.TEST        # ← Test action (load testing)
    
elif is_test and self.return_type in CustomResult.__subclasses__():
    self.hook_type = HookType.TEST        # ← Custom test action
    
elif is_check:
    self.hook_type = HookType.CHECK       # ← Validation/assertion
    
elif is_metric:
    self.hook_type = HookType.METRIC      # ← Custom metric collection
    
else:
    self.hook_type = HookType.ACTION      # ← General action (setup/teardown)
```

**Key Insight**: The `@step()` decorator alone does NOT make a test workflow. The **return type** must be a `CallResult` subclass (like `HTTPResponse`, `GraphQLResponse`, etc.) for the hook to become `HookType.TEST`.

---

#### CallResult Subclasses (Test Return Types)

These return types indicate the method is a load test action:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     CALLRESULT SUBCLASSES (TEST TYPES)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  HTTP Testing:                                                               │
│  • HTTPResponse      - Standard HTTP response                               │
│  • HTTP2Response     - HTTP/2 response                                      │
│  • HTTP3Response     - HTTP/3 (QUIC) response                               │
│                                                                              │
│  API Testing:                                                                │
│  • GraphQLResponse   - GraphQL query response                               │
│  • GRPCResponse      - gRPC call response                                   │
│                                                                              │
│  Database Testing:                                                           │
│  • MySQLResponse     - MySQL query response                                 │
│  • PostgresResponse  - PostgreSQL query response                            │
│  • MongoDBResponse   - MongoDB operation response                           │
│  • RedisResponse     - Redis command response                               │
│                                                                              │
│  Messaging Testing:                                                          │
│  • KafkaResponse     - Kafka produce/consume response                       │
│  • RabbitMQResponse  - RabbitMQ message response                            │
│                                                                              │
│  WebSocket/Realtime:                                                         │
│  • WebsocketResponse - WebSocket message response                           │
│  • UDPResponse       - UDP packet response                                  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

#### Complete Example: Test vs Non-Test Workflows

```python
from hyperscale.graph import Workflow, step, action, depends
from hyperscale.testing import URL, HTTPResponse, Headers


class LoadTestWorkflow(Workflow):
    """
    TEST WORKFLOW - Gets multiple cores based on priority.
    
    This is a test workflow because:
    1. Has @step() decorated method
    2. Return type is HTTPResponse (a CallResult subclass)
    3. Calls self.client.http.get() which returns HTTPResponse
    
    Result: HookType.TEST → participates in priority-based core allocation
    """
    vus = 10000           # Virtual users (can be large!)
    duration = "5m"
    priority = "high"     # Optional: LOW, NORMAL, HIGH, EXCLUSIVE, AUTO (default)
    
    @step()
    async def test_api_endpoint(
        self,
        url: URL = 'https://api.example.com/users',
        headers: Headers = {'Authorization': 'Bearer token123'}
    ) -> HTTPResponse:    # ← This return type makes it HookType.TEST
        """Load test the users API endpoint."""
        return await self.client.http.get(url, headers=headers)
    
    @step()
    async def test_post_data(
        self,
        url: URL = 'https://api.example.com/data',
    ) -> HTTPResponse:    # ← Also HookType.TEST
        """Load test data submission."""
        return await self.client.http.post(url, json={"key": "value"})


class SetupWorkflow(Workflow):
    """
    NON-TEST WORKFLOW - Always gets 1 core.
    
    This is NOT a test workflow because:
    1. Uses @action() decorator (not @step())
    2. Return type is None (not a CallResult)
    
    Result: HookType.ACTION → single core, runs sequentially
    """
    vus = 1
    duration = "30s"
    
    @action()
    async def setup_test_data(self) -> None:  # ← None return = HookType.ACTION
        """Prepare test data before load testing."""
        # This runs on a single core
        self.context['api_key'] = 'test-key-123'
        self.context['base_url'] = 'https://api.example.com'


class UtilityWorkflow(Workflow):
    """
    NON-TEST WORKFLOW - @step() with dict return.
    
    This is NOT a test workflow because:
    1. Has @step() decorated method
    2. BUT return type is dict (NOT a CallResult subclass)
    
    Result: HookType.ACTION → single core
    """
    vus = 1000  # VUs don't matter - still gets 1 core
    duration = "1m"
    
    @step()
    async def process_data(self) -> dict:     # ← dict return = HookType.ACTION
        """Process data - not a load test."""
        await asyncio.sleep(0.1)
        return {"processed": True, "count": 100}


@depends('SetupWorkflow')
class DependentLoadTest(Workflow):
    """
    TEST WORKFLOW with dependency.
    
    This workflow:
    1. Waits for SetupWorkflow to complete
    2. Receives context from SetupWorkflow
    3. Is a test workflow (HTTPResponse return)
    """
    vus = 5000
    duration = "3m"
    
    @step()
    async def authenticated_request(
        self,
        url: URL = 'https://api.example.com/protected',
    ) -> HTTPResponse:
        """Use context from SetupWorkflow."""
        api_key = self.context.get('api_key', '')
        return await self.client.http.get(
            url, 
            headers={'X-API-Key': api_key}
        )
```

---

#### Detection Logic in Distributed Manager

```python
def _is_test_workflow(self, workflow) -> bool:
    """
    Determine if a workflow is a test workflow.
    
    A workflow is a test workflow if it has ANY hook with
    hook_type == HookType.TEST. The Hook class sets this
    automatically based on return type annotations.
    """
    import inspect
    from hyperscale.core.hooks import Hook
    
    for name, member in inspect.getmembers(workflow):
        if isinstance(member, Hook) and member.hook_type == HookType.TEST:
            return True
    return False
```

---

#### Core Allocation Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      CORE ALLOCATION BY WORKFLOW TYPE                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ TEST WORKFLOW                                                          │ │
│  │                                                                         │ │
│  │  Detection:                                                             │ │
│  │  • @step() decorator + CallResult return type (HTTPResponse, etc.)     │ │
│  │  • Hook.hook_type == HookType.TEST                                      │ │
│  │                                                                         │ │
│  │  Core Allocation:                                                       │ │
│  │  • Based on workflow.priority (default: AUTO)                          │ │
│  │  • AUTO: 1 to 100% of pool (single workflow gets all cores)            │ │
│  │  • LOW:  1 to 25% of pool                                               │ │
│  │  • NORMAL: 25% to 75% of pool                                           │ │
│  │  • HIGH: 75% to 100% of pool                                            │ │
│  │  • EXCLUSIVE: 100% of pool                                              │ │
│  │                                                                         │ │
│  │  Example: pool=8 cores, priority=NORMAL, vus=50000                      │ │
│  │  → Gets 2-6 cores (25-75% of 8)                                         │ │
│  │  → 50000 VUs distributed across cores (e.g., ~8333 VUs/core)           │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ NON-TEST WORKFLOW                                                       │ │
│  │                                                                         │ │
│  │  Detection:                                                             │ │
│  │  • @step() with non-CallResult return (dict, None, etc.)               │ │
│  │  • @action(), @check(), @metric() decorators                            │ │
│  │  • Hook.hook_type != HookType.TEST                                      │ │
│  │                                                                         │ │
│  │  Core Allocation:                                                       │ │
│  │  • ALWAYS 1 core (regardless of vus, priority, or pool size)           │ │
│  │  • Non-test workflows don't parallelize load testing                   │ │
│  │  • Used for setup, teardown, data processing, etc.                     │ │
│  │                                                                         │ │
│  │  Example: pool=8 cores, vus=10000                                       │ │
│  │  → Gets 1 core (VUs don't affect allocation for non-test)              │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ⚠️  IMPORTANT: VUs ≠ Cores                                                 │
│      VUs (virtual users) can be 50,000+ and are distributed across cores.   │
│      Core allocation is determined by priority, NOT by VU count.            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

#### WorkflowDispatch Message Structure

The manager sends both `vus` and `cores` to workers:

```python
@dataclass(slots=True)
class WorkflowDispatch(Message):
    """Dispatch a single workflow to a worker."""
    job_id: str              # Parent job identifier
    workflow_id: str         # Unique workflow instance ID
    workflow: bytes          # Cloudpickled Workflow class
    context: bytes           # Cloudpickled context dict
    vus: int                 # Virtual users (can be 50k+)
    cores: int               # CPU cores to allocate (from priority)
    timeout_seconds: float   # Execution timeout
    fence_token: int         # Fencing token for at-most-once
    context_version: int     # Layer version for staleness detection
    dependency_context: bytes # Context from dependencies
```

Workers allocate `cores` CPU cores and distribute `vus` virtual users across them.

---

### Virtual Users (VUs) and Steps

#### What is a Virtual User (VU)?

A **Virtual User (VU)** represents a single, continuously looping instance of a workflow. Each VU simulates one user performing a sequence of steps repeatedly for the duration of the test.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           VIRTUAL USER CONCEPT                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  workflow.vus = 10000    →   10,000 simulated users                         │
│  workflow.duration = "5m" →   Each user runs for 5 minutes                  │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  VU #1                                                              │    │
│  │  ┌────────────────────────────────────────────────────────────────┐ │    │
│  │  │  Loop until duration expires:                                   │ │    │
│  │  │    → Execute @step() method 1                                   │ │    │
│  │  │    → Execute @step() method 2                                   │ │    │
│  │  │    → Execute @step() method N                                   │ │    │
│  │  │    → Record metrics (latency, status, etc.)                     │ │    │
│  │  │    → Repeat...                                                  │ │    │
│  │  └────────────────────────────────────────────────────────────────┘ │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ... × 10,000 concurrent virtual users                                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**VU Distribution Across Cores**:

When a test workflow gets multiple cores (based on priority), VUs are evenly distributed:

```
Example: vus=10000, cores=4

  Core 0: 2,500 VUs running in parallel
  Core 1: 2,500 VUs running in parallel
  Core 2: 2,500 VUs running in parallel
  Core 3: 2,500 VUs running in parallel
  ─────────────────────────────────────
  Total:  10,000 VUs across 4 cores
```

---

#### What is a Step?

A **Step** is an async method decorated with `@step()` that defines a single action in the workflow loop. Steps are the building blocks of load tests.

```python
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, Headers, HTTPResponse


class APILoadTest(Workflow):
    vus = 5000
    duration = "3m"
    
    @step()
    async def get_users(
        self,
        url: URL = 'https://api.example.com/users',
    ) -> HTTPResponse:
        """Each VU calls this step repeatedly for 3 minutes."""
        return await self.client.http.get(url)
    
    @step()
    async def get_user_details(
        self,
        url: URL = 'https://api.example.com/users/1',
    ) -> HTTPResponse:
        """Called after get_users, then loop restarts."""
        return await self.client.http.get(url)
```

**Step Execution Order**:

```
VU Loop Iteration:
  1. Execute get_users() → record metrics
  2. Execute get_user_details() → record metrics
  3. Repeat until duration expires
```

---

#### Optimized Args (Performance Optimization)

**The Problem**: Load testing overhead can skew results. When testing API performance, we don't want to measure:
- DNS lookup time
- Header serialization time
- JSON encoding time
- SSL handshake overhead (for new connections)

These are test infrastructure costs, not the target system's actual performance.

**The Solution**: Hyperscale uses **Optimized Args** - special type-annotated keyword arguments that are pre-processed BEFORE the test loop starts.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         OPTIMIZED ARGS CONCEPT                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  BEFORE WORKFLOW EXECUTION (once, at startup):                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1. Parse @step() method signatures                                  │    │
│  │  2. Find keyword args with Optimized type hints (URL, Headers, etc.)│    │
│  │  3. Extract default values                                           │    │
│  │  4. Call .optimize() on each:                                        │    │
│  │     • URL: DNS lookup, address resolution                           │    │
│  │     • Headers: Serialize to bytes/string format                     │    │
│  │     • Data: JSON encode, compute content-length                     │    │
│  │  5. Store optimized values for reuse                                 │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                            │                                                 │
│                            ▼                                                 │
│  DURING WORKFLOW EXECUTION (every loop iteration):                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1. Use pre-resolved IP address (skip DNS)                          │    │
│  │  2. Use pre-serialized headers (skip encoding)                      │    │
│  │  3. Use pre-encoded data (skip JSON serialization)                  │    │
│  │  4. Measure ONLY the actual HTTP request/response time              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Result: Metrics reflect true target system performance,                    │
│          not test infrastructure overhead.                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

#### Available Optimized Arg Types

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        OPTIMIZED ARG TYPES                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  HTTP/Network:                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  URL       │ Pre-resolves DNS, caches IP address                    │    │
│  │            │ Supports HTTP, HTTP2, HTTP3, GraphQL, WebSocket, etc. │    │
│  │            │                                                         │    │
│  │  Headers   │ Pre-serializes headers to wire format                  │    │
│  │            │ HTTP/1.1: "Key: Value\r\n" string                      │    │
│  │            │ HTTP/2+: [(b'key', b'value'), ...] tuples              │    │
│  │            │                                                         │    │
│  │  Data      │ Pre-encodes request body                               │    │
│  │            │ dict/list → JSON bytes (via orjson)                    │    │
│  │            │ Pydantic model → JSON bytes                            │    │
│  │            │ Computes content-length and content-type               │    │
│  │            │                                                         │    │
│  │  Params    │ Pre-encodes URL query parameters                       │    │
│  │            │ {"key": "value"} → "?key=value"                        │    │
│  │            │                                                         │    │
│  │  Cookies   │ Pre-formats cookie header                              │    │
│  │            │ {"session": "abc"} → "session=abc"                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Authentication:                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Auth      │ Pre-computes authentication headers                    │    │
│  │            │ Basic, Bearer, OAuth, etc.                             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  GraphQL:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Query     │ Pre-validates and formats GraphQL query                │    │
│  │  Mutation  │ Pre-validates and formats GraphQL mutation             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  File Transfer:                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  File      │ Pre-reads file content, computes metadata              │    │
│  │  Directory │ Pre-scans directory structure                          │    │
│  │  FileGlob  │ Pre-resolves glob patterns                             │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  gRPC/Protobuf:                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Protobuf  │ Pre-validates protobuf message structure               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Email/SMTP:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Email     │ Pre-formats email message (MIME encoding, etc.)        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

#### Complete Example with Optimized Args

```python
from hyperscale.graph import Workflow, step
from hyperscale.testing import (
    URL,
    Headers,
    Data,
    Params,
    Cookies,
    Auth,
    HTTPResponse,
)


class OptimizedAPITest(Workflow):
    """
    Load test demonstrating all major optimized arg types.
    
    BEFORE execution starts, Hyperscale:
    1. Resolves 'api.example.com' → 93.184.216.34
    2. Serializes headers → "Authorization: Bearer token\r\n..."
    3. JSON-encodes data → b'{"action":"create"}'
    4. Encodes params → "?page=1&limit=100"
    5. Formats cookies → "session=abc123"
    
    DURING execution:
    - Uses cached IP (no DNS lookup per request)
    - Uses pre-serialized headers (no encoding per request)
    - Uses pre-encoded JSON (no serialization per request)
    - Metrics measure ONLY actual HTTP latency
    """
    vus = 10000
    duration = "5m"
    priority = "high"
    
    @step()
    async def get_with_auth(
        self,
        url: URL = 'https://api.example.com/users',
        headers: Headers = {
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIs...',
            'Accept': 'application/json',
        },
        params: Params = {'page': 1, 'limit': 100},
    ) -> HTTPResponse:
        """
        GET request with pre-optimized:
        - URL (DNS pre-resolved)
        - Headers (pre-serialized)
        - Query params (pre-encoded)
        """
        return await self.client.http.get(
            url,
            headers=headers,
            params=params,
        )
    
    @step()
    async def post_json_data(
        self,
        url: URL = 'https://api.example.com/actions',
        headers: Headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIs...',
        },
        data: Data = {
            'action': 'create',
            'resource': 'user',
            'metadata': {'source': 'load_test'},
        },
    ) -> HTTPResponse:
        """
        POST request with pre-optimized:
        - URL (DNS pre-resolved)
        - Headers (pre-serialized)
        - JSON body (pre-encoded via orjson)
        - Content-Length (pre-computed)
        """
        return await self.client.http.post(
            url,
            headers=headers,
            data=data,
        )
    
    @step()
    async def request_with_cookies(
        self,
        url: URL = 'https://api.example.com/session',
        cookies: Cookies = {
            'session_id': 'abc123xyz',
            'user_pref': 'dark_mode',
        },
    ) -> HTTPResponse:
        """
        Request with pre-formatted cookies.
        """
        return await self.client.http.get(url, cookies=cookies)
```

---

#### How Optimization Works (URL Example)

```python
# From hyperscale/core/testing/models/url/url.py

class URL(OptimizedArg):
    def __init__(self, url: str):
        self.data = url
        self.optimized: Optional[OptimizedUrl] = None
    
    async def optimize(self, request_type: RequestType):
        """Called ONCE before workflow execution starts."""
        if self.optimized is not None:
            return  # Already optimized, skip
        
        # Create optimized URL with correct protocol
        self.optimized = OptimizedUrl(
            self.data,
            family=address_family,  # IPv4/IPv6
            protocol=protocol,       # TCP/UDP/QUIC
        )
        
        # Pre-resolve DNS based on request type
        match request_type:
            case RequestType.HTTP | RequestType.HTTP2 | RequestType.HTTP3:
                await self.optimized.lookup()  # DNS → IP address
            case RequestType.FTP:
                await self.optimized.lookup_ftp()
            case RequestType.SMTP:
                await self.optimized.lookup_smtp()
            # ... etc.
```

**Timeline**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        OPTIMIZATION TIMELINE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  T=0: Workflow submitted                                                     │
│       │                                                                      │
│       ▼                                                                      │
│  T=1: Parse @step() signatures, extract optimized args                      │
│       │                                                                      │
│       ▼                                                                      │
│  T=2: url.optimize() → DNS lookup (50-200ms typically)                      │
│       headers.optimize() → serialize (< 1ms)                                │
│       data.optimize() → JSON encode (< 1ms)                                 │
│       │                                                                      │
│       ▼                                                                      │
│  T=3: START metrics collection                                              │
│       │                                                                      │
│       ├──► VU 1:    HTTP GET (uses cached IP, pre-serialized headers)      │
│       │              └─ Latency: 15ms (measured)                            │
│       │                                                                      │
│       ├──► VU 2:    HTTP GET (uses cached IP, pre-serialized headers)      │
│       │              └─ Latency: 12ms (measured)                            │
│       │                                                                      │
│       └──► VU N:    ... (all use same optimized values)                     │
│                                                                              │
│  DNS lookup cost (200ms) paid ONCE, not per request.                        │
│  With 10,000 VUs × 100 requests each = 1,000,000 requests                   │
│  Savings: 200ms × 1,000,000 = 55+ hours of DNS overhead eliminated!         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Step Dependencies and the Step DAG

Steps within a workflow can depend on other steps, forming a **Directed Acyclic Graph (DAG)**. This determines execution order within each VU's loop.

#### Declaring Step Dependencies

Pass string names of other steps to `@step()`:

```python
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse


class APITestWorkflow(Workflow):
    """
    Step DAG:
    
            authenticate
            /          \
       get_users    get_config    ← Run in parallel (same dependency)
            \          /
           process_data
    """
    vus = 5000
    duration = "3m"
    
    @step()  # No args = root step (no dependencies)
    async def authenticate(
        self,
        url: URL = 'https://api.example.com/auth',
    ) -> HTTPResponse:
        """First step - authenticates and returns token."""
        return await self.client.http.post(url, json={"user": "test"})
    
    @step('authenticate')  # Depends on 'authenticate'
    async def get_users(
        self,
        url: URL = 'https://api.example.com/users',
        authenticate: HTTPResponse | None = None,  # ← Gets authenticate's result!
    ) -> HTTPResponse:
        """Runs after authenticate. Can access auth response via kwarg."""
        # authenticate kwarg contains the HTTPResponse from authenticate step
        token = authenticate.json().get('token') if authenticate else None
        return await self.client.http.get(url)
    
    @step('authenticate')  # Also depends on 'authenticate' (parallel to get_users)
    async def get_config(
        self,
        url: URL = 'https://api.example.com/config',
    ) -> HTTPResponse:
        """Runs in parallel with get_users (both depend only on authenticate)."""
        return await self.client.http.get(url)
    
    @step('get_users', 'get_config')  # Depends on BOTH get_users AND get_config
    async def process_data(
        self,
        url: URL = 'https://api.example.com/process',
        get_users: HTTPResponse | None = None,    # ← Gets get_users result
        get_config: HTTPResponse | None = None,   # ← Gets get_config result
    ) -> HTTPResponse:
        """Final step - waits for both parallel steps to complete."""
        # Can access results from both previous steps
        users = get_users.json() if get_users else []
        config = get_config.json() if get_config else {}
        return await self.client.http.post(url, json={"users": users})
```

---

#### DAG Execution Order

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STEP DAG EXECUTION                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Each VU executes the DAG in topological order:                             │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Layer 0: [authenticate]           ← Execute, store result          │    │
│  │                  │                                                   │    │
│  │                  ▼                                                   │    │
│  │  Layer 1: [get_users, get_config]  ← Execute in parallel            │    │
│  │                  │                                                   │    │
│  │                  ▼                                                   │    │
│  │  Layer 2: [process_data]           ← Wait for both, then execute    │    │
│  │                  │                                                   │    │
│  │                  ▼                                                   │    │
│  │  Loop back to Layer 0 until duration expires                        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Steps in the same layer (same dependencies) run concurrently.              │
│  Metrics are collected for each step separately.                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

#### Dependency Rules

| Pattern | Meaning |
|---------|---------|
| `@step()` | Root step, no dependencies |
| `@step('a')` | Depends on step `a` |
| `@step('a', 'b')` | Depends on BOTH `a` AND `b` |
| `@step('a')` + `@step('a')` | Both depend on `a`, run in parallel |

**Important Constraints**:

1. **Workflow Islands**: Steps can ONLY reference other steps within the SAME workflow class. Cross-workflow data sharing uses `@state()` methods only.

2. **Acyclic Only**: Dependencies must form a DAG. Circular dependencies will cause errors.

3. **String Names**: Dependencies are the **function names** as strings, not the functions themselves.

---

### VU-Isolated Context and Step Data Passing

#### Each VU Gets an Isolated Context Copy

When a VU starts its loop iteration, it receives a **shallow copy** of the workflow context:

```python
# From WorkflowRunner._spawn_vu()
context: Dict[str, Any] = dict(context)  # ← Fresh copy for this VU
```

This ensures:
- **No cross-VU interference**: VU #1's step results don't affect VU #2
- **Clean slate each iteration**: Each loop starts fresh
- **Thread safety**: No shared mutable state between concurrent VUs

---

#### Step Results Stored Under Function Name

After each step completes, its result is stored in the VU's context under the step's function name:

```python
# From WorkflowRunner._spawn_vu()
for complete in completed:
    step_name = complete.get_name()       # e.g., "authenticate"
    result = complete.result()            # HTTPResponse object
    context[step_name] = result           # context["authenticate"] = HTTPResponse
```

---

#### Accessing Previous Step Data

Subsequent steps access previous results via **keyword arguments** with matching names:

```python
# Hyperscale matches kwarg names to context keys
for hook in hook_set.values():
    hook.context_args.update(
        {key: context[key] for key in context if key in hook.kwarg_names}
    )
```

**Example**:

```python
@step('authenticate')
async def get_users(
    self,
    url: URL = 'https://api.example.com/users',
    authenticate: HTTPResponse | None = None,  # ← Matches context['authenticate']
) -> HTTPResponse:
    """
    The 'authenticate' kwarg will receive the HTTPResponse
    from the authenticate() step because:
    1. 'authenticate' is in hook.kwarg_names
    2. context['authenticate'] exists (from previous step)
    3. Hyperscale passes context['authenticate'] to this kwarg
    """
    if authenticate and authenticate.status_code == 200:
        token = authenticate.json().get('token')
        # Use token in this request
    return await self.client.http.get(url)
```

---

#### Optimized Args Override Context

**Important**: If a keyword argument has an `OptimizedArg` type hint (`URL`, `Headers`, `Data`, etc.), the optimized value takes precedence over context lookup.

```python
@step('step_one')
async def step_two(
    self,
    url: URL = 'https://api.example.com',  # ← OptimizedArg - NOT from context!
    step_one: HTTPResponse | None = None,   # ← From context (not OptimizedArg type)
) -> HTTPResponse:
    # 'url' uses the pre-optimized URL value
    # 'step_one' gets the HTTPResponse from step_one's execution
    return await self.client.http.get(url)
```

---

#### Complete Data Flow Example

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    VU DATA FLOW THROUGH STEP DAG                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  VU #42 Loop Iteration:                                                     │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  1. VU starts with fresh context copy:                              │    │
│  │     context = {}                                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                            │                                                 │
│                            ▼                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  2. Execute authenticate():                                          │    │
│  │     result = HTTPResponse(status=200, body={"token": "abc123"})     │    │
│  │     context["authenticate"] = result                                 │    │
│  │                                                                      │    │
│  │     context = {"authenticate": HTTPResponse(...)}                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                            │                                                 │
│                            ▼                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  3. Execute get_users(authenticate=context["authenticate"]):        │    │
│  │     # authenticate kwarg receives the HTTPResponse from step 2      │    │
│  │     result = HTTPResponse(status=200, body=[{user1}, {user2}])      │    │
│  │     context["get_users"] = result                                   │    │
│  │                                                                      │    │
│  │  3. Execute get_config() in PARALLEL:                               │    │
│  │     result = HTTPResponse(status=200, body={"theme": "dark"})       │    │
│  │     context["get_config"] = result                                  │    │
│  │                                                                      │    │
│  │     context = {                                                      │    │
│  │       "authenticate": HTTPResponse(...),                            │    │
│  │       "get_users": HTTPResponse(...),                               │    │
│  │       "get_config": HTTPResponse(...)                               │    │
│  │     }                                                                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                            │                                                 │
│                            ▼                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  4. Execute process_data(                                            │    │
│  │       get_users=context["get_users"],                               │    │
│  │       get_config=context["get_config"]                              │    │
│  │     ):                                                               │    │
│  │     # Both kwargs receive results from parallel steps               │    │
│  │     result = HTTPResponse(status=201, body={"processed": True})     │    │
│  │     context["process_data"] = result                                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                            │                                                 │
│                            ▼                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  5. Loop complete - VU #42 starts fresh iteration                   │    │
│  │     (context reset for next loop)                                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Meanwhile, VU #1, #2, ... #41, #43, ... #5000 are doing the same thing    │
│  with their own isolated context copies.                                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

#### One Client Return Per Test Step

Each test step can make multiple client calls, but only **ONE** response can be returned for metrics:

```python
@step()
async def multi_call_step(
    self,
    url1: URL = 'https://api.example.com/check',
    url2: URL = 'https://api.example.com/data',
) -> HTTPResponse:
    """
    Can call multiple clients, but only return one for metrics.
    """
    # Call 1 - not measured (result discarded for metrics)
    check_response = await self.client.http.get(url1)
    
    if check_response.status_code != 200:
        # Early exit - still need to return HTTPResponse
        return check_response
    
    # Call 2 - THIS is what gets measured (returned)
    return await self.client.http.post(url2, json={"checked": True})
```

**Best Practice**: One client call per step for clear metrics.

---

#### Workflows Are Islands

Steps can ONLY depend on other steps within the **same workflow class**:

```python
class WorkflowA(Workflow):
    @step()
    async def step_a(self) -> HTTPResponse: ...

class WorkflowB(Workflow):
    @step('step_a')  # ❌ ERROR: Can't reference WorkflowA's step
    async def step_b(self) -> HTTPResponse: ...
```

**Cross-workflow communication** uses `@state()` methods and workflow-level `Context`:

```python
class WorkflowA(Workflow):
    @step()
    async def get_data(self) -> HTTPResponse:
        return await self.client.http.get(url)
    
    @state('WorkflowB')  # Share state TO WorkflowB
    def share_token(self) -> Provide[str]:
        return self.context.get('token', '')


@depends('WorkflowA')
class WorkflowB(Workflow):
    @state('WorkflowA')  # Receive state FROM WorkflowA
    def receive_token(self, share_token: str | None = None) -> Use[str]:
        return share_token
    
    @step()
    async def use_token(self) -> HTTPResponse:
        token = self.context.get('share_token', '')  # From WorkflowA
        return await self.client.http.get(url, headers={'Auth': token})
```

---

### Step 2: Priority-Based Thread Allocation

**Critical**: Thread allocation is calculated from the **TOTAL pool size** (all registered workers' cores), NOT available cores. This determines how many cores the workflow MAY request.

**StagePriority Allocation Ranges**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PRIORITY → THREAD ALLOCATION                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  TOTAL_POOL = sum(worker.total_cores for all registered workers)           │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Priority    │ Min Threads         │ Max Threads                    │    │
│  │  ────────────┼─────────────────────┼────────────────────────────────│    │
│  │  LOW         │ 1                   │ ceil(TOTAL_POOL × 0.25)       │    │
│  │  NORMAL      │ ceil(TOTAL_POOL×0.25)│ ceil(TOTAL_POOL × 0.75)      │    │
│  │  HIGH        │ ceil(TOTAL_POOL×0.75)│ TOTAL_POOL                   │    │
│  │  EXCLUSIVE   │ TOTAL_POOL          │ TOTAL_POOL (100%)             │    │
│  │  AUTO        │ 1                   │ TOTAL_POOL                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Example: TOTAL_POOL = 24 cores (3 workers × 8 cores each)                  │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Priority    │ Min Threads │ Max Threads                            │    │
│  │  ────────────┼─────────────┼────────────────────────────────────────│    │
│  │  LOW         │ 1           │ 6  (25% of 24)                         │    │
│  │  NORMAL      │ 6           │ 18 (75% of 24)                         │    │
│  │  HIGH        │ 18          │ 24 (100% of 24)                        │    │
│  │  EXCLUSIVE   │ 24          │ 24 (takes all cores)                   │    │
│  │  AUTO        │ 1           │ 24                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ⚠️  IMPORTANT: This is the ALLOCATION RANGE, not the final count.         │
│      The Provisioner bins multiple workflows into batches that fit          │
│      within TOTAL_POOL, distributing threads within these ranges.           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Provisioner.partion_by_priority() Algorithm**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PROVISIONER PARTITIONING                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Input: List of workflow configs:                                           │
│  [                                                                           │
│    {"workflow_name": "LoadTest", "priority": HIGH, "is_test": True},        │
│    {"workflow_name": "DataLoad", "priority": AUTO, "is_test": False},       │
│    {"workflow_name": "Metrics",  "priority": LOW,  "is_test": True},        │
│  ]                                                                           │
│                                                                              │
│  Algorithm:                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 1. Sort by priority (HIGH first), then by is_test                   │    │
│  │                                                                      │    │
│  │ 2. Non-test workflows → bypass batch (threads = 0, run sequentially)│    │
│  │                                                                      │    │
│  │ 3. For test workflows:                                               │    │
│  │    a. Calculate min/max threads from priority + TOTAL_POOL          │    │
│  │    b. Group workflows into batches that fit within TOTAL_POOL       │    │
│  │    c. Higher priority gets more threads within range                │    │
│  │    d. Distribute remaining threads to higher priority workflows     │    │
│  │                                                                      │    │
│  │ 4. Return: List[List[Tuple[workflow_name, priority, threads]]]      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Output Example (TOTAL_POOL = 24):                                          │
│  [                                                                           │
│    [("DataLoad", AUTO, 0)],           # Non-test: bypass batch             │
│    [("LoadTest", HIGH, 18),           # HIGH gets 18 threads               │
│     ("Metrics", LOW, 6)],             # LOW gets remaining 6               │
│  ]                                                                           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 3: VU Provisioning

After thread allocation, VUs are distributed among the allocated threads.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         VU PROVISIONING                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Formula:                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  vus_per_thread = workflow.vus // threads                           │    │
│  │  remainder_vus  = workflow.vus % threads                            │    │
│  │                                                                      │    │
│  │  # Each thread gets vus_per_thread                                  │    │
│  │  # Last thread gets vus_per_thread + remainder_vus                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Example: workflow.vus = 2000, threads = 6                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  vus_per_thread = 2000 // 6 = 333                                   │    │
│  │  remainder_vus  = 2000 % 6  = 2                                     │    │
│  │                                                                      │    │
│  │  workflow_vus = [333, 333, 333, 333, 333, 335]                      │    │
│  │                  ↑    ↑    ↑    ↑    ↑    ↑                         │    │
│  │                  T1   T2   T3   T4   T5   T6 (gets remainder)       │    │
│  │                                                                      │    │
│  │  Total: 333×5 + 335 = 1665 + 335 = 2000 ✓                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Result Structure:                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  workflow_vus: Dict[str, List[int]] = {                             │    │
│  │      "LoadTest": [333, 333, 333, 333, 333, 335],  # 6 threads       │    │
│  │      "Metrics":  [166, 167],                       # 2 threads       │    │
│  │  }                                                                   │    │
│  │                                                                      │    │
│  │  Each list entry = VUs for that thread/worker                       │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 4: Dependency Graph & Execution Order

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     DEPENDENCY GRAPH CONSTRUCTION                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Input Workflows:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Workflow("Setup")                                                   │    │
│  │  Workflow("LoadTest")                                                │    │
│  │  DependentWorkflow(                                                  │    │
│  │      workflow=Workflow("Validate"),                                  │    │
│  │      dependencies=["LoadTest"]                                       │    │
│  │  )                                                                   │    │
│  │  DependentWorkflow(                                                  │    │
│  │      workflow=Workflow("Report"),                                    │    │
│  │      dependencies=["Validate", "LoadTest"]                           │    │
│  │  )                                                                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Constructed Graph (networkx.DiGraph):                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │      Setup ─────────┐                                               │    │
│  │                     │                                               │    │
│  │      LoadTest ──────┼──────► Validate ──────► Report                │    │
│  │            │        │              │              ▲                 │    │
│  │            │        │              │              │                 │    │
│  │            └────────┼──────────────┼──────────────┘                 │    │
│  │                     │                                               │    │
│  │  Sources: [Setup, LoadTest]                                         │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  BFS Traversal Order:                                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Layer 0: {Setup, LoadTest}     # Run in parallel (no deps)         │    │
│  │  Layer 1: {Validate}            # Waits for LoadTest                │    │
│  │  Layer 2: {Report}              # Waits for Validate + LoadTest     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Execution:                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Time ────────────────────────────────────────────────────────────► │    │
│  │                                                                      │    │
│  │  Layer 0:  [Setup]──────►                                           │    │
│  │            [LoadTest]────────────►                                  │    │
│  │                                                                      │    │
│  │  Layer 1:                         [Validate]──────►                 │    │
│  │                                   (receives LoadTest context)       │    │
│  │                                                                      │    │
│  │  Layer 2:                                          [Report]──────►  │    │
│  │                                                    (receives both)  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 5: Context Management

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CONTEXT FLOW                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Context Structure:                                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  class Context:                                                      │    │
│  │      _context: Dict[str, WorkflowContext]                           │    │
│  │      # workflow_name → {key: value, ...}                            │    │
│  │                                                                      │    │
│  │  class WorkflowContext:                                              │    │
│  │      _values: Dict[str, Tuple[Any, int]]  # key → (value, timestamp)│    │
│  │                                                                      │    │
│  │  # Timestamps ensure LWW (Last-Write-Wins) for conflict resolution  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Context Hooks (Using @context() decorator):                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  class LoadTestWorkflow(Workflow):                                  │    │
│  │                                                                      │    │
│  │      @context()                                                      │    │
│  │      async def provide_results(self) -> Provide[Dict]:              │    │
│  │          # StateAction.PROVIDE - writes to context                  │    │
│  │          return {"total_requests": 10000, "success_rate": 0.99}     │    │
│  │                                                                      │    │
│  │  class ValidateWorkflow(Workflow):                                  │    │
│  │                                                                      │    │
│  │      @context(workflows=["LoadTestWorkflow"])                       │    │
│  │      async def use_results(self, *, data: Dict) -> Use[bool]:       │    │
│  │          # StateAction.USE - reads from specified workflow context  │    │
│  │          return data["success_rate"] > 0.95                         │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Flow:                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  Worker 1                Manager                Worker 2             │    │
│  │     │                       │                      │                 │    │
│  │     │ Run LoadTest          │                      │                 │    │
│  │     │                       │                      │                 │    │
│  │     │ ① Workflow completes  │                      │                 │    │
│  │     │    context updated    │                      │                 │    │
│  │     │                       │                      │                 │    │
│  │     │ ② WorkflowProgress    │                      │                 │    │
│  │     │   + context_updates   │                      │                 │    │
│  │     │──────────────────────►│                      │                 │    │
│  │     │                       │                      │                 │    │
│  │     │                       │ ③ Store context      │                 │    │
│  │     │                       │    Sync to peers     │                 │    │
│  │     │                       │────────────────────► │                 │    │
│  │     │                       │    (ContextUpdate)   │                 │    │
│  │     │                       │                      │                 │    │
│  │     │                       │ ④ Dispatch Validate  │                 │    │
│  │     │                       │    + LoadTest context│                 │    │
│  │     │                       │─────────────────────►│                 │    │
│  │     │                       │                      │                 │    │
│  │     │                       │                      │ ⑤ Validate runs │    │
│  │     │                       │                      │    uses context │    │
│  │     │                       │                      │                 │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Manager State for Workflow Execution

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MANAGER WORKFLOW EXECUTION STATE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  class ManagerServer:                                                        │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ # Core tracking                                                      │    │
│  │ _jobs: Dict[str, JobProgress]                                       │    │
│  │   # job_id → aggregated progress                                    │    │
│  │                                                                      │    │
│  │ _workflow_assignments: Dict[str, str]                               │    │
│  │   # workflow_id → worker_node_id                                    │    │
│  │                                                                      │    │
│  │ _workflow_retries: Dict[str, Tuple[int, bytes, set[str]]]          │    │
│  │   # workflow_id → (retry_count, original_dispatch, failed_workers)  │    │
│  │   # NOTE: Only for WORKER FAILURE (SWIM dead), NOT workflow errors  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ # NEW: Provisioning state                                            │    │
│  │                                                                      │    │
│  │ _provisioner: Provisioner                                           │    │
│  │   # Thread allocation calculator (uses TOTAL pool)                  │    │
│  │                                                                      │    │
│  │ _total_pool_size: int                                               │    │
│  │   # Sum of total_cores from all registered workers (cached)        │    │
│  │   # Updated on worker registration/death                            │    │
│  │                                                                      │    │
│  │ _job_workflow_configs: Dict[str, Dict[str, WorkflowConfig]]        │    │
│  │   # job_id → {workflow_name: config}                                │    │
│  │   # Config: {priority, is_test, threads, vus_per_thread}           │    │
│  │                                                                      │    │
│  │ _job_dependency_graphs: Dict[str, List[Dict[str, Workflow]]]       │    │
│  │   # job_id → execution layers (BFS traversal order)                 │    │
│  │                                                                      │    │
│  │ _job_current_layer: Dict[str, int]                                  │    │
│  │   # job_id → current executing layer index                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ # NEW: Context state                                                 │    │
│  │                                                                      │    │
│  │ _job_contexts: Dict[str, Context]                                   │    │
│  │   # job_id → Context object (shared across workflows in job)       │    │
│  │                                                                      │    │
│  │ _context_clock: Dict[str, Dict[str, int]]                           │    │
│  │   # job_id → {workflow_name: lamport_timestamp}                     │    │
│  │   # For conflict resolution in context updates                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Complete Job Execution State Machine

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    JOB EXECUTION STATE MACHINE (Manager)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                           ┌──────────────────┐                               │
│                           │    SUBMITTED     │                               │
│                           │                  │                               │
│                           │ • Receive job    │                               │
│                           │ • Parse workflows│                               │
│                           └────────┬─────────┘                               │
│                                    │                                         │
│                         classify & provision                                 │
│                                    │                                         │
│                                    ▼                                         │
│                           ┌──────────────────┐                               │
│                           │   CLASSIFYING    │                               │
│                           │                  │                               │
│                           │ • Detect is_test │                               │
│                           │ • Build dep graph│                               │
│                           │ • BFS traversal  │                               │
│                           └────────┬─────────┘                               │
│                                    │                                         │
│                                    ▼                                         │
│                           ┌──────────────────┐                               │
│                           │   PROVISIONING   │                               │
│                           │                  │                               │
│                           │ • Calc threads   │                               │
│                           │   from TOTAL pool│                               │
│                           │ • Calc VUs/thread│                               │
│                           └────────┬─────────┘                               │
│                                    │                                         │
│                   ┌────────────────┼────────────────┐                        │
│                   │                │                │                        │
│                   ▼                ▼                ▼                        │
│          ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│          │   QUEUED     │  │  DISPATCHING │  │   FAILED     │               │
│          │              │  │              │  │              │               │
│          │ Insufficient │  │ Capacity OK  │  │ No workers   │               │
│          │ capacity     │  │ • Quorum req │  │ available    │               │
│          └──────┬───────┘  │ • Dispatch   │  └──────────────┘               │
│                 │          └──────┬───────┘                                  │
│                 │                 │                                          │
│        capacity available         │                                          │
│                 │                 ▼                                          │
│                 └────────► ┌──────────────────┐                              │
│                            │     RUNNING      │                              │
│                            │                  │                              │
│                            │ • Per-layer exec │                              │
│                            │ • Context sync   │                              │
│                            │ • Progress track │                              │
│                            └────────┬─────────┘                              │
│                                     │                                        │
│                   ┌─────────────────┼─────────────────┐                      │
│                   │                 │                 │                      │
│                   ▼                 ▼                 ▼                      │
│          ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│          │  COMPLETING  │  │   FAILED     │  │  CANCELLED   │               │
│          │              │  │              │  │              │               │
│          │ All layers   │  │ Workflow     │  │ User cancel  │               │
│          │ complete     │  │ error        │  │              │               │
│          └──────┬───────┘  └──────────────┘  └──────────────┘               │
│                 │                                                            │
│                 ▼                                                            │
│          ┌──────────────┐                                                    │
│          │  COMPLETED   │                                                    │
│          │              │                                                    │
│          │ Success!     │                                                    │
│          │ Results ready│                                                    │
│          └──────────────┘                                                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Layer-Based Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LAYER-BASED EXECUTION FLOW                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Manager executes dependency layers sequentially, workflows within          │
│  each layer in parallel:                                                     │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  async def _execute_job(self, job_id: str):                         │    │
│  │      layers = self._job_dependency_graphs[job_id]                   │    │
│  │      context = self._job_contexts[job_id]                           │    │
│  │                                                                      │    │
│  │      for layer_idx, layer_workflows in enumerate(layers):           │    │
│  │          self._job_current_layer[job_id] = layer_idx                │    │
│  │                                                                      │    │
│  │          # Dispatch all workflows in layer (parallel)               │    │
│  │          dispatch_tasks = []                                         │    │
│  │          for workflow_name, workflow in layer_workflows.items():    │    │
│  │              # Get predecessor context for dependent workflows      │    │
│  │              dep_context = self._get_dependency_context(            │    │
│  │                  job_id, workflow                                    │    │
│  │              )                                                       │    │
│  │                                                                      │    │
│  │              # Dispatch with VUs from provisioning                  │    │
│  │              config = self._job_workflow_configs[job_id][name]     │    │
│  │              dispatch_tasks.append(                                  │    │
│  │                  self._dispatch_workflow(                            │    │
│  │                      job_id, workflow, config, dep_context          │    │
│  │                  )                                                   │    │
│  │              )                                                       │    │
│  │                                                                      │    │
│  │          # Wait for all workflows in layer to complete              │    │
│  │          await asyncio.gather(*dispatch_tasks)                      │    │
│  │                                                                      │    │
│  │          # Sync context updates from completed workflows            │    │
│  │          await self._sync_layer_context(job_id, layer_idx)          │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Example Timeline (3 workers, 24 cores total):                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Time ────────────────────────────────────────────────────────────► │    │
│  │                                                                      │    │
│  │  Layer 0:                                                            │    │
│  │  ┌────────────────────────────────────────────────────┐              │    │
│  │  │ Setup (1 thread, non-test) ─────►                  │              │    │
│  │  │ LoadTest (18 threads, HIGH, 333 VUs/thread) ──────────────►│     │    │
│  │  │ Analytics (6 threads, LOW, 166 VUs/thread) ───────────────►│     │    │
│  │  └────────────────────────────────────────────────────┘              │    │
│  │                                     ↓ context synced                 │    │
│  │  Layer 1:                                                            │    │
│  │  ┌──────────────────────────────────────────────────────────────┐   │    │
│  │  │ Validate (6 threads, receives LoadTest context) ──────►      │   │    │
│  │  └──────────────────────────────────────────────────────────────┘   │    │
│  │                                     ↓ context synced                 │    │
│  │  Layer 2:                                                            │    │
│  │  ┌──────────────────────────────────────────────────────────────┐   │    │
│  │  │ Report (1 thread, receives all context) ─────►               │   │    │
│  │  └──────────────────────────────────────────────────────────────┘   │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Cross-Manager Context Synchronization

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CROSS-MANAGER CONTEXT SYNC                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  When a workflow completes, its context updates must be synchronized        │
│  to peer managers for fault tolerance:                                       │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  ContextUpdate (new message type):                                  │    │
│  │  ┌────────────────────────────────────────────────────────────────┐ │    │
│  │  │  job_id: str                                                    │ │    │
│  │  │  workflow_name: str                                             │ │    │
│  │  │  context_values: Dict[str, Tuple[Any, int]]  # key→(val, ts)  │ │    │
│  │  │  source_manager: str                                            │ │    │
│  │  │  lamport_clock: int                                             │ │    │
│  │  └────────────────────────────────────────────────────────────────┘ │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Sync Flow:                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  Manager 1 (Leader)        Manager 2            Manager 3           │    │
│  │       │                        │                    │               │    │
│  │       │ Workflow completes     │                    │               │    │
│  │       │ with context update    │                    │               │    │
│  │       │                        │                    │               │    │
│  │       │ ① Update local context │                    │               │    │
│  │       │    with timestamp      │                    │               │    │
│  │       │                        │                    │               │    │
│  │       │ ② ContextUpdate        │                    │               │    │
│  │       │───────────────────────►│                    │               │    │
│  │       │                        │                    │               │    │
│  │       │ ② ContextUpdate        │                    │               │    │
│  │       │────────────────────────────────────────────►│               │    │
│  │       │                        │                    │               │    │
│  │       │                        │ ③ Apply if ts >    │               │    │
│  │       │                        │    current ts      │               │    │
│  │       │                        │                    │ ③ Apply if    │    │
│  │       │                        │                    │    ts > curr  │    │
│  │       │                        │                    │               │    │
│  │                                                                      │    │
│  │  Conflict Resolution: Last-Write-Wins (LWW) using Lamport timestamps│    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Handler in Manager:                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  @tcp.receive()                                                      │    │
│  │  async def context_update(self, addr, data, clock_time):            │    │
│  │      update = ContextUpdate.load(data)                              │    │
│  │                                                                      │    │
│  │      # Only apply if newer than our current context                 │    │
│  │      current_ts = self._context_clock.get(                          │    │
│  │          update.job_id, {}                                          │    │
│  │      ).get(update.workflow_name, 0)                                 │    │
│  │                                                                      │    │
│  │      if update.lamport_clock > current_ts:                          │    │
│  │          context = self._job_contexts[update.job_id]                │    │
│  │          for key, (value, ts) in update.context_values.items():     │    │
│  │              await context.update(                                   │    │
│  │                  update.workflow_name, key, value, timestamp=ts     │    │
│  │              )                                                       │    │
│  │          self._context_clock[update.job_id][update.workflow_name] = │    │
│  │              update.lamport_clock                                    │    │
│  │                                                                      │    │
│  │      return b'ok'                                                    │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Implementation Order

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    IMPLEMENTATION ORDER                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Phase 1: Workflow Classification & Provisioning                            │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 1.1 Add _classify_workflow() method to detect test workflows        │    │
│  │     • Inspect hooks for HookType.TEST                               │    │
│  │     • Return bool indicating is_test                                │    │
│  │                                                                      │    │
│  │ 1.2 Add _calculate_total_pool_size() method                         │    │
│  │     • Sum total_cores from all registered workers                   │    │
│  │     • Cache in _total_pool_size, update on worker changes           │    │
│  │                                                                      │    │
│  │ 1.3 Add _provision_workflows() method                               │    │
│  │     • Create configs with is_test, priority                         │    │
│  │     • Call Provisioner.partion_by_priority(configs)                 │    │
│  │     • Calculate VUs per thread for each workflow                    │    │
│  │     • Store in _job_workflow_configs                                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Phase 2: Dependency Graph & Execution Order                                │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 2.1 Add _build_dependency_graph() method                            │    │
│  │     • Parse DependentWorkflow relationships                         │    │
│  │     • Build networkx.DiGraph                                        │    │
│  │     • BFS traversal to get execution layers                         │    │
│  │     • Store in _job_dependency_graphs                               │    │
│  │                                                                      │    │
│  │ 2.2 Update job_submission handler                                   │    │
│  │     • Classify workflows                                            │    │
│  │     • Build dependency graph                                        │    │
│  │     • Provision threads and VUs                                     │    │
│  │     • Check capacity before accepting                               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Phase 3: Context Management                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 3.1 Add ContextUpdate message type                                  │    │
│  │     • job_id, workflow_name, context_values, lamport_clock          │    │
│  │                                                                      │    │
│  │ 3.2 Add context_update handler                                      │    │
│  │     • Receive from peer managers                                    │    │
│  │     • Apply with LWW conflict resolution                            │    │
│  │                                                                      │    │
│  │ 3.3 Update workflow_progress handler                                │    │
│  │     • Extract context updates from WorkflowProgress                 │    │
│  │     • Store in _job_contexts                                        │    │
│  │     • Broadcast to peer managers                                    │    │
│  │                                                                      │    │
│  │ 3.4 Update WorkflowDispatch to include dep context                  │    │
│  │     • Serialize relevant context for dependent workflows            │    │
│  │     • Worker deserializes and uses in execution                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Phase 4: Layer-Based Execution                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 4.1 Add _execute_job_layer() method                                 │    │
│  │     • Dispatch all workflows in current layer                       │    │
│  │     • Wait for layer completion                                     │    │
│  │     • Sync context before next layer                                │    │
│  │                                                                      │    │
│  │ 4.2 Add _advance_to_next_layer() method                             │    │
│  │     • Check all layer workflows complete                            │    │
│  │     • Increment _job_current_layer                                  │    │
│  │     • Dispatch next layer if exists                                 │    │
│  │                                                                      │    │
│  │ 4.3 Update workflow completion handling                             │    │
│  │     • Track per-layer completion                                    │    │
│  │     • Trigger next layer when current completes                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Phase 5: Worker Integration                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 5.1 Update WorkflowDispatch message                                 │    │
│  │     • Add dependency_context field (serialized Context)            │    │
│  │     • Add vus_per_thread field (calculated VUs)                    │    │
│  │                                                                      │    │
│  │ 5.2 Update WorkflowProgress message                                 │    │
│  │     • Add context_updates field (for Provide hooks)                │    │
│  │     • Include Lamport timestamps                                    │    │
│  │                                                                      │    │
│  │ 5.3 Worker uses context in execution                                │    │
│  │     • Deserialize dependency context                                │    │
│  │     • Make available to Use hooks                                   │    │
│  │     • Serialize Provide hook results                                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Final Results Flow

This section documents how workflow results, context, and errors flow back through the system after execution completes.

### Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      FINAL RESULTS FLOW OVERVIEW                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Worker                  Manager                  Gate                Client │
│    │                        │                       │                    │   │
│    │ Execute workflow       │                       │                    │   │
│    │                        │                       │                    │   │
│    │ ① WorkflowFinalResult  │                       │                    │   │
│    │   (results, context,   │                       │                    │   │
│    │    error)              │                       │                    │   │
│    │───────────────────────►│                       │                    │   │
│    │                        │                       │                    │   │
│    │                        │ Store context         │                    │   │
│    │                        │ Sync to peers         │                    │   │
│    │                        │ Advance layers        │                    │   │
│    │                        │                       │                    │   │
│    │                        │ ② JobFinalResult      │                    │   │
│    │                        │   (per-DC results)    │                    │   │
│    │                        │──────────────────────►│                    │   │
│    │                        │                       │                    │   │
│    │                        │       OR (no gates)   │                    │   │
│    │                        │───────────────────────────────────────────►│   │
│    │                        │                       │                    │   │
│    │                        │                       │ ③ GlobalJobResult  │   │
│    │                        │                       │   (aggregated +    │   │
│    │                        │                       │    per-DC results) │   │
│    │                        │                       │───────────────────►│   │
│    │                        │                       │                    │   │
│                                                                              │
│  Key Principle: Workflow is NOT complete until final result is received     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Message Types

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      FINAL RESULT MESSAGE TYPES                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ WorkflowFinalResult (Worker → Manager)                                 │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  @dataclass                                                             │ │
│  │  class WorkflowFinalResult(Message):                                   │ │
│  │      job_id: str                                                        │ │
│  │      workflow_id: str                                                   │ │
│  │      status: str              # COMPLETED | FAILED                      │ │
│  │      results: bytes           # Cloudpickled WorkflowStats             │ │
│  │      context_updates: bytes   # Cloudpickled context dict              │ │
│  │      error: str | None = None # Error message (no traceback)           │ │
│  │                                                                         │ │
│  │  Note: WorkflowStats already contains:                                 │ │
│  │    • run_id: int              # Execution instance ID                  │ │
│  │    • elapsed: float           # Execution time                         │ │
│  │    • results: List[ResultSet] # Per-step results with stats           │ │
│  │    • metrics: List[MetricsSet]                                         │ │
│  │    • checks: List[CheckSet]                                            │ │
│  │    • aps: float               # Actions per second                     │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ JobFinalResult (Manager → Gate OR Client)                              │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  @dataclass                                                             │ │
│  │  class JobFinalResult(Message):                                        │ │
│  │      job_id: str                                                        │ │
│  │      datacenter: str                                                    │ │
│  │      status: str              # COMPLETED | FAILED | PARTIAL           │ │
│  │      workflow_results: list[WorkflowResult]  # Per-workflow results    │ │
│  │      total_completed: int     # Total successful actions               │ │
│  │      total_failed: int        # Total failed actions                   │ │
│  │      errors: list[str]        # All error messages                     │ │
│  │      elapsed_seconds: float   # Max elapsed across workflows           │ │
│  │                                                                         │ │
│  │  @dataclass                                                             │ │
│  │  class WorkflowResult(Message):                                        │ │
│  │      workflow_id: str                                                   │ │
│  │      workflow_name: str                                                 │ │
│  │      status: str              # COMPLETED | FAILED                      │ │
│  │      results: bytes           # Cloudpickled WorkflowStats             │ │
│  │      error: str | None                                                  │ │
│  │                                                                         │ │
│  │  Note: Context is NOT included - gates don't need it                   │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ GlobalJobResult (Gate → Client)                                        │ │
│  ├────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  @dataclass                                                             │ │
│  │  class GlobalJobResult(Message):                                       │ │
│  │      job_id: str                                                        │ │
│  │      status: str              # COMPLETED | FAILED | PARTIAL           │ │
│  │                                                                         │ │
│  │      # Per-datacenter breakdown                                         │ │
│  │      per_datacenter_results: list[JobFinalResult]                      │ │
│  │                                                                         │ │
│  │      # Cross-DC aggregated stats                                        │ │
│  │      aggregated: AggregatedJobStats                                    │ │
│  │                                                                         │ │
│  │      # Summary                                                          │ │
│  │      total_completed: int     # Sum across all DCs                     │ │
│  │      total_failed: int        # Sum across all DCs                     │ │
│  │      successful_datacenters: int                                        │ │
│  │      failed_datacenters: int                                            │ │
│  │      errors: list[str]        # All errors from all DCs                │ │
│  │      elapsed_seconds: float   # Max elapsed across all DCs             │ │
│  │                                                                         │ │
│  │  @dataclass                                                             │ │
│  │  class AggregatedJobStats(Message):                                    │ │
│  │      total_requests: int                                                │ │
│  │      successful_requests: int                                           │ │
│  │      failed_requests: int                                               │ │
│  │      overall_rate: float      # Combined rate (requests/sec)           │ │
│  │      avg_latency_ms: float                                              │ │
│  │      p50_latency_ms: float                                              │ │
│  │      p95_latency_ms: float                                              │ │
│  │      p99_latency_ms: float                                              │ │
│  │                                                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 1: Worker Sends Final Result

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    WORKER FINAL RESULT FLOW                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Workflow execution completes:                                               │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  (results_run_id, results, context, error, status) =                │    │
│  │      await self._remote_manger.execute_workflow(...)                │    │
│  │                                                                      │    │
│  │  # results: WorkflowStats (has run_id, elapsed, step stats, etc.)  │    │
│  │  # context: Context (updated by Provide hooks)                      │    │
│  │  # error: Exception | None                                          │    │
│  │  # status: CoreWorkflowStatus                                       │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Worker sends final result:                                                  │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  final_result = WorkflowFinalResult(                                │    │
│  │      job_id=dispatch.job_id,                                        │    │
│  │      workflow_id=dispatch.workflow_id,                              │    │
│  │      status=WorkflowStatus.COMPLETED.value if not error             │    │
│  │             else WorkflowStatus.FAILED.value,                       │    │
│  │      results=cloudpickle.dumps(results),  # WorkflowStats           │    │
│  │      context_updates=cloudpickle.dumps(                             │    │
│  │          context.dict() if context else {}                          │    │
│  │      ),                                                              │    │
│  │      error=str(error) if error else None,                           │    │
│  │  )                                                                   │    │
│  │                                                                      │    │
│  │  await self._send_final_result(final_result)                        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Core freeing (always in finally block):                                    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  finally:                                                            │    │
│  │      self._free_cores(dispatch.workflow_id)  # ← ALWAYS called      │    │
│  │      self._increment_version()                                       │    │
│  │      # ... cleanup tracking dicts                                    │    │
│  │                                                                      │    │
│  │  Cores freed on:                                                     │    │
│  │    ✓ COMPLETED (success)                                            │    │
│  │    ✓ FAILED (error)                                                 │    │
│  │    ✓ CANCELLED (user cancel)                                        │    │
│  │    ✓ Any exception                                                  │    │
│  │                                                                      │    │
│  │  Note: Cores freed AFTER sending final result but REGARDLESS of     │    │
│  │        whether send succeeded. This prevents core leaks.            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 2: Manager Processes Final Result

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MANAGER FINAL RESULT PROCESSING                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  @tcp.receive()                                                      │    │
│  │  async def workflow_final_result(self, addr, data, clock_time):     │    │
│  │      result = WorkflowFinalResult.load(data)                        │    │
│  │                                                                      │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      # 1. Handle error case (NO RETRY - just mark as failed)       │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      if result.error:                                                │    │
│  │          # Mark workflow as FAILED immediately - no retry           │    │
│  │          self._workflow_final_results[result.workflow_id] = result  │    │
│  │          if self._is_job_complete(result.job_id):                   │    │
│  │              await self._send_job_final_result(result.job_id)       │    │
│  │          return                                                      │    │
│  │                                                                      │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      # 2. Store context for dependent workflows                     │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      context_updates = cloudpickle.loads(result.context_updates)    │    │
│  │      job_context = self._job_contexts[result.job_id]                │    │
│  │      workflow_name = self._get_workflow_name(result.workflow_id)    │    │
│  │                                                                      │    │
│  │      for key, value in context_updates.items():                     │    │
│  │          await job_context.update(workflow_name, key, value)        │    │
│  │                                                                      │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      # 3. Sync context to peer managers                             │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      await self._broadcast_context_update(                          │    │
│  │          result.job_id, workflow_name, context_updates              │    │
│  │      )                                                               │    │
│  │                                                                      │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      # 4. Store final result                                        │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      self._workflow_final_results[result.workflow_id] = result      │    │
│  │                                                                      │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      # 5. Check layer completion → advance                          │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      if self._is_layer_complete(result.job_id):                     │    │
│  │          await self._advance_to_next_layer(result.job_id)           │    │
│  │                                                                      │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      # 6. Check job completion → send final result                  │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      if self._is_job_complete(result.job_id):                       │    │
│  │          await self._send_job_final_result(result.job_id)           │    │
│  │                                                                      │    │
│  │      return b'ok'                                                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Key Principle:                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  Workflow is NOT complete until:                                    │    │
│  │    1. Worker sends WorkflowFinalResult                              │    │
│  │    2. Manager receives and processes it                             │    │
│  │    3. Manager stores in _workflow_final_results                     │    │
│  │                                                                      │    │
│  │  Progress updates (WorkflowProgress) are for monitoring only.       │    │
│  │  Final result is required for job completion.                       │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 3: Manager Sends Job Result

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MANAGER SENDS JOB FINAL RESULT                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  When all workflows in a job complete (or fail):                            │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  async def _send_job_final_result(self, job_id: str):               │    │
│  │      # Gather all workflow results                                   │    │
│  │      workflow_results = []                                           │    │
│  │      total_completed = 0                                             │    │
│  │      total_failed = 0                                                │    │
│  │      errors = []                                                     │    │
│  │      max_elapsed = 0.0                                               │    │
│  │                                                                      │    │
│  │      for wf_id, wf_result in self._workflow_final_results.items():  │    │
│  │          if wf_result.job_id != job_id:                             │    │
│  │              continue                                                │    │
│  │                                                                      │    │
│  │          stats = cloudpickle.loads(wf_result.results)               │    │
│  │          workflow_results.append(WorkflowResult(                    │    │
│  │              workflow_id=wf_id,                                      │    │
│  │              workflow_name=stats.get("workflow", ""),               │    │
│  │              status=wf_result.status,                                │    │
│  │              results=wf_result.results,  # Keep pickled             │    │
│  │              error=wf_result.error,                                  │    │
│  │          ))                                                          │    │
│  │                                                                      │    │
│  │          total_completed += stats.get("stats", {}).get("succeeded")│    │
│  │          total_failed += stats.get("stats", {}).get("failed", 0)   │    │
│  │          max_elapsed = max(max_elapsed, stats.get("elapsed", 0))   │    │
│  │          if wf_result.error:                                         │    │
│  │              errors.append(wf_result.error)                          │    │
│  │                                                                      │    │
│  │      # Determine job status                                          │    │
│  │      if all(r.status == "completed" for r in workflow_results):    │    │
│  │          status = "completed"                                        │    │
│  │      elif all(r.status == "failed" for r in workflow_results):     │    │
│  │          status = "failed"                                           │    │
│  │      else:                                                           │    │
│  │          status = "partial"                                          │    │
│  │                                                                      │    │
│  │      job_result = JobFinalResult(                                   │    │
│  │          job_id=job_id,                                              │    │
│  │          datacenter=self._node_id.datacenter,                        │    │
│  │          status=status,                                              │    │
│  │          workflow_results=workflow_results,                          │    │
│  │          total_completed=total_completed,                            │    │
│  │          total_failed=total_failed,                                  │    │
│  │          errors=errors,                                              │    │
│  │          elapsed_seconds=max_elapsed,                                │    │
│  │      )                                                               │    │
│  │                                                                      │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      # Send to Gate OR Client                                       │    │
│  │      # ─────────────────────────────────────────────────────────    │    │
│  │      if self._known_gates:                                           │    │
│  │          await self._send_to_primary_gate(job_result)               │    │
│  │      else:                                                           │    │
│  │          # Direct client mode                                        │    │
│  │          callback = self._job_callbacks.get(job_id)                 │    │
│  │          if callback:                                                │    │
│  │              await self._send_to_client(callback, job_result)       │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Note: Context is NOT included in JobFinalResult                            │
│  Gates do not need context - it's internal to manager execution             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step 4: Gate Aggregates and Sends to Client

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GATE CROSS-DC AGGREGATION                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Gate receives JobFinalResult from each datacenter:                         │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  @tcp.receive()                                                      │    │
│  │  async def job_final_result(self, addr, data, clock_time):          │    │
│  │      result = JobFinalResult.load(data)                             │    │
│  │                                                                      │    │
│  │      # Store per-DC result                                          │    │
│  │      self._dc_final_results[result.job_id][result.datacenter] = result│   │
│  │                                                                      │    │
│  │      # Check if all DCs complete                                    │    │
│  │      if self._all_datacenters_complete(result.job_id):              │    │
│  │          await self._send_global_result_to_client(result.job_id)    │    │
│  │                                                                      │    │
│  │      return b'ok'                                                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Aggregation logic:                                                          │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  async def _send_global_result_to_client(self, job_id: str):        │    │
│  │      dc_results = self._dc_final_results[job_id]                    │    │
│  │                                                                      │    │
│  │      # Aggregate stats across DCs                                   │    │
│  │      total_completed = sum(r.total_completed for r in dc_results)   │    │
│  │      total_failed = sum(r.total_failed for r in dc_results)         │    │
│  │      all_errors = [e for r in dc_results for e in r.errors]        │    │
│  │      max_elapsed = max(r.elapsed_seconds for r in dc_results)       │    │
│  │                                                                      │    │
│  │      successful_dcs = sum(1 for r in dc_results if r.status == "completed")│
│  │      failed_dcs = sum(1 for r in dc_results if r.status == "failed")│    │
│  │                                                                      │    │
│  │      # Determine global status                                       │    │
│  │      if failed_dcs == len(dc_results):                              │    │
│  │          status = "failed"                                           │    │
│  │      elif successful_dcs == len(dc_results):                        │    │
│  │          status = "completed"                                        │    │
│  │      else:                                                           │    │
│  │          status = "partial"                                          │    │
│  │                                                                      │    │
│  │      # Build aggregated stats                                        │    │
│  │      aggregated = self._compute_aggregated_stats(dc_results)        │    │
│  │                                                                      │    │
│  │      global_result = GlobalJobResult(                               │    │
│  │          job_id=job_id,                                              │    │
│  │          status=status,                                              │    │
│  │          per_datacenter_results=list(dc_results.values()),          │    │
│  │          aggregated=aggregated,                                      │    │
│  │          total_completed=total_completed,                            │    │
│  │          total_failed=total_failed,                                  │    │
│  │          successful_datacenters=successful_dcs,                      │    │
│  │          failed_datacenters=failed_dcs,                              │    │
│  │          errors=all_errors,                                          │    │
│  │          elapsed_seconds=max_elapsed,                                │    │
│  │      )                                                               │    │
│  │                                                                      │    │
│  │      callback = self._job_callbacks.get(job_id)                     │    │
│  │      if callback:                                                    │    │
│  │          await self._send_to_client(callback, global_result)        │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Client receives:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  GlobalJobResult:                                                    │    │
│  │  ├── status: "completed" | "failed" | "partial"                     │    │
│  │  ├── per_datacenter_results: [                                      │    │
│  │  │     JobFinalResult(datacenter="us-east-1", ...),                 │    │
│  │  │     JobFinalResult(datacenter="eu-west-1", ...),                 │    │
│  │  │   ]                                                               │    │
│  │  ├── aggregated: AggregatedJobStats(                                │    │
│  │  │     total_requests=50000,                                        │    │
│  │  │     successful_requests=49500,                                   │    │
│  │  │     overall_rate=5000.0,  # Combined across DCs                 │    │
│  │  │     avg_latency_ms=45.2,                                         │    │
│  │  │     p99_latency_ms=210.5,                                        │    │
│  │  │   )                                                               │    │
│  │  ├── errors: ["Workflow X failed: connection timeout", ...]        │    │
│  │  └── elapsed_seconds: 10.5                                          │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Error Handling Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ERROR HANDLING FLOW                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  Worker: Workflow fails with error                                  │    │
│  │           │                                                          │    │
│  │           ▼                                                          │    │
│  │  WorkflowFinalResult(status="failed", error="...", results=...)    │    │
│  │           │                                                          │    │
│  │           ▼                                                          │    │
│  │  ─────────────────────────────────────────────────────────────────  │    │
│  │                                                                      │    │
│  │  Manager: Receives error result                                     │    │
│  │           │                                                          │    │
│  │           │  NO RETRY on workflow errors:                           │    │
│  │           │                                                          │    │
│  │           ├─► Mark workflow as FAILED immediately                   │    │
│  │           ├─► Store error in _workflow_final_results                │    │
│  │           └─► Check job completion                                  │    │
│  │               │                                                      │    │
│  │               ▼                                                      │    │
│  │  ─────────────────────────────────────────────────────────────────  │    │
│  │                                                                      │    │
│  │  Job complete with errors:                                          │    │
│  │           │                                                          │    │
│  │           ├───► Gates present?                                      │    │
│  │           │         │                                                │    │
│  │           │    YES: │                                                │    │
│  │           │         └─► Send JobFinalResult(status="failed"|"partial")│   │
│  │           │                to Gate                                   │    │
│  │           │                   │                                      │    │
│  │           │                   ▼                                      │    │
│  │           │             Gate aggregates, sends GlobalJobResult      │    │
│  │           │             to Client with error details                │    │
│  │           │                                                          │    │
│  │           │    NO (direct client mode):                             │    │
│  │           │         └─► Send JobFinalResult(status="failed"|"partial")│   │
│  │           │                directly to Client                       │    │
│  │           │                                                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Status Definitions:                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  COMPLETED: All workflows in all DCs succeeded                      │    │
│  │  FAILED:    All workflows in ALL DCs failed (no usable results)    │    │
│  │  PARTIAL:   Some workflows/DCs succeeded, some failed              │    │
│  │             (partial results available)                             │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Important Distinction - Error vs Worker Failure:                            │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                      │    │
│  │  WORKFLOW ERROR (workflow returns error result):                    │    │
│  │    • NO RETRY - error is final                                      │    │
│  │    • Workflow marked FAILED immediately                             │    │
│  │    • Error included in final result to client                       │    │
│  │                                                                      │    │
│  │  WORKER FAILURE (SWIM detects worker is DEAD):                      │    │
│  │    • Retry workflow on different worker (see Worker Failure section)│    │
│  │    • Worker excluded from future dispatch for this workflow        │    │
│  │    • If max retries exhausted, then mark FAILED                    │    │
│  │                                                                      │    │
│  │  Rationale:                                                          │    │
│  │    • Worker failure = work never completed (worker crashed)         │    │
│  │    • Workflow error = work completed with error (retrying futile)  │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Complete Final Results State Machine

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    FINAL RESULTS STATE MACHINE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                        ┌──────────────────┐                                  │
│                        │    DISPATCHED    │                                  │
│                        │                  │                                  │
│                        │ Workflow sent to │                                  │
│                        │ worker           │                                  │
│                        └────────┬─────────┘                                  │
│                                 │                                            │
│                     worker executes                                          │
│                     sends WorkflowFinalResult                                │
│                                 │                                            │
│              ┌──────────────────┼──────────────────┐                         │
│              │                  │                  │                         │
│              ▼                  ▼                  ▼                         │
│    ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                   │
│    │ RESULT_OK    │   │ RESULT_ERROR │   │ NO_RESULT    │                   │
│    │              │   │              │   │ (timeout)    │                   │
│    │ • Store      │   │ • NO RETRY   │   │              │                   │
│    │   results    │   │ • Mark as    │   │ • Treat as   │                   │
│    │ • Store      │   │   FAILED     │   │   failure    │                   │
│    │   context    │   │ • Store error│   │              │                   │
│    └──────┬───────┘   └──────┬───────┘   └──────┬───────┘                   │
│           │                  │                  │                           │
│           │                  │                  │                           │
│           ▼                  ▼                  ▼                           │
│    ┌─────────────────────────────────────────────────────────────────┐      │
│    │                    WORKFLOW COMPLETE                             │      │
│    │                                                                  │      │
│    │  Workflow is marked complete when:                              │      │
│    │  • WorkflowFinalResult received with status=COMPLETED           │      │
│    │  • OR WorkflowFinalResult received with status=FAILED           │      │
│    │  • OR timeout waiting for result (treated as FAILED)            │      │
│    │                                                                  │      │
│    │  NO RETRY on workflow errors - errors are final.                │      │
│    │                                                                  │      │
│    │  Cores freed: In worker's finally block (always)               │      │
│    └─────────────────────────────────────────────────────────────────┘      │
│                          │                                                   │
│                          │ all workflows complete                            │
│                          ▼                                                   │
│    ┌─────────────────────────────────────────────────────────────────┐      │
│    │                      JOB COMPLETE                                │      │
│    │                                                                  │      │
│    │  Manager builds JobFinalResult:                                 │      │
│    │  • Aggregates all workflow results                              │      │
│    │  • Collects all errors                                          │      │
│    │  • Determines status (completed|failed|partial)                 │      │
│    │  • Sends to Gate (or Client if no gates)                        │      │
│    └─────────────────────────────────────────────────────────────────┘      │
│                          │                                                   │
│                          │ Gate receives from all DCs                        │
│                          ▼                                                   │
│    ┌─────────────────────────────────────────────────────────────────┐      │
│    │                    GLOBAL JOB COMPLETE                           │      │
│    │                                                                  │      │
│    │  Gate builds GlobalJobResult:                                   │      │
│    │  • Per-datacenter results (detailed)                            │      │
│    │  • Cross-DC aggregated stats                                    │      │
│    │  • Combined errors list                                         │      │
│    │  • Sends to Client                                              │      │
│    └─────────────────────────────────────────────────────────────────┘      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Context Flow Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONTEXT VS RESULTS FLOW                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                          CONTEXT                                     │    │
│  │                                                                      │    │
│  │  Purpose: Share state between dependent workflows                   │    │
│  │                                                                      │    │
│  │  Flow:                                                               │    │
│  │    Worker ──context_updates──► Manager                              │    │
│  │                                   │                                  │    │
│  │                     ┌─────────────┼─────────────┐                   │    │
│  │                     ▼             ▼             ▼                   │    │
│  │               Store in     Sync to peer   Include in               │    │
│  │            _job_contexts    managers     dependent                  │    │
│  │                                          workflow                   │    │
│  │                                          dispatch                   │    │
│  │                                                                      │    │
│  │  NOT sent to: Gates, Clients                                        │    │
│  │  Gates don't need context - it's internal execution state          │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                          RESULTS                                     │    │
│  │                                                                      │    │
│  │  Purpose: Report execution stats, errors, metrics                   │    │
│  │                                                                      │    │
│  │  Flow:                                                               │    │
│  │    Worker ──WorkflowStats──► Manager                                │    │
│  │                                 │                                    │    │
│  │                     ┌───────────┴───────────┐                       │    │
│  │                     ▼                       ▼                       │    │
│  │            JobFinalResult           JobFinalResult                  │    │
│  │            (to Gate)                (to Client, no gates)           │    │
│  │                 │                                                    │    │
│  │                 ▼                                                    │    │
│  │          GlobalJobResult                                            │    │
│  │          (to Client)                                                │    │
│  │                                                                      │    │
│  │  Sent to: Gates AND Clients                                         │    │
│  │  Contains: WorkflowStats (stats, metrics, errors, timing)          │    │
│  │                                                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Context Consistency Protocol

This section details how context is synchronized across managers to ensure dependent
workflows always see the correct, latest context from their dependencies.

### Workflow Context API

Context enables workflows to share state with their dependents. This is critical for
scenarios where one workflow produces data (e.g., authentication tokens, session IDs)
that subsequent workflows need to consume.

#### Decorators and Type Hints

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       WORKFLOW CONTEXT API                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Decorators:                                                                 │
│  ────────────────────────────────────────────────────────────────────────   │
│                                                                              │
│    @state('WorkflowName', ...)                                               │
│      • Marks a method for context interaction                               │
│      • MUST specify target workflow name(s) as string arguments             │
│      • If no args provided → no context flows (nothing to select from)      │
│                                                                              │
│    @depends('WorkflowName', ...)                                             │
│      • Wraps a Workflow class to declare execution dependencies             │
│      • Dependent workflow executes AFTER all specified dependencies         │
│      • Can specify multiple dependencies as separate string arguments       │
│                                                                              │
│  Type Hints (Return Types):                                                  │
│  ────────────────────────────────────────────────────────────────────────   │
│                                                                              │
│    Provide[T]                                                                │
│      • Indicates the method PROVIDES context to specified workflow(s)       │
│      • Return value is stored in context                                    │
│      • Method name becomes the context KEY                                  │
│                                                                              │
│    Use[T]                                                                    │
│      • Indicates the method USES context from specified workflow(s)         │
│      • Keyword argument names must match context keys                       │
│      • Values are injected from context; use default for missing keys       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Complete Example

```python
from hyperscale import Workflow, depends, state, step
from hyperscale.core.hooks import Provide, Use


class AuthWorkflow(Workflow):
    """First workflow - authenticates and provides token to dependents."""
    vus = 100
    duration = "30s"

    @step()
    async def login(self, url: URL = 'https://api.example.com/login') -> HTTPResponse:
        return await self.client.http.post(url, json={"user": "test"})
    
    @state('DataWorkflow')  # ← Share WITH DataWorkflow
    def auth_token(self) -> Provide[str]:  # ← Method name = context key
        """Provides authentication token to DataWorkflow."""
        return self.login.response.json()['token']


@depends('AuthWorkflow')  # ← Wait for AuthWorkflow to complete first
class DataWorkflow(Workflow):
    """Second workflow - uses token from AuthWorkflow."""
    vus = 100
    duration = "30s"

    @state('AuthWorkflow')  # ← Receive FROM AuthWorkflow
    def get_token(self, auth_token: str | None = None) -> Use[str]:  # ← kwarg matches key
        """Receives authentication token from AuthWorkflow."""
        return auth_token  # Will be injected with the token value

    @step()
    async def fetch_data(self, url: URL = 'https://api.example.com/data') -> HTTPResponse:
        token = self.get_token()  # Access the consumed token
        return await self.client.http.get(
            url, 
            headers={"Authorization": f"Bearer {token}"}
        )
```

#### Context Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CONTEXT FLOW                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Execution Order (determined by @depends):                                   │
│                                                                              │
│  ┌─────────────────────┐                                                    │
│  │  Layer 0            │                                                    │
│  │  ─────────────────  │                                                    │
│  │  AuthWorkflow runs  │                                                    │
│  │  (no dependencies)  │                                                    │
│  └──────────┬──────────┘                                                    │
│             │                                                                │
│             │  @state('DataWorkflow')                                       │
│             │  def auth_token() -> Provide[str]:                            │
│             │      return 'eyJhbGc...'                                      │
│             │                                                                │
│             ▼                                                                │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                     CONTEXT STORAGE                                  │    │
│  │  ┌─────────────────────────────────────────────────────────────┐    │    │
│  │  │  context['AuthWorkflow']['auth_token'] = 'eyJhbGc...'       │    │    │
│  │  └─────────────────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│             │                                                                │
│             │  ┌──────────────────────────────────────────┐                 │
│             │  │  DISTRIBUTED: Quorum sync at layer      │                 │
│             │  │  boundary ensures all managers have     │                 │
│             │  │  context before Layer 1 dispatches      │                 │
│             │  └──────────────────────────────────────────┘                 │
│             │                                                                │
│             ▼                                                                │
│  ┌─────────────────────┐                                                    │
│  │  Layer 1            │                                                    │
│  │  ─────────────────  │                                                    │
│  │  DataWorkflow runs  │                                                    │
│  │  @depends('Auth')   │                                                    │
│  └──────────┬──────────┘                                                    │
│             │                                                                │
│             │  @state('AuthWorkflow')                                       │
│             │  def get_token(auth_token=None) -> Use[str]:                  │
│             │                     ▲                                         │
│             │                     │                                         │
│             │          ┌──────────┴──────────┐                              │
│             │          │  Kwarg 'auth_token' │                              │
│             │          │  matches context    │                              │
│             │          │  key 'auth_token'   │                              │
│             │          │  ─────────────────  │                              │
│             │          │  Injected value:    │                              │
│             │          │  'eyJhbGc...'       │                              │
│             │          └─────────────────────┘                              │
│             │                                                                │
│             ▼                                                                │
│       DataWorkflow.get_token() returns 'eyJhbGc...'                         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Context API Rules Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       CONTEXT API RULES                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  PROVIDER (sends context):                                                   │
│  ────────────────────────────────────────────────────────────────────────   │
│    @state('TargetWorkflow')      ← Specify WHO receives this context        │
│    def method_name(...):         ← Method name becomes context KEY          │
│        -> Provide[T]             ← Declares providing intent                │
│        return value              ← Return value is stored as context VALUE  │
│                                                                              │
│  CONSUMER (receives context):                                                │
│  ────────────────────────────────────────────────────────────────────────   │
│    @depends('SourceWorkflow')    ← Ensures source runs first (class level) │
│    @state('SourceWorkflow')      ← Specify WHO to receive FROM              │
│    def consume(                                                              │
│        kwarg_name: T | None = None  ← Kwarg name MUST match context key    │
│    ):                                                                        │
│        -> Use[T]                 ← Declares consuming intent                │
│        return kwarg_name         ← Use the injected value                   │
│                                                                              │
│  KEY MATCHING:                                                               │
│  ────────────────────────────────────────────────────────────────────────   │
│    Provider method name  ──────────────►  Consumer kwarg name               │
│    e.g., 'auth_token'    ◄─── MUST MATCH ───►  'auth_token'                 │
│                                                                              │
│  BIDIRECTIONAL CONTRACT:                                                     │
│  ────────────────────────────────────────────────────────────────────────   │
│    • Provider MUST name the target: @state('ConsumerWorkflow')              │
│    • Consumer MUST name the source: @state('ProviderWorkflow')              │
│    • Context only flows when BOTH sides agree on the relationship           │
│    • @state() with NO args = no context flows (no workflow selected)        │
│                                                                              │
│  MULTIPLE TARGETS/SOURCES:                                                   │
│  ────────────────────────────────────────────────────────────────────────   │
│    @state('WorkflowA', 'WorkflowB')  ← Share with multiple workflows        │
│    @depends('WorkflowA', 'WorkflowB') ← Depend on multiple workflows        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### The Problem

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONTEXT SYNC RACE CONDITION                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Timeline (problematic):                                                     │
│  ───────────────────────────────────────────────────────────────────────    │
│  Manager A                      Manager B                    Worker (on B)   │
│  ───────────────────────────────────────────────────────────────────────    │
│  WorkflowFinalResult                                                         │
│    (context: {auth: token123})                                              │
│      │                                                                       │
│      ├─► Store context locally                                              │
│      │                                                                       │
│      ├─► Broadcast to B ──────────► (in flight...)                          │
│      │                                                                       │
│      ├─► Advance to layer 2                                                 │
│      │                                                                       │
│      ├─► Dispatch DependentWorkflow ──────────────────────► Receives!       │
│      │   to Worker on Manager B                              But context    │
│                                        │                     hasn't arrived │
│                                        ▼                     at Manager B!  │
│                                    Receives context                          │
│                                    (too late!)                               │
│  ───────────────────────────────────────────────────────────────────────    │
│                                                                              │
│  Result: DependentWorkflow executes with STALE or MISSING context!          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Distributed Consistency Approaches Analyzed

Before choosing our approach, we analyzed how major distributed systems solve this:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              DISTRIBUTED CONSISTENCY APPROACHES COMPARISON                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. REDIS SENTINEL / REDIS CLUSTER                                           │
│  ────────────────────────────────────────────────────────────────────────   │
│  Mechanism:                                                                  │
│    • Asynchronous replication from master to replicas                       │
│    • Gossip-based cluster state                                             │
│    • Failover via Sentinel consensus                                        │
│                                                                              │
│  For Context Sync:                                                           │
│    ❌ Async replication means writes can be lost during failover            │
│    ❌ We can't afford lost context updates                                  │
│                                                                              │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  2. ETCD / RAFT CONSENSUS                                                    │
│  ────────────────────────────────────────────────────────────────────────   │
│  Mechanism:                                                                  │
│    • Strong consistency via Raft log replication                            │
│    • Every write goes through leader                                        │
│    • Leader replicates log entry to majority BEFORE acknowledging           │
│    • Committed = in majority's log                                          │
│                                                                              │
│  For Context Sync:                                                           │
│    ✅ Strong consistency - no lost writes                                   │
│    ✅ We already have leader election                                       │
│    ❌ Every context key update would need consensus (high latency)          │
│    ❌ Log grows unbounded (need compaction)                                 │
│                                                                              │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  3. COCKROACHDB / SPANNER - HYBRID LOGICAL CLOCKS (HLC)                      │
│  ────────────────────────────────────────────────────────────────────────   │
│  Mechanism:                                                                  │
│    • HLC = max(physical_time, last_hlc) + logical_counter                   │
│    • Combines wall-clock ordering with logical consistency                  │
│    • MVCC: reads at timestamp T see consistent snapshot as of T            │
│    • Spanner uses TrueTime (GPS + atomic clocks) for global ordering        │
│                                                                              │
│  For Context Sync:                                                           │
│    ✅ Global ordering without coordination                                  │
│    ✅ Physical time component aids debugging                                │
│    ✅ Snapshot reads at specific version                                    │
│    ❌ Requires reasonably synchronized clocks (NTP usually sufficient)      │
│                                                                              │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  4. CASSANDRA - TUNABLE CONSISTENCY WITH LWW                                 │
│  ────────────────────────────────────────────────────────────────────────   │
│  Mechanism:                                                                  │
│    • Write to N replicas, wait for W acknowledgments                        │
│    • Read from R replicas, return highest timestamp                         │
│    • Consistency levels: ONE, QUORUM, ALL                                   │
│    • Last-Write-Wins (LWW) with timestamps for conflict resolution          │
│                                                                              │
│  For Context Sync:                                                           │
│    ✅ Flexible consistency levels                                           │
│    ✅ Quorum writes ensure durability                                       │
│    ✅ LWW handles concurrent writes                                         │
│    ❌ Wall-clock skew can cause "wrong" winner                              │
│                                                                              │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  5. DYNAMODB / RIAK - VECTOR CLOCKS + APPLICATION RESOLUTION                 │
│  ────────────────────────────────────────────────────────────────────────   │
│  Mechanism:                                                                  │
│    • Vector clock per key tracks causal history                             │
│    • On conflict, ALL versions returned to application                      │
│    • Application decides how to merge                                       │
│    • Anti-entropy (Merkle trees) for background sync                        │
│                                                                              │
│  For Context Sync:                                                           │
│    ✅ Precise causal tracking                                               │
│    ✅ No lost updates (all kept until resolved)                             │
│    ❌ Complex: application must handle conflicts                            │
│    ❌ Vector clock size grows with writers                                  │
│                                                                              │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  6. CRDTs (CONFLICT-FREE REPLICATED DATA TYPES)                              │
│  ────────────────────────────────────────────────────────────────────────   │
│  Mechanism:                                                                  │
│    • Data structures with mathematically-proven merge functions             │
│    • LWWRegister: Last-writer-wins with timestamp                           │
│    • GCounter: Grow-only counter (sum of per-node counters)                 │
│    • Merge is associative, commutative, idempotent                          │
│                                                                              │
│  For Context Sync:                                                           │
│    ✅ No coordination needed - always merge                                 │
│    ✅ Eventually consistent automatically                                   │
│    ❌ Limited to CRDT-compatible types                                      │
│    ❌ "Eventually" may not be fast enough                                   │
│                                                                              │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  7. SINGLE-WRITER PATTERN (KAFKA PARTITION LEADER)                           │
│  ────────────────────────────────────────────────────────────────────────   │
│  Mechanism:                                                                  │
│    • Each partition has exactly one leader                                  │
│    • Only leader accepts writes                                             │
│    • Followers replicate from leader                                        │
│    • No conflicts possible (single source of truth)                         │
│                                                                              │
│  For Context Sync:                                                           │
│    ✅ Simplest consistency model                                            │
│    ✅ No conflicts by design                                                │
│    ✅ We already have job leader                                            │
│    ❌ Leader is bottleneck/SPOF for that job                                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Comparison Matrix

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         APPROACH COMPARISON MATRIX                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Approach              │ Consistency │ Latency │ Complexity │ Failure       │
│  ──────────────────────┼─────────────┼─────────┼────────────┼──────────────│
│  Async Replication     │ Eventual    │ Low     │ Low        │ May lose     │
│  (Redis)               │             │         │            │ writes       │
│  ──────────────────────┼─────────────┼─────────┼────────────┼──────────────│
│  Raft Log              │ Strong      │ High    │ High       │ Leader       │
│  (etcd)                │ (linear.)   │         │            │ election     │
│  ──────────────────────┼─────────────┼─────────┼────────────┼──────────────│
│  HLC + MVCC            │ Strong      │ Medium  │ Medium     │ Timestamp    │
│  (Spanner)             │             │         │            │ based        │
│  ──────────────────────┼─────────────┼─────────┼────────────┼──────────────│
│  Quorum + LWW          │ Tunable     │ Medium  │ Medium     │ Quorum       │
│  (Cassandra)           │             │         │            │ tolerant     │
│  ──────────────────────┼─────────────┼─────────┼────────────┼──────────────│
│  Vector Clocks         │ Causal      │ Low     │ High       │ App          │
│  (Dynamo)              │             │         │            │ resolves     │
│  ──────────────────────┼─────────────┼─────────┼────────────┼──────────────│
│  CRDTs                 │ Eventual    │ Low     │ Medium     │ Automatic    │
│                        │             │         │            │ merge        │
│  ──────────────────────┼─────────────┼─────────┼────────────┼──────────────│
│  Single-Writer         │ Strong      │ Low     │ Low        │ Leader       │
│                        │             │         │            │ recovery     │
│  ──────────────────────┴─────────────┴─────────┴────────────┴──────────────│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Chosen Approach: Hybrid Single-Writer + Quorum Replication

We combine the best properties from multiple approaches:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     CHOSEN: HYBRID APPROACH                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  From etcd/Raft:                                                             │
│    → Single leader (job leader) is source of truth                          │
│    → Quorum confirmation before advancing                                   │
│                                                                              │
│  From Cassandra:                                                             │
│    → Tunable consistency (QUORUM for context sync)                          │
│    → LWW for any edge-case conflicts                                        │
│                                                                              │
│  From Spanner:                                                               │
│    → Context embedded in dispatch (like snapshot reads)                     │
│    → Version number for stale detection                                     │
│                                                                              │
│  From Kafka:                                                                 │
│    → Single-writer per partition (job)                                      │
│    → No conflicts by construction                                           │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Key Insight: Layers are natural synchronization points.                     │
│  A dependent workflow in layer N+1 can ONLY depend on workflows             │
│  from layers ≤ N. Therefore: sync context at layer boundaries.              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Protocol Specification

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONTEXT CONSISTENCY PROTOCOL                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Principle: Single-Writer + Quorum Replication + Embedded Context            │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                                                                      │   │
│  │  1. Job leader is SINGLE WRITER for job's context                   │   │
│  │     → No conflicts possible (only one writer)                       │   │
│  │     → Simplest consistency model                                    │   │
│  │                                                                      │   │
│  │  2. Workers send results to their manager                           │   │
│  │     → Manager forwards context updates to job leader                │   │
│  │     → Only leader applies updates to authoritative context          │   │
│  │                                                                      │   │
│  │  3. Layer boundaries trigger quorum sync                            │   │
│  │     → Leader creates versioned snapshot                             │   │
│  │     → Leader broadcasts to peers, waits for quorum ack              │   │
│  │     → Peers store snapshot (for failover)                           │   │
│  │                                                                      │   │
│  │  4. Dispatch includes context snapshot                              │   │
│  │     → No extra fetch needed                                         │   │
│  │     → Version number for stale detection                            │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Manager State (New Fields):                                                 │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                                                                      │   │
│  │  _job_contexts: Dict[job_id, Context]                               │   │
│  │    # Authoritative context (only job leader writes)                 │   │
│  │                                                                      │   │
│  │  _job_layer_version: Dict[job_id, int]                              │   │
│  │    # Monotonically increasing per job                               │   │
│  │    # Incremented when layer completes and context is synced         │   │
│  │                                                                      │   │
│  │  _job_leaders: Dict[job_id, str]                                    │   │
│  │    # job_id → leader_node_id                                        │   │
│  │    # Set when job is first accepted                                 │   │
│  │                                                                      │   │
│  │  _context_lamport_clock: int                                        │   │
│  │    # For per-key LWW timestamps (edge cases)                        │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Protocol Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PROTOCOL FLOW                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Step 1: Workflow Completes with Context Updates                            │
│  ────────────────────────────────────────────────────────────────────────   │
│                                                                              │
│  WorkflowFinalResult includes:                                               │
│    context_updates: bytes      # Serialized Dict[key, value]                │
│    context_timestamps: bytes   # Serialized Dict[key, lamport_clock]        │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  # On receiving manager (may or may not be job leader):             │   │
│  │                                                                      │   │
│  │  async def workflow_final_result(self, addr, data, clock_time):     │   │
│  │      result = WorkflowFinalResult.load(data)                        │   │
│  │      job_leader = self._job_leaders[result.job_id]                  │   │
│  │                                                                      │   │
│  │      if self._node_id != job_leader:                                │   │
│  │          # Forward context to job leader                            │   │
│  │          await self._forward_context_to_leader(                     │   │
│  │              result.job_id, result.context_updates,                 │   │
│  │              result.context_timestamps                              │   │
│  │          )                                                          │   │
│  │      else:                                                          │   │
│  │          # We are job leader - apply directly                       │   │
│  │          await self._apply_context_updates(                         │   │
│  │              result.job_id, result.workflow_id,                     │   │
│  │              result.context_updates, result.context_timestamps      │   │
│  │          )                                                          │   │
│  │                                                                      │   │
│  │      # ... rest of result handling                                  │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Step 2: Job Leader Applies Context (LWW)                                    │
│  ────────────────────────────────────────────────────────────────────────   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  async def _apply_context_updates(                                  │   │
│  │      self, job_id, workflow_id, updates_bytes, timestamps_bytes     │   │
│  │  ):                                                                 │   │
│  │      updates = cloudpickle.loads(updates_bytes)                     │   │
│  │      timestamps = cloudpickle.loads(timestamps_bytes)               │   │
│  │      context = self._job_contexts[job_id]                           │   │
│  │      workflow_name = self._get_workflow_name(workflow_id)           │   │
│  │                                                                      │   │
│  │      for key, value in updates.items():                             │   │
│  │          timestamp = timestamps.get(key, self._context_lamport_clock)│   │
│  │          await context.update(                                      │   │
│  │              workflow_name, key, value,                             │   │
│  │              timestamp=timestamp,                                   │   │
│  │              source_node=self._node_id                              │   │
│  │          )                                                          │   │
│  │                                                                      │   │
│  │      self._context_lamport_clock += 1                               │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Step 3: Layer Completion Triggers Quorum Sync                               │
│  ────────────────────────────────────────────────────────────────────────   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  async def _sync_context_and_advance(self, job_id: str):             │   │
│  │      # Only job leader does this                                    │   │
│  │      assert self._job_leaders[job_id] == self._node_id              │   │
│  │                                                                      │   │
│  │      # 1. Increment layer version                                   │   │
│  │      new_version = self._job_layer_version[job_id] + 1              │   │
│  │      self._job_layer_version[job_id] = new_version                  │   │
│  │                                                                      │   │
│  │      # 2. Create context snapshot                                   │   │
│  │      context = self._job_contexts[job_id]                           │   │
│  │      snapshot = ContextLayerSync(                                   │   │
│  │          job_id=job_id,                                             │   │
│  │          layer_version=new_version,                                 │   │
│  │          context_snapshot=cloudpickle.dumps(context.dict()),       │   │
│  │          source_node_id=self._node_id                               │   │
│  │      )                                                              │   │
│  │                                                                      │   │
│  │      # 3. Broadcast to peers and WAIT for quorum                    │   │
│  │      confirmations = await self._broadcast_context_sync(snapshot)   │   │
│  │                                                                      │   │
│  │      if confirmations < self._quorum_size:                          │   │
│  │          raise QuorumTimeoutError("Context sync failed")            │   │
│  │                                                                      │   │
│  │      # 4. ONLY THEN advance to next layer                           │   │
│  │      await self._dispatch_next_layer(job_id)                        │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Step 4: Dependent Workflow Dispatch Includes Context                        │
│  ────────────────────────────────────────────────────────────────────────   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  WorkflowDispatch (updated fields):                                  │   │
│  │    ...                                                               │   │
│  │    context_version: int           # Expected layer version          │   │
│  │    dependency_context: bytes      # Context from dependencies       │   │
│  │                                                                      │   │
│  │  # Extracting just what the workflow needs:                         │   │
│  │  def _extract_dependency_context(self, job_id, workflow_name):      │   │
│  │      dependencies = self._get_workflow_dependencies(workflow_name)   │   │
│  │      context = self._job_contexts[job_id]                           │   │
│  │      relevant = {}                                                   │   │
│  │      for dep_workflow in dependencies:                              │   │
│  │          if dep_workflow in context._context:                       │   │
│  │              relevant[dep_workflow] = context[dep_workflow].dict()  │   │
│  │      return cloudpickle.dumps(relevant)                             │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### New Messages

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CONTEXT SYNC MESSAGES                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  @dataclass                                                                  │
│  class ContextForward(Message):                                              │
│      """Non-leader forwards context updates to job leader"""                │
│      job_id: str                                                             │
│      workflow_id: str                                                        │
│      context_updates: bytes      # Serialized dict                          │
│      context_timestamps: bytes   # Per-key Lamport timestamps               │
│      source_manager: str         # Who received from worker                 │
│                                                                              │
│  @dataclass                                                                  │
│  class ContextLayerSync(Message):                                            │
│      """Job leader broadcasts at layer completion"""                        │
│      job_id: str                                                             │
│      layer_version: int          # Monotonic per job                        │
│      context_snapshot: bytes     # Full context as of this layer            │
│      source_node_id: str         # Job leader's node ID                     │
│                                                                              │
│  @dataclass                                                                  │
│  class ContextLayerSyncAck(Message):                                         │
│      """Peer confirms receipt of context sync"""                            │
│      job_id: str                                                             │
│      layer_version: int                                                      │
│      applied: bool               # True if applied, False if stale          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Conflict Resolution (Edge Cases)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LWW CONFLICT RESOLUTION                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Already implemented in WorkflowContext.set():                               │
│    • If new_timestamp > existing_timestamp: accept                          │
│    • If new_timestamp <= existing_timestamp: reject (stale)                 │
│                                                                              │
│  Enhanced for tie-breaking (same timestamp):                                 │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                                                                      │   │
│  │  async def set(self, key, value, timestamp, source_node=None):       │   │
│  │      async with self._write_lock:                                    │   │
│  │          existing_ts = self._timestamps.get(key)                     │   │
│  │          existing_src = self._sources.get(key)                       │   │
│  │                                                                      │   │
│  │          should_update = (                                           │   │
│  │              existing_ts is None or                                  │   │
│  │              timestamp > existing_ts or                              │   │
│  │              (timestamp == existing_ts and                           │   │
│  │               source_node and existing_src and                       │   │
│  │               source_node > existing_src)  # Tiebreaker             │   │
│  │          )                                                           │   │
│  │                                                                      │   │
│  │          if should_update:                                           │   │
│  │              self._context[key] = value                              │   │
│  │              self._timestamps[key] = timestamp                       │   │
│  │              self._sources[key] = source_node                        │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Note: With single-writer (job leader), conflicts should not occur.         │
│  LWW is defensive programming for edge cases (leader failover, etc.)        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Correctness Guarantees

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       CORRECTNESS GUARANTEES                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. ORDERING                                                                 │
│     Layer N+1 workflows NEVER execute before layer N context is             │
│     synced to quorum.                                                        │
│                                                                              │
│  2. CONSISTENCY                                                              │
│     Single writer (job leader) means no conflicts. LWW with                 │
│     timestamps handles edge cases (failover).                               │
│                                                                              │
│  3. DURABILITY                                                               │
│     Quorum confirmation means majority has context before advancing.        │
│     If leader fails, another manager has the snapshot.                      │
│                                                                              │
│  4. NO EXTRA FETCHES                                                         │
│     Context is embedded in WorkflowDispatch. Worker has everything          │
│     it needs immediately.                                                    │
│                                                                              │
│  5. VERSION VERIFICATION                                                     │
│     context_version in dispatch allows worker to detect stale               │
│     dispatches (e.g., from a lagging manager).                              │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Corrected Timeline:                                                         │
│  ───────────────────────────────────────────────────────────────────────    │
│  Job Leader (A)              Manager B                                       │
│  ───────────────────────────────────────────────────────────────────────    │
│  WorkflowFinalResult                                                         │
│    (context: {auth: token123})                                              │
│      │                                                                       │
│      ├─► Store context locally                                              │
│      │                                                                       │
│      ├─► Layer complete!                                                    │
│      │                                                                       │
│      ├─► Broadcast ContextLayerSync ──────► Receives, stores                │
│      │                                          │                            │
│      │   ◄──────────────────────────────────── Sends ack                    │
│      │                                                                       │
│      ├─► Quorum reached ✓                                                   │
│      │                                                                       │
│      ├─► NOW dispatch layer 2 ────────────► Receives dispatch               │
│      │   (includes context_version=2,       (has correct context!)          │
│      │    dependency_context={auth: ...})                                   │
│  ───────────────────────────────────────────────────────────────────────    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Drawbacks and Mitigations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DRAWBACKS                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. LEADER BOTTLENECK                                                        │
│     ────────────────────────────────────────────────────────────────────    │
│     • All context updates funnel through job leader                         │
│     • Leader does more work than peers                                      │
│                                                                              │
│     Mitigation: Layer batching reduces frequency. One leader per JOB,       │
│                 not per cluster - load distributed across jobs.             │
│                                                                              │
│  2. LEADER FAILURE RECOVERY                                                  │
│     ────────────────────────────────────────────────────────────────────    │
│     • If leader fails mid-layer, context updates in flight may be lost     │
│     • New leader must recover from last quorum-synced snapshot              │
│                                                                              │
│     Mitigation: Layer snapshots are quorum-replicated. Worst case:          │
│                 re-execute current layer (idempotent workflows help).       │
│                                                                              │
│  3. QUORUM UNAVAILABILITY                                                    │
│     ────────────────────────────────────────────────────────────────────    │
│     • If < quorum managers available, can't advance layers                  │
│     • Job blocks waiting for quorum                                         │
│                                                                              │
│     Mitigation: Circuit breaker + configurable timeout. Return partial      │
│                 results or fail job with clear error.                       │
│                                                                              │
│  4. INCREASED MESSAGE SIZE                                                   │
│     ────────────────────────────────────────────────────────────────────    │
│     • Context embedded in every WorkflowDispatch                            │
│     • Large contexts = larger messages                                      │
│                                                                              │
│     Mitigation: Only include dependencies' context, not full context.       │
│                 Compress large contexts.                                     │
│                                                                              │
│  5. NOT SUITABLE FOR FINE-GRAINED UPDATES                                    │
│     ────────────────────────────────────────────────────────────────────    │
│     • Designed for layer-boundary sync                                      │
│     • High-frequency mid-workflow updates would be slow                     │
│                                                                              │
│     Mitigation: Context is for workflow outputs, not streaming data.        │
│                 Use separate mechanism for real-time data if needed.        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Integration with Existing Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATACENTER ROUTING COMPATIBILITY                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Impact Analysis:                                                            │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Component              │ Impact      │ Notes                         │  │
│  ├────────────────────────┼─────────────┼───────────────────────────────┤  │
│  │ Gate → Manager submit  │ None        │ Context sync is internal      │  │
│  ├────────────────────────┼─────────────┼───────────────────────────────┤  │
│  │ DC health routing      │ Integrates  │ Quorum issues = degraded DC   │  │
│  ├────────────────────────┼─────────────┼───────────────────────────────┤  │
│  │ Manager → Worker       │ Larger msgs │ Context embedded in dispatch  │  │
│  ├────────────────────────┼─────────────┼───────────────────────────────┤  │
│  │ Worker → Manager       │ Extra hop   │ Non-leader forwards to leader │  │
│  ├────────────────────────┼─────────────┼───────────────────────────────┤  │
│  │ Cross-DC dependencies  │ N/A         │ Not supported (each DC indep) │  │
│  ├────────────────────────┼─────────────┼───────────────────────────────┤  │
│  │ Fencing tokens         │ Synergistic │ Both provide staleness detect │  │
│  ├────────────────────────┼─────────────┼───────────────────────────────┤  │
│  │ Progress deduplication │ Minor fix   │ Use layer_version as key      │  │
│  └────────────────────────┴─────────────┴───────────────────────────────┘  │
│                                                                              │
│  Limitation: Cross-DC Context Sync                                           │
│  ────────────────────────────────────────────────────────────────────────   │
│  NOT SUPPORTED: Workflows in DC-1 depending on context from DC-2            │
│  Current design: Each DC runs full job independently                        │
│  If needed: Gate becomes cross-DC coordinator (significant change)          │
│                                                                              │
│  Two Types of Leaders (Clarification):                                       │
│  ────────────────────────────────────────────────────────────────────────   │
│  CLUSTER LEADER: One per manager cluster, handles cluster ops (SWIM)        │
│  JOB LEADER: One per job per DC, handles that job's context                 │
│                                                                              │
│  These are different roles - a follower manager can be job leader:          │
│    Manager A: Cluster Leader, Job Leader for Job-1, Job-3                   │
│    Manager B: Follower,       Job Leader for Job-2                          │
│    Manager C: Follower,       Job Leader for Job-4, Job-5                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Gate Per-Job Leadership Architecture

This section documents the distributed job ownership model for gates, enabling horizontal scaling and fault tolerance without single-leader bottlenecks.

### Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GATE PER-JOB LEADERSHIP MODEL                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  PROBLEM: Single cluster-leader model bottlenecks at high job volumes       │
│  SOLUTION: Each job has its own leader gate, distributed via consistent hash│
│                                                                              │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐                 │
│  │  Gate-1  │   │  Gate-2  │   │  Gate-3  │   │  Gate-4  │                 │
│  │ [0-25%]  │   │ [25-50%] │   │ [50-75%] │   │ [75-100%]│                 │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘                 │
│       │              │              │              │                        │
│       │    Job-abc ──┴──────────────│              │                        │
│       │    (owner: Gate-2)          │              │                        │
│       │                             │              │                        │
│       └── Job-xyz ──────────────────┴──────────────│                        │
│           (owner: Gate-3)                          │                        │
│                                                    │                        │
│                                                    └── Job-123              │
│                                                        (owner: Gate-4)      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Architecture Components

The architecture consists of five key components that work together:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      COMPONENT SUMMARY                                       │
├───────────────────────┬─────────────────────────────────────────────────────┤
│  Component            │  Status         │  Description                      │
├───────────────────────┼─────────────────┼───────────────────────────────────┤
│  1. Consistent Hashing│  IMPLEMENTED    │  Foundation for job distribution  │
│  2. Lease-Based Owner │  IMPLEMENTED    │  Job ownership with TTL           │
│  3. Direct DC Routing │  IMPLEMENTED    │  DC managers send to job leader   │
│  4. Client Reconnect  │  IMPLEMENTED    │  Client computes job owner        │
│  5. Fencing Tokens    │  IMPLEMENTED    │  Stale update protection          │
└───────────────────────┴─────────────────┴───────────────────────────────────┘
```

---

### Component 1: Consistent Hashing Ring

**Status: IMPLEMENTED**

**Decision**: Sophisticated approach - Use consistent hashing to deterministically map jobs to gates.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONSISTENT HASHING RING                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  How It Works:                                                               │
│  ─────────────────────────────────────────────────────────────────────────  │
│  1. Each gate is assigned a position on a virtual ring (0 to 2^32-1)        │
│  2. Jobs are hashed to a position on the same ring                          │
│  3. Job owner = first gate clockwise from job's hash position               │
│  4. Backup = next gate clockwise (for failover)                             │
│                                                                              │
│  Ring Visualization:                                                         │
│                          0                                                   │
│                          │                                                   │
│                    ┌─────┼─────┐                                             │
│                   /      │      \                                            │
│                 Gate-1   │       Gate-2                                      │
│                /         │          \                                        │
│              /           │            \                                      │
│  270° ─────┼─────────────┼─────────────┼───── 90°                           │
│              \           │            /                                      │
│                \         │          /                                        │
│                 Gate-4   │     Gate-3                                        │
│                   \      │      /                                            │
│                    └─────┼─────┘                                             │
│                          │                                                   │
│                         180                                                  │
│                                                                              │
│  Example:                                                                    │
│  ─────────────────────────────────────────────────────────────────────────  │
│  hash("job-abc") = 135° → Owner: Gate-2 (at 90°), Backup: Gate-3 (at 180°) │
│  hash("job-xyz") = 315° → Owner: Gate-1 (at 0°),  Backup: Gate-2 (at 90°)  │
│                                                                              │
│  Benefits:                                                                   │
│  ─────────────────────────────────────────────────────────────────────────  │
│  • Adding/removing gate only affects ~1/N of jobs                           │
│  • Deterministic - any node can compute ownership without coordination      │
│  • Client can compute owner directly (no queries needed)                    │
│  • Natural load balancing across gates                                       │
│                                                                              │
│  Data Structures:                                                            │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  class ConsistentHashRing:                                                   │
│      """Consistent hash ring for gate job distribution"""                   │
│                                                                              │
│      def __init__(self, virtual_nodes: int = 150):                          │
│          self._ring: dict[int, str] = {}    # hash → node_id               │
│          self._sorted_keys: list[int] = []  # sorted hash positions         │
│          self._virtual_nodes = virtual_nodes                                 │
│                                                                              │
│      def add_node(self, node_id: str) -> None:                              │
│          """Add a gate to the ring with virtual nodes"""                    │
│          for i in range(self._virtual_nodes):                                │
│              key = hash(f"{node_id}:{i}") % (2**32)                         │
│              self._ring[key] = node_id                                       │
│          self._sorted_keys = sorted(self._ring.keys())                       │
│                                                                              │
│      def remove_node(self, node_id: str) -> None:                           │
│          """Remove a gate from the ring"""                                  │
│          self._ring = {k: v for k, v in self._ring.items()                  │
│                       if v != node_id}                                       │
│          self._sorted_keys = sorted(self._ring.keys())                       │
│                                                                              │
│      def get_node(self, key: str) -> str:                                   │
│          """Get the owner gate for a job_id"""                              │
│          if not self._ring:                                                  │
│              raise NoGatesAvailable()                                        │
│          hash_val = hash(key) % (2**32)                                      │
│          idx = bisect.bisect(self._sorted_keys, hash_val)                    │
│          if idx == len(self._sorted_keys):                                   │
│              idx = 0                                                         │
│          return self._ring[self._sorted_keys[idx]]                           │
│                                                                              │
│      def get_nodes(self, key: str, count: int = 2) -> list[str]:            │
│          """Get owner and N-1 backup gates for a job_id"""                  │
│          nodes = []                                                          │
│          hash_val = hash(key) % (2**32)                                      │
│          idx = bisect.bisect(self._sorted_keys, hash_val)                    │
│          while len(nodes) < count and len(nodes) < len(set(self._ring)):    │
│              if idx >= len(self._sorted_keys):                               │
│                  idx = 0                                                     │
│              node = self._ring[self._sorted_keys[idx]]                       │
│              if node not in nodes:                                           │
│                  nodes.append(node)                                          │
│              idx += 1                                                        │
│          return nodes                                                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Component 2: Lease-Based Job Ownership

**Status: IMPLEMENTED**

**Decision**: Sophisticated approach - Jobs have leases with TTL that must be renewed.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LEASE-BASED JOB OWNERSHIP                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Why Leases:                                                                 │
│  ─────────────────────────────────────────────────────────────────────────  │
│  • Consistent hash determines INITIAL owner                                 │
│  • Lease confirms ACTIVE ownership                                          │
│  • If owner fails, lease expires and backup can claim                       │
│  • Prevents split-brain: only one lease holder at a time                    │
│                                                                              │
│  Lease Lifecycle:                                                            │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                    │
│  │   CLAIMED   │────▶│   ACTIVE    │────▶│   EXPIRED   │                    │
│  │             │     │             │     │             │                    │
│  │ fence_token │     │ renewing... │     │ backup can  │                    │
│  │ assigned    │     │             │     │ claim       │                    │
│  └─────────────┘     └──────┬──────┘     └─────────────┘                    │
│         ▲                   │                   │                            │
│         │                   │ renewal           │ backup claims              │
│         │                   ▼                   ▼                            │
│         │            ┌─────────────┐     ┌─────────────┐                    │
│         │            │   ACTIVE    │     │   CLAIMED   │                    │
│         │            │  (renewed)  │     │  (new owner)│                    │
│         │            └─────────────┘     │ fence+1     │                    │
│         │                                └─────────────┘                    │
│         │                                       │                            │
│         └───────────────────────────────────────┘                            │
│                        (cycle continues)                                     │
│                                                                              │
│  Lease State:                                                                │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  @dataclass(slots=True)                                                      │
│  class GateJobLease:                                                         │
│      """Lease for job ownership"""                                          │
│      job_id: str                                                             │
│      owner_node_id: str           # Current lease holder                    │
│      fence_token: int             # Monotonic, increments on ownership change│
│      lease_acquired: float        # time.monotonic() when acquired          │
│      lease_duration: float = 30.0 # TTL in seconds                          │
│      backup_node_id: str | None = None  # Next in consistent hash ring      │
│                                                                              │
│      @property                                                               │
│      def is_expired(self) -> bool:                                          │
│          return time.monotonic() > self.lease_acquired + self.lease_duration│
│                                                                              │
│      def renew(self) -> None:                                                │
│          self.lease_acquired = time.monotonic()                              │
│                                                                              │
│  Lease Operations:                                                           │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  async def claim_job_lease(self, job_id: str) -> GateJobLease:              │
│      """Claim ownership of a job (on first submission)"""                   │
│      nodes = self._hash_ring.get_nodes(job_id, count=2)                     │
│      owner = nodes[0]                                                        │
│      backup = nodes[1] if len(nodes) > 1 else None                          │
│                                                                              │
│      lease = GateJobLease(                                                   │
│          job_id=job_id,                                                      │
│          owner_node_id=owner,                                                │
│          fence_token=1,                                                      │
│          lease_acquired=time.monotonic(),                                    │
│          backup_node_id=backup,                                              │
│      )                                                                       │
│      self._job_leases[job_id] = lease                                        │
│      return lease                                                            │
│                                                                              │
│  async def claim_expired_lease(self, job_id: str) -> GateJobLease | None:   │
│      """Backup claims an expired lease"""                                   │
│      lease = self._job_leases.get(job_id)                                   │
│      if not lease or not lease.is_expired:                                   │
│          return None                                                         │
│      if lease.backup_node_id != self._node_id.full:                         │
│          return None  # Not the backup                                       │
│                                                                              │
│      # Claim with incremented fence token                                    │
│      new_backup = self._hash_ring.get_nodes(job_id, count=3)[2:]            │
│      new_lease = GateJobLease(                                               │
│          job_id=job_id,                                                      │
│          owner_node_id=self._node_id.full,                                   │
│          fence_token=lease.fence_token + 1,                                  │
│          lease_acquired=time.monotonic(),                                    │
│          backup_node_id=new_backup[0] if new_backup else None,              │
│      )                                                                       │
│      self._job_leases[job_id] = new_lease                                    │
│      return new_lease                                                        │
│                                                                              │
│  Lease Renewal Loop:                                                         │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  async def _lease_renewal_loop(self):                                        │
│      """Background task to renew leases for owned jobs"""                   │
│      while self._running:                                                    │
│          for job_id, lease in list(self._job_leases.items()):               │
│              if lease.owner_node_id == self._node_id.full:                   │
│                  if not lease.is_expired:                                    │
│                      lease.renew()                                           │
│          await asyncio.sleep(lease.lease_duration / 3)  # Renew at 1/3 TTL  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Component 3: Direct DC-to-Job-Leader Result Routing

**Status: IMPLEMENTED**

**Decision**: Sophisticated approach - DC managers send results directly to job leader gate.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DIRECT RESULT ROUTING                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Why Direct Routing:                                                         │
│  ─────────────────────────────────────────────────────────────────────────  │
│  • No intermediate hops = lower latency                                     │
│  • Job leader gate aggregates results directly                              │
│  • Less load on cluster leader gate                                         │
│                                                                              │
│  Flow Diagram:                                                               │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│                    ┌─────────────────────────────────────┐                   │
│                    │         Gate Cluster                │                   │
│                    │  ┌──────┐  ┌──────┐  ┌──────┐      │                   │
│                    │  │Gate-1│  │Gate-2│  │Gate-3│      │                   │
│                    │  │      │  │ (job │  │      │      │                   │
│                    │  │      │  │leader)│  │      │      │                   │
│                    │  └──────┘  └───▲──┘  └──────┘      │                   │
│                    └────────────────┼───────────────────┘                   │
│                                     │                                        │
│         ┌───────────────────────────┼───────────────────────────┐           │
│         │                           │                           │           │
│         │ JobFinalResult            │ JobFinalResult            │           │
│         │                           │                           │           │
│  ┌──────┴──────┐             ┌──────┴──────┐             ┌──────┴──────┐   │
│  │   DC-ALPHA  │             │   DC-BETA   │             │   DC-GAMMA  │   │
│  │   Manager   │             │   Manager   │             │   Manager   │   │
│  │   Cluster   │             │   Cluster   │             │   Cluster   │   │
│  └─────────────┘             └─────────────┘             └─────────────┘   │
│                                                                              │
│  Manager-Side Implementation:                                                │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  # Managers need to know job → gate owner mapping                           │
│  # This is embedded in the job dispatch from gate                           │
│                                                                              │
│  async def send_job_final_result(self, job_id: str, result: JobFinalResult):│
│      # Get job leader gate from stored job info                             │
│      job_info = self._job_info[job_id]                                       │
│      job_leader_gate = job_info.origin_gate  # Stored when job dispatched   │
│                                                                              │
│      # Send directly to job leader gate                                      │
│      await self.send_tcp(                                                    │
│          job_leader_gate,                                                    │
│          "job_final_result",                                                 │
│          result.dump(),                                                      │
│      )                                                                       │
│                                                                              │
│  Gate-Side Implementation:                                                   │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  async def job_final_result(self, addr, data, clock_time):                  │
│      result = JobFinalResult.load(data)                                      │
│      lease = self._job_leases.get(result.job_id)                            │
│                                                                              │
│      # Verify we're the owner (fence token check)                           │
│      if not self._owns_job(result.job_id, result.fence_token):              │
│          # Ring changed or lease transferred                                 │
│          actual_owner = self._hash_ring.get_node(result.job_id)             │
│          await self.forward_result(actual_owner, result)                     │
│          return b'forwarded'                                                 │
│                                                                              │
│      # Aggregate with other DC results                                       │
│      await self._aggregate_dc_result(result)                                 │
│                                                                              │
│  Edge Case - Ring Changed:                                                   │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  If gate added/removed while job running:                                    │
│  1. DC manager sends to old owner (from stored job_info)                    │
│  2. Old owner detects "I don't own this" via hash ring                      │
│  3. Old owner forwards to new owner                                          │
│  4. New owner processes (fence token prevents duplicates)                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Component 4: Client Reconnection

**Status: IMPLEMENTED**

**Decision**: Sophisticated approach - Clients compute job owner deterministically.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CLIENT RECONNECTION                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Why Client Computes Owner:                                                  │
│  ─────────────────────────────────────────────────────────────────────────  │
│  • After disconnect, client knows exactly where to reconnect                │
│  • No need to query gates for "who owns my job?"                            │
│  • Client maintains same hash ring as gates                                  │
│                                                                              │
│  Client State:                                                               │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  class HyperscaleClient:                                                     │
│      def __init__(self, gate_addrs: list[tuple[str, int]]):                 │
│          self._gate_addrs = gate_addrs                                       │
│          self._hash_ring = ConsistentHashRing()                              │
│          self._job_callbacks: dict[str, asyncio.Future] = {}                │
│                                                                              │
│          # Initialize ring with known gates                                  │
│          for host, port in gate_addrs:                                       │
│              self._hash_ring.add_node(f"{host}:{port}")                      │
│                                                                              │
│      async def submit_job(self, job: Job) -> JobAck:                        │
│          # Compute owner from job_id                                         │
│          owner = self._hash_ring.get_node(job.job_id)                        │
│          host, port = owner.split(":")                                       │
│                                                                              │
│          # Submit to owner                                                   │
│          return await self.send_tcp(                                         │
│              (host, int(port)),                                              │
│              "job_submission",                                               │
│              job.dump(),                                                     │
│          )                                                                   │
│                                                                              │
│  Reconnection Logic:                                                         │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  async def reconnect_to_job(self, job_id: str, max_retries: int = 3):       │
│      """Reconnect to job after disconnect"""                                │
│      for attempt in range(max_retries):                                      │
│          owner = self._hash_ring.get_node(job_id)                            │
│          host, port = owner.split(":")                                       │
│                                                                              │
│          try:                                                                │
│              response = await self.send_tcp(                                 │
│                  (host, int(port)),                                          │
│                  "register_callback",                                        │
│                  RegisterCallback(job_id=job_id).dump(),                     │
│              )                                                               │
│              if response.success:                                            │
│                  return True                                                 │
│          except (ConnectionError, TimeoutError):                             │
│              pass                                                            │
│                                                                              │
│          # Gate might have failed, wait for lease transfer                   │
│          await asyncio.sleep(LEASE_DURATION / 2)                             │
│                                                                              │
│      raise ReconnectFailed(job_id)                                           │
│                                                                              │
│  Ring Update Protocol:                                                       │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  Clients receive ring updates via push notifications:                        │
│                                                                              │
│  async def handle_ring_update(self, update: RingUpdate):                    │
│      """Gate cluster sends ring updates to clients"""                       │
│      if update.type == "add":                                                │
│          self._hash_ring.add_node(update.node_id)                            │
│      elif update.type == "remove":                                           │
│          self._hash_ring.remove_node(update.node_id)                         │
│                                                                              │
│  Timeline (Reconnect After Gate Failure):                                    │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  t=0    Client connected to Gate-2 for job-abc                               │
│  t=5    Gate-2 crashes                                                       │
│  t=5    Client detects disconnect                                            │
│  t=6    Client computes owner: hash("job-abc") → Gate-2 (still in ring)     │
│  t=6    Client tries Gate-2, fails                                           │
│  t=6    Client waits LEASE_DURATION/2 = 15s                                  │
│  t=21   Client retries: Gate-3 now owns (lease transferred)                 │
│  t=21   Client connects to Gate-3, registers callback                       │
│  t=21   Client receives remaining updates ✓                                  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Component 5: Fencing Tokens

**Status: IMPLEMENTED**

**Decision**: Simple approach - Monotonic fence tokens reject stale operations.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    FENCING TOKENS                                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Why Fencing Tokens:                                                         │
│  ─────────────────────────────────────────────────────────────────────────  │
│  • Prevent stale updates from old owner after lease transfer                │
│  • Simple, proven pattern (used in ZooKeeper, etcd, etc.)                   │
│  • No consensus needed - just monotonic comparison                          │
│                                                                              │
│  How It Works:                                                               │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  1. Job created with fence_token = 1                                         │
│  2. Each ownership transfer increments fence_token                           │
│  3. All operations include fence_token                                       │
│  4. Receiver rejects if received_token < current_token                       │
│                                                                              │
│  Fence Token in Messages:                                                    │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  # All job-related messages include fence token                              │
│                                                                              │
│  @dataclass(slots=True)                                                      │
│  class JobDispatch(Message):                                                 │
│      job_id: str                                                             │
│      fence_token: int  # ← Must match current owner's token                 │
│      workflows: list[bytes]                                                  │
│      # ...                                                                   │
│                                                                              │
│  @dataclass(slots=True)                                                      │
│  class JobFinalResult(Message):                                              │
│      job_id: str                                                             │
│      fence_token: int  # ← Proves result is from valid ownership period     │
│      datacenter: str                                                         │
│      # ...                                                                   │
│                                                                              │
│  @dataclass(slots=True)                                                      │
│  class JobStatusPush(Message):                                               │
│      job_id: str                                                             │
│      fence_token: int  # ← Client can detect ownership changes              │
│      # ...                                                                   │
│                                                                              │
│  Validation Logic:                                                           │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  def validate_fence_token(self, job_id: str, received_token: int) -> bool:  │
│      """Reject operations with stale fence tokens"""                        │
│      lease = self._job_leases.get(job_id)                                    │
│      if not lease:                                                           │
│          return False  # Unknown job                                         │
│      if received_token < lease.fence_token:                                  │
│          return False  # Stale token from old owner                         │
│      if received_token > lease.fence_token:                                  │
│          # Future token - might be from new owner we don't know yet         │
│          # Accept and update our lease info                                  │
│          self._update_lease_from_newer_token(job_id, received_token)         │
│      return True                                                             │
│                                                                              │
│  Scenario: Stale Update Rejected                                             │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  t=0    Gate-2 owns job-abc (fence=1)                                        │
│  t=1    Gate-2 dispatches to DC-ALPHA (fence=1)                             │
│  t=2    Gate-2 crashes                                                       │
│  t=5    Gate-3 claims lease (fence=2)                                        │
│  t=10   DC-ALPHA returns result (fence=1)  ← STALE!                         │
│  t=10   Gate-3 rejects: received_token(1) < current(2)                       │
│  t=11   DC-ALPHA retries with updated fence from Gate-3                     │
│                                                                              │
│  Scenario: Split-Brain Prevention                                            │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  t=0    Gate-2 owns job-abc (fence=1)                                        │
│  t=1    Network partition: Gate-2 isolated from Gate-3                      │
│  t=2    Gate-2 thinks it still owns job (lease not expired locally)         │
│  t=2    Gate-3 claims lease (fence=2) - sees Gate-2 as dead                 │
│  t=3    Gate-2 sends update (fence=1)                                        │
│  t=3    Receiver rejects: fence=1 < current=2                                │
│  t=4    Gate-2 learns it's not owner anymore, stops processing              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Component Interactions

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    COMPONENT SYNERGIES                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐                                                         │
│  │ Consistent Hash │─────────────────────────────────────┐                  │
│  │    (Foundation) │                                     │                  │
│  └────────┬────────┘                                     │                  │
│           │ determines initial                           │                  │
│           │ owner & backup                               │                  │
│           ▼                                              ▼                  │
│  ┌─────────────────┐                            ┌─────────────────┐         │
│  │  Lease-Based    │                            │ Client Reconnect│         │
│  │  Ownership      │                            │ (computes owner)│         │
│  └────────┬────────┘                            └────────┬────────┘         │
│           │ fence token                                  │ queries          │
│           │ assigned                                     │ job owner        │
│           ▼                                              │                  │
│  ┌─────────────────┐                                     │                  │
│  │ Fencing Tokens  │◀────────────────────────────────────┘                  │
│  │ (prevents stale)│                                                         │
│  └────────┬────────┘                                                         │
│           │ validates                                                        │
│           │ operations                                                       │
│           ▼                                                                  │
│  ┌─────────────────┐                                                         │
│  │ Direct DC Route │                                                         │
│  │ (low latency)   │                                                         │
│  └─────────────────┘                                                         │
│                                                                              │
│  Data Flow:                                                                  │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  1. Client submits job → hash_ring.get_node(job_id) → Gate-X               │
│  2. Gate-X claims lease → fence_token=1                                     │
│  3. Gate-X dispatches to DCs → includes fence_token                         │
│  4. DCs complete → send results to Gate-X (job leader)                      │
│  5. Gate-X aggregates, sends to client                                       │
│                                                                              │
│  Failure Handling:                                                           │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  • Gate-X fails → lease expires → Gate-Y (backup) claims → fence+1         │
│  • Stale results from DCs (fence=1) rejected by Gate-Y (fence=2)           │
│  • Client reconnects to Gate-Y (computed via hash ring)                     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Testing Approach

All tests follow this pattern:

```python
# examples/servers/test_<feature>.py

async def main():
    # 1. Setup cluster with appropriate logging
    LoggingConfig.directory = os.getcwd()
    
    # 2. Start nodes in order: gates → managers → workers
    gate = GateServer(...)
    await gate.start()
    await asyncio.sleep(3)  # Wait for leader election
    
    manager = ManagerServer(..., gate_addrs=[...])
    await manager.start()
    await asyncio.sleep(3)  # Wait for registration
    
    worker = WorkerServer(..., seed_managers=[...])
    await worker.start()
    await asyncio.sleep(2)  # Wait for registration
    
    # 3. Run test scenario
    client = HyperscaleClient(gate_tcp_addrs=[...])
    await client.start()
    job_id = await client.submit_job(...)
    result = await client.wait_for_completion(job_id)
    
    # 4. Validate results
    assert result.status == "completed"
    
    # 5. Cleanup (in reverse order, with timeouts)
    await client.stop()
    await worker.stop()  # Note: workers use stop(), not graceful_shutdown()
    await manager.graceful_shutdown()
    await gate.graceful_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

**Debug Workflow**:
1. Let user test with `timeout 180 python examples/servers/test_<name>.py 2>&1 | tail -100`
2. Watch for warnings/exceptions
3. Kill test if error found
4. Fix the issue
5. Commit with descriptive message
6. Push to branch
7. Repeat until test passes

---

### Key Files Reference

| File | Purpose |
|------|---------|
| `hyperscale/distributed_rewrite/nodes/gate.py` | Gate node - job dispatch, results aggregation |
| `hyperscale/distributed_rewrite/nodes/manager.py` | Manager node - workflow dispatch, worker tracking |
| `hyperscale/distributed_rewrite/nodes/worker.py` | Worker node - workflow execution |
| `hyperscale/distributed_rewrite/nodes/client.py` | Client API for job submission |
| `hyperscale/distributed_rewrite/models/distributed.py` | All message types (dataclasses) |
| `hyperscale/distributed_rewrite/swim/health_aware_server.py` | Base server with SWIM protocol |
| `hyperscale/distributed_rewrite/swim/health/federated_health_monitor.py` | Cross-cluster health monitoring |
| `hyperscale/distributed_rewrite/env/env.py` | Configuration via environment variables |
| `hyperscale/core/hooks/hook.py` | Hook types including `HookType.TEST` |
| `hyperscale/core/jobs/workers/provisioner.py` | Priority-based core allocation |
| `hyperscale/reporting/results.py` | Results merging and aggregation |

---

---

## Implemented Feature Documentation

This section documents features that have been implemented, including their architecture, configuration, and usage patterns.

### Terminal UI Architecture

The Terminal UI provides real-time visual feedback during test execution with workflow progress, metrics, and statistics.

#### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Terminal UI Architecture                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  HyperscaleInterface                      │  │
│  │                                                           │  │
│  │  • Coordinates UI components                              │  │
│  │  • Cycles through active workflows                        │  │
│  │  • Handles updates from InterfaceUpdatesController        │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           │                                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                      Terminal                             │  │
│  │                                                           │  │
│  │  • Raw terminal control (ANSI escape sequences)          │  │
│  │  • Manages Canvas layout                                  │  │
│  │  • Handles refresh rate and rendering                     │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           │                                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                       Canvas                              │  │
│  │                                                           │  │
│  │  • Contains Sections arranged in rows                     │  │
│  │  • Handles resize and layout calculations                 │  │
│  │  • Manages padding (horizontal/vertical)                  │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           │                                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                      Sections                             │  │
│  │                                                           │  │
│  │  • Group related components                               │  │
│  │  • Support auto-width and fixed-width modes              │  │
│  │  • Handle component visibility toggling                   │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           │                                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                     Components                            │  │
│  │                                                           │  │
│  │  • Header: ASCII art title with gradient colors           │  │
│  │  • ProgressBar: Animated progress with fill/background    │  │
│  │  • Spinner: Multiple animation styles (dots, bars, etc.)  │  │
│  │  • Counter: Numeric display with formatting               │  │
│  │  • TotalRate: Requests/second over entire run             │  │
│  │  • WindowedRate: Recent requests/second (sliding window)  │  │
│  │  • ScatterPlot: Plotille-based latency visualization      │  │
│  │  • Table: Tabulated statistics display                    │  │
│  │  • Text/MultilineText: Status messages                    │  │
│  │  • Timer: Elapsed time display                            │  │
│  │  • StatusBar/AnimatedStatusBar: Status indicators         │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Component Hierarchy

```python
# Main interface entry point
interface = HyperscaleInterface(updates_controller)
interface.initialize(workflows, terminal_mode="full")
await interface.run()

# Terminal modes:
# - "full": Complete TUI with all components
# - "ci": Simplified output for CI environments
# - "none": No UI output (headless)
```

#### Key Files

| File | Purpose |
|------|---------|
| `hyperscale/ui/__init__.py` | Main exports (HyperscaleInterface, InterfaceUpdatesController) |
| `hyperscale/ui/hyperscale_interface.py` | Interface orchestration, workflow cycling |
| `hyperscale/ui/interface_updates_controller.py` | Async update queue management |
| `hyperscale/ui/components/terminal/terminal.py` | Raw terminal control |
| `hyperscale/ui/components/terminal/canvas.py` | Layout engine |
| `hyperscale/ui/components/terminal/section.py` | Section container |
| `hyperscale/ui/styling/` | Colors, attributes, stylization |

#### Update Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                      UI Update Flow                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Worker Progress  ──►  RemoteGraphManager  ──►  Updates Queue  │
│       │                      │                       │          │
│       │                      │                       ▼          │
│       │               ┌──────┴──────┐    InterfaceUpdatesController
│       │               │             │               │           │
│       │               ▼             ▼               ▼           │
│       │        Stats Update   Progress Update   Workflow List   │
│       │               │             │               │           │
│       │               └──────┬──────┘               │           │
│       │                      │                      │           │
│       │                      ▼                      ▼           │
│       │              HyperscaleInterface._run() loop            │
│       │                      │                                  │
│       │                      ▼                                  │
│       │              Set active components for                  │
│       │              current workflow                           │
│       │                      │                                  │
│       │                      ▼                                  │
│       │              Terminal.trigger_render()                  │
│       │                      │                                  │
│       └──────────────────────┴──────────────────────────────────│
│                                                                 │
│  Refresh rate: Configurable via _interval (default ~30fps)     │
│  Workflow cycling: update_interval (default 3 seconds)         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

### Reporting Architecture

Hyperscale supports exporting test results to numerous backends for analysis and visualization.

#### Supported Backends

| Category | Backends |
|----------|----------|
| **Time Series** | InfluxDB, TimescaleDB, AWS Timestream, Prometheus, Graphite |
| **Cloud Storage** | S3, Google Cloud Storage, BigQuery, BigTable |
| **Databases** | PostgreSQL, MySQL, SQLite, MongoDB, Cassandra, CosmosDB, Redis |
| **Monitoring** | Datadog, NewRelic, Cloudwatch, Honeycomb, Netdata |
| **Metrics** | StatsD, DogStatsD, Telegraf, Telegraf-StatsD |
| **Message Queue** | Kafka |
| **File Formats** | JSON, CSV, XML |
| **Serverless** | AWS Lambda |
| **Custom** | CustomReporter (user-defined) |

#### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Reporting Architecture                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                      Reporter[T]                          │  │
│  │                                                           │  │
│  │  • Generic reporter with backend type parameter           │  │
│  │  • Factory pattern for backend instantiation              │  │
│  │  • Unified submit() interface                             │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           │                                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Backend Config                         │  │
│  │                                                           │  │
│  │  • PostgresConfig, InfluxDBConfig, S3Config, etc.        │  │
│  │  • Connection parameters                                  │  │
│  │  • Batching and retry settings                            │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           │                                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  Metrics/Results                          │  │
│  │                                                           │  │
│  │  • WorkflowMetric: Per-workflow statistics                │  │
│  │  • WorkflowMetricSet: Collection of workflow metrics      │  │
│  │  • StepMetricSet: Per-step breakdown                      │  │
│  │  • ResultSet: Final aggregated results                    │  │
│  │  • MetricsSet: Timing and throughput metrics              │  │
│  │  • CheckSet: Validation check results                     │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Usage Example

```python
from hyperscale.reporting import Reporter, PostgresConfig, ReporterTypes

# Configure backend
config = PostgresConfig(
    host="localhost",
    port=5432,
    database="hyperscale_results",
    username="user",
    password="password",
)

# Create reporter
reporter = Reporter[PostgresConfig](
    reporter_type=ReporterTypes.Postgres,
    config=config,
)

# Submit results
await reporter.connect()
await reporter.submit(workflow_metrics)
await reporter.close()
```

#### Key Files

| File | Purpose |
|------|---------|
| `hyperscale/reporting/reporter.py` | Generic Reporter class, backend factory |
| `hyperscale/reporting/results.py` | Result aggregation and merging |
| `hyperscale/reporting/common/types.py` | ReporterTypes enum |
| `hyperscale/reporting/common/results_types.py` | Metric data classes |
| `hyperscale/reporting/<backend>/` | Per-backend implementation |

---

### Local Execution Mode

Local mode enables single-machine testing without distributed infrastructure.

#### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Local Execution Mode                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                     LocalRunner                           │  │
│  │                                                           │  │
│  │  • Entry point for local test execution                   │  │
│  │  • Manages worker subprocess pool                         │  │
│  │  • Coordinates UI and results collection                  │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           │                                     │
│            ┌──────────────┼──────────────┐                     │
│            │              │              │                      │
│            ▼              ▼              ▼                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│  │LocalServer  │  │LocalServer  │  │LocalServer  │  ...       │
│  │Pool Worker 1│  │Pool Worker 2│  │Pool Worker N│            │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘            │
│         │                │                │                     │
│         └────────────────┼────────────────┘                     │
│                          │                                      │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  RemoteGraphManager                       │  │
│  │                                                           │  │
│  │  • Manages workflow dispatch to workers                   │  │
│  │  • Collects results and progress                          │  │
│  │  • Feeds InterfaceUpdatesController                       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Worker Count: Auto-detected via psutil.cpu_count(logical=False)│
│  Communication: In-process TCP (localhost bindings)             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Usage

```python
from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.core.graph import Workflow

# Create runner
runner = LocalRunner(
    host="localhost",
    port=8080,
    workers=4,  # Optional, defaults to CPU cores
)

# Define workflows
workflows = [
    (["tag1"], MyWorkflow()),
]

# Execute
await runner.run(
    test_name="my_test",
    workflows=workflows,
    terminal_mode="full",  # "full", "ci", or "none"
    timeout="5m",
)
```

#### Key Files

| File | Purpose |
|------|---------|
| `hyperscale/core/jobs/runner/local_runner.py` | LocalRunner entry point |
| `hyperscale/core/jobs/runner/local_server_pool.py` | Worker subprocess pool |
| `hyperscale/core/jobs/graphs/remote_graph_manager.py` | Workflow dispatch |

---

### Rate Limiting Implementation (AD-24)

Rate limiting prevents any single client from overwhelming the system while adapting behavior based on system health.

#### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Rate Limiting Architecture                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              HybridOverloadDetector (AD-18)               │  │
│  │                                                           │  │
│  │  Provides health state: HEALTHY / BUSY / STRESSED /       │  │
│  │  OVERLOADED based on latency, CPU, memory signals         │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           │                                     │
│                           ▼                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                 AdaptiveRateLimiter                       │  │
│  │                                                           │  │
│  │  Health-gated rate limiting:                              │  │
│  │  • HEALTHY: Per-operation limits apply                    │  │
│  │  • BUSY: LOW priority shed + per-operation limits         │  │
│  │  • STRESSED: Per-client fair-share limiting               │  │
│  │  • OVERLOADED: Only CRITICAL requests pass                │  │
│  └────────────────────────┬─────────────────────────────────┘  │
│                           │                                     │
│            ┌──────────────┴──────────────┐                     │
│            │                             │                      │
│            ▼                             ▼                      │
│  ┌─────────────────────┐      ┌─────────────────────┐         │
│  │ SlidingWindowCounter│      │ Per-Client Stress   │         │
│  │                     │      │ Counters            │         │
│  │ Per-operation limits│      │                     │         │
│  │ (100 req/10s for    │      │ Fair-share limits   │         │
│  │ job_submit, etc.)   │      │ when stressed       │         │
│  └─────────────────────┘      └─────────────────────┘         │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                   Request Priority                        │  │
│  │                                                           │  │
│  │  CRITICAL (0): Health checks, cancellation, final results│  │
│  │  HIGH (1): Job submission, workflow dispatch             │  │
│  │  NORMAL (2): Progress updates, stats queries             │  │
│  │  LOW (3): Debug requests, non-essential sync             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### SlidingWindowCounter

The SlidingWindowCounter provides deterministic rate limiting without the edge cases of token bucket algorithms:

```python
effective_count = current_window_count + previous_window_count * (1 - window_progress)
```

Example:
- Window size: 60 seconds
- Previous window: 100 requests
- Current window: 30 requests
- 15 seconds into current window (25% progress)
- Effective count = 30 + 100 * 0.75 = 105

#### Configuration

```python
# Environment variables for rate limiting
RATE_LIMIT_DEFAULT_BUCKET_SIZE: int = 100
RATE_LIMIT_DEFAULT_REFILL_RATE: float = 10.0
RATE_LIMIT_CLIENT_IDLE_TIMEOUT: float = 300.0
RATE_LIMIT_CLEANUP_INTERVAL: float = 60.0
RATE_LIMIT_MAX_RETRIES: int = 3
RATE_LIMIT_MAX_TOTAL_WAIT: float = 60.0
RATE_LIMIT_BACKOFF_MULTIPLIER: float = 1.5
```

#### Per-Operation Limits

| Operation | Max Requests | Window (seconds) |
|-----------|--------------|------------------|
| stats_update | 500 | 10.0 |
| heartbeat | 200 | 10.0 |
| progress_update | 300 | 10.0 |
| job_submit | 50 | 10.0 |
| job_status | 100 | 10.0 |
| workflow_dispatch | 100 | 10.0 |
| cancel | 20 | 10.0 |
| reconnect | 10 | 10.0 |

#### Client-Side Cooperation

The `CooperativeRateLimiter` enables clients to respect server rate limits:

```python
limiter = CooperativeRateLimiter()

# Before sending request
await limiter.wait_if_needed("job_submit")

# After receiving 429 response
if response.status == 429:
    retry_after = float(response.headers.get("Retry-After", 1.0))
    limiter.handle_rate_limit("job_submit", retry_after)
```

#### Key Files

| File | Purpose |
|------|---------|
| `hyperscale/distributed_rewrite/reliability/rate_limiting.py` | All rate limiting components |
| `hyperscale/distributed_rewrite/reliability/overload.py` | HybridOverloadDetector |
| `hyperscale/distributed_rewrite/reliability/load_shedding.py` | RequestPriority enum |

---

### Three-Signal Health Detection (AD-19)

The three-signal health model provides nuanced health tracking beyond simple alive/dead status.

#### The Three Signals

```
┌─────────────────────────────────────────────────────────────────┐
│                   Three-Signal Health Model                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐
│  │ Signal 1: LIVENESS                                          │
│  │                                                             │
│  │ "Is the node alive and responsive?"                         │
│  │                                                             │
│  │ • UDP ping/ack from SWIM protocol                           │
│  │ • Timeout: LIVENESS_PROBE_TIMEOUT (1.0s)                    │
│  │ • Period: LIVENESS_PROBE_PERIOD (10.0s)                     │
│  │ • Failure threshold: LIVENESS_PROBE_FAILURE_THRESHOLD (3)   │
│  └─────────────────────────────────────────────────────────────┘
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐
│  │ Signal 2: READINESS                                         │
│  │                                                             │
│  │ "Can the node accept new work?"                             │
│  │                                                             │
│  │ • Capacity check (available cores/slots)                    │
│  │ • Overload state from HybridOverloadDetector                │
│  │ • Not accepting if: at capacity, overloaded, draining       │
│  │ • Timeout: READINESS_PROBE_TIMEOUT (2.0s)                   │
│  └─────────────────────────────────────────────────────────────┘
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐
│  │ Signal 3: PROGRESS                                          │
│  │                                                             │
│  │ "Is the node making forward progress?"                      │
│  │                                                             │
│  │ States:                                                     │
│  │ • IDLE: No active work, but healthy                         │
│  │ • PROGRESSING: Completing work (throughput > 0)             │
│  │ • STALLED: Active work but no recent completions            │
│  │ • STUCK: Extended period without progress                   │
│  └─────────────────────────────────────────────────────────────┘
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Routing Decisions

The three signals combine to produce routing decisions:

| Liveness | Readiness | Progress | Decision |
|----------|-----------|----------|----------|
| ✓ | ✓ | PROGRESSING/IDLE | **ROUTE** - Send work |
| ✓ | ✗ | Any | **HOLD** - Don't send new work |
| ✓ | ✓ | STALLED | **INVESTIGATE** - Probe further |
| ✓ | Any | STUCK | **DRAIN** - Complete existing, no new |
| ✗ | Any | Any | **EVICT** - Node is dead |

#### Health State Protocol

```python
class HealthSignals(Protocol):
    """Protocol defining the three-signal health interface."""

    @property
    def liveness(self) -> bool:
        """Is the node alive and responsive?"""
        ...

    @property
    def readiness(self) -> bool:
        """Can the node accept work?"""
        ...

    @property
    def progress_state(self) -> ProgressState:
        """Is the node making progress?"""
        ...

    def get_routing_decision(self) -> RoutingDecision:
        """Get routing decision based on combined signals."""
        ...
```

#### Correlation Detection

The NodeHealthTracker prevents cascade evictions when multiple nodes fail simultaneously (likely network issue):

```python
tracker = NodeHealthTracker[WorkerHealthState]()

# Check if we should evict (with correlation detection)
evict_decision = tracker.should_evict("worker-1")
if evict_decision.should_evict:
    if evict_decision.correlated_failures:
        # Investigate network issue, don't evict
        pass
    else:
        # Safe to evict
        pass
```

#### Configuration

```python
# Health probe settings
LIVENESS_PROBE_TIMEOUT: float = 1.0
LIVENESS_PROBE_PERIOD: float = 10.0
LIVENESS_PROBE_FAILURE_THRESHOLD: int = 3
LIVENESS_PROBE_SUCCESS_THRESHOLD: int = 1

READINESS_PROBE_TIMEOUT: float = 2.0
READINESS_PROBE_PERIOD: float = 10.0
READINESS_PROBE_FAILURE_THRESHOLD: int = 3
READINESS_PROBE_SUCCESS_THRESHOLD: int = 1

STARTUP_PROBE_TIMEOUT: float = 5.0
STARTUP_PROBE_PERIOD: float = 5.0
STARTUP_PROBE_FAILURE_THRESHOLD: int = 30  # Allow slow startups (150s)
STARTUP_PROBE_SUCCESS_THRESHOLD: int = 1
```

#### SWIM Piggyback

Health signals are piggybacked on SWIM protocol messages for efficiency:

```python
@dataclass
class HealthPiggyback:
    node_id: str
    node_type: str  # "worker" | "manager" | "gate"
    is_alive: bool = True
    accepting_work: bool = True
    capacity: int = 0
    throughput: float = 0.0
    expected_throughput: float = 0.0
    overload_state: str = "healthy"
    timestamp: float = field(default_factory=time.monotonic)
```

#### Key Files

| File | Purpose |
|------|---------|
| `hyperscale/distributed_rewrite/health/tracker.py` | NodeHealthTracker, HealthSignals protocol |
| `hyperscale/distributed_rewrite/health/worker_health.py` | WorkerHealthState implementation |
| `hyperscale/distributed_rewrite/health/worker_health_manager.py` | Manager-side health tracking |

---

### Adaptive Healthcheck Extensions (AD-26)

Allows workers to request deadline extensions for long-running operations with graceful exhaustion handling.

#### Extension Grant Formula

Extensions use logarithmic decay to prevent indefinite delays:

```
grant = max(min_grant, base_deadline / 2^(extension_count + 1))
```

| Extension # | Formula | Grant (base=30s) | Cumulative |
|-------------|---------|------------------|------------|
| 1 | 30 / 2^1 | 15.0s | 15.0s |
| 2 | 30 / 2^2 | 7.5s | 22.5s |
| 3 | 30 / 2^3 | 3.75s | 26.25s |
| 4 | 30 / 2^4 | 1.875s | 28.125s |
| 5 | 30 / 2^5 | 1.0s (min) | 29.125s |
| 6+ | — | denied | — |

#### Graceful Exhaustion

When extensions run out, the system provides warning and grace period:

```
┌─────────────────────────────────────────────────────────────────┐
│                Graceful Exhaustion Timeline                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Extension 1  Extension 2  Extension 3  Extension 4  Extension 5│
│      │            │            │            │            │      │
│      ▼            ▼            ▼            ▼            ▼      │
│  ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐     │
│  │ 15s  │    │ 7.5s │    │3.75s │    │1.875s│    │ 1s   │     │
│  │grant │    │grant │    │grant │    │grant │    │grant │     │
│  └──────┘    └──────┘    └──────┘    └──────┘    └──┬───┘     │
│                                                      │          │
│                                           ┌──────────▼────────┐│
│                                           │  WARNING SENT     ││
│                                           │  (remaining <= 1) ││
│                                           └──────────┬────────┘│
│                                                      │          │
│                                                      ▼          │
│                                           ┌─────────────────┐  │
│                                           │  EXHAUSTED      │  │
│                                           │                 │  │
│                                           │  Grace Period   │  │
│                                           │  (10s default)  │  │
│                                           │                 │  │
│                                           │  Worker can:    │  │
│                                           │  • Checkpoint   │  │
│                                           │  • Save state   │  │
│                                           │  • Clean up     │  │
│                                           └────────┬────────┘  │
│                                                    │            │
│                                                    ▼            │
│                                           ┌─────────────────┐  │
│                                           │  EVICTION       │  │
│                                           │  (after grace)  │  │
│                                           └─────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Extension Tracker State

```python
@dataclass(slots=True)
class ExtensionTracker:
    worker_id: str
    base_deadline: float = 30.0
    min_grant: float = 1.0
    max_extensions: int = 5
    warning_threshold: int = 1      # Extensions remaining to trigger warning
    grace_period: float = 10.0      # Seconds after exhaustion before kill

    extension_count: int = 0
    last_progress: float = 0.0
    total_extended: float = 0.0
    last_extension_time: float = field(default_factory=time.monotonic)
    exhaustion_time: float | None = None
    warning_sent: bool = False

    def request_extension(
        self,
        reason: str,
        current_progress: float,
    ) -> tuple[bool, float, str | None, bool]:
        """
        Returns: (granted, extension_seconds, denial_reason, is_warning)
        """
        ...

    @property
    def is_exhausted(self) -> bool: ...

    @property
    def is_in_grace_period(self) -> bool: ...

    @property
    def grace_period_remaining(self) -> float: ...

    @property
    def should_evict(self) -> bool:
        """True if exhausted AND grace period expired."""
        ...
```

#### Extension Response Fields

```python
@dataclass
class HealthcheckExtensionResponse:
    granted: bool
    extension_seconds: float
    new_deadline: float
    remaining_extensions: int
    denial_reason: str | None = None
    is_exhaustion_warning: bool = False    # True if about to exhaust
    grace_period_remaining: float = 0.0    # Seconds remaining after exhaustion
    in_grace_period: bool = False          # True if exhausted but within grace
```

#### Configuration

```python
# Environment variables
EXTENSION_BASE_DEADLINE: float = 30.0
EXTENSION_MIN_GRANT: float = 1.0
EXTENSION_MAX_EXTENSIONS: int = 5
EXTENSION_EVICTION_THRESHOLD: int = 3
EXTENSION_EXHAUSTION_WARNING_THRESHOLD: int = 1
EXTENSION_EXHAUSTION_GRACE_PERIOD: float = 10.0
```

#### Key Files

| File | Purpose |
|------|---------|
| `hyperscale/distributed_rewrite/health/extension_tracker.py` | ExtensionTracker, ExtensionTrackerConfig |
| `hyperscale/distributed_rewrite/health/worker_health_manager.py` | WorkerHealthManager integration |
| `hyperscale/distributed_rewrite/models/distributed.py` | HealthcheckExtensionRequest/Response |

---

### Zombie Job Prevention & Detection

Multiple mechanisms work together to detect and prevent zombie jobs (jobs that appear running but are actually stuck or orphaned).

#### Detection Mechanisms

```
┌─────────────────────────────────────────────────────────────────┐
│                   Zombie Detection Mechanisms                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. TIMEOUT DETECTION                                           │
│     ├─ Per-workflow timeout (user-configured)                   │
│     ├─ Checked during progress updates                          │
│     └─ Triggers workflow failure and cleanup                    │
│                                                                 │
│  2. SWIM DEAD DETECTION                                         │
│     ├─ SWIM protocol detects unresponsive workers              │
│     ├─ States: alive → suspect → dead                          │
│     ├─ Dead workers trigger workflow reassignment              │
│     └─ Reap interval: MANAGER_DEAD_WORKER_REAP_INTERVAL (15m)  │
│                                                                 │
│  3. PROGRESS HEALTH (AD-19)                                     │
│     ├─ Three-signal model tracks progress state                │
│     ├─ States: IDLE → PROGRESSING → STALLED → STUCK            │
│     ├─ STUCK triggers investigation and potential eviction     │
│     └─ Correlation detection prevents cascade evictions        │
│                                                                 │
│  4. LEASE EXPIRY                                                │
│     ├─ Gates hold time-limited leases for jobs                 │
│     ├─ Lease duration: configurable per-job                    │
│     ├─ Expired leases allow other gates to take over           │
│     └─ Prevents single-gate failures from blocking jobs        │
│                                                                 │
│  5. ORPHAN WORKFLOW SCANNER (New)                               │
│     ├─ Manager periodically queries workers for active workflows│
│     ├─ Compares against manager's workflow assignments          │
│     ├─ Marks orphaned workflows as failed                       │
│     ├─ Interval: ORPHAN_SCAN_INTERVAL (120s)                    │
│     └─ Worker timeout: ORPHAN_SCAN_WORKER_TIMEOUT (5s)          │
│                                                                 │
│  6. EXTENSION EXHAUSTION (AD-26)                                │
│     ├─ Workers have limited extension requests                  │
│     ├─ Exhaustion triggers warning, then grace period           │
│     ├─ Grace period expiry triggers eviction                    │
│     └─ Prevents infinitely-extending stuck workflows            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Prevention Mechanisms

```
┌─────────────────────────────────────────────────────────────────┐
│                  Zombie Prevention Mechanisms                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. FENCE TOKENS                                                │
│     ├─ Monotonically increasing token per job                  │
│     ├─ Prevents stale updates from old job executions          │
│     ├─ Gates reject results with outdated fence tokens         │
│     └─ Incremented on: retry, failover, reassignment           │
│                                                                 │
│  2. VERSIONED CLOCK                                             │
│     ├─ Per-entity Lamport timestamps                           │
│     ├─ All state updates include clock version                 │
│     ├─ Rejects updates with older clock values                 │
│     └─ Ensures consistent ordering across DCs                  │
│                                                                 │
│  3. CANCELLATION POLLING                                        │
│     ├─ Workers poll manager for job cancellation status        │
│     ├─ Interval: WORKER_CANCELLATION_POLL_INTERVAL (5s)        │
│     ├─ Catches cancellations even if push notification fails   │
│     └─ Self-termination on discovering cancelled state         │
│                                                                 │
│  4. QUORUM CONFIRMATION                                         │
│     ├─ Critical state changes require manager quorum           │
│     ├─ Prevents split-brain scenarios                          │
│     └─ Failed quorum blocks state transition                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Orphan Workflow Scanner

The orphan scanner runs periodically on managers to detect workflows that:
- Are tracked by the manager but not running on any worker
- Are running on workers but not tracked by the manager

```python
async def _orphan_workflow_scan_loop(self) -> None:
    """Background loop that scans for orphaned workflows."""
    while not self._shutdown_event.is_set():
        try:
            await asyncio.sleep(self._orphan_scan_interval)

            # Get all known workflow IDs from manager state
            known_workflow_ids = set(self._workflow_assignments.keys())

            # Query each worker for active workflows
            worker_workflows: dict[str, set[str]] = {}
            for worker_id, registration in self._workers.items():
                active_ids = await self._query_worker_workflows(
                    worker_id,
                    registration.address,
                )
                worker_workflows[worker_id] = active_ids

            # Find orphans: known to manager but not on any worker
            all_worker_workflows = set()
            for workflows in worker_workflows.values():
                all_worker_workflows.update(workflows)

            orphaned = known_workflow_ids - all_worker_workflows

            # Mark orphaned workflows as failed
            for workflow_id in orphaned:
                await self._mark_workflow_failed(
                    workflow_id,
                    "Orphaned - not found on any worker",
                )
```

#### Configuration

```python
# Dead node reaping
MANAGER_DEAD_WORKER_REAP_INTERVAL: float = 900.0  # 15 minutes
MANAGER_DEAD_PEER_REAP_INTERVAL: float = 900.0
MANAGER_DEAD_GATE_REAP_INTERVAL: float = 900.0
WORKER_DEAD_MANAGER_REAP_INTERVAL: float = 900.0

# Job cleanup
COMPLETED_JOB_MAX_AGE: float = 300.0  # 5 minutes
FAILED_JOB_MAX_AGE: float = 3600.0    # 1 hour
JOB_CLEANUP_INTERVAL: float = 60.0

# Orphan scanning
ORPHAN_SCAN_INTERVAL: float = 120.0       # 2 minutes
ORPHAN_SCAN_WORKER_TIMEOUT: float = 5.0

# Cancellation polling
WORKER_CANCELLATION_POLL_INTERVAL: float = 5.0
```

---

### Per-Workflow Result Streaming

Results are streamed from workers to managers to gates to clients as workflows complete, rather than waiting for entire jobs to finish.

#### Streaming Flow

```
┌─────────────────────────────────────────────────────────────────┐
│               Per-Workflow Result Streaming                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Worker                Manager               Gate        Client │
│    │                     │                    │            │    │
│    │─ WorkflowResult ───►│                    │            │    │
│    │  (wf-001 complete)  │                    │            │    │
│    │                     │─ WorkflowResult ──►│            │    │
│    │                     │  (aggregated)      │            │    │
│    │                     │                    │─ Stream ──►│    │
│    │                     │                    │  Result    │    │
│    │                     │                    │            │    │
│    │─ WorkflowResult ───►│                    │            │    │
│    │  (wf-002 complete)  │                    │            │    │
│    │                     │─ WorkflowResult ──►│            │    │
│    │                     │                    │─ Stream ──►│    │
│    │                     │                    │            │    │
│    │                     │                    │            │    │
│    │  [All workflows complete]                │            │    │
│    │                     │                    │            │    │
│    │                     │─ JobComplete ─────►│            │    │
│    │                     │                    │─ Final ───►│    │
│    │                     │                    │  Summary   │    │
│                                                                 │
│  Benefits:                                                      │
│  • Real-time progress visibility                                │
│  • Early failure detection                                      │
│  • Lower latency for time-sensitive results                     │
│  • Memory efficiency (results processed incrementally)          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Client API

```python
client = HyperscaleClient(gate_tcp_addrs=[...])
await client.start()

# Submit job
job_id = await client.submit_job(submission)

# Stream results as they arrive
async for workflow_result in client.stream_workflow_results(job_id):
    print(f"Workflow {workflow_result.workflow_id}: {workflow_result.status}")
    # Process individual workflow results...

# Or wait for all results
final_result = await client.wait_for_completion(job_id)
```

---

### Time Alignment for Cross-DC Aggregation

When aggregating results across datacenters, clock skew must be handled to produce accurate timing metrics.

#### Clock Synchronization

```
┌─────────────────────────────────────────────────────────────────┐
│              Cross-DC Time Alignment                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Problem: Different DCs have different wall-clock times         │
│                                                                 │
│  DC-West (PDT)        DC-East (EDT)        DC-EU (CET)         │
│  10:00:00.000         13:00:00.050         19:00:00.120        │
│       │                    │                    │               │
│       │  Clock skew: 50ms  │  Clock skew: 70ms  │              │
│       │                    │                    │               │
│                                                                 │
│  Solution: Versioned Clock with Lamport timestamps              │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ VersionedClock                                            │  │
│  │                                                           │  │
│  │ • Logical clock increments on each event                  │  │
│  │ • Merged with received clock on message receipt           │  │
│  │ • Provides total ordering without wall-clock dependency   │  │
│  │                                                           │  │
│  │ clock_value = max(local_clock, received_clock) + 1        │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  For latency metrics:                                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Monotonic Time Basis                                      │  │
│  │                                                           │  │
│  │ • All timing within a node uses time.monotonic()          │  │
│  │ • Cross-node timing uses relative deltas                  │  │
│  │ • Aggregation preserves statistical properties            │  │
│  │   (min, max, mean, percentiles all computed from deltas)  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

### Datacenter List Query

Clients can query gates for the list of registered datacenters.

#### API

```python
# Client-side
client = HyperscaleClient(gate_tcp_addrs=[...])
await client.start()

# Query available datacenters
datacenters = await client.get_datacenters()
# Returns: ["us-west-1", "us-east-1", "eu-west-1", ...]

# Submit job to specific datacenters
submission = JobSubmission(
    workflows=[...],
    target_datacenters=["us-west-1", "us-east-1"],
)
```

#### Message Types

```python
@dataclass
class DatacenterListRequest:
    """Request to list available datacenters."""
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))

@dataclass
class DatacenterListResponse:
    """Response containing available datacenters."""
    request_id: str
    datacenters: list[str]
    timestamp: float = field(default_factory=time.time)
```

#### Handler (Gate)

```python
@tcp.receive()
async def datacenter_list(self, addr, data, clock_time):
    """Handle datacenter list query from client."""
    request = DatacenterListRequest.load(data)

    # Collect datacenter IDs from known managers
    datacenter_ids = list(self._datacenter_status.keys())

    response = DatacenterListResponse(
        request_id=request.request_id,
        datacenters=datacenter_ids,
    )

    return response.dump()
```

---

### Known Issues to Investigate

---

### Commands for Quick Resume

```bash
# Run all existing tests
python examples/servers/test_single_worker.py
python examples/servers/test_workflow_end_to_end.py  
python examples/servers/test_workflow_stats_push.py
python examples/servers/test_gate_results_aggregation.py

# Check for regressions
cd /home/ada/Projects/hyperscale
git status
git log --oneline -10

# Current branch
git branch --show-current  # AL-distributed-wip
```

---

## License

See the main project LICENSE file.


---

## Worker → Manager Progress Update Architecture

### Overview

Workers collect progress updates from their local workflow execution (via `RemoteGraphManager`) and send them to the job leader Manager. This system is designed to be:

1. **Lossless** - Every progress update is captured (no dropped samples)
2. **Backpressure-aware** - Respects Manager overload signals
3. **Lifecycle-immediate** - Status transitions (STARTED, COMPLETED, FAILED) are sent immediately
4. **Rate-controlled** - Regular progress updates are batched to avoid Manager spam

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    WORKER PROGRESS UPDATE FLOW                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Local Workflow Execution (Subprocess Pool)                                  │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  RemoteGraphController (subprocess)                                   │   │
│  │                                                                       │   │
│  │  ┌─────────────────┐    ┌─────────────────┐                          │   │
│  │  │ push_workflow_  │    │ aggregate_      │                          │   │
│  │  │ status_update   │───►│ status_updates  │                          │   │
│  │  │ (0.1s schedule) │    │ (0.05s schedule)│                          │   │
│  │  └─────────────────┘    └────────┬────────┘                          │   │
│  │                                  │                                    │   │
│  │                     completion_state.status_update_queue              │   │
│  │                                  │                                    │   │
│  └──────────────────────────────────┼───────────────────────────────────┘   │
│                                     │                                        │
│  Worker (Main Process)              │                                        │
│  ┌──────────────────────────────────┼───────────────────────────────────┐   │
│  │                                  ▼                                    │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │   │
│  │  │               RemoteGraphManager (Leader Process)               │ │   │
│  │  │                                                                 │ │   │
│  │  │  ┌───────────────────────┐    ┌──────────────────────────────┐ │ │   │
│  │  │  │ _wait_for_workflow_   │    │ get_availability()           │ │ │   │
│  │  │  │ completion loop       │    │ (sync, non-blocking)         │ │ │   │
│  │  │  │                       │    │                              │ │ │   │
│  │  │  │ • Poll status queue   │    │ Returns: (assigned,          │ │ │   │
│  │  │  │ • Update stats        │    │           completed,         │ │ │   │
│  │  │  │ • Call callback       │    │           available)         │ │ │   │
│  │  │  └───────────┬───────────┘    └──────────────────────────────┘ │ │   │
│  │  │              │                                                  │ │   │
│  │  └──────────────┼──────────────────────────────────────────────────┘ │   │
│  │                 │                                                     │   │
│  │                 ▼                                                     │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐ │   │
│  │  │               _monitor_workflow_progress()                       │ │   │
│  │  │                                                                  │ │   │
│  │  │  • Convert WorkflowStatusUpdate → WorkflowProgress              │ │   │
│  │  │  • Add core allocation info from CoreAllocator                  │ │   │
│  │  │  • Add CPU/memory metrics                                       │ │   │
│  │  │  • Call _send_progress_update() [BUFFER]                        │ │   │
│  │  │                                                                  │ │   │
│  │  └───────────────────────────────┬─────────────────────────────────┘ │   │
│  │                                  │                                    │   │
│  │          ┌───────────────────────┴───────────────────────┐           │   │
│  │          │                                               │           │   │
│  │          ▼                                               ▼           │   │
│  │  ┌───────────────────────┐                 ┌────────────────────────┐│   │
│  │  │ _progress_buffer      │                 │ _transition_workflow_  ││   │
│  │  │ (dict: workflow_id →  │                 │ status()               ││   │
│  │  │  latest progress)     │                 │                        ││   │
│  │  │                       │                 │ For: STARTED,          ││   │
│  │  │ Latest-wins: only     │                 │      COMPLETED,        ││   │
│  │  │ most recent per       │                 │      FAILED            ││   │
│  │  │ workflow kept         │                 │                        ││   │
│  │  └───────────┬───────────┘                 │ → Immediate send       ││   │
│  │              │                             │   (bypass buffer)      ││   │
│  │              │                             └───────────┬────────────┘│   │
│  │              ▼                                         │             │   │
│  │  ┌───────────────────────┐                             │             │   │
│  │  │ _progress_flush_loop  │                             │             │   │
│  │  │ (background task)     │                             │             │   │
│  │  │                       │                             │             │   │
│  │  │ • Sleep for interval  │                             │             │   │
│  │  │   (50ms default)      │                             │             │   │
│  │  │ • Check backpressure  │                             │             │   │
│  │  │ • Clear buffer        │                             │             │   │
│  │  │ • Send to job leader  │                             │             │   │
│  │  └───────────┬───────────┘                             │             │   │
│  │              │                                         │             │   │
│  │              └─────────────────────┬───────────────────┘             │   │
│  │                                    │                                  │   │
│  └────────────────────────────────────┼─────────────────────────────────┘   │
│                                       │                                      │
│                                       ▼                                      │
│                     ┌─────────────────────────────────────┐                 │
│                     │   _send_progress_to_job_leader()    │                 │
│                     │                                     │                 │
│                     │ Routes to the Manager that          │                 │
│                     │ dispatched this workflow (not       │                 │
│                     │ necessarily primary manager)        │                 │
│                     │                                     │                 │
│                     │ Handles:                            │                 │
│                     │ • Job leader discovery              │                 │
│                     │ • Failover to new leader            │                 │
│                     │ • Circuit breaker per manager       │                 │
│                     └──────────────────┬──────────────────┘                 │
│                                        │                                     │
└────────────────────────────────────────┼─────────────────────────────────────┘
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │   Manager (TCP)     │
                              │                     │
                              │ workflow_progress() │
                              │ handler             │
                              └─────────────────────┘
```

### Key Components

#### 1. RemoteGraphManager State Tracking

The `RemoteGraphManager` maintains core availability as simple state (not a queue):

```python
class RemoteGraphManager:
    def __init__(self, ...):
        # Latest core availability state (assigned, completed, available)
        # Updated atomically - readers get current value immediately
        self._latest_availability: tuple[int, int, int] = (0, 0, 0)

    def get_availability(self) -> tuple[int, int, int]:
        """
        Get the current core availability state.

        Returns (assigned, completed, available) tuple.
        This is NON-BLOCKING and returns immediately.
        """
        return self._latest_availability

    def _update_available_cores(self, assigned: int, completed: int):
        """Update state atomically and notify if cores freed."""
        available = self._threads - max(assigned - completed, 0)
        self._latest_availability = (assigned, completed, available)

        # Instant callback if cores became available
        if self._on_cores_available and available > 0:
            self._on_cores_available(available)
```

**Why state-based, not queue-based?**
- Progress updates are cumulative (totals, not deltas)
- We only care about the *current* state, not history
- Queue-based `await queue.get()` blocked when empty, causing 5+ second delays
- State-based reads are instant and non-blocking

#### 2. Progress Buffer (Latest-Wins)

The Worker maintains a simple buffer that keeps only the latest progress per workflow:

```python
class WorkerServer:
    def __init__(self, ...):
        self._progress_buffer: dict[str, WorkflowProgress] = {}
        self._progress_buffer_lock = asyncio.Lock()
        self._progress_flush_interval: float = env.WORKER_PROGRESS_FLUSH_INTERVAL  # 50ms

    async def _send_progress_update(self, progress: WorkflowProgress) -> None:
        """
        Buffer a progress update for batched sending.

        Instead of sending immediately, updates are collected in a buffer
        and flushed periodically by _progress_flush_loop.
        """
        async with self._progress_buffer_lock:
            # Latest-wins: only keep most recent per workflow
            self._progress_buffer[progress.workflow_id] = progress
```

**Why latest-wins?**
- Progress is cumulative (`completed_count` is total, not delta)
- Old samples are superseded by newer ones
- No need for complex aggregation
- Memory bounded: O(active_workflows)

#### 3. Flush Loop (Backpressure-Aware)

```python
async def _progress_flush_loop(self) -> None:
    """Background loop that flushes buffered progress to manager."""
    while self._running:
        # Respect backpressure signals from managers
        effective_interval = self._get_effective_flush_interval()
        await asyncio.sleep(effective_interval)

        # Drop updates under heavy backpressure
        if self._get_max_backpressure_level() >= BackpressureLevel.REJECT:
            async with self._progress_buffer_lock:
                self._progress_buffer.clear()
            continue

        # Get and clear buffer atomically
        async with self._progress_buffer_lock:
            if not self._progress_buffer:
                continue
            updates_to_send = dict(self._progress_buffer)
            self._progress_buffer.clear()

        # Send to job leaders
        if self._healthy_manager_ids:
            for workflow_id, progress in updates_to_send.items():
                await self._send_progress_to_job_leader(progress)

def _get_effective_flush_interval(self) -> float:
    """Increase interval when managers signal backpressure."""
    base = self._progress_flush_interval  # 50ms
    if self._backpressure_delay_ms > 0:
        return base + (self._backpressure_delay_ms / 1000.0)
    return base
```

#### 4. Lifecycle Events (Immediate Send)

Status transitions bypass the buffer for immediate visibility:

```python
async def _transition_workflow_status(
    self,
    progress: WorkflowProgress,
    new_status: WorkflowStatus,
    start_time: float | None = None,
) -> None:
    """
    Transition workflow to a new status with IMMEDIATE send.

    This is the ONLY method that should change workflow status.
    Lifecycle events (STARTED, COMPLETED, FAILED) are always sent
    immediately to ensure visibility even for short workflows.
    """
    progress.status = new_status.value
    progress.timestamp = time.monotonic()
    progress.collected_at = time.time()

    if start_time is not None:
        progress.elapsed_seconds = time.monotonic() - start_time

    # Always send lifecycle transitions immediately (bypass buffer)
    if self._healthy_manager_ids:
        await self._send_progress_update_direct(progress)
```

### Job Leader Routing

Progress updates are routed to the Manager that dispatched the workflow:

```python
async def _send_progress_to_job_leader(
    self,
    progress: WorkflowProgress,
) -> bool:
    """
    Send progress to the job leader for this workflow.

    Routes to the manager that dispatched (job leader).
    Handles failover if job leader becomes unhealthy.
    """
    workflow_id = progress.workflow_id
    job_leader_addr = self._workflow_job_leader.get(workflow_id)

    # Try job leader first
    if job_leader_addr:
        success = await self._try_send_progress_to_addr(progress, job_leader_addr)
        if success:
            return True

        # Job leader failed - need to find new leader
        # Query any healthy manager for the current leader

    # Fallback: query healthy managers for job leader
    for manager_id in list(self._healthy_manager_ids):
        manager_info = self._known_managers.get(manager_id)
        if manager_info:
            success = await self._try_send_progress_to_addr(
                progress,
                (manager_info.host, manager_info.tcp_port)
            )
            if success:
                # Ack includes current job leader address - update routing
                return True

    return False
```

### Configuration

Environment variables in `Env`:

```python
# Worker progress update configuration
WORKER_PROGRESS_UPDATE_INTERVAL: float = 0.1    # How often to poll status queue (100ms)
WORKER_PROGRESS_FLUSH_INTERVAL: float = 0.05    # How often to flush buffer (50ms)

# Backpressure (AD-23)
# Managers can signal workers to slow down progress updates
# by including BackpressureSignal in progress acks
```

### Flow Comparison: Before vs After

**Before (Inline Rate-Limiting):**
```
[status update] → [rate limit check] → [send if time passed]
                          ↓
                   (DROP if too soon)
```
- Updates could be dropped
- No backpressure awareness
- Competed with flush loop

**After (Buffer + Flush):**
```
[status update] → [_progress_buffer] → [flush loop] → [send]
                   (latest-wins)       (controlled)
```
- No updates dropped (latest kept)
- Backpressure-aware
- Single unified mechanism
- Lifecycle events bypass for immediacy

### Integration with Windowed Stats

This Worker → Manager flow feeds into the Manager's `WindowedStatsCollector`:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    END-TO-END PROGRESS FLOW                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────┐    ┌────────────┐                                           │
│  │  Worker 1  │    │  Worker 2  │                                           │
│  │            │    │            │                                           │
│  │ [buffer]   │    │ [buffer]   │     Worker → Manager                      │
│  │ [flush]    │    │ [flush]    │     (This section)                        │
│  └─────┬──────┘    └─────┬──────┘                                           │
│        │                 │                                                   │
│        │ WorkflowProgress│                                                   │
│        │ (50ms batched)  │                                                   │
│        │                 │                                                   │
│        └────────┬────────┘                                                   │
│                 ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                    MANAGER                                               ││
│  │                                                                          ││
│  │  workflow_progress() ──► WindowedStatsCollector                          ││
│  │         │                    │                                           ││
│  │         │                    │ (time-bucketed windows)                   ││
│  │         │                    │ (drift tolerance)                         ││
│  │         │                    │ (aggregation)                             ││
│  │         │                    ▼                                           ││
│  │         │              [flush closed windows]                            ││
│  │         │                    │                                           ││
│  └─────────┼────────────────────┼───────────────────────────────────────────┘│
│            │                    │                                            │
│            │                    │ WindowedStatsPush                          │
│            │                    │ (50ms aggregated)                          │
│            ▼                    ▼                                            │
│   ┌─────────────────┐    ┌─────────────────┐                                │
│   │ Job tracking    │    │ Client/Gate     │     Manager → Client           │
│   │ (internal)      │    │ (streaming)     │     (Next section)             │
│   └─────────────────┘    └─────────────────┘                                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Time-Windowed Streaming Stats System

### Overview

The streaming stats system provides real-time progress updates from workers to clients while:
1. **Correlating stats across workers by time** - Stats from different workers within the same time window are aggregated together
2. **Preventing client spam** - One aggregated push per window interval instead of per-worker updates
3. **Bounding memory usage** - Windows are cleared after each push cycle
4. **Supporting hierarchical aggregation** - Manager aggregates for direct clients; Gate aggregates across DCs

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    TIME-WINDOWED STATS FLOW                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Workers (rapid updates ~1s)                                                │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐                        │
│  │Worker 1 │  │Worker 2 │  │Worker 3 │  │Worker N │                        │
│  │ t=0.1s  │  │ t=0.15s │  │ t=0.12s │  │ t=0.18s │  ← collected_at        │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘    (Unix timestamp)    │
│       │            │            │            │                              │
│       └────────────┴─────┬──────┴────────────┘                              │
│                          ▼                                                  │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    MANAGER - WindowedStatsCollector                   │  │
│  ├───────────────────────────────────────────────────────────────────────┤  │
│  │                                                                       │  │
│  │  Time Windows (100ms buckets):                                        │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │  │
│  │  │ Window T=0  │  │ Window T=1  │  │ Window T=2  │  ...              │  │
│  │  │ [0ms-100ms) │  │[100ms-200ms)│  │[200ms-300ms)│                   │  │
│  │  │             │  │             │  │             │                   │  │
│  │  │ Worker1 ──┐ │  │ Worker2 ──┐ │  │ Worker1 ──┐ │                   │  │
│  │  │ Worker3 ──┼─│  │ Worker4 ──┼─│  │ Worker2 ──┼─│                   │  │
│  │  │ Worker2 ──┘ │  │ Worker1 ──┘ │  │ Worker3 ──┘ │                   │  │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                   │  │
│  │         │                │                │                           │  │
│  │         ▼                ▼                ▼                           │  │
│  │    [aggregate]      [aggregate]      [aggregate]                      │  │
│  │         │                │                │                           │  │
│  │         └────────────────┼────────────────┘                           │  │
│  │                          │                                            │  │
│  │  Flush Timer (100ms)     │                                            │  │
│  │  ────────────────────────┼──────────────────────────────              │  │
│  │                          ▼                                            │  │
│  │              ┌───────────────────────┐                                │  │
│  │              │  Closed windows only  │                                │  │
│  │              │  (T < current - drift)│                                │  │
│  │              └───────────┬───────────┘                                │  │
│  │                          │                                            │  │
│  └──────────────────────────┼────────────────────────────────────────────┘  │
│                             │                                               │
│          ┌──────────────────┴──────────────────┐                           │
│          │                                     │                           │
│          ▼                                     ▼                           │
│  ┌───────────────────┐               ┌─────────────────────┐               │
│  │  Direct Client    │               │       Gate          │               │
│  │  (aggregated)     │               │   (unaggregated)    │               │
│  │                   │               │                     │               │
│  │  WindowedStatsPush│               │  WindowedStatsPush  │               │
│  │  - window_start   │               │  - window_start     │               │
│  │  - window_end     │               │  - window_end       │               │
│  │  - aggregated:    │               │  - per_worker:      │               │
│  │    completed,     │               │    [{worker_id,     │               │
│  │    failed,        │               │      completed,     │               │
│  │    rate,          │               │      failed, ...}]  │               │
│  │    step_stats     │               │                     │               │
│  └───────────────────┘               └──────────┬──────────┘               │
│                                                 │                          │
│                                                 ▼                          │
│                                      ┌─────────────────────┐               │
│                                      │  Gate Aggregation   │               │
│                                      │  (same windowing)   │               │
│                                      │                     │               │
│                                      │  Correlates windows │               │
│                                      │  across DCs         │               │
│                                      └──────────┬──────────┘               │
│                                                 │                          │
│                                                 ▼                          │
│                                      ┌─────────────────────┐               │
│                                      │      Client         │               │
│                                      │   (aggregated)      │               │
│                                      └─────────────────────┘               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Time Window Bucketing

Stats are bucketed by their `collected_at` Unix timestamp into discrete windows:

```python
WINDOW_SIZE_MS = 100  # 100ms windows
DRIFT_TOLERANCE_MS = 50  # Allow 50ms clock drift between workers

def get_window_bucket(collected_at: float) -> int:
    """Convert Unix timestamp to window bucket number."""
    return int(collected_at * 1000 / WINDOW_SIZE_MS)

def is_window_closed(bucket: int, now: float) -> bool:
    """Check if a window can be flushed (all expected stats have arrived)."""
    window_end_ms = (bucket + 1) * WINDOW_SIZE_MS
    current_ms = now * 1000
    # Window is closed when current time exceeds window_end + drift tolerance
    return current_ms > window_end_ms + DRIFT_TOLERANCE_MS
```

### WindowedStatsCollector Class

Located at `hyperscale/distributed_rewrite/jobs/windowed_stats_collector.py`:

```python
@dataclass
class WindowBucket:
    """Stats collected within a single time window."""
    window_start: float  # Unix timestamp of window start
    window_end: float    # Unix timestamp of window end
    job_id: str
    workflow_id: str
    worker_stats: dict[str, WorkflowProgress]  # worker_id -> progress
    created_at: float    # When this bucket was created (for cleanup)

class WindowedStatsCollector:
    """
    Collects workflow progress updates into time-correlated windows.
    
    Thread-safe for concurrent progress updates from multiple workers.
    """
    
    def __init__(
        self,
        window_size_ms: float = 100.0,
        drift_tolerance_ms: float = 50.0,
        max_window_age_ms: float = 5000.0,  # Cleanup windows older than 5s
    ):
        self._window_size_ms = window_size_ms
        self._drift_tolerance_ms = drift_tolerance_ms
        self._max_window_age_ms = max_window_age_ms
        
        # Buckets indexed by (job_id, workflow_id, bucket_number)
        self._buckets: dict[tuple[str, str, int], WindowBucket] = {}
        self._lock = asyncio.Lock()
    
    async def add_progress(
        self,
        worker_id: str,
        progress: WorkflowProgress,
    ) -> None:
        """Add a progress update to the appropriate time window."""
        bucket_num = self._get_bucket_number(progress.collected_at)
        key = (progress.job_id, progress.workflow_id, bucket_num)
        
        async with self._lock:
            if key not in self._buckets:
                self._buckets[key] = WindowBucket(
                    window_start=bucket_num * self._window_size_ms / 1000,
                    window_end=(bucket_num + 1) * self._window_size_ms / 1000,
                    job_id=progress.job_id,
                    workflow_id=progress.workflow_id,
                    worker_stats={},
                    created_at=time.time(),
                )
            
            self._buckets[key].worker_stats[worker_id] = progress
    
    async def flush_closed_windows(
        self,
        aggregate: bool = True,
    ) -> list[WindowedStatsPush]:
        """
        Flush all closed windows and return them for pushing.
        
        Args:
            aggregate: If True, aggregate stats within window.
                      If False, return per-worker stats (for Gate forwarding).
        
        Returns:
            List of WindowedStatsPush messages ready for client/gate.
        """
        now = time.time()
        results = []
        keys_to_remove = []
        
        async with self._lock:
            for key, bucket in self._buckets.items():
                _, _, bucket_num = key
                
                if self._is_window_closed(bucket_num, now):
                    if aggregate:
                        push = self._aggregate_bucket(bucket)
                    else:
                        push = self._unaggregated_bucket(bucket)
                    results.append(push)
                    keys_to_remove.append(key)
                
                # Also cleanup very old windows (missed or stuck)
                elif (now - bucket.created_at) * 1000 > self._max_window_age_ms:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self._buckets[key]
        
        return results
    
    def _aggregate_bucket(self, bucket: WindowBucket) -> WindowedStatsPush:
        """Aggregate all worker stats in a bucket into single stats."""
        total_completed = 0
        total_failed = 0
        total_rate = 0.0
        step_stats_by_name: dict[str, StepStats] = {}
        
        for progress in bucket.worker_stats.values():
            total_completed += progress.completed_count
            total_failed += progress.failed_count
            total_rate += progress.rate_per_second
            
            for step in progress.step_stats:
                if step.step_name in step_stats_by_name:
                    existing = step_stats_by_name[step.step_name]
                    step_stats_by_name[step.step_name] = StepStats(
                        step_name=step.step_name,
                        completed_count=existing.completed_count + step.completed_count,
                        failed_count=existing.failed_count + step.failed_count,
                        total_count=existing.total_count + step.total_count,
                    )
                else:
                    step_stats_by_name[step.step_name] = step
        
        return WindowedStatsPush(
            job_id=bucket.job_id,
            workflow_id=bucket.workflow_id,
            window_start=bucket.window_start,
            window_end=bucket.window_end,
            completed_count=total_completed,
            failed_count=total_failed,
            rate_per_second=total_rate,
            step_stats=list(step_stats_by_name.values()),
            worker_count=len(bucket.worker_stats),
            is_aggregated=True,
        )
```

### Message Types

```python
@dataclass(slots=True)
class WindowedStatsPush(Message):
    """
    Time-windowed stats push to client or gate.
    
    When is_aggregated=True (for clients):
        - Contains aggregated stats across all workers in window
        - step_stats are merged by step name
        
    When is_aggregated=False (for gates):
        - per_worker_stats contains individual worker progress
        - Gate performs its own aggregation across DCs
    """
    job_id: str
    workflow_id: str
    workflow_name: str = ""
    window_start: float = 0.0  # Unix timestamp
    window_end: float = 0.0    # Unix timestamp
    
    # Aggregated stats (when is_aggregated=True)
    completed_count: int = 0
    failed_count: int = 0
    rate_per_second: float = 0.0
    step_stats: list[StepStats] = field(default_factory=list)
    worker_count: int = 0
    
    # Per-worker stats (when is_aggregated=False, for gate forwarding)
    per_worker_stats: list[WorkerWindowStats] = field(default_factory=list)
    
    is_aggregated: bool = True
    datacenter: str = ""  # Set by manager when forwarding to gate


@dataclass(slots=True)
class WorkerWindowStats(Message):
    """Individual worker stats within a time window."""
    worker_id: str
    completed_count: int = 0
    failed_count: int = 0
    rate_per_second: float = 0.0
    step_stats: list[StepStats] = field(default_factory=list)
```

### Manager Integration

The Manager integrates the WindowedStatsCollector into its workflow progress handling:

```python
class ManagerServer:
    def __init__(self, ...):
        ...
        # Windowed stats for streaming to clients
        self._windowed_stats = WindowedStatsCollector(
            window_size_ms=env.STATS_WINDOW_SIZE_MS,      # Default: 100ms
            drift_tolerance_ms=env.STATS_DRIFT_TOLERANCE_MS,  # Default: 50ms
        )
        
    async def workflow_progress(self, addr, data, clock_time):
        """Handle workflow progress update from worker."""
        progress = WorkflowProgress.load(data)
        
        # Add to windowed collector for streaming
        worker_id = self._resolve_worker_id_from_addr(addr)
        await self._windowed_stats.add_progress(worker_id, progress)
        
        # ... existing progress handling ...
    
    async def _windowed_stats_push_loop(self):
        """Background loop to flush and push windowed stats."""
        interval = self._env.STATS_PUSH_INTERVAL  # Default: 100ms
        
        while self._running:
            await asyncio.sleep(interval / 1000)
            
            # Determine if we're pushing to clients or gates
            has_gates = bool(self._gate_addrs or self._known_gates)
            
            # Flush closed windows
            pushes = await self._windowed_stats.flush_closed_windows(
                aggregate=not has_gates  # Aggregate for clients, not for gates
            )
            
            if not pushes:
                continue
            
            if has_gates:
                # Forward unaggregated to gates
                for push in pushes:
                    push.datacenter = self._node_id.datacenter
                    await self._forward_stats_to_gates(push)
            else:
                # Push aggregated to clients
                for push in pushes:
                    await self._push_stats_to_client(push)
```

### Gate Integration

Gates receive unaggregated windowed stats from managers and perform cross-DC aggregation:

```python
class GateServer:
    def __init__(self, ...):
        ...
        # Collect stats from all DCs for cross-DC aggregation
        self._dc_windowed_stats: dict[str, WindowedStatsCollector] = {}
        
    @tcp.receive()
    async def windowed_stats_push(self, addr, data, clock_time):
        """Receive windowed stats from a manager."""
        push = WindowedStatsPush.load(data)
        
        # Store in per-DC collector
        dc_id = push.datacenter
        if dc_id not in self._dc_windowed_stats:
            self._dc_windowed_stats[dc_id] = WindowedStatsCollector()
        
        # Re-add each worker's stats to preserve window alignment
        for worker_stats in push.per_worker_stats:
            # Create a synthetic progress for the collector
            progress = WorkflowProgress(
                job_id=push.job_id,
                workflow_id=push.workflow_id,
                collected_at=push.window_start,  # Use window start for alignment
                completed_count=worker_stats.completed_count,
                ...
            )
            await self._dc_windowed_stats[dc_id].add_progress(
                f"{dc_id}:{worker_stats.worker_id}",
                progress,
            )
        
        return b'ok'
    
    async def _gate_windowed_stats_push_loop(self):
        """Aggregate across DCs and push to clients."""
        interval = self._env.STATS_PUSH_INTERVAL
        
        while self._running:
            await asyncio.sleep(interval / 1000)
            
            # Collect and aggregate from all DCs
            all_pushes: dict[tuple[str, str, float], list[WindowedStatsPush]] = {}
            
            for dc_id, collector in self._dc_windowed_stats.items():
                pushes = await collector.flush_closed_windows(aggregate=True)
                for push in pushes:
                    key = (push.job_id, push.workflow_id, push.window_start)
                    if key not in all_pushes:
                        all_pushes[key] = []
                    all_pushes[key].append(push)
            
            # Aggregate same-window stats across DCs
            for key, dc_pushes in all_pushes.items():
                aggregated = self._aggregate_dc_pushes(dc_pushes)
                await self._push_stats_to_client(aggregated)
```

### Client Integration

The client receives windowed stats via a new `on_progress_update` callback:

```python
class HyperscaleClient:
    async def submit_job(
        self,
        workflows: list[type],
        ...
        on_status_update: Callable[[JobStatusPush], None] | None = None,
        on_progress_update: Callable[[WindowedStatsPush], None] | None = None,  # NEW
        on_workflow_result: Callable[[WorkflowResultPush], None] | None = None,
        ...
    ) -> str:
        """
        Submit a job for execution.
        
        Args:
            ...
            on_status_update: Callback for job status changes (started, completed, failed)
            on_progress_update: Callback for streaming progress stats (time-windowed)
            on_workflow_result: Callback for workflow completion results
        """
        ...
        if on_progress_update:
            self._progress_callbacks[job_id] = on_progress_update
    
    @tcp.receive()
    async def windowed_stats_push(self, addr, data, clock_time):
        """Handle windowed stats push from manager/gate."""
        push = WindowedStatsPush.load(data)
        
        callback = self._progress_callbacks.get(push.job_id)
        if callback:
            try:
                callback(push)
            except Exception:
                pass
        
        return b'ok'
```

### Client Rate Limiting (Stats Updates Only)

The client applies rate limiting specifically to `windowed_stats_push` to prevent overwhelming the callback:

```python
class HyperscaleClient:
    def __init__(self, ...):
        ...
        # Rate limit for progress updates (stats streaming)
        self._progress_rate_limit = RateLimiter(
            max_per_second=env.CLIENT_PROGRESS_RATE_LIMIT,  # Default: 20/sec
            burst=env.CLIENT_PROGRESS_BURST,  # Default: 5
        )
    
    @tcp.receive()
    async def windowed_stats_push(self, addr, data, clock_time):
        """Handle windowed stats push with rate limiting."""
        # Apply rate limiting - drop if over limit
        if not self._progress_rate_limit.try_acquire():
            return b'rate_limited'
        
        push = WindowedStatsPush.load(data)
        
        callback = self._progress_callbacks.get(push.job_id)
        if callback:
            try:
                callback(push)
            except Exception:
                pass
        
        return b'ok'
```

### Configuration

New environment variables in `Env`:

```python
# Stats windowing
STATS_WINDOW_SIZE_MS: float = 100.0        # Window bucket size
STATS_DRIFT_TOLERANCE_MS: float = 50.0     # Clock drift tolerance
STATS_PUSH_INTERVAL: float = 100.0         # How often to flush windows (ms)

# Client rate limiting (progress updates only)
CLIENT_PROGRESS_RATE_LIMIT: float = 20.0   # Max progress callbacks per second
CLIENT_PROGRESS_BURST: int = 5             # Burst allowance
```

### Memory Management

Windows are automatically cleaned up:

1. **On flush**: Closed windows are removed after being pushed
2. **Age-based cleanup**: Windows older than `max_window_age_ms` (default 5s) are dropped
3. **Job completion**: All windows for a job are cleared when job completes

```python
async def cleanup_job_windows(self, job_id: str) -> None:
    """Remove all windows for a completed job."""
    async with self._lock:
        keys_to_remove = [
            key for key in self._buckets.keys()
            if key[0] == job_id
        ]
        for key in keys_to_remove:
            del self._buckets[key]
```

### Sequence Diagram

```
Worker1     Worker2     Manager           Gate           Client
   │           │           │                │               │
   │──progress─▶│          │                │               │
   │  t=0.12s  │──progress─▶                │               │
   │           │  t=0.15s  │                │               │
   │           │           │                │               │
   │           │      [bucket 0: W1, W2]    │               │
   │           │           │                │               │
   │           │      (100ms flush timer)   │               │
   │           │           │                │               │
   │           │      [window closed]       │               │
   │           │           │                │               │
   │           │           │──(unaggregated)─▶              │
   │           │           │  WindowedStats  │              │
   │           │           │                │              │
   │           │           │                │──(aggregated)─▶
   │           │           │                │ WindowedStats │
   │           │           │                │               │
   │           │           │                │     [callback]│
```

---

## Bootstrap & Service Discovery

### Design Goals

The bootstrap system must satisfy these requirements:

1. **Environment Agnostic**: Works identically on bare metal, VMs, containers, and Kubernetes
2. **No External Dependencies**: No etcd, Consul, Zookeeper, or other coordination services
3. **Fast Convergence**: New nodes join the cluster in sub-second time under normal conditions
4. **Churn Resilient**: Handles frequent node restarts, rolling deployments, and autoscaling
5. **Robust Under Failure**: Continues operating when some seeds are unavailable
6. **Simple Configuration**: Minimal config required - just seed addresses or DNS name

### Architecture Decision

**Decision**: Hybrid DNS + Static Seeds with Parallel Probing

After evaluating multiple approaches, we chose a hybrid strategy that:
- Accepts static seed addresses (bare metal friendly)
- Optionally accepts DNS names for dynamic discovery (Kubernetes friendly)
- Probes all candidates in parallel with short timeouts
- Succeeds on first response (any live peer is sufficient)
- Hands off to SWIM gossip once joined

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BOOTSTRAP ARCHITECTURE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │   Static     │     │     DNS      │     │   Health     │                │
│  │   Seeds      │     │   Resolver   │     │    Cache     │                │
│  │              │     │              │     │              │                │
│  │ 10.0.1.5:9000│     │ managers.svc │     │ Recently     │                │
│  │ 10.0.1.6:9000│     │ → [IP1, IP2] │     │ alive peers  │                │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                │
│         │                    │                    │                         │
│         └────────────────────┼────────────────────┘                         │
│                              │                                              │
│                              ▼                                              │
│                    ┌─────────────────┐                                      │
│                    │   Candidate     │                                      │
│                    │   Aggregator    │                                      │
│                    │                 │                                      │
│                    │ Dedup + Merge   │                                      │
│                    └────────┬────────┘                                      │
│                             │                                               │
│                             ▼                                               │
│         ┌───────────────────────────────────────────┐                       │
│         │           PARALLEL PROBER                  │                       │
│         │                                            │                       │
│         │  ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐      │                       │
│         │  │Probe│  │Probe│  │Probe│  │Probe│      │                       │
│         │  │ #1  │  │ #2  │  │ #3  │  │ #4  │ ...  │                       │
│         │  └──┬──┘  └──┬──┘  └──┬──┘  └──┬──┘      │                       │
│         │     │        │        │        │          │                       │
│         │     └────────┴────┬───┴────────┘          │                       │
│         │                   │                       │                       │
│         │            First Success                  │                       │
│         │            (cancel rest)                  │                       │
│         └───────────────────┬───────────────────────┘                       │
│                             │                                               │
│                             ▼                                               │
│                    ┌─────────────────┐                                      │
│                    │  SWIM Cluster   │                                      │
│                    │     Join        │                                      │
│                    │                 │                                      │
│                    │ Gossip takes    │                                      │
│                    │ over from here  │                                      │
│                    └─────────────────┘                                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Discovery Approaches Evaluated

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **Static Seeds** | Simple, predictable, works everywhere | Requires config updates when seeds change | ✅ Use as primary |
| **DNS-Based** | Dynamic, K8s-native via headless services | TTL caching, stale records | ✅ Use as supplement |
| **Multicast/Broadcast** | Zero config, auto-discovery | Blocked by cloud providers, no cross-subnet | ❌ Rejected |
| **External Service (etcd/Consul)** | Feature-rich, proven | External dependency, operational burden | ❌ Rejected |
| **Shared Storage** | Works with NFS/S3 | Latency, complexity, another dependency | ❌ Rejected |
| **Port Scanning** | No config needed | Slow, looks malicious, security alerts | ❌ Rejected |

### Chosen Solution: DNS + Seeds with Parallel Probing

The key insight: **bootstrap is a one-time operation per node startup**. Once joined, SWIM handles all membership changes. We only need to find *one* live peer to join through.

#### Why This Works Under Churn

```
Timeline showing node C crashing and replacement C' joining:
─────────────────────────────────────────────────────────────────────────────
t=0     Cluster healthy: [A, B, C, D, E] all running
t=1     Pod C crashes, orchestrator starts replacement C'
t=2     DNS still returns C's old IP (TTL not expired)
t=3     New node F tries to join, resolves [A, B, C_old, D, E]
t=4     F probes ALL in parallel with 500ms timeout
t=5     A responds first (50ms) → F joins via A, cancels other probes
t=6     C_old probe times out (ignored, F already joined)
t=7     DNS updates, now returns [A, B, C', D, E]
t=8     C' bootstrap probes, joins via any live peer
t=9     SWIM gossip propagates C' membership to all nodes
─────────────────────────────────────────────────────────────────────────────

Key points:
- Parallel probing means one dead node doesn't block join
- 500ms timeout prevents long waits for unreachable hosts
- First responder wins - we don't wait for all probes
- SWIM handles ongoing membership after initial join
```

### Bootstrap Protocol

#### State Machine

```
                              ┌─────────────┐
                              │   INITIAL   │
                              └──────┬──────┘
                                     │
                              resolve candidates
                                     │
                                     ▼
                              ┌─────────────┐
                     ┌───────▶│  RESOLVING  │◀───────┐
                     │        └──────┬──────┘        │
                     │               │               │
                     │        candidates ready       │
                     │               │               │
                     │               ▼               │
                     │        ┌─────────────┐        │
                     │        │   PROBING   │        │
                     │        └──────┬──────┘        │
                     │               │               │
                     │     ┌─────────┴─────────┐     │
                     │     │                   │     │
                     │  success             all fail │
                     │     │                   │     │
                     │     ▼                   ▼     │
                     │ ┌────────┐      ┌───────────┐ │
                     │ │ JOINED │      │  BACKOFF  │─┘
                     │ └────────┘      └───────────┘
                     │                       │
                     │                  max retries
                     │                       │
                     │                       ▼
                     │               ┌──────────────┐
                     └───────────────│    FAILED    │
                                     └──────────────┘
```

#### Sequence Diagram: Successful Join

```
    New Node              Seed A             Seed B (dead)         Seed C
        │                    │                    │                    │
        │──── resolve() ────▶│                    │                    │
        │◀─── [A, B, C] ─────│                    │                    │
        │                    │                    │                    │
        ├─────── PING ──────▶│                    │                    │
        ├─────── PING ───────┼───────────────────▶│                    │
        ├─────── PING ───────┼────────────────────┼───────────────────▶│
        │                    │                    │                    │
        │◀────── PONG ───────│                    │     (timeout)      │
        │                    │              (500ms)│                    │
        │    [cancel B, C probes]                 │                    │
        │                    │                    │                    │
        │───── JOIN_REQ ────▶│                    │                    │
        │◀──── JOIN_ACK ─────│                    │                    │
        │                    │                    │                    │
        │   [SWIM gossip begins]                  │                    │
        │◀───── GOSSIP ──────│                    │                    │
        │                    │                    │                    │
     JOINED               ACTIVE              DEAD                  ACTIVE
```

#### Sequence Diagram: All Seeds Down, Retry with Backoff

```
    New Node              Seed A (down)      Seed B (down)      Seed C (down)
        │                      │                  │                  │
        │──── resolve() ──────▶│                  │                  │
        │◀─── [A, B, C] ───────│                  │                  │
        │                      │                  │                  │
        ├─────── PING ────────▶│                  │                  │
        ├─────── PING ─────────┼─────────────────▶│                  │
        ├─────── PING ─────────┼──────────────────┼─────────────────▶│
        │                      │                  │                  │
        │                (500ms timeout)    (500ms timeout)   (500ms timeout)
        │                      │                  │                  │
        │   [all probes failed]│                  │                  │
        │                      │                  │                  │
        │   [backoff: 500ms]   │                  │                  │
        │        ...           │                  │                  │
        │                      │                  │                  │
        │──── resolve() ──────▶│                  │                  │
        │◀─── [A, B, C] ───────│ (A comes back up)│                  │
        │                      │                  │                  │
        ├─────── PING ────────▶│                  │                  │
        │◀────── PONG ─────────│                  │                  │
        │                      │                  │                  │
        │───── JOIN_REQ ──────▶│                  │                  │
        │◀──── JOIN_ACK ───────│                  │                  │
        │                      │                  │                  │
     JOINED                 ACTIVE             DOWN                DOWN
```

### DNS Resolution

#### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DNS RESOLVER                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐                                                        │
│  │  DNSConfig      │                                                        │
│  │                 │                                                        │
│  │  - name: str    │     ┌─────────────────────────────────────────┐       │
│  │  - port: int    │────▶│            AsyncDNSResolver              │       │
│  │  - timeout: 2.0 │     │                                          │       │
│  │  - cache_ttl: 5 │     │  ┌──────────────────────────────────┐   │       │
│  └─────────────────┘     │  │       Resolution Cache           │   │       │
│                          │  │                                   │   │       │
│                          │  │  name → (addresses, expiry_time) │   │       │
│                          │  └──────────────────────────────────┘   │       │
│                          │                                          │       │
│                          │  resolve(name) → list[PeerAddress]      │       │
│                          │                                          │       │
│                          │  Uses asyncio.get_event_loop()          │       │
│                          │       .getaddrinfo() for non-blocking   │       │
│                          └─────────────────────────────────────────┘       │
│                                                                              │
│  Resolution Flow:                                                           │
│  ┌────────┐    ┌─────────┐    ┌─────────┐    ┌──────────────┐             │
│  │ Check  │───▶│ Cache   │───▶│ Return  │    │              │             │
│  │ Cache  │    │ Valid?  │yes │ Cached  │    │   Resolve    │             │
│  └────────┘    └────┬────┘    └─────────┘    │   via DNS    │             │
│                     │ no                      │              │             │
│                     └────────────────────────▶│ getaddrinfo  │             │
│                                               └──────┬───────┘             │
│                                                      │                      │
│                                               ┌──────▼───────┐             │
│                                               │ Update Cache │             │
│                                               │ + Return     │             │
│                                               └──────────────┘             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### DNS TTL Considerations

```
Problem: DNS caching returns stale IPs for crashed pods

┌──────────────────────────────────────────────────────────────────────────┐
│                                                                           │
│  Time    DNS Response         Actual Cluster       Issue                 │
│  ────    ────────────         ──────────────       ─────                 │
│  t=0     [A, B, C]            [A, B, C]            None                  │
│  t=1     [A, B, C]            [A, B, C']           C crashed, C' started │
│  t=2     [A, B, C] (cached)   [A, B, C']           Stale C in DNS        │
│  t=3     [A, B, C'] (updated) [A, B, C']           Resolved              │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘

Solution: Parallel probing with short timeouts

- Probe ALL resolved addresses simultaneously
- Use 500ms timeout (not TCP default 30s)
- Dead IPs timeout while live ones respond
- First responder wins, cancel the rest
- Stale DNS entries cause 500ms delay, not blocking failure
```

### Peer Probing

#### Parallel Probe Strategy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PARALLEL PROBE EXECUTION                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Input: candidates = [(10.0.1.5, 9000), (10.0.1.6, 9000), (10.0.1.7, 9000)] │
│  Timeout: 500ms per probe                                                   │
│  Max concurrent: 10 (configurable)                                          │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                      │   │
│  │  t=0ms    ┌──────┐   ┌──────┐   ┌──────┐                           │   │
│  │           │Probe │   │Probe │   │Probe │   All start simultaneously│   │
│  │           │ :5   │   │ :6   │   │ :7   │                           │   │
│  │           └──┬───┘   └──┬───┘   └──┬───┘                           │   │
│  │              │          │          │                                │   │
│  │  t=50ms      │          │          │                                │   │
│  │              ▼          │          │                                │   │
│  │           ┌──────┐      │          │   :5 responds first!          │   │
│  │           │ PONG │      │          │                                │   │
│  │           └──────┘      │          │                                │   │
│  │              │          │          │                                │   │
│  │              │     ┌────┴────┐ ┌───┴───┐                           │   │
│  │              │     │ CANCEL  │ │CANCEL │   Cancel remaining probes │   │
│  │              │     └─────────┘ └───────┘                           │   │
│  │              │                                                      │   │
│  │              ▼                                                      │   │
│  │        Return (10.0.1.5, 9000)                                     │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Worst case (all dead):                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                                                                      │   │
│  │  t=0ms    ┌──────┐   ┌──────┐   ┌──────┐                           │   │
│  │           │Probe │   │Probe │   │Probe │                           │   │
│  │           │ :5   │   │ :6   │   │ :7   │                           │   │
│  │           └──┬───┘   └──┬───┘   └──┬───┘                           │   │
│  │              │          │          │                                │   │
│  │  t=500ms     ▼          ▼          ▼    All timeout together       │   │
│  │          TIMEOUT    TIMEOUT    TIMEOUT                              │   │
│  │                                                                      │   │
│  │        Return None (trigger backoff + retry)                        │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Probe Protocol

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PROBE WIRE PROTOCOL                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Request (PING):                                                            │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  0                   1                   2                   3     │    │
│  │  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1  │    │
│  │ +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+  │    │
│  │ |     'P'       |     'I'       |     'N'       |     'G'       |  │    │
│  │ +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+  │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Response (PONG):                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  0                   1                   2                   3     │    │
│  │  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1  │    │
│  │ +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+  │    │
│  │ |     'P'       |     'O'       |     'N'       |     'G'       |  │    │
│  │ +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+  │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Simple 4-byte exchange:                                                    │
│  - Fast to send/receive                                                     │
│  - Easy to validate                                                         │
│  - No serialization overhead                                                │
│  - Works with any TCP implementation                                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Health-Aware Peer Cache

To accelerate subsequent bootstrap attempts (e.g., after network blip), we cache recently-responsive peers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         HEALTH-AWARE PEER CACHE                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        PeerHealthCache                               │   │
│  │                                                                      │   │
│  │  ┌────────────────────────────────────────────────────────────┐    │   │
│  │  │  (host, port)  │  last_seen   │  success_count  │  state   │    │   │
│  │  ├────────────────┼──────────────┼─────────────────┼──────────┤    │   │
│  │  │  10.0.1.5:9000 │  1704067200  │       47        │  HEALTHY │    │   │
│  │  │  10.0.1.6:9000 │  1704067180  │       12        │  HEALTHY │    │   │
│  │  │  10.0.1.7:9000 │  1704066000  │        0        │  EXPIRED │    │   │
│  │  └────────────────┴──────────────┴─────────────────┴──────────┘    │   │
│  │                                                                      │   │
│  │  Methods:                                                            │   │
│  │  - record_success(addr): Update last_seen, increment count          │   │
│  │  - record_failure(addr): Decrement count, mark stale if zero        │   │
│  │  - get_healthy_peers(): Return peers seen within TTL                │   │
│  │  - evict_expired(): Remove entries older than cache_ttl             │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Usage in Candidate Aggregation:                                            │
│                                                                              │
│     1. Get candidates from DNS/seeds                                        │
│     2. Get healthy peers from cache                                         │
│     3. Prioritize: cached healthy → DNS/seeds → all others                 │
│     4. Probe in priority order (still parallel, but start with likely-live) │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Failure Scenarios

#### Scenario Matrix

| Scenario | Behavior | Recovery Time |
|----------|----------|---------------|
| 1 of N seeds down | Parallel probe, others respond | < 100ms |
| All seeds down temporarily | Backoff + retry until one recovers | backoff intervals |
| DNS returns stale IPs | Stale IPs timeout, live ones respond | + 500ms worst case |
| Network partition (split brain) | Nodes join different partitions | Requires SWIM partition healing |
| Total cluster failure | Retry indefinitely with backoff | Until first node recovers |
| DNS completely unavailable | Fall back to static seeds | Immediate if seeds configured |

#### Backoff Strategy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        EXPONENTIAL BACKOFF                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Attempt    Base Delay    Jitter (0-25%)    Actual Delay    Cumulative     │
│  ───────    ──────────    ──────────────    ────────────    ──────────     │
│     1         500ms          0-125ms        500-625ms        ~560ms        │
│     2        1000ms          0-250ms       1000-1250ms       ~1.7s         │
│     3        2000ms          0-500ms       2000-2500ms       ~3.9s         │
│     4        4000ms         0-1000ms       4000-5000ms       ~8.4s         │
│     5        8000ms         0-2000ms       8000-10000ms     ~17.4s         │
│     6       15000ms         0-3750ms      15000-18750ms     ~34.3s         │
│    ...        ...             ...             ...             ...           │
│    N       15000ms (cap)   0-3750ms      15000-18750ms       ...           │
│                                                                              │
│  Configuration:                                                             │
│  - initial_backoff: 500ms                                                   │
│  - max_backoff: 15000ms (15 seconds)                                        │
│  - backoff_multiplier: 2.0                                                  │
│  - jitter_factor: 0.25 (25% randomization)                                  │
│                                                                              │
│  Why jitter?                                                                │
│  - Prevents thundering herd when multiple nodes retry simultaneously        │
│  - Spreads load on recovering seeds                                         │
│  - Reduces contention during cluster-wide restarts                          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Configuration

#### BootstrapConfig

```python
@dataclass(slots=True)
class BootstrapConfig:
    """Configuration for cluster bootstrap."""
    
    # Static seed addresses (tried first)
    seeds: list[str] = field(default_factory=list)
    
    # DNS name for dynamic discovery (optional, supplements seeds)
    dns_name: str | None = None
    
    # Default port when not specified in address
    default_port: int = 9000
    
    # Probe timeout per candidate (short to enable fast failure detection)
    probe_timeout: float = 0.5  # 500ms
    
    # Maximum concurrent probes (prevent socket exhaustion)
    max_concurrent_probes: int = 10
    
    # Backoff configuration
    initial_backoff: float = 0.5      # 500ms
    max_backoff: float = 15.0         # 15 seconds
    backoff_multiplier: float = 2.0
    jitter_factor: float = 0.25       # 25% randomization
    
    # DNS resolution timeout
    dns_timeout: float = 2.0
    
    # Health cache TTL (how long to remember responsive peers)
    health_cache_ttl: float = 60.0    # 1 minute
```

#### Environment-Specific Examples

```yaml
# Bare Metal / Static IPs
bootstrap:
  seeds:
    - "10.0.1.5:9000"
    - "10.0.1.6:9000"
    - "10.0.1.7:9000"

# Kubernetes (Headless Service)
bootstrap:
  dns_name: "managers.hyperscale.svc.cluster.local"
  default_port: 9000

# Hybrid (DNS primary, static fallback)
bootstrap:
  dns_name: "managers.prod.internal"
  seeds:
    - "10.0.1.5:9000"  # Fallback if DNS fails
  default_port: 9000
```

### Bootstrap Module Structure

```
hyperscale/distributed_rewrite/bootstrap/
├── __init__.py                 # Public exports
├── bootstrap.py                # Main Bootstrapper class
├── dns/
│   ├── __init__.py
│   ├── resolver.py             # AsyncDNSResolver
│   └── models/
│       ├── __init__.py
│       ├── dns_config.py       # DNSConfig dataclass
│       └── dns_result.py       # DNSResult dataclass
├── probing/
│   ├── __init__.py
│   ├── parallel_prober.py      # ParallelProber class
│   └── models/
│       ├── __init__.py
│       ├── probe_config.py     # ProbeConfig dataclass
│       └── probe_result.py     # ProbeResult dataclass
├── cache/
│   ├── __init__.py
│   ├── peer_health_cache.py    # PeerHealthCache class
│   └── models/
│       ├── __init__.py
│       └── peer_entry.py       # PeerCacheEntry dataclass
└── models/
    ├── __init__.py
    ├── bootstrap_config.py     # BootstrapConfig dataclass
    ├── bootstrap_result.py     # BootstrapResult dataclass
    ├── bootstrap_state.py      # BootstrapState enum
    └── peer_address.py         # PeerAddress dataclass
```

### Example Implementations

#### Integration with ManagerServer

```python
class ManagerServer(HealthAwareServer):
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "default",
        # New: Bootstrap configuration (replaces seed_managers)
        bootstrap_config: BootstrapConfig | None = None,
        # Legacy: Still supported for backwards compatibility
        seed_managers: list[tuple[str, int]] | None = None,
        ...
    ):
        ...
        
        # Initialize bootstrapper
        if bootstrap_config:
            self._bootstrapper = Bootstrapper(bootstrap_config)
        elif seed_managers:
            # Legacy: Convert seed_managers to BootstrapConfig
            self._bootstrapper = Bootstrapper(
                BootstrapConfig(
                    seeds=[f"{host}:{port}" for host, port in seed_managers]
                )
            )
        else:
            self._bootstrapper = None
    
    async def start(self) -> None:
        await self.start_server(init_context=self.env.get_swim_init_context())
        
        # Bootstrap: discover peers before joining cluster
        if self._bootstrapper:
            bootstrap_result = await self._bootstrapper.bootstrap()
            
            if bootstrap_result.success:
                # Join cluster via discovered peer
                await self.join_cluster(bootstrap_result.peer.to_udp_addr())
                
                # Register with the peer to get full cluster topology
                await self._register_with_peer(bootstrap_result.peer.to_tcp_addr())
        
        # Continue with normal startup...
        await self._task_runner.run(self.start_probe_cycle)
        ...
```

#### Integration with WorkerServer

```python
class WorkerServer(HealthAwareServer):
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "default",
        # New: Bootstrap configuration
        bootstrap_config: BootstrapConfig | None = None,
        # Legacy: Still supported
        seed_managers: list[tuple[str, int]] | None = None,
    ):
        ...
        
        # Workers bootstrap to find managers
        if bootstrap_config:
            self._bootstrapper = Bootstrapper(bootstrap_config)
        elif seed_managers:
            self._bootstrapper = Bootstrapper(
                BootstrapConfig(
                    seeds=[f"{host}:{port}" for host, port in seed_managers]
                )
            )
        else:
            self._bootstrapper = None
    
    async def start(self, timeout: float | None = None) -> None:
        await self.start_server(init_context=self.env.get_swim_init_context())
        
        # Bootstrap: find at least one manager
        if self._bootstrapper:
            result = await self._bootstrapper.bootstrap()
            
            if result.success:
                # Register with discovered manager
                success = await self._register_with_manager(result.peer.to_tcp_addr())
                
                if success:
                    # Manager returns full topology in registration response
                    # _known_managers populated by _register_with_manager
                    pass
            else:
                raise RuntimeError(f"Failed to bootstrap: {result.error}")
        
        # Join SWIM cluster with all known managers
        for manager in self._known_managers.values():
            await self.join_cluster((manager.udp_host, manager.udp_port))
        
        # Continue with normal startup...
```

#### Integration with GateServer

```python
class GateServer(HealthAwareServer):
    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
        dc_id: str = "global",
        # New: Per-role bootstrap configs
        gate_bootstrap: BootstrapConfig | None = None,
        manager_bootstrap: dict[str, BootstrapConfig] | None = None,  # dc_id -> config
        # Legacy
        gate_peers: list[tuple[str, int]] | None = None,
        datacenter_managers: dict[str, list[tuple[str, int]]] | None = None,
        ...
    ):
        ...
        
        # Gate peer discovery
        if gate_bootstrap:
            self._gate_bootstrapper = Bootstrapper(gate_bootstrap)
        elif gate_peers:
            self._gate_bootstrapper = Bootstrapper(
                BootstrapConfig(
                    seeds=[f"{h}:{p}" for h, p in gate_peers]
                )
            )
        else:
            self._gate_bootstrapper = None
        
        # Per-datacenter manager discovery
        self._dc_bootstrappers: dict[str, Bootstrapper] = {}
        if manager_bootstrap:
            for dc_id, config in manager_bootstrap.items():
                self._dc_bootstrappers[dc_id] = Bootstrapper(config)
        elif datacenter_managers:
            for dc_id, addrs in datacenter_managers.items():
                self._dc_bootstrappers[dc_id] = Bootstrapper(
                    BootstrapConfig(
                        seeds=[f"{h}:{p}" for h, p in addrs]
                    )
                )
    
    async def start(self) -> None:
        await self.start_server(init_context=self.env.get_swim_init_context())
        
        # Bootstrap gate cluster
        if self._gate_bootstrapper:
            result = await self._gate_bootstrapper.bootstrap()
            if result.success:
                await self.join_cluster(result.peer.to_udp_addr())
        
        # Bootstrap per-datacenter manager connections
        for dc_id, bootstrapper in self._dc_bootstrappers.items():
            result = await bootstrapper.bootstrap()
            if result.success:
                # Store discovered manager for this DC
                self._dc_primary_managers[dc_id] = result.peer.to_tcp_addr()
        
        # Continue with normal startup...
```

#### Bootstrapper Core Implementation

```python
class Bootstrapper:
    """
    Discovers and connects to cluster peers.
    
    Combines DNS resolution, static seeds, and health caching
    to find live peers quickly. Uses parallel probing with short
    timeouts for fast convergence even when some candidates are dead.
    """
    
    def __init__(self, config: BootstrapConfig):
        self._config = config
        self._dns_resolver = AsyncDNSResolver(
            timeout=config.dns_timeout,
            cache_ttl=config.health_cache_ttl,
        )
        self._prober = ParallelProber(
            timeout=config.probe_timeout,
            max_concurrent=config.max_concurrent_probes,
        )
        self._health_cache = PeerHealthCache(ttl=config.health_cache_ttl)
        self._state = BootstrapState.INITIAL
    
    async def bootstrap(self) -> BootstrapResult:
        """
        Discover and connect to a live peer.
        
        Returns BootstrapResult with the first responsive peer,
        or an error if all candidates fail after retries.
        """
        backoff = self._config.initial_backoff
        
        while True:
            self._state = BootstrapState.RESOLVING
            candidates = await self._resolve_candidates()
            
            if not candidates:
                self._state = BootstrapState.BACKOFF
                await self._sleep_with_jitter(backoff)
                backoff = min(backoff * self._config.backoff_multiplier, 
                             self._config.max_backoff)
                continue
            
            self._state = BootstrapState.PROBING
            result = await self._prober.probe_first_success(candidates)
            
            if result.success:
                self._state = BootstrapState.JOINED
                self._health_cache.record_success(result.peer)
                return BootstrapResult(success=True, peer=result.peer)
            
            # All probes failed - backoff and retry
            self._state = BootstrapState.BACKOFF
            await self._sleep_with_jitter(backoff)
            backoff = min(backoff * self._config.backoff_multiplier,
                         self._config.max_backoff)
    
    async def _resolve_candidates(self) -> list[PeerAddress]:
        """Aggregate candidates from all sources."""
        candidates: list[PeerAddress] = []
        seen: set[tuple[str, int]] = set()
        
        # Priority 1: Recently healthy peers from cache
        for peer in self._health_cache.get_healthy_peers():
            key = (peer.host, peer.port)
            if key not in seen:
                candidates.append(peer)
                seen.add(key)
        
        # Priority 2: Static seeds
        for seed in self._config.seeds:
            peer = PeerAddress.parse(seed, self._config.default_port)
            key = (peer.host, peer.port)
            if key not in seen:
                candidates.append(peer)
                seen.add(key)
        
        # Priority 3: DNS resolution
        if self._config.dns_name:
            dns_peers = await self._dns_resolver.resolve(
                self._config.dns_name,
                self._config.default_port,
            )
            for peer in dns_peers:
                key = (peer.host, peer.port)
                if key not in seen:
                    candidates.append(peer)
                    seen.add(key)
        
        return candidates
    
    async def _sleep_with_jitter(self, base_delay: float) -> None:
        """Sleep with randomized jitter to prevent thundering herd."""
        jitter = base_delay * self._config.jitter_factor * random.random()
        await asyncio.sleep(base_delay + jitter)
```

---
