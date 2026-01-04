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
- [Backpressure & Degradation](#backpressure--degradation)
- [Scaling Operations](#scaling-operations)
- [State Management](#state-management)
- [Security](#security)
- [Message Protocol Reference](#message-protocol-reference)
- [Module Structure](#module-structure)

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
│  1. Consistent Hashing│  UNIMPLEMENTED  │  Foundation for job distribution  │
│  2. Lease-Based Owner │  UNIMPLEMENTED  │  Job ownership with TTL           │
│  3. Direct DC Routing │  UNIMPLEMENTED  │  DC managers send to job leader   │
│  4. Client Reconnect  │  UNIMPLEMENTED  │  Client computes job owner        │
│  5. Fencing Tokens    │  UNIMPLEMENTED  │  Stale update protection          │
└───────────────────────┴─────────────────┴───────────────────────────────────┘
```

---

### Component 1: Consistent Hashing Ring

**Status: UNIMPLEMENTED**

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

**Status: UNIMPLEMENTED**

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

**Status: UNIMPLEMENTED**

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

**Status: UNIMPLEMENTED**

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

**Status: UNIMPLEMENTED**

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

### Implementation Order

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    IMPLEMENTATION ROADMAP                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Order  │ Component           │ Depends On       │ Status                   │
│  ───────┼─────────────────────┼──────────────────┼────────────────────────  │
│  1      │ Consistent Hashing  │ None             │ IMPLEMENTED ✓            │
│  2      │ Lease-Based Owner   │ #1               │ UNIMPLEMENTED            │
│  3      │ Fencing Tokens      │ #2               │ UNIMPLEMENTED            │
│  4      │ Direct DC Routing   │ #1, #2, #3       │ UNIMPLEMENTED            │
│  5      │ Client Reconnect    │ #1, #3           │ UNIMPLEMENTED            │
│                                                                              │
│  Each component will be:                                                     │
│  1. Implemented                                                              │
│  2. Tested with integration test                                             │
│  3. Debugged and fixed                                                       │
│  4. Committed                                                                │
│  5. Marked as IMPLEMENTED in this document                                   │
│  6. Committed again with documentation update                                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Session Handoff: Implementation Continuation Guide

This section provides all context needed for another AI session to resume implementation.

### Current State (As of Last Session)

#### What's Working ✓
1. **Gate-to-Manager Federated Health Monitoring**: Implemented via `FederatedHealthMonitor`
2. **Manager-to-Gate Symmetric Monitoring**: Managers also use federated health for gate monitoring
3. **Cross-Cluster Probing Protocol**: `xprobe`/`xack` messages with namespaced incarnations
4. **Gate Results Aggregation**: Working correctly - latency percentiles interpolated, per-DC stats preserved
5. **TCP Length-Prefixed Framing**: Reliable message delivery implemented
6. **Priority-Based Core Allocation**: Managers allocate cores based on `StagePriority`, not VUs
7. **Context Consistency Protocol**: LWW with timestamps and source node tiebreakers
8. **SWIM Configuration**: Externalized to `Env` class
9. **Workflow Execution Pipeline**: Test workflows correctly report completion counts
   - Fixed: `RemoteGraphManager.get_workflow_update()` now returns the update
   - Fixed: Manager extracts counts from `WorkflowStats` for fast-completing workflows
   - Note: Non-test workflows (no `CallResult` return type) correctly report zero counts

#### What's Partially Working ⚠
1. **Manager Cleanup on Shutdown**: `Manager stop failed` warnings during test cleanup

#### What's Not Implemented ✗
See "Remaining Components" below.

---

### Remaining Components (In Implementation Order)

#### Component 1: Consistent Hashing Ring ✓ IMPLEMENTED
**Purpose**: Deterministic job-to-gate assignment for stable ownership

**Location**: `hyperscale/distributed_rewrite/routing/consistent_hash.py`

**Implementation**:
```python
class ConsistentHashRing:
    def __init__(self, virtual_nodes: int = 150):
        # 150 vnodes provides <10% CV distribution

    def add_node(self, node_id: str) -> None:
        # Idempotent, thread-safe

    def remove_node(self, node_id: str) -> None:
        # Idempotent, thread-safe

    def get_node(self, key: str) -> str | None:
        # O(log n) lookup via binary search

    def get_backup(self, key: str) -> str | None:
        # Returns different node from primary

    def get_nodes_for_key(self, key: str, count: int) -> list[str]:
        # For replication scenarios
```

**Key Properties**:
- **Deterministic**: Same key always maps to same node
- **Minimal redistribution**: ~23% keys move when adding 4th node
- **Thread-safe**: RLock-protected operations
- **Even distribution**: CV < 10% with 150 virtual nodes

**Integration Points** (pending):
- Gate uses hash ring in `job_submission` handler to determine initial owner
- Client uses hash ring to find job owner for reconnection

**Test File**: `examples/servers/test_consistent_hashing.py`
- 9 test cases covering all functionality
- Thread safety tested with 8000 concurrent ops

---

#### Component 2: Lease-Based Job Ownership
**Purpose**: Time-bounded ownership to prevent split-brain during failures

**Implementation Plan**:
```
Location: hyperscale/distributed_rewrite/leases/job_lease.py

@dataclass
class JobLease:
    job_id: str
    owner_node: str
    fence_token: int
    expires_at: float  # monotonic time
    lease_duration: float = 30.0
    
class LeaseManager:
    def __init__(self, node_id: str):
        self._leases: dict[str, JobLease] = {}
        self._node_id = node_id
        
    def acquire(self, job_id: str) -> JobLease | None:
        """Acquire lease if not held or expired"""
        ...
    
    def renew(self, job_id: str) -> bool:
        """Extend lease if still owner"""
        ...
    
    def release(self, job_id: str) -> None:
        """Explicitly release lease"""
        ...
    
    def _cleanup_expired(self) -> None:
        """Background task to clean expired leases"""
        ...
```

**Integration Points**:
- Gate acquires lease when becoming job owner (via hash ring or on job submission)
- Lease renewal happens in background heartbeat loop
- Backup gate monitors primary's lease via state sync

**Test File**: `examples/servers/test_lease_ownership.py`
```python
# Test: lease acquisition succeeds for unclaimed job
# Test: lease renewal extends expiry
# Test: backup claims lease after primary expires
# Test: fence token increments on each claim
```

---

#### Component 3: Fencing Tokens
**Purpose**: Prevent stale updates from old owners

**Implementation Plan**:
```
Location: Integrate into existing message models

# Update JobFinalResult, JobStatusPush, etc.
@dataclass  
class JobFinalResult(Message):
    ...
    fence_token: int = 0  # Add to existing model
    
# Gate validation
def validate_fence_token(self, job_id: str, received_token: int) -> bool:
    current = self._job_fence_tokens.get(job_id, 0)
    if received_token < current:
        return False  # Stale update, reject
    self._job_fence_tokens[job_id] = received_token
    return True
```

**Integration Points**:
- Gate includes fence_token in `JobDispatch` to managers
- Managers include fence_token in `JobFinalResult` to gates
- Gate validates fence_token before accepting results

**Test File**: `examples/servers/test_fencing_tokens.py`
```python
# Test: stale result (old fence) rejected
# Test: valid result (current fence) accepted
# Test: new owner's results (higher fence) accepted
```

---

#### Component 4: Direct DC-to-Job-Leader Routing
**Purpose**: Results go directly to job leader, not cluster leader

**Implementation Plan**:
```
# In Manager.job_final_result handler:
# Instead of sending to cluster leader, send to job leader

def _send_job_final_result(self, job_id: str, result: JobFinalResult):
    job_leader = self._job_leaders.get(job_id)
    if job_leader == self._node_id.full:
        # We are the job leader, aggregate locally
        self._aggregate_and_forward_to_gate(result)
    else:
        # Forward to job leader
        self.send_tcp(job_leader, "job_final_result", result.dump())

# Similar pattern for gates forwarding to job-owning gate
```

**Integration Points**:
- `JobDispatch` includes `job_leader_addr` field
- DCs route results back to specified leader
- If leader unreachable, use backup from hash ring

**Test File**: `examples/servers/test_direct_routing.py`
```python
# Test: results route to job leader, not cluster leader
# Test: failover to backup when leader unreachable
```

---

#### Component 5: Client Reconnection
**Purpose**: Clients can reconnect after gate failure and resume job tracking

**Implementation Plan**:
```
Location: hyperscale/distributed_rewrite/nodes/client.py

class HyperscaleClient:
    def __init__(self, gate_addrs: list[tuple[str, int]]):
        self._hash_ring = ConsistentHashRing()
        for addr in gate_addrs:
            self._hash_ring.add_node(f"{addr[0]}:{addr[1]}")
    
    def reconnect(self, job_id: str) -> JobResult | None:
        """Reconnect to job owner and get current status"""
        owner = self._hash_ring.get_node(job_id)
        backup = self._hash_ring.get_backup(job_id)
        
        # Try owner first, then backup
        for gate_addr in [owner, backup]:
            try:
                return self._fetch_job_status(gate_addr, job_id)
            except ConnectionError:
                continue
        raise AllGatesUnreachable()
```

**Integration Points**:
- Client stores hash ring of known gates
- On disconnect, client computes owner and reconnects
- Gate's `job_status_request` handler returns current status

**Test File**: `examples/servers/test_client_reconnection.py`
```python
# Test: client reconnects after gate failure
# Test: client finds job on backup gate
# Test: client receives missed status updates
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
1. Run test with `timeout 180 python examples/servers/test_<name>.py 2>&1 | tail -100`
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

### Known Issues to Investigate

1. ~~**Workflow Execution Not Completing**~~ **RESOLVED**
   - ~~Jobs return `PARTIAL` with `total_completed=0`~~
   - **Root cause 1**: `RemoteGraphManager.get_workflow_update()` missing return statement
   - **Root cause 2**: Manager used progress-based counts only, missing fast workflows
   - **Fix**: Added return statement; extract counts from `WorkflowStats["stats"]`

2. **Manager Shutdown Failures**
   - `Manager stop failed` during cleanup
   - May be race condition with background tasks

3. **Circuit Breaker False Positives**
   - `[CircuitBreakerOpen] ELECTION` errors during single-node tests
   - Single-node clusters shouldn't have election circuit breaker issues

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

