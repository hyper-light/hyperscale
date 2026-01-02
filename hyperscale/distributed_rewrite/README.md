# Hyperscale Distributed Architecture

A high-performance, fault-tolerant distributed workflow execution system designed for multi-datacenter deployments with high CPU and memory utilization per node.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [Node Types](#node-types)
  - [Communication Protocols](#communication-protocols)
  - [Leadership Election](#leadership-election)
- [Component Diagrams](#component-diagrams)
- [Data Flow](#data-flow)
- [Failure Handling](#failure-handling)
- [State Management](#state-management)
- [Security](#security)
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

### Key Design Principles

1. **Workers are the source of truth** - Workers maintain authoritative state for their own workflows
2. **Passive state discovery** - Serf-style heartbeat embedding in SWIM messages
3. **Quorum-based provisioning** - Manager decisions require quorum confirmation
4. **Lease-based execution** - Gates use leases for at-most-once DC semantics
5. **Graceful degradation** - Load shedding under pressure, LHM-aware timeouts

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

### Leadership Election

Hierarchical lease-based leadership with LHM (Local Health Multiplier) eligibility:

```
┌─────────────────────────────────────────────────────────────────┐
│                    LEADERSHIP ELECTION                           │
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
│  New Leader Election:                                            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 1. Leader failure detected via SWIM                       │  │
│  │ 2. Pre-voting phase among eligible managers               │  │
│  │ 3. Candidate with lowest LHM + highest priority wins      │  │
│  │ 4. New leader announces with new term number              │  │
│  └───────────────────────────────────────────────────────────┘  │
│                            │                                     │
│                            ▼                                     │
│  State Synchronization:                                          │
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
│  │ • MAX_MESSAGE_SIZE: 10MB                                  │  │
│  │ • MAX_DECOMPRESSED_SIZE: 100MB                            │  │
│  │ • Compression bomb detection                              │  │
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

## Testing

Run the test suite:

```bash
python examples/test_distributed_rewrite.py
```

Current test coverage: 47+ tests covering:
- SWIM protocol (probing, suspicion, gossip)
- Leadership election (pre-voting, flapping)
- State embedding (heartbeat serialization)
- Distributed messages (all message types)
- Worker/Manager/Gate functionality
- State sync and retry mechanisms
- Per-core workflow assignment

---

## License

See the main project LICENSE file.

