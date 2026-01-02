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
│  │      │ STATE: SYNCING        │                                           ││
│  │      │ (not counted in quorum)                                          ││
│  │      │                       │                                           ││
│  │      │ ③ TCP: StateSyncRequest                                          ││
│  │      │──────────────────────►│ (to leader)                               ││
│  │      │                       │                                           ││
│  │      │ ④ TCP: ManagerStateSnapshot                                      ││
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

