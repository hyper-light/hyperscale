# Raft Consensus for Manager & Gate Clusters

## Hard Constraints

- **No partial implementation.** Every phase must be fully integrated, callable, and used before moving on.
- **Cyclomatic complexity < 5** for every function/method. Use early returns, walrus operators, match statements.
- **No nested if/else if** chains -- use match, dispatch dicts, early returns.
- **No memory leaks** -- every stored per-job/per-node structure must have a cleanup path.
- **No orphaned asyncio tasks** -- use TaskRunner for background work, never raw `asyncio.create_task`.
- **Backpressure is first-class** -- every queue, buffer, and pipeline must have bounded capacity and backpressure signaling.
- **Modern Python** -- walrus operators, match statements, type hints, `str` enums.

## Codebase State

- Active code lives in `hyperscale/distributed/` (NOT `distributed_rewrite/`)
- No Raft code exists on main -- must be built from scratch
- Production WAL exists in `distributed/ledger/wal/` (WALWriter with group commit, CRC32, backpressure, recovery)
- Nodes decomposed: `nodes/manager/` (18 files), `nodes/gate/` (21 files)
- State classes: `ManagerState` (state.py), `GateRuntimeState` (state.py)
- Job managers: `JobManager`, `GateJobManager`
- SWIM handles failure detection; Raft handles consensus

---

## Phase 1: Core Raft Infrastructure

Build in `hyperscale/distributed/raft/`. Every file fully tested.

### New Files (12)

```
hyperscale/distributed/raft/
    __init__.py
    raft_node.py               # Core algorithm (election, replication, commit)
    raft_log.py                # In-memory log with 1-based indexing
    models/
        __init__.py
        log_entry.py           # RaftLogEntry
        command_types.py       # RaftCommandType (13 manager + NO_OP)
        commands.py            # RaftCommand
        gate_command_types.py  # GateRaftCommandType (9 gate + NO_OP)
        gate_commands.py       # GateRaftCommand
        messages.py            # RequestVote, AppendEntries + responses
        raft_state.py          # Persistent/volatile/leader state
```

### Key Designs

**RaftLog**: Synchronous in-memory, 1-based. `_snapshot_index`/`_snapshot_term` for compaction. Bounded: `_max_entries` with oldest-first eviction after snapshot.

**RaftNode**: Single asyncio.Lock per node. Roles: follower/candidate/leader. Election timeout 150-300ms randomized, heartbeat 50ms, tick 10ms. `command_type: str` to support both enums. Cleanup: `destroy()` releases all state.

**Models**: All `@dataclass(slots=True)`. Messages extend existing `Message` base class. `command_type` fields use `str` for cross-enum compatibility.

---

## Phase 2: Consensus Coordinators + State Machines

### New Files (5)

```
hyperscale/distributed/raft/
    raft_consensus.py          # Per-job RaftNode coordinator (manager)
    gate_raft_consensus.py     # Per-job RaftNode coordinator (gate)
    state_machine.py           # Apply committed entries to JobManager
    gate_state_machine.py      # Apply committed entries to GateJobManager
    logging_models.py          # Structured Raft logging models
```

**RaftConsensus**: Manages dict of per-job RaftNode. Background tick via TaskRunner (not raw tasks). Bounded: max concurrent Raft instances with backpressure on `create_job_raft()`. Cleanup: `destroy_job_raft()` called on job completion, releases node + log memory.

**State Machines**: Dispatch dict mapping command_type -> handler. Each handler is a small focused method. NO_OP returns immediately. Unknown types logged + ignored (no crash).

**Logging**: Follow existing `hyperscale_logging_models.py` patterns exactly.

---

## Phase 3: Job Manager Wrappers

### New Files (2)

```
hyperscale/distributed/raft/
    raft_job_manager.py        # Wraps JobManager (13 mutations through Raft)
    gate_raft_job_manager.py   # Wraps GateJobManager (9 mutations through Raft)
```

Every mutating method: serialize command, propose through consensus, return success/failure. Reads bypass Raft (eventual consistency). `_propose_command()` helper handles serialization + timeout.

---

## Phase 4: Expanded Command Types

### Manager: +8 command types in RaftCommandType

```
ASSUME_JOB_LEADERSHIP, TAKEOVER_JOB_LEADERSHIP, RELEASE_JOB_LEADERSHIP
INITIATE_CANCELLATION, COMPLETE_CANCELLATION
PROVISION_CONFIRMED
FLUSH_STATS_WINDOW
NODE_MEMBERSHIP_EVENT
```

Add fields to RaftCommand, handlers to RaftStateMachine, methods to RaftJobManager.

### Gate: +11 command types in GateRaftCommandType

```
ASSUME_GATE_LEADERSHIP, TAKEOVER_GATE_LEADERSHIP, RELEASE_GATE_LEADERSHIP, PROCESS_LEADERSHIP_CLAIM
UPDATE_DC_MANAGER, RELEASE_DC_MANAGERS
CREATE_LEASE, RELEASE_LEASE
SET_JOB_SUBMISSION, SET_WORKFLOW_DC_RESULT
GATE_MEMBERSHIP_EVENT
```

Add fields to GateRaftCommand, handlers to GateStateMachine, methods to GateRaftJobManager.

---

## Phase 5: Node Integration

### Manager (`nodes/manager/server.py`)

- Initialize RaftStateMachine, RaftConsensus, RaftJobManager in `__init__`
- Add `_send_raft_message()` TCP callback
- Add 4 TCP handlers: raft_request_vote, raft_request_vote_response, raft_append_entries, raft_append_entries_response
- Hook SWIM on_node_join/on_node_dead -> RaftConsensus.on_node_join/leave
- Replace ALL direct mutation calls with Raft-routed equivalents

### Gate (`nodes/gate/server.py`)

- Initialize GateStateMachine, GateRaftConsensus, GateRaftJobManager
- Add `_send_gate_raft_message()` TCP callback
- Add 4 TCP handlers with `gate_raft_*` prefix
- Hook SWIM callbacks -> GateRaftConsensus
- Replace ALL direct state mutations with Raft-routed equivalents

---

## Phase 6: Persistence, Snapshots, Stats, Membership

### Raft WAL Persistence

**New File**: `distributed/raft/raft_wal.py`

Adapter wrapping existing WALWriter for Raft-specific entries. Binary format: `[4:crc32][4:length][8:term][8:index][N:command]`. Bounded write queue with backpressure. Recovery reads + validates on startup.

### Snapshot Support

**New File**: `distributed/raft/snapshot.py`

Add `InstallSnapshot` to messages.py. Leader creates snapshot when log > threshold. Followers behind snapshot receive full state. Compaction removes old entries from memory + WAL.

### Stats Replication

**New File**: `distributed/raft/replicated_stats_store.py`

`add_progress()` is local hot path. `flush_and_replicate()` proposes aggregated windows through Raft. Bounded buffer for pending flushes.

### Membership Event Replication

**New File**: `distributed/raft/replicated_membership_log.py`

SWIM join/leave/suspect events proposed through Raft. All nodes process in committed order.

---

## File Summary (32 files total)

| File | Action | Phase |
|------|--------|-------|
| `raft/__init__.py` | Create | 1 |
| `raft/raft_node.py` | Create | 1 |
| `raft/raft_log.py` | Create | 1 |
| `raft/models/__init__.py` | Create | 1 |
| `raft/models/log_entry.py` | Create | 1 |
| `raft/models/command_types.py` | Create | 1 |
| `raft/models/commands.py` | Create | 1 |
| `raft/models/gate_command_types.py` | Create | 1 |
| `raft/models/gate_commands.py` | Create | 1 |
| `raft/models/messages.py` | Create | 1 |
| `raft/models/raft_state.py` | Create | 1 |
| `raft/raft_consensus.py` | Create | 2 |
| `raft/gate_raft_consensus.py` | Create | 2 |
| `raft/state_machine.py` | Create | 2 |
| `raft/gate_state_machine.py` | Create | 2 |
| `raft/logging_models.py` | Create | 2 |
| `raft/raft_job_manager.py` | Create | 3 |
| `raft/gate_raft_job_manager.py` | Create | 3 |
| `raft/models/command_types.py` | Modify | 4 |
| `raft/models/commands.py` | Modify | 4 |
| `raft/state_machine.py` | Modify | 4 |
| `raft/raft_job_manager.py` | Modify | 4 |
| `raft/models/gate_command_types.py` | Modify | 4 |
| `raft/models/gate_commands.py` | Modify | 4 |
| `raft/gate_state_machine.py` | Modify | 4 |
| `raft/gate_raft_job_manager.py` | Modify | 4 |
| `nodes/manager/server.py` | Modify | 5 |
| `nodes/gate/server.py` | Modify | 5 |
| `raft/raft_wal.py` | Create | 6 |
| `raft/snapshot.py` | Create | 6 |
| `raft/replicated_stats_store.py` | Create | 6 |
| `raft/replicated_membership_log.py` | Create | 6 |

## Verification

Tests written per-phase in `tests/unit/raft/` and `tests/integration/`. User runs them.
