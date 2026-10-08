# D1 — Raft persistence with a persisted identity

*Design, 2026-10-05; built (stages A-C) the same day. Decision D1 of `docs/REMAINING_WORK_PLAN.md`; your storage
decision (2026-09-30): "we can write to disk, but we just can't assume what that
disk will be". An empty disk joins as a new member, an intact disk of its own
resumes, and an untrustworthy disk is set aside and treated as empty.*

## 1. What is wrong today

Every Raft group a node takes part in is volatile: the membership group
(AD-52), each manager's per-job groups (`RaftConsensus`), and each gate's
per-job groups (`GateRaftConsensus`). Safety holds only because a restarted
process never reuses an id. `NodeId.full` embeds the start time, so a
restarted process is a new member, and the old one is removed as dead.

That has three costs:

1. **REGIONAL durability is weaker than it claims.** A REGIONAL ledger commit
   is on disk only at the job leader. Followers acknowledge AppendEntries from
   memory, and their `JobLedgerReplica` is memory-only. Suppose the leader's
   disk is lost while its followers restart, as in a datacenter power loss
   where the leader's disk does not come back. Then an entry acknowledged as
   REGIONAL is gone.
2. **A full-cluster restart refounds the cluster.** When every member restarts
   at once (a power loss, or a restart wider than a quorum), no old member
   survives to keep the group. The cohort founds a new cluster with a new
   `cluster_uuid`. Gates then see a regenerated datacenter and forget what
   they learned of it. Every restart also pays for a learner catch-up it
   would not need.
3. **Each restart costs a membership change.** The new id must be added and
   promoted, and the dead one removed, in every group (`reconcile_membership`).

## 2. Decisions

| # | Question | Decision | Why |
|---|---|---|---|
| P1 | Which groups persist? | **All of a node's groups**: the membership group and every per-job group | Per-job groups key voters by the same node id as the membership group (`ClusterMembership.node_addresses`). A node that resumes its id must keep its votes and log in every group it was in, or it is an amnesiac voter (Raft §5.2/§5.4). Persisting per-job logs is also what makes REGIONAL mean "on a quorum of disks" (cost 1). |
| P2 | One store per group, or one per node? | **One store per node, multiplexing every group** (records keyed by group id) | Thousands of per-job groups would mean thousands of files and fsyncs. One file group-commits across groups (TiKV/CockroachDB raft-engine practice), so N concurrent groups share one fsync. |
| P3 | Record encoding | **msgspec structs in a CRC-framed envelope**; no pickle | The store is read back from a disk that cannot be assumed trustworthy. Unpickling it is arbitrary code execution; msgspec decodes data only. |
| P4 | Identity | **A persisted identity file**: node id (with its `created_ms`), membership participation, and a 128-bit stamp drawn from the node's random source. Every store file carries the stamp in its header | It lets a node prove a store is its own. A store copied from another node, or left behind by an earlier identity, fails the stamp check and is set aside. |
| P5 | When is a disk untrustworthy? | The identity file is missing or fails decoding. Or a store file's header stamp does not match. Or a record fails CRC **with valid records after it**. Or a record breaks a structural invariant (term regresses, or an index skips) | A torn tail (a bad last record with nothing valid after it) is the normal result of power loss. That record was never fsynced, so it was never acknowledged, and dropping it is safe. Damage in the middle is not explained by power loss. |
| P6 | What happens to an untrustworthy disk? | The Raft directory is renamed to `raft.set-aside.{wall_ms}`, a `RaftStoreSetAside` event is logged with the reason, and the node starts as empty (new identity) | It is never deleted unread: an operator can investigate. Only the newest set-aside is kept (`RAFT_SET_ASIDE_RETAINED`, default 1): it describes the latest failure, older ones are superseded, and unbounded retention would fill the disk across repeated failures. |
| P7 | No usable disk | **Volatile mode** (`VolatileRaftStorage`): a fresh identity each start, as before D1, for a node constructed without a store. The run commands always give a node its data directory's store and, as they already do for an unusable data directory, fail loudly when it cannot be opened at all -- never silently run without the durability the ledger beside it needs | No substrate assumptions: correctness never depends on the disk, only resumption does. Mixed clusters work, because a volatile node is just a node that never resumes. |
| P8 | fsync rule | Raft Figure 2: term, vote and log are on stable storage **before** any RPC reply that depends on them. The leader counts itself toward commit only for entries it has persisted. It may send to followers before its own fsync (Raft thesis §10.2.1) | This is the minimum Raft safety needs. Sending before the local fsync hides the fsync from commit latency. |
| P9 | Snapshots | A snapshot (state, last index and term, configuration) is a store record, persisted before the log prefix it covers is cut | After a restart, a compacted group must rebuild from snapshot plus suffix. |
| P10 | Ended groups | `destroy` writes a `GroupReleased` record. Compaction (the existing `WALWriter.rewrite` under `_file_lock`) rewrites the file without released groups' records and without log prefixes their snapshots cover | Bounds disk use by live groups, not by history: no leak. |
| P11 | Compaction trigger | When the file's dead bytes exceed its live bytes (amortized O(1) per record) | Not an arbitrary size: rewriting at half dead keeps total writes at most 2× the live data written (the classic log-structured bound). |
| P12 | Applying after a restart | `commit_index` and `last_applied` restart at the snapshot index (Raft: volatile). State machines rebuild by re-applying committed entries as the leader's commit index reaches them. `JobLedgerReplica` and the membership state are in-memory and rebuilt that way; nothing is applied twice to durable state | The manager's own job ledger is separate (NodeWAL), so the Raft re-apply touches only in-memory replicas. This is verified in stage C (below). |

## 3. On-disk layout

```
{data_dir}/raft/
    identity            # msgspec: format_version, node_id_full, participation, stamp
    store.wal           # CRC-framed records (envelope below), group-committed
```

The envelope is the existing `RaftWALEntry` framing, generalized: `[crc32][length][body]`.
The body is a msgspec-encoded tagged union:

- `StoreHeader(stamp, format_version)`: the first record of every file.
- `HardState(group_id, term, voted_for)`
- `Entries(group_id, entries: list[RaftLogEntry])`
- `TruncateFrom(group_id, index)`
- `Snapshot(group_id, last_index, last_term, configuration, state)`
- `GroupReleased(group_id)`
- `ParticipationAdvanced(participation)`: membership, when the node leaves a group (`_abandon`).

`RaftLogEntry` becomes msgspec-encodable. Its `command` stays bytes; its HLC fields are plain ints.

## 4. Interfaces

```python
class RaftGroupStorage(Protocol):
    async def save_hard_state(self, term: int, voted_for: str | None) -> None: ...
    async def append(self, entries: list[RaftLogEntry]) -> None: ...
    async def truncate_from(self, index: int) -> None: ...
    async def save_snapshot(self, snapshot: RaftSnapshotRecord) -> None: ...
    async def release(self) -> None: ...
```

- `DurableRaftGroupStorage(store, group_id)` writes through `RaftStore`, a node-level single writer that group-commits.
- `VolatileRaftGroupStorage` is an explicit no-op implementation that is injected in volatile mode. It is not a `None` fallback.
- `RaftNode` takes `storage: RaftGroupStorage` and `recovered: RecoveredGroupState | None` (term, vote, log, snapshot) as required constructor arguments.
- `RaftStore.open(filesystem, directory, random_source, logger)` returns `(identity, recovered_groups)`, or sets the store aside (P5/P6).

Persistence points in `RaftNode`, each awaited under the group's lock before the reply or send that depends on it:

- `_step_down` / term adoption → `save_hard_state`;
- the campaign start (term + 1, self-vote) → `save_hard_state`, before any RequestVote goes out;
- a granted RequestVote → `save_hard_state`, before the response;
- a follower's AppendEntries: `truncate_from` and `append`, then `save_hard_state` if the term moved, all before a success response;
- leader appends (no-op at term start, configuration entries, proposals) → `append`. The leader's own match index advances only after the append returns;
- InstallSnapshot → `save_snapshot`, before the response;
- compaction (`compact_through`) → `save_snapshot`, then the in-memory cut.

PreVote changes no persistent state and writes nothing.

## 5. Wiring

- **Identity.** `HealthAwareServer` constructs `NodeId` with the persisted `created_ms` when the store resumed, and fresh otherwise. The identity is opened before the server's id is fixed, so the server constructor receives the opened store (injection, not a module global).
- **Membership group.** `ClusterMembership` first resumes from a recovered membership group, with its `cluster_uuid`, participation and log, and forms from it. It founds or joins only when it recovered none. If the resumed group cannot elect within the existing abandon window (its other voters lost their disks), the existing `_abandon` → refound path applies, and the participation advance is persisted.
- **Per-job groups.** `RaftConsensus` and `GateRaftConsensus` recreate every recovered, unreleased group at start, before the transport accepts Raft traffic. The voters come from the recovered configuration. Re-applying committed entries rebuilds `JobLedgerReplica`.
- **Volatile mode** wiring is unchanged from today, apart from the explicit `VolatileRaftGroupStorage`.

## 6. Verification (VOPR first)

1. **Store VOPR** (`SimFilesystem`, seeded):
   - random group operations, crash at random points, with fsync reordering and torn tails;
   - recovery equals the acknowledged prefix of every group: nothing acknowledged is lost, and nothing unacknowledged is invented;
   - mid-file corruption and stamp mismatch → set aside, never resumed;
   - compaction under concurrent appends loses nothing.
2. **Raft VOPR with persistence** (`RaftGroupSimulation`):
   - crash-restarts come back under the **same** id with the disk intact (resume) or wiped (new id), alongside today's new-id restarts;
   - the existing invariants hold: election safety, leader completeness, state-machine safety;
   - plus: no entry a client saw committed is lost across a full-group power loss;
   - mutation teeth: with the vote fsync removed, or with the follower's append not awaited before success, the VOPR must find a violation.
3. **Ledger durability**: REGIONAL-committed entries survive the leader's disk loss plus every follower's power loss. This fails today.
4. **E2E (you run)**: restart a whole 3-manager datacenter. The `cluster_uuid` is unchanged, no `DatacenterRegenerated` is logged, and in-flight jobs complete.

## 7. Stages

- **A.** `RaftStore` with identity, records, recovery, set-aside and compaction, plus the store VOPR.
- **B.** The `RaftGroupStorage` protocol, the `RaftNode` persistence points, and the Raft VOPR with persistence and its mutation teeth.
- **C.** Wiring: identity into `NodeId`, membership resume, per-job group recovery, replica rebuild. Verify P12 for the per-job state machine.
- **D.** E2E test (unrun), and the AD-52 sections on storage (`AD_52.md:372-373`, §16 group commit) rewritten to the built design.

Each stage lands importable and green before the next starts. Env fields come in with the stage that reads them: the `RAFT_STORE_*` group-commit batch bounds are derived from the ledger WAL writer's measured settings, not new constants, and `RAFT_SET_ASIDE_RETAINED` comes in with stage A.
