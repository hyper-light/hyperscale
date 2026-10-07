# Remaining Work Plan — every open Absent, Partial and REFACTOR item

*Written 2026-10-05 from a full re-grade of ASSESSMENT.md's ledgers against the
working tree (six independent audits; per-row receipts in the session
scratchpad `regrade/*.md`). This plan supersedes ASSESSMENT.md §2.2/§2.3/§4 as
the work list. ASSESSMENT.md itself is rewritten in Phase 9.*

## Where the ledgers stand

| Ledger | Old Partial / Absent | Now Built | Doc-obsolete | Still Partial | Still Absent |
|---|---|---|---|---|---|
| AD-1–36 | 12 / 0 | 1 | 4 | 7 | 0 |
| AD-37–53 + delta absents | 5 / 16 | 9 | 5 | 5 | 2 |
| architecture.md §1 | 25 / 0 | 2 | 15 | 8 | 0 |
| architecture.md §2 | 25 / 10 | 7 | 19 | 8 | 1 |
| architecture.md §3 | 28 / 7 | 10 | 13 | 12 | 0 |
| root + dev + delta | 68 / 18 | 22 | 5 | 50 | 9 |

"Doc-obsolete" means the code does something deliberately different and
better, so the doc changes, not the code. That covers per-job VSR (Raft is the
design, per your decision), Merkle anti-entropy and acknowledgment windows
(Raft log catch-up and the ledger replicator do these jobs), the WAL buffer
layer (the ledger WAL stack provides group commit, durable waits and recovery),
and the bootstrap module (AD-52's seed locators, join and watch replace it).

## Decisions taken on merit

The audits flagged these as owner decisions. Each is decided below on the
stated grounds. Where a decision of yours already applies, it is cited.

| # | Question | Decision | Why |
|---|---|---|---|
| D1 | Raft log: volatile, or persisted? | Wire `RaftWAL` with an identity stamp and integrity checks; a node whose disk is empty or untrustworthy joins as new | Your storage decision (2026-09-30): "we can write to disk, but we just can't assume what that disk will be" |
| D2 | Ledger read consistency (AD-38 Part 8) | Build a STRONG `get_job` on the existing `RaftNode.read_index` | It is the more correct option, and the primitive already exists and is tested (it serves membership reads) |
| D3 | Bootstrap module | Supersede it with AD-52; delete the 16 unread `DiscoveryConfig` fields; keep `max_concurrent_probes`, renamed to what it actually limits (DNS concurrency) | AD-52 join and seed locators are built and E2E-tested. A second bootstrap would be a parallel protocol |
| D4 | Discovery connection pool and sticky binding | Delete them | They would duplicate the transport connection cache the server already keeps |
| D5 | `Provision*` request/commit handlers | Delete them | Nothing sends them, and per-dispatch quorum is already provided by the AD-3 leader plus Raft |
| D6 | Circuit breaker: queue-and-replay vs reject-or-reroute | Keep reject-or-reroute and fix the doc | A replayed dispatch can carry a stale fence token, and routing already fails over |
| D7 | Cyclomatic-complexity bar | 3, as CLAUDE.md states, enforced by a ratchet lint with a snapshot of current violators | CLAUDE.md is the project's own rule. A ratchet stops new violations without a big-bang rewrite |
| D8 | One class per file | No exemptions. `models/distributed.py` (131 classes) and the log-model aggregators get split | CLAUDE.md: "One class per file. Period." |
| D9 | AD-26 throughput witness (BOCPD) | Wire it, with `reset_stream` cleanup shipped in the same change | Built and tested. Wiring it without cleanup would leak one entry per workflow |
| D10 | AD-32 per-destination isolation | Per-destination concurrency bounds inside the node-wide ones | One slow peer must not take the slots of every other peer. Bounds come from configuration, not constants |
| D11 | LoggerStream WAL modes | Keep them and fix D-4/D-5 (FSYNC_BATCH not awaited; batch writes swallow errors) | Fix what is found |
| D12 | AD-1 "never overriding" | Change the doc: the 26 overrides are deliberate template hooks | The code is the better design |
| D13 | Result streaming (G-206) | Add a `stream_workflow_results` async iterator over the existing callback path | Small, and the documented API then exists |
| D14 | GLOBAL durability (G-259) | Change the doc to match the code: copies in 2 or more regions | A 3-of-5-region quorum is unreachable for most deployments, and the code is tested |
| D15 | Fence token source (AD-10 banner) | Change the doc: per-job counters | A Raft term is shared by every job in a group, so it cannot fence jobs individually |
| D16 | Numeric SLO criteria (G-264, AD-52 §16) | Keep them, and measure them with probe scripts you run | Claims need measurements |

Items that stay with you are listed under **Needs you** at the end.

## Phases

Each item is landed with a test that fails without it. Mutation checks run
in the scratchpad, never on the live tree. The full SIM suite reruns at
every phase boundary.

### Phase 1 — live defects ✅ (2026-10-05)

1. ✅ WAL recovery leak: replay never marked recovered entries applied, so nothing compacted them and every tick checkpointed (`NodeWAL.mark_applied_through`).
2. ✅ The gate's datacenter watch followers stop with the gate.
3. ✅ Gate dispatch retry: follows leader redirects; bounded by `derive_datacenter_leader_failover_seconds`, not 9/1.0/5.0; paced at one leader heartbeat; one deadline per datacenter (`RetryExecutor.execute(deadline_at=)`).
4. ✅ `RobustMessageQueue` is FIFO across primary and overflow. The WAL queue refuses writes when full; it never drops an accepted write, which had a future that would never resolve.
5. ✅ SRV resolution: one permit per lookup (the nested acquire deadlocked at the permit bound).
6. ✅ Phantom `Metrics.record_counter` replaced, and the stale threshold derived from the AD-35 strategies. New lint `test_no_phantom_member_calls` checks 2,992 typed member calls. It found the dead `job_leadership_notification` handler (no sender, phantom tracker call), which was deleted along with its model.
7. ✅ `last_synced_lsn` is monotonic.
8. ✅ Logger FSYNC_BATCH: `log()` returns once durable; every written file is synced; a sync failure fails its waiters; close syncs before resolving; `batch()` raises its failures.
9. ✅ Replay refuses events for a terminal job, as live does (`JobEventApplier`).
10. ✅ Gate `stop()` cancels **every** background loop (it used to cancel 1 of 10) before closing the ledger and caches.
11. ✅ Gate discovery maintenance runs under the TaskRunner (raw `create_task` removed).
12. ✅ DC-manager writes go through `GateRuntimeState.set_job_dc_manager`; `persist_accepted_job` is typed.

### Phase 2 — WAL disk reclamation ✅ (2026-10-05)

- Each checkpoint cuts the log, atomically, to what the oldest checkpoint still on disk needs (`NodeWAL.discard_through` → `WALWriter.rewrite`).
  - Checkpoint file names carry their LSN (`checkpoint_{ms}_{lsn}.bin`); a name without one keeps the whole log.
  - Retention and "latest" order checkpoints by LSN, then creation time. Ordering by name sorted LSNs as text, and wall time can step backwards.
- Group commits and rewrites share the writer's file lock, so no append is lost to a rename.
- Terminal jobs whose archive write is still owed ride the checkpoint. Before this, a checkpoint silently dropped them from recovery's terminal sweep, despite the documented guarantee.
- Test `test_wal_reclamation.py`, VOPR-style:
  - 12 seeds of a 400-operation workload with seeded EIO and crashes: every acknowledged job recovers, and no checkpoint-covered frame stays in the log;
  - the log is bounded after quiesce;
  - a damaged newest checkpoint falls back through the kept log;
  - appends racing a cut are all kept (12 seeds);
  - an owed archive survives its frames' cut and a restart.

  Mutation-checked: removing the cut fails 13 tests, removing the owed-archive carry fails 3, removing the lock fails 1.

### Phase 3 — wiring what is built (S–M) — in progress

Done (2026-10-05):

- ✅ **AD-11.** State sync retries targets that are refused or still starting, with backoff spanning one sync timeout. A timeout is not re-spent.
- ✅ **AD-26-2.** The H6 throughput witness is live, with H5 deciding. Its streams end with their workflow or worker. `WorkerHealthManager.on_worker_removed` is now actually called, via the registry; it never was, so trackers and ledger entries leaked. The extension policy comes fully from Env.
- ✅ **G-8, and a deeper defect.** Every worker report wiped all of the manager's core reservations, causing double booking, and the ack's confirm then subtracted the same cores twice. Fix:
  - Reservations are kept per dispatch.
  - Every report and every dispatch ack carries the worker allocator's availability version.
  - Stale reports never overwrite newer ones.
  - Progress reports refresh free cores.

  Tested by `test_worker_core_accounting.py` (40 VOPR seeds); the mutations "wipe on heartbeat", "no progress ordering" and "no immediate drop on ack" all fail it.
- ✅ **G-55.** Every circuit breaker reads `CIRCUIT_BREAKER_*`. The manager's dead `_gate_circuit` and `_quorum_circuit` were deleted.
- ✅ **G-65 (already met).** Before any dependent dispatch, the layer context reaches a quorum through `_replicate_job_state_for_dispatch`. `RaftJobManager.update_context` moves to Phase 6's deletions.
- ✅ **AD-7.** The dead push-after-failover was deleted; pull recovery covers it. The doc is corrected in Phase 9.
- ✅ **G-43a/G-46a.** The node clock is injected into the capacity estimator and the spillover evaluator; the dead estimator methods were deleted.
- ✅ **G-25.** Duplicate acks carry `was_duplicate`/`original_job_id`. A fallback ack no longer names the retry's fresh job ID.
- ✅ **G-39.** Gate datacenter health is the worse of the managers' view and SLO compliance (`SLOHealthClassifier`).

- ✅ **G-40.** Predictor confidence uses the pressures' own scale (uncertainty over capacity), not the literals 20 and 1e8.
- ✅ **G-57.** AD-45 observability:
  - `ObservedLatencyRecorded` and `StaleObservationsDecayed` are logged;
  - `route_learning_*{dc_id}` appears in `cluster --metrics`.

  `BlendedLatencyComputed` became those metrics instead: a per-decision log would fire for every datacenter on every job. Doc fix in Phase 9.
- ✅ **G-56.** `ADAPTIVE_ROUTING_EWMA_ALPHA` = 1/8, the SRTT gain of RFC 6298 §2.
- ✅ **G-247.**
  - Router counters: `routing_decisions_total{bucket}`, `routing_exclusions_total{reason}`, `routing_fallback_used_total{from_dc,to_dc}`, `routing_cooldowns_total`.
  - AD-36 criteria 1 and 2 are tested against the real router.
  - Switch and hold-down metrics are doc-obsolete: the router is stateless per job, and rendezvous tie-breaking replaces hold-down. Doc fix in Phase 9.
- ✅ **Worker `--managers`.** Resolved through seed locators (`resolve_seed_addresses`, retried within the boot timeout).
- ✅ **Gate backpressure → client.** A shed submission carries `retry_after_seconds` = `OVERLOAD_SAMPLE_INTERVAL_SECONDS`, a new Env field replacing the 1.0 literals in the gate and manager. The replication-quorum retry hint is one replication round (it was the literal 2.0).
- ✅ **dev G-76.** `JobLeadershipAcquired` is journaled on manager and gate takeover; `JobState.leader_id` survives replay and checkpoints.
- ✅ **Found on the way:** a cut WAL recovered with no frames reused LSNs at or below the checkpoint's, so acknowledged writes were lost at the next restart. Recovery now resumes numbering after the checkpoint (`restore_checkpointed_lsn`). The VOPR test is reproducible: checkpoint names come from a stepped wall clock.
- ✅ **Job lease cleanup.**
  - It ran on a raw task the gate never stopped; it is now a TaskRunner loop cancelled with the others.
  - Its failures went to an error callback nobody set (silently dropped); they are now logged (`JobLeaseExpiryCallbackFailed`).
  - The `print`s are gone, and the expiry hook is set before the loop starts.
  - 2026-10-06: the expiry hook itself is removed. With lease import gone, every lease is the local gate's own, so `on_lease_expired` only ever returned early. The hook, its `JobLeaseExpiryCallbackFailed` logging and `acquire`'s other-holder/`force` path went with it. Gate orphan detection is SWIM gate failure (`mark_jobs_orphaned_by_gate`).

Remaining:
- CI nightly VOPR: time the default sweep once sim16 is done (no concurrent heavy sims), then set `--sim-vopr-count` and `HYPERSCALE_SIM_SOAK` within the job's timeout.
- DiscoveryService logger: moved to Phase 6, with the pool/sticky deletion (D4), so its 60 construction sites change once.

### Phase 4 — decided implementations (M–L) ✅ (2026-10-05)

- ✅ **D2, AD-38 Part 8 read consistency.** `JobStatusQuery` carries a level: `ReadConsistency` = EVENTUAL, SESSION, BOUNDED_STALENESS or STRONG.
  - EVENTUAL reads, and reads of a terminal status, are answered locally.
  - The job's leader answers the rest. For STRONG, a manager first re-syncs to a quorum, and a gate re-commits its replica to a quorum.
  - A manager follower answers SESSION when its leader's synced view is at least as new as what the reader has seen, and BOUNDED_STALENESS when time since arrival plus the sync's transit bound is within the limit. Otherwise it forwards once.
  - The client carries its session view per tracked job.
  - `hyperscale job status` gains `--consistency` and `--max-staleness`.
- ✅ **Found on the way:** the gate answered no client's `job_status` or `ping`. Its receivers were named `receive_job_status_request` and `receive_gate_ping`, so every gated status poll, every gate ping, and `job status --gates` failed. Both are renamed to the managers' action names. New lint `test_every_sent_action_has_a_receiver`: every sent action is received somewhere, and every client action by each tier it can address. E2E test `test_cli_gated_job_status.py` covers it (unrun).

- ✅ **D13, `HyperscaleClient.stream_workflow_results(job_id)`.**
  - Replayed results come first, then live ones, each exactly once.
  - The stream ends when `wait_for_job` would return, and raises what it would.
  - A reader that stops early leaves nothing running.
- ✅ **G-26, G-20 and G-27, AD-40 across gates.**
  - `GateJobReplica.idempotency_key` is adopted by every gate committing the replica.
  - Prepare refuses another job holding the key.
  - **Found on the way:** the replicating gate counted its own vote without preparing locally, so two gates admitting one key at once both committed. It now prepares its own vote first, and withdraws it on failure.

- ✅ **D10, AD-32 per-destination send bounds.** Each destination has its own bound (`OUTGOING_QUEUE_SIZE`), taken before a node-wide slot, so requests queued behind a silent peer hold no node-wide slot. The whole request (its waits, its dial, its reply) fits one deadline, and a destination is forgotten once its last request settles.
- ✅ **AD-52 §10 staleness consumers.**
  - `ClusterWatchFollower` reports each entry into and exit from disconnected mode once, as it happens. The gate (per datacenter) and the worker log it as `ClusterWatchConnectivityChanged`.
  - A gate's `cluster --metrics` exports `cluster_watch_staleness_seconds` (`+Inf` before the first observation), `cluster_watch_disconnected` and `cluster_watch_applied_index` per datacenter.
  - The membership VOPR checks every reported transition against the cache.
- ✅ **AD-52 §17 datacenter regeneration (P-AD52-3, the reset option).** A datacenter whose watch answers as a different `cluster_uuid` loses its AD-45 observed latency and AD-42 SLO violation clock, once, logged as `DatacenterRegenerated`. §10 and §17 now describe what is built: there is no replicated DC catalog, and each gate learns datacenters by configuration, join and watch.

- ✅ **AppendEntries pipelining: measured, not built.** Every per-job group's proposals come from the job ledger, which commits a job's entries one at a time (`JobCommitSequencer`, pinned by `test_a_jobs_entries_replicate_in_append_order`). So no group ever has two proposals to overlap, and the per-peer coalescing outbox already overlaps groups. The membership group proposes at operator rate. §16 now says this.
  - **Found on the way:** `RaftJobManager`'s wrappers, `ReplicatedStatsStore` and `ReplicatedMembershipLog` are never constructed or called; they move to Phase 6.

- ✅ **D1 design** (`docs/architecture/D1_RAFT_PERSISTENCE.md`). Every Raft group of a node (membership and per-job) persists to one node-level, group-committed store under a persisted, stamped identity. An empty disk joins as new, an intact own disk resumes, and an untrustworthy disk is set aside.
- ✅ **D1 stage A: `RaftStore`** (`raft/store/`). CRC-framed msgspec records (no pickle), identity stamp, a torn-last-frame-only recovery rule, set-aside (newest kept, `RAFT_SET_ASIDE_RETAINED`), and live-only compaction at dead > live. Truncation and set-aside require a stable second read. Store VOPR (`test_raft_store_vopr.py`, 40 seeds x 60 rounds, power loss, torn tails, concurrent compaction); 6 mutants plus the flaky-read guard are caught.
- ✅ **D1 stage B: `RaftNode` persistence points.** Every reply is flushed in a `finally` under the lock. The candidate persists term and vote before asking for votes. The leader sends, then persists, and counts itself only through `_durable_index`. Also: `recover`, `release`, and fail-stop on a failed write. Wired through `RaftConsensus`, `GateRaftConsensus`, `ClusterMembership` and the integrations as a required `storage` (servers pass `VolatileRaftStorage` until stage C).
  - The Raft VOPR gained disks (`resume_probability`, whole-group power loss, disk latency, crash-after-reply). Its new oracles read durable bytes: a candidate's vote must be durable before it asks, a granted vote before it answers, an append's entries before it answers, and a commit must be on a quorum of disks.
  - **Found on the way:**
    - The harness let a crashed incarnation's drained writes send answers; it now drops them.
    - The idle `WALWriter` polled every 500 µs: ~1,500 wakeups/s and 4.5% of a core per writer, measured, now 0. The batch timeout was deleted.
    - `RaftWAL` was dead code whose own test pinned silently stopping at mid-file corruption. It is deleted.
    - The idempotency WAL had no checksums (a flipped job id would replay as valid) and destroyed the bytes after a bad frame. It now has the `HSIL` format header and CRC frames, preserves discarded bytes, and sets aside an unrecognized file.
- ✅ **D1 stage C: wiring.**
  - `hyperscale run manager|gate` opens the node's store (`opened_raft_store`) before building the server. The server runs as the identity the store holds (`node_created_ms`); an identity of another placement is set aside.
  - `ClusterMembership.start` resumes the membership group of the member it is. `_abandon` persists the next participation before releasing the group (a crash in between releases a stale group, never resumes it).
  - Job coordinators recover their groups before the tick loop and release a group on disk when its job ends, but not at shutdown.
  - A group's creation (member id, initial voters) is its first record. Re-applying committed entries after a restart rebuilds only in-memory replicas (P12 verified: `LEDGER_APPEND` into `JobLedgerReplica`).
  - E2E (yours to run): `tests/integration/cli/test_cli_datacenter_restart_resumes.py`.

### Phase 5 — ratchet lints ✅ (2026-10-05)

These are snapshot ratchets in `tests/simulation/lints/`, sharing `ratchet.py`. A site that is new or worse fails, and so does one that improved or disappeared until its snapshot is lowered (`uv run python -m tests.simulation.lints.<lint>` regenerates the snapshot). Today's counts:
- `test_complexity_ceiling`: McCabe at most 3, counted the way radon counts but in stdlib `ast` (no tool install), D7. 2,697 functions over the ceiling.
- `test_one_class_per_file` (D8): 208 files.
- `test_dataclass_conventions`: `slots=True` and inside `models/`. 290 dataclasses.
- `test_no_swallowed_exceptions`: an `except` that only passes; it only stops new ones. Existing sites are not worked unless Raft-related (your decision, 2026-10-06).
- `test_no_inline_imports`: 101 functions.
- `test_no_task_runner_logging`: 122 functions.
- Already in place: phantom attributes and member calls, raw asyncio tasks, sent actions with no receiver.

Each lint was checked for teeth against a planted probe module, and a lowered entry fails as stale.

### Phase 6 — dead code ✅ mostly (2026-10-05)

Each target was re-verified against the current tree before deletion (no production reference, including by string name). Tests that only exercised dead code went with it, and the ratchet snapshots were lowered.
- ✅ D3: 15 unread `DiscoveryConfig` fields and their validator. `max_concurrent_probes` became `max_concurrent_dns_resolutions` (Env `DISCOVERY_MAX_CONCURRENT_DNS_RESOLUTIONS`). `PeerInfo.should_evict` deleted.
- ✅ D4: `discovery/pool/` (925 lines). Sticky binding is out of `DiscoveryService` (and its `use_sticky` kwarg out of its two callers); the type parameter, the pool and sticky metrics are gone too. The one meaningful test was kept as `test_select_peers_fill.py`.
- ✅ D5: `Provision*` handlers, models, `ProvisionState`, the manager-state fields and lock, the in-flight entries, and E2E `section_15` (it asserted only those dicts).
- ✅ `NodeHealthTracker` ×2, `TokenBucket`, and the manager's unread `FederatedHealthMonitor`.
- ✅ `_check_gate_raft_leader_takeover`. The client leadership tracker keeps only fence validation and leader updates. The client's orphan tracking had no producer left (its check loop is deleted), so it went too: state, methods, the `OrphanedJob` model.
- ✅ `health/probes.py`, its 12 Env fields and getters. `test_stop_honours_caller_cancel` was re-pointed at `FederatedHealthMonitor`, **which found a live bug:** its probe loop swallowed its own cancellation, so a cancel aimed at a stopping caller was lost. It now re-raises. `select_worker` / `select_peer_manager` (uncallable, a `TypeError`) deleted.
- ✅ The four AD-37 predicates and `MESSAGE_CLASS_TO_PRIORITY`; `CLIENT_PROGRESS_*`.
- ✅ The 9 gate re-export shims, `env/memory_parser.py`, `broadcast_tcp`/`broadcast_udp`, `WorkerState`'s duplicate manager registry. `classify_update_tier` now exists once (the gate calls the coordinator's).
- ✅ Raft: `RaftJobManager`/`GateRaftJobManager`, `ReplicatedStatsStore`, `ReplicatedMembershipLog`. **Plus the cascade:** with the wrappers gone, only `LEDGER_APPEND` had a proposer. 43 command types, both command dataclasses and both state machines became one `LedgerAppendCommand` (msgspec; no Raft command is unpickled any more, D1 reads them from disk) and one `LedgerStateMachine`. The consensus and integration constructors lost `job_manager`/`leadership_tracker`/`manager_state`/`gate_state`.
- Left, and why:
  - ✅ decided 2026-10-06: the top-level `hyperscale/{monitoring,tools,versioning}` packages stay as they are. They're your in-progress work ("CPU resource limiter"), not dead code;
  - `OrphanedJobInfo`: used only by `test_client_leadership_transfer.py`'s self-contained fake client;
  - aligning the `"progress_update"`/`"stats_update"` labels.

### Open from SIM17 (2026-10-05)

- ✅ `test_multiprocess_l2_extension.py` re-pinned (8 pass). Wiring the AD-26 throughput witness (D9) turned on the full H5 evaluator. Before that the route fell back to the legacy grant. H5 judges a first request against the zero-progress baseline, so the dispatch-time request, and the autonomous one on a single long action, are denied `no_advancement`. Decision instants and completions are unchanged (37.80582, 22.354678). This is what "progress-backed only" calls for.
- ✅ `test_multiprocess_workflow_lifecycle.py::test_a_long_job_making_progress_is_not_declared_stuck` (2026-10-05): not a core/jobs defect. The harness's steady workflow returned a plain dict, which made it an ACTION workflow, and core runs an ACTION workflow's steps once by contract. Its step now returns a `CustomResult` (built inside the builder, so it pickles by value past the restricted unpickler), which makes it a TEST workflow whose VUs repeat for the 50 s duration. All 16 tests in the file pass, and this proves TEST workflows run under SIM.

### Phase 7 — conversions (L; the lints hold the line)

- `task_runner.run(logger.log, …)` → `await logger.log(…)` under `hyperscale/distributed` (183 sites). ✅ 99 sites in async functions are converted (2026-10-05; the ratchet snapshot is lowered, and test stubs got async loggers). ✅ Of the 83 in synchronous functions:
  - The ones whose callers reach them from async code are async now: the federated probe-error hook, the lease-expiry hook, gate datacenter selection, the gate result-producer validators, the manager's peer worker snapshots and `register_worker`, and the health-alert loggers.
  - Those with no caller were dead code and are deleted: `ManagerLeadershipCoordinator`'s hooks and `detect_split_brain`, `get_progress_state`, the version-skew worker/peer side, and a duplicate `WorkflowProgressHandler`.
  - Left on the ratchet, deliberately: sites invoked synchronously by their contract. These are asyncio done-callbacks and `datagram_received` (protocol callbacks cannot await); Raft and SWIM leader-state transitions, which are synchronous under their locks; the federated monitor's ack and timeout handlers; and `is_message_fresh`, a per-gossip hot path. Making those async would add a coroutine to a hot path or turn a lock-held transition into a yield point, just to emit a log line. Engines are not changed beyond what you authorize, and `core/jobs` is left to its peer owner.
- Existing `except …: pass` sites: out of scope unless Raft-related; abort/shutdown sites stay as they are (your decision, 2026-10-05/06).
- ✅ Hoisted the inline imports in `distributed`: 27 files, each checked by importing it and all four node entrypoints in a fresh interpreter, then the full suite. The ones left are cycle-breakers: `Env`'s config getters import modules that import `Env`, plus `rate_limiting`'s import of `models`. The reporting backends' imports stay too (optional third-party dependencies, imported lazily).
- `Any` → Protocols and generics. ✅ Started (2026-10-05):
  - Canonical seams: `runtime.SendTcp` (a bound `send_tcp`) and `runtime.RunTask` (a bound `TaskRunner.run`).
  - `send_tcp`/`send_udp` now declare what they take and return: `bytes | Message` in, `tuple[bytes | Exception, int]` out. The old `D`/`R` struct type-vars were wrong.
  - All 54 bare-`callable` worker callbacks are typed.
  - ✅ Done (2026-10-05): 178 `Any` annotations → 2, both justified:
    - `Checkpoint.job_states` is a persisted msgspec contract. A record type would refuse older checkpoints, and msgspec treats `object` as a custom type.
    - `restricted_loads` mirrors `pickle.loads`.
  - What replaced them:
    - concrete collaborators under TYPE_CHECKING;
    - generics: `LWWRegister[V]`, `LWWMap[K, V]`, `Run[T]`, `RetryResult[T]`, and ParamSpec for retries;
    - TypedDicts, one per file, for every stats/metrics/snapshot dict shape;
    - precise callback signatures.
  - Checks: model pickles byte-identical; eager-annotation scan 0; import sweep clean on 3.12/3.13.
- ✅ Dockerfile, release.yml and devcontainer use uv only (2026-10-05):
  - The release image `uv tool install`s exactly the version the workflow published.
  - release.yml's image build-args had never matched the Dockerfile ARGs, so they were silently ignored; fixed.
  - requirements.txt/.in were superseded by uv.lock and removed.
  - Smoke-tested: an image built from a current-source wheel runs `hyperscale --help`.
  - Note: the *published* 0.7.2 CLI crashes at import when uvloop is installed (`get_event_loop` under uvloop's policy). The current source does not, so the next release fixes the image.
- ✅ Python 3.12/3.13 support restored (2026-10-05).
  - The cause: 3.14 evaluates annotations lazily, so unquoted annotations that are bound only under TYPE_CHECKING, or that use `callable | None`, crashed the worker (and every node package) at import on 3.12/3.13. CI and the release image run 3.13.
  - A static eager-annotation scan, plus a fresh-import sweep on 3.12/3.13/3.14, are clean outside `core/engines`.

### Found and fixed while running the E2E and integration suites (2026-10-05)

- **CLI E2E:** all four named files now pass on the live tree: restart_resumes 2/2, gated_job_status 1/1, leader_leases 2/2, gate_follows_manager_resize 1/1.
  - **Manager restart:** a restarted manager answered `hello` (and adopted a founding) before `start()` resumed its disk group. A peer then founded a new cluster over it and its recovered group was released. It now refuses until it is running.
  - **Gate resize tracking:** a gate never followed the membership of a datacenter that joined or registered with it. It now starts the watch wherever it ingests that datacenter's manager report.
- **tests/integration/raft:** 111/111 pass (was 28 failed + 11 errors). Every failure was a stale test, rewritten to keep its invariant. The 15 vacuous cancellation tests now drive real code and are mutation-checked.
- **Gate single-workflow cancel:**
  - `CANCELLING` / `ALREADY_COMPLETED` were dropped from the aggregate (the client was told NOT_FOUND).
  - The cancel was sent to every datacenter, not the job's.
  - There was no fallback past a dead first manager.
- **Gate final-result forwarding could loop between gates.** A result no gate leads, or two gates each believing the other leads, ping-ponged until timeouts unwound it. A forwarded result now arrives on `job_final_result_forwarded`, which never forwards again (mutation-checked: without the guard the test recurses forever). The gate-side retry on the leader forward was removed, because the manager's completion-notice obligation already resends until acknowledged.
- **Worker AD-19 throughput was never recorded,** so heartbeats reported 0, and the AD-26 H6 throughput witness was blind. Completions are now recorded where workflows finish. This changes extension decisions: run SIM.
- **Found while typing, all fixed:**
  - **Worker discovery off the loop:** the worker submitted a *sync* discovery callback to the task runner, which ran it in a thread executor. It called `task_runner.run` off the event loop. The async registration is now submitted directly.
  - **`KeyError: 'is_test'`:** a job leader learning a workflow from a worker's report raised this. The workflow is now learned with its results unmerged; a registered workflow keeps its own classification. Covered by a new test.
  - **SWIM per-peer lock leak:** every peer address ever probed or joined kept a lock forever, and probe/ping-req targets come off the network. Locks are now leases that the last holder removes (`ContextValueLease`). A test covers no growth, exclusion, reentrancy and cancellation. The dead per-peer `write_context(target, b"OK")` was removed too.
  - **Annotations corrected:**
    - the server's `Handler` alias (3 parameters; replies `bytes | Message`);
    - `send_tcp`/`send_udp`/`Transport`;
    - `WorkerDisseminatorStats`.
  - **Smaller cleanups:**
    - removed `JobManager.update_context`, which was dead and could never have worked (`Context` has no `__setitem__`);
    - fixed a test assertion that was always true;
    - deleted two queues nothing used.
- **Dead code removed:**
  - `WorkerExecutor`, a dead wrapper whose progress loop duplicated the live one;
  - five unused worker models;
  - `JobForwardingTracker`, a dead parallel forwarder that counted failed sends as successes;
  - the manager's dead cancellation completion-event, initiated-at and per-workflow-lock state, including a pop on the wrong key.

### ✅ Replay protection for every frame (2026-10-06)

Distributed nodes had none. The transport checked only `msgspec`-model payloads; every distributed message is a dataclass its handler decodes, and its id was never pickled. A captured frame replayed byte for byte ran its handler again.

Now:
- Every frame carries a per-send Snowflake (`frame_id`) inside its AES-GCM-authenticated body: `…clock(64) request_id(8) frame_id(8) data_len(4) data`.
- `ReplayGuard.validate_frame` checks every frame: TCP and UDP, requests and replies.
- Duplicates are keyed on the encryption nonce. Snowflakes of different senders collide; 96 random bits don't.
- A watermark (the newest timestamp evicted from the bounded set) refuses anything that could have been forgotten. It starts at the guard's start minus its max age, so frames captured before a restart cannot be replayed into it.
- A resend is a new encryption and is accepted.
- No wall clocks are compared across hosts after start.
- Cost: about 0.2 µs per frame each side.
- `Message.dump()` stamps `message_id` and `sender_incarnation`.

Tests:
- `test_frame_replay_protection.py`: a 40-seed guard VOPR against a watermark model, Snowflake collisions included, plus real-socket TCP and UDP capture-and-replay and resend tests.
- Mutants: dropping the check, the watermark or nonce keying each fail it.

### Phase 8 — REFACTOR.md program (L)

In the order the complexity lint ranks them:
- manager `job_submission` (631 lines, CC 68);
- gate `handle_submission` (CC 53);
- `_advance_formation` (CC 59);
- the remaining manager, gate and health_aware_server domains move into composed classes;
- ✅ `models/distributed.py` split into 127 one-class files (2026-10-05). Per your decision, `distributed.py` stays the wire namespace: it re-homes each class's `__module__`, so pickles are byte-identical (checked for all 127), mixed versions interoperate, and persisted data loads. ✅ The other eight multi-class files in `models/` (`gate_replication`, `internal`, `worker_state`, `jobs`, `crdt`, `client`, `coordinates`, `restricted_unpickler`) are split the same way: 40 classes, pickles byte-identical, unit suite green. `models/` now holds one class per file. Remaining: the 101 multi-class files in `distributed/`, split in scratch the same way (every class and moved function re-homed). Of these, 9 had to move their main class to a `_model`/`_base`/`_impl` file because a namespace module can't define a class its split-out siblings import.
  - Verified: 354 classes pickle byte-identically, and all 1,059 modules import fresh on 3.13 and 3.14.
  - Five tests that monkeypatched a split module's seam now patch the files that read it.
  - SIM's `swap_defaults` rebinds every module holding a seam, so it needs no change.
  - The other 89 multi-class files, decided 2026-10-06: 16 are being split one class per file (pickled classes keep their wire namespace): `core/runtime/{filesystem,real_filesystem}`, `logging/exceptions`, `reporting/{cloudwatch/cloudwatch_config,time_aligned_results}`, and 11 non-vendored ui files. These stay: `logging/hyperscale_logging_models.py` (multi-class by design: CLAUDE.md puts every logger model there), the vendored plotille and tabulate modules under ui, and `commands/cli/` (the CLI framework is off-limits). The 60 in engines need your explicit OK, the 3 in core/jobs belong to the core/jobs owner, and the 3 in tools are your in-progress work;
- move the 30 dataclasses that sit outside `models/`.

Rules for every move: behavior-preserving; public messages and actions unchanged; full SIM run between moves; each move one commit.

### Phase 9 — tests, probes, documents

- **Tests:** ✅ done (2026-10-05) except rolling upgrade, which waits on the wire-evolution decision. Each suite is seeded and mutation-checked, and together they found and fixed 19 real bugs:
  - **Adversarial input:**
    - `RestrictedUnpickler` was bypassable through dotted names under pickle protocol 4+ (`logging` + `os.system`). Fixed in both copies.
    - A TCP reply with no separators raised `ValueError` out of its task.
    - Unsolicited UDP replies grew a queue per forged pair. UDP replies are now correlated by request id: a stale reply was being handed to the next request (a stale cross-cluster `xack`), and no per-peer queue is kept.
    - A `None` founders digest matched before the first resize.
    - `_adopt` left half-applied state on a damaged founding.
  - **Spillover, t-digest and reporters:**
    - The t-digest grew without bound and its quantiles ran non-monotone.
    - A bigger job could get a shorter wait; a free datacenter reported a non-zero wait.
    - Spillover could pick a datacenter smaller than the primary's spare capacity.
    - Gate reporters had no deadline or close, and every later job's results were logged as the first job's.
    - A hung client reporter blocked job completion. `REPORTER_SUBMISSION_TIMEOUT_SECONDS`; a failed connect is closed too.
  - **Clock, overload and fanout (SIM):**
    - AD-52 §11 lease round numbers were reused, which could credit a lease no member backed.
    - Gate `JobLease`s were never freed, and renewal had a 1 s floor.
    - Workers freed a running workflow's cores early.
    - An AD-24 refusal was misread as an ack, dropping final results.
    - Readiness refreshed only on heartbeats: 100 jobs took 97.6 s, now 14.5 s.
    - Late progress re-created AD-30 tracking for a finished job.
  - **Also found and fixed:**
    - The frame cap (1 MiB) had drifted below `MAX_MESSAGE_SIZE` (3 MiB). It is now derived: message size plus AES-GCM overhead.
    - A watch's `wait_seconds` (inf/NaN) is capped by `CLUSTER_WATCH_WAIT_SECONDS`.
    - The worker's pending-result loop crashed on its own error log; it now sleeps until the next result is due.
    - `WorkerState` ignored `WORKER_THROUGHPUT_INTERVAL_SECONDS` and `WORKER_COMPLETION_TIMES_MAX_SAMPLES`.
    - The result backoff cap is the AD-30 threshold.
    - The AD-24 overload retry hint is `OVERLOAD_SAMPLE_INTERVAL_SECONDS`, and the idle cleanup is `RATE_LIMIT_CLIENT_IDLE_TIMEOUT`.
    - `GateConfig`, the reporter-task maps and the job forwarding tracker were dead and are deleted.
  - Every complexity rise from these fixes was restructured away rather than snapshotted: total 20,764 → 20,669, with no new violators.
- **Probes you run:** throughput+RSS, stats ingest, spike, and the AD-52 §16 benchmarks (D16).
- **Doc sweep, from the line ranges each audit file lists:**
  - architecture.md: bootstrap, Merkle, ack windows, VSR, buffer layer, and Parts 12.5–12.7;
  - AD_38, AD_41, AD_42, AD_43, AD_44, AD_45 and AD_52;
  - AD_52_PLAN;
  - FIX.md, TODO.md, WAL.md, SCENARIOS, SCAN, the compliance reports, and simulation_framework.md;
  - ASSESSMENT.md, rewritten as a fresh grade.

## Needs you

- **Wire evolution for `Message` dataclasses** (found 2026-10-05). Adding a field to a slotted `Message` dataclass breaks mixed-version clusters both ways: measured on 3.12/3.13/3.14, old reads new → AttributeError; new reads old → field unset, AttributeError on read. Today any new message field is a breaking change for rolling upgrades (AD-25). A fix is a `Message`-level `__getstate__`/`__setstate__` keyed by field name that fills defaults and drops unknown fields. That changes the pickled state format once, so it needs your call on the cutover.
- **✅ Datacenter leases: decided and removed 2026-10-06.** They were built but never acquired. `DatacenterLeaseManager`, `DatacenterLease`, `LeaseTransfer`/`LeaseTransferAck`, the gate's `lease_transfer` handler and its DC-lease state and snapshot fields are deleted. architecture.md principle 4 now names what gives at-most-once DC semantics: per-job gate leadership and fencing tokens (Raft-committed `GateJobReplica`), AD-40 idempotency, and manager-side fencing.
- **✅ Unpickler and by-value workflows: decided 2026-10-06.** Running a workflow means running the submitter's code, so the trust boundary is the authenticated frame, not the unpickler: every frame is AES-GCM-authenticated under the cluster secret (refused when weak; per-user cookie or per-run secret by default) and replay-checked (frame id + nonce, 2026-10-06). The `RestrictedUnpickler` stays as defense in depth for non-workflow payloads. This is the model cloudpickle-based schedulers use; documented in `docs/architecture.md` security section.
- **AD-24 limits:** the nodes don't use Env's `RATE_LIMIT_*` bucket settings or `get_rate_limit_config()`; the per-operation table is `AdaptiveRateLimitConfig`'s built-in default. Unify them through Env? That changes limits the storm SIM pins, so it needs measurement.
- **✅ Gate lease import/export: decided and deleted 2026-10-06.** Gate takeover never needed it. It commits a `GateJobReplica` through Raft whose fence is strictly above every fence known for the job, and dispatch and manager fencing read that fence, never the `JobLeaseManager`'s. The job lease is local to the gate that admitted the job.
- **✅ Client completion waits on local reporters: decided 2026-10-06 — keep.** `wait_for_job` returning means the job's result files exist (the contract scripts rely on); each reporter's connect+submit and close are bounded by `reporter_submission_timeout_seconds`, so a hung reporter delays completion by a bounded time, never forever.
- **The distributed server's `@task` hook** (`hyperscale.distributed.server.task`, exported) has never worked. `_get_task_hooks` never matches, `_tasks` is never filled, and it double-wraps kwargs. Nothing uses it: nodes call the task runner directly. Delete it, or wire it?

- **Threading in executors and vendored SSH** (G-70): CLAUDE.md forbids threading; the engines use it. Do you want a carve-out, or a conversion? It's your call because of the engine-authorization rule. ✅ Everywhere outside the engines it is gone (2026-10-05). The log record's `thread_id` and the hook and arg snowflake seeds now take the process id: on a one-thread asyncio process that is the writer's identity, and on Linux the main thread's id *is* the pid. An unused import and an unread attribute were deleted. Lint `test_no_threading` holds it at zero, engines excepted until you decide.
- **✅ Security defaults (P8): decided and implemented 2026-10-06.** Weak secrets refused everywhere; the per-user cluster cookie (`hyperscale/commands/run/cluster_cookie.py`) backs `--acm-secret` / `MERCURY_SYNC_AUTH_SECRET`; `LocalRunner`/`ServerRunner` generate a per-run secret (`core/jobs/runner/run_secret.py`). Engine TLS verification implemented 2026-10-06: `setup_client(..., verify_tls=True)` verifies every engine client (HTTP/3 follows the context); Workflows opt out with `verify_tls = False`; `hyperscale ping <tls-protocol> --insecure`; Workflow `cert_path`/`key_path`/`reset_connections` now actually reach `setup_client` (the config only took keys it already held truthy). Tests: `test_engine_tls_verification.py`, `test_cli_ping_tls.py` (mutation-checked).
  - **Cluster secret.** Today `Env.MERCURY_SYNC_AUTH_SECRET` defaults to `"hyperscale-secret"`, a published value on the weak list, which only warns outside `HYPERSCALE_ENV=production`. Every unconfigured cluster therefore authenticates with a public key, and by-value workflows mean that key admits code execution.
    - Decision: refuse weak secrets everywhere (the production-only escape hatch goes) and remove the published default.
    - A node with no secret configured uses a per-user cluster cookie, the Erlang `~/.erlang.cookie` model: 32 random bytes from `secrets`, created once with mode 0600 under the user's config directory. Nodes of one user on one host share it with zero configuration, and multi-host clusters distribute it or set `MERCURY_SYNC_AUTH_SECRET`.
    - When the cookie cannot be created, the command fails with that instruction rather than running unauthenticated (no substrate assumptions).
    - Tests pass explicit test secrets.
  - **Engine TLS verification.** Every engine client sets `check_hostname = False` and `CERT_NONE`. Decision: verify by default (RFC 9110 §4.3.4; k6, the comparator, verifies by default and has `insecureSkipTLSVerify`), with an explicit per-workflow opt-out for self-signed staging targets. The handshake cost is the same one k6 pays.
- **`RETRY_BUDGET_DEFAULT`** 10 vs 20, and the late-result policy (P-AD44-1). I'll propose both with measurements in Phase 3; you confirm.
- **E2E tests to run:** `tests/integration/cli/test_cli_leader_leases.py` and `tests/integration/cli/test_cli_gate_follows_manager_resize.py`.
