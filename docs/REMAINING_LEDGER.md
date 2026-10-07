# Remaining Ledger — Partial and Absent items vs. the design docs

*Date: 2026-10-06 · Commit: `1a31203d` (branch `AL-rework-commands`) · Read-only regrade.*

Source of the item list: ASSESSMENT.md §2.2/§2.3 and its per-ledger grade rows
(`.scratch/assess-project/grades/*.md`, graded 2026-08-21, delta 2026-08-23).
Every row the original grading called Partial or Absent is regraded here against
the current tree as one of: **Built** (wiring file:line + test), **Doc-obsolete**
(the code deliberately does something better, or an owner decision retires the
bar; plan decision cited), **Still Partial**, or **Still Absent**. Plan references
are to `docs/REMAINING_WORK_PLAN.md` (D1–D16, Phases 1–9).

## Summary

| Ledger | Old Partial / Absent | Built | Doc-obsolete | Still Partial | Still Absent |
|---|---|---|---|---|---|
| AD-1–36 | 12 / 0 | 4 | 4 | 4 | 0 |
| AD-37–53 + delta absents | 5 / 16 | 11 | 6 | 4 | 0 |
| architecture.md §1 | 25 / 0 | 13 | 7 | 5 | 0 |
| architecture.md §2 | 25 / 10 | 14 | 17 | 4 | 0 |
| architecture.md §3 | 28 / 7 | 18 | 15 | 2 | 0 |
| root docs + delta partials | 37 / 8 | 24 | 4 | 17 | 0 |
| dev docs | 25 / 7 | 11 | 1 | 15 | 5 |
| **Total** | **157 / 48** | **95** | **54** | **51** | **5** |

These counts are lower than the plan's table (5/2, 8/0, 8/1, 12/0, 50/9). Work
landed after that table was written closed more rows: AD-41 enforcement, AD-43
heartbeat capacity, AD-54 workflow state machine, the real HLC, all job event
types, REGIONAL/GLOBAL replicators, F_FULLFSYNC, FIX.md §1.1/§1.2, and AD-52
membership. Root and dev are now counted separately; together they are 61/90
old, 32 still Partial and 5 still Absent. A few mechanisms appear in more than
one ledger: complexity and god files (AD-27-1, P-AD52 lint note, R-G59/60,
D-83/84), and AD-44 late results / `RETRY_BUDGET_DEFAULT` (P-AD44-1, A3-G-50).

### The ten most important remaining items

1. **D-95: CLOSED.** `NodeWAL` used to cut at the first corrupt frame anywhere in the file. It now accepts only a torn tail and otherwise refuses to start with `WALUntrustworthyError` (AD-38 Part 3.2). The same change closed a length-field gap in `RaftStoreCodec`, which had silently truncated acknowledged records after a mid-file frame whose length was damaged.
2. ~~**A2-G-266: the gate takeover replica (`GateJobReplica`) is not persisted before its prepare-ack.**~~ BUILT.
   Every prepare, commit, abort rollback and reap of a job's replica is written to the gate's Raft store (`KeyedStateRecord`, versioned, identity-stamped) before the ack that depends on it, and recovered at gate start before any replica RPC (`replication_coordinator.py` `recover_durable_replicas`). It stays a two-phase commit, not a Raft group: AD-40 key exclusivity spans jobs. `docs/architecture.md:148` corrected. Tests: `tests/unit/distributed/gate/test_gate_replica_durability.py`, `tests/unit/simulation/sim/test_multiprocess_gate_replica_durability.py`.
3. ~~**D-70: the client ignores `JobAck.retry_after_seconds`.**~~ BUILT (b56858c2, 10a82097).
   `ClientJobSubmitter._backoff_before_retry` waits out every hinted refusal, the last one included, jittered upward; un-hinted refusals back off from the Env base. Tests: `tests/unit/distributed/client/test_client_retry_hints.py`, `test_multiprocess_rejection_storm.py`.
4. **R-G59 / R-G63 / AD-27-1 / D-84: thin servers.**
   The files grew during the complexity splits: manager `server.py` 14,604 lines, gate 10,102, `HealthAwareServer` 7,601. Phase 8's move into composed domains has not started. Size L.
5. **R-G60 / D-83: complexity ≤ 3.**
   About 1,567 functions are over the ceiling, 245 of them in `distributed/`. The ratchet holds the line. Size L.
6. ~~**AD-26-2: the H8 outcome posterior is learned and gossiped but never read.**~~ BUILT (9b29a188).
   `alpha_budget` weights H6's α by the class's failure posterior; a K-S test at that α confirms a BOCPD change point before a deny; the H6 witness is fed per-interval rates from progress reports. See the AD26-2 entry below.
7. **P-AD44-1 / A3-G-50: CLOSED.** Late DC results follow `BEST_EFFORT_LATE_RESULT_POLICY` (`log` default: `LateDatacenterResult`, not aggregated; `update`: provisional release, straggler fold and re-push, AD-38 terminal once at window close). The four AD-44 metrics and three log models exist; `RETRY_BUDGET_DEFAULT` stays 10, decided on evidence (AD_44.md Part 6).
8. ~~**AD-24-1: the nodes ignore Env `RATE_LIMIT_*` and `get_rate_limit_config()`.**~~ BUILT (864f4fda).
   `AdaptiveRateLimitConfig.from_env` derives every limit from the node's Env (`reliability/rate_limit_derivation.py`); the hand-set `RateLimitConfig` and the unused client-side `CooperativeRateLimiter` are deleted (925b345c).
9. **Unmeasured performance claims (P-AD52-3, A2-G-264/258/247, R-G3/35/39).**
   There is no `tests/benchmarks/` and no throughput/RSS, ingest, spike, AD-38 latency, AD-52 §16 or 5000× probe. AD-36's "< 10 s reroute" has no test (the DC-loss bound is 30 s). Size M.
10. ~~**D-42 / D-13: nightly CI and continuous invariants.**~~ BUILT (2026-10-07).
    `vopr-swarm-nightly` (time-budgeted, one job per suite, soak included) and `vopr-swarm-weekly` (seeds 1-500 per suite, five shards) run `run_swarm.py`, which keeps a failing-seed ledger the next night replays first; the nightly `vopr` job runs the soak. Every `ClusterHarness` scenario runs the full continuous catalog. See D-42, D-13 and D-13b below.

Also notable, small (closed 2026-10-07): the unfed `ManagerDiscoveryCoordinator` was deleted (AD-28-1); `DiscoveryService` logs failed DNS lookups (R-G67); the raw-task lint covers all of `hyperscale/` (R-G66). Doc banners are stale: architecture.md:21085 still says read consistency is "Not built" and AD_52.md contradicts its own Status (FIX.md's §1.1/§1.2 status was corrected, R-G51).

Also closed 2026-10-07 (thirteen S rows): A1-G-14, A1-G-56, A1-G-42, A1-G-48, A1-G-77, A3-G-9, R-G11, R-G51, P-COMPLIANCE-1, D-9, D-11 (Doc-obsolete), D-66, D-81. The per-ledger counts above are as of the regrade.

## Ledger: AD-1–36
Counts: old P/A 12/0 → Built 4 · Doc-obsolete 4 · Still Partial 4 · Still Absent 0

Note: none of AD_1/7/9/11/16/24/28/32.md has changed since 2026-01 (git log), so every Doc-obsolete row below still owes its Phase 9 doc edit.

### Still Partial / Still Absent

#### AD26-2 — H8 outcome posterior is learned but never consumed — CLOSED 2026-10-06 — size M
- Closed: the H5 evaluator composes the H6 α with the class posterior via `HierarchicalAlphaTuner.alpha_budget(class, alpha_H6, floor, ceiling)` (p-value weighting, AD_26.md §H8a). The witness confirms a BOCPD change point with K-S at that level, fed each workflow's own progress rate (§H6 "Feed"). Outcomes count once per workflow (`AppliedOutcomeWindow`).
- Proof: tests/unit/distributed/health/test_outcome_weighted_alpha.py (learned outcomes flip a decision) and test_throughput_witness_feed.py (a collapse is denied at the controlled α; false denials stay within that α). Both fail when the wiring or the feed is removed.

#### AD24-1 — Rate-limit Env settings are dead; doc still describes token buckets — CLOSED 2026-10-06 (864f4fda, 925b345c): limits derived from Env by `AdaptiveRateLimitConfig.from_env` (`reliability/rate_limit_derivation.py`); the dead fields, both getters, `RateLimitConfig` and the client-side `CooperativeRateLimiter` are deleted; AD_24.md rewritten
- Doc: docs/architecture/AD_24.md:9-46 "token bucket rate limiting … `class TokenBucket`"; Env `RATE_LIMIT_DEFAULT_BUCKET_SIZE`/`REFILL_RATE`.
- Exists: live server-authoritative per-op `SlidingWindowCounter` limiter with 429 + Retry-After (reliability/rate_limiting.py; wired manager/server.py:761, gate/server.py:448). `TokenBucket` deleted (plan Phase 6). Tests: tests/unit/distributed/reliability/test_rate_limiting*.py.
- Missing: `Env.get_rate_limit_config()` (env/env.py:1560) and `get_rate_limit_retry_config()` (:1574) have zero callers. So `RATE_LIMIT_DEFAULT_BUCKET_SIZE`, `RATE_LIMIT_DEFAULT_REFILL_RATE`, `RATE_LIMIT_CLEANUP_INTERVAL`, `RATE_LIMIT_MAX_RETRIES`, `RATE_LIMIT_MAX_TOTAL_WAIT` and `RATE_LIMIT_BACKOFF_MULTIPLIER` (env.py:735-743) are unread, and the per-op table is `AdaptiveRateLimitConfig`'s built-in default. This is plan "Needs you" item "AD-24 limits", still open. The doc's token-bucket design is doc-obsolete, because the sliding window is the live algorithm.

#### AD27-1 — Gate "final cleanup" (thin server) — STILL PARTIAL — size L
- Doc: docs/architecture/AD_27.md "Proposed Structure" + migration step 5 "Final cleanup of gate.py".
- Exists: coordinators/handlers under nodes/gate/ (dispatch, replication, health, job_failover, leadership, orphan_job, peer, stats coordinators; handlers/tcp_*.py; datacenter_manager_selector.py). The dead `GateCancellationCoordinator`, the `if coordinator … else inline` duplicates and `GateConfig` are gone (0 matches for `if self._.*coordinator` in gate/server.py).
- Missing: gate/server.py is **10,102 lines** (it was 6,901 when graded; it grew through the complexity-ceiling function splits), still one god class (`GateServer`, gate/server.py:291). manager/server.py is 14,604 lines and swim/health_aware_server.py 7,601. This is plan Phase 8 (open).

#### AD28-1 — Discovery: manager-side discovery services never populated — CLOSED (deleted, 2026-10-07) — size S
- Doc: docs/architecture/AD_28.md 5-layer pipeline (DNS → security → locality → rendezvous/EWMA selection → pool).
- Exists: selection is live on the gate's dispatch path (nodes/gate/server.py:739 `DatacenterManagerSelector`; nodes/gate/dispatch_coordinator.py:771 `ordered_managers`, :791/:823 record success/failure → `select_peers`) and on the client (nodes/client/targets.py:123-148). The pool and sticky binding were deleted per D4 (discovery/pool/ is empty). Tests: tests/unit/distributed/gate/test_datacenter_manager_selector.py, tests/unit/distributed/discovery/test_select_peers_fill.py.
- Closed: `ManagerDiscoveryCoordinator` (nodes/manager/discovery.py), its maintenance task and `ManagerConfig.discovery_failure_decay_interval_seconds` are deleted. No manager decision would read a ranked selection: dispatch allocates cores over every worker in the DC (jobs/worker_pool.py `_select_workers_for_allocation`: AD-17 buckets, then most unreserved cores, `excluded_worker_ids` for failures) and peer sync reaches every active peer (nodes/manager/sync.py `sync_state_from_manager_peers`). AD_28.md updated. `DISCOVERY_FAILURE_DECAY_INTERVAL` stays (gate and worker read it). Locality (`DiscoveryConfig.datacenter_id/region_id`, discovery/models/discovery_config.py:119-125) is honoured only where the config sets them; it was not verified per tier.

### Now Built / Doc-obsolete
- AD1 Composition over inheritance — Doc-obsolete: plan D12 (the base-method overrides are deliberate template hooks). AD_1.md:9-21 still says "instead of overriding"; the doc fix is owed.
- AD7 Worker failover push — Doc-obsolete: plan Phase 3 "AD-7" (the dead push was deleted; pull recovery covers it). Live path: nodes/worker/server.py:485 `register_on_node_dead` → :2144 `_handle_manager_failure_async` (invalidate the transport, `select_new_primary_manager`, orphan the workflows). AD_7.md:19 still names `_report_active_workflows_to_manager()`, which no longer exists.
- AD8 Optimistic core freeing from progress — Built: nodes/manager/server.py:8895 → :8944 `_update_worker_cores_from_workflow_progress` → `WorkerPool.update_worker_cores_from_progress(…, worker_cores_version)` + `signal_cores_available`. Test: tests/unit/simulation/sim/test_worker_core_accounting.py (40 seeds). It uses `worker_available_cores` + availability version rather than the doc's `cores_completed` arithmetic. That is the better design (plan Phase 3 G-8), and the doc wording is owed.
- AD9 Retry with original dispatch bytes — Doc-obsolete: the orphaned `_workflow_retries` state is gone. Retries requeue the parsed pending workflow with `excluded_worker_ids` (jobs/workflow_dispatcher.py:1811-1894, models/pending_workflow.py:48, worker_pool.py:1101-1118) and a fresh fence. Test: tests/unit/distributed/jobs/test_workflow_dispatch_routing.py. AD_9.md is still the byte-replay text.
- AD11 State-sync retry with backoff — Built: nodes/manager/sync.py:82-96 (`RetryConfig`, full-jitter backoff spanning one sync timeout; refused/not-ready retryable), used at :128 and :243. Test: tests/unit/distributed/manager/test_state_sync_retries.py.
- AD16 Four-state DC health table — Doc-obsolete: the code classifies `worker_count == 0` as BUSY and adds INITIALIZING (datacenters/datacenter_health_manager.py:249-265, :319). The doc table (AD_16.md:22) still says UNHEALTHY, and so do the module's **own docstrings** (datacenter_health_manager.py:8 and :179). Fix all three.
- AD19-1 Systemic-failure eviction hold — Built: health/systemic_failure.py `is_systemic_failure` (>50%, at least 2), wired at nodes/manager/server.py:5246 (`_enforce_worker_deadlines` holds every eviction; `_systemic_eviction_hold` stops retry-budget charging at :2210, :2747, :4310). `NodeHealthTracker` was deleted (plan Phase 6). Test: tests/unit/distributed/manager/test_systemic_eviction_hold.py.
- AD32-2 Per-destination isolation — Built, as decided in plan D10: per-destination TCP semaphores bounded by `OUTGOING_QUEUE_SIZE`, taken before a node-wide slot and forgotten when the last request settles (server/server/mercury_sync_base_server.py:305-307, :1193-1198). Test: tests/unit/simulation/sim/test_send_destination_isolation.py. AD_32.md's per-destination `RobustMessageQueue` state machine and `outgoing_request_manager.py` are doc-obsolete under D10, and the doc is unchanged.

### New gaps found
- Dead Env fields from AD-32: `OUTGOING_OVERFLOW_SIZE` and `OUTGOING_MAX_DESTINATIONS` (env/env.py:1035-1038) and `Env.get_outgoing_queue_config()` (:1788) have zero consumers since D10 chose semaphores. Delete them, or the "all settings are real Env fields" rule is violated. Size S.
- (Closed) Dead rate-limit Env surface removed; see AD24-1.
- (Closed) `ManagerDiscoveryCoordinator` was a constructed-but-unfed object; deleted, see AD28-1.
- The in-code docstrings of datacenters/datacenter_health_manager.py:8,179 contradict its own BUSY-on-zero-workers behaviour (:259).

## Ledger: AD-37–53 + delta absents
Counts: old P/A 5/16 → Built 11 · Doc-obsolete 6 (incl. P-AUDIT-1, closed by scope decision) · Still Partial 4 (P-AD52-3 and P-AD52PLAN-3 share one gap) · Still Absent 0

Items: the 10 Partial/Absent rows of `grades/ad-37-53.md` (P-AD38-1, P-AD41-1, P-AD44-1,
P-AUDIT-1, P-COMPLIANCE-1, P-AD52-1/2/3, P-AD52PLAN-2/3) plus the 11 rows of
`grades/delta-absents.md` (#1–#10, #8 split a/b).

### Still Partial / Still Absent

#### P-AD52-3 / P-AD52PLAN-3 (item 5.2) — AD-52 §16 performance SLOs unmeasured — STILL PARTIAL — size M
- Doc: AD_52.md §16 "Performance targets" table (membership commit p50<5ms/p99<50ms, ReadIndex p50<2ms, watch delta p99<50ms, cold bootstrap p99<10s, join+promote p99<30s); AD_52_PLAN.md:1471-1485 "Add `tests/benchmarks/cluster/` … CI fails on > 20% slowdown"; plan D16 "measure them with probe scripts you run".
- Exists: every mechanism the targets measure (ClusterMembership, `RaftNode.read_index`, `handle_watch` at `hyperscale/distributed/cluster/cluster_membership.py:1856`); phi accrual measured once in the AD text (8.7s/22s).
- Missing: no benchmark/probe for any §16 row — `tests/benchmarks/` does not exist, no probe script names a §16 metric (REMAINING_WORK_PLAN Phase 9 "Probes you run … AD-52 §16 benchmarks" still open). Rest of PLAN-3 is built or doc-obsolete (below); the plan's "24h chaos nightly" (PLAN 5.1) is the `vopr` job in `.github/workflows/ci.yml:112-129` (180 min cap) with `--sim-vopr-count`/`HYPERSCALE_SIM_SOAK` still untuned (plan Phase 3 "Remaining").

#### P-AD44-1 — Retry-budget default and late-DC-result policy — BUILT (2026-10-06)
- Late results: `BEST_EFFORT_LATE_RESULT_POLICY` (`env/env.py`, `reliability/late_result_policy.py`); gate `_is_late_datacenter_result` / `_log_late_datacenter_result` (log), `_release_provisional_result` / `_fold_straggler_result` (update); AD_44.md "Late DC Results".
- `RETRY_BUDGET_DEFAULT = 10` kept on merit (SRE/Finagle/Envoy floors, sweep over `RetryBudgetManager`); reasoning in the Env comment and AD_44.md Part 6.
- Tests: `tests/unit/simulation/sim/test_multiprocess_best_effort_late_results.py`, `tests/unit/distributed/reliability/test_ad44_late_results_and_observability.py`.

#### P-COMPLIANCE-1 — Gate compliance report "no action items" — CLOSED 2026-10-07 — size S
- Closed: Deleted the unwired `GatePeerCoordinator.on_peer_confirmed`; the wired `GateServer._on_peer_confirmed` (registered with SWIM) stays. Compliance report's action item closed.
- Doc: gate AD compliance report (2026-01-13) "fully compliant / Action Items: None".
- Exists: every 2026-08 finding cured — `GateCancellationCoordinator` deleted; `GateLeadershipCoordinator`'s 3 methods all called; `GateDispatchCoordinator` is down to the two called methods (`dispatch_coordinator.py:282,745`, dead `submit_job`/`fence_token` NameError gone); `reap_expired_prepared` runs (`nodes/gate/server.py:9324`); `_push_global_job_result` defined once (:4485).
- Missing: `GatePeerCoordinator.on_peer_confirmed` (`nodes/gate/peer_coordinator.py:120`) has no caller — the server registers its own inline twin `GateServer._on_peer_confirmed` (`nodes/gate/server.py:698,4889`), which reads `_modular_state` and skips the coordinator's debug log. Delete one (built-but-unwired duplicate). Report text itself needs the Phase 9 doc sweep.

### Now Built / Doc-obsolete
- P-AD38-1 Tiered durability — Built: all 8 event types emitted (`nodes/manager/server.py:7897` time_out_job, :9746 report_progress, :11931/:13631/:14027 fail_job; `nodes/gate/server.py:3277` acknowledge_cancellation, :3458/:3482) and applied (`ledger/job_event_applier.py:44-49`); REGIONAL replicator wired (`nodes/manager/server.py:1144`), gate REGIONAL+GLOBAL (`nodes/gate/server.py:1423-1424`, level chosen at :3503-3512, GLOBAL = 2+ regions per D14); checkpoint cadence + reclamation (plan Phase 2); leveled reads (plan D2). Tests `tests/unit/distributed/ledger/test_job_event_log_completeness.py`, `tests/integration/raft/test_ledger_region_span.py`, `tests/unit/distributed/manager/test_job_status_consistency.py`. (VSR leg: Doc-obsolete, see #1.)
- P-AD41-1 Resource guards — Built: `ResourceEnforcer` (WARN→THROTTLE→KILL→EVICT, 2σ kill gate, `resources/resource_enforcer.py`) constructed `nodes/manager/server.py:708-720` (`RESOURCE_GUARD_ENABLED=True`, `env.py:418`), judged per progress report :9075-9100, budgets from submission :9015, throttle RPC to worker :9187-9206 → `WorkflowThrottleHandler` (`nodes/worker/server.py:503`); `ManagerResourceGossip` consumed (:5745, :8750, :10425); gate `DatacenterResourceAggregator` (`nodes/gate/server.py:1036`). Test `tests/unit/distributed/resources/test_resource_enforcer.py`.
- P-AUDIT-1 Audit findings — hang/task classes Built (raw-task lint `tests/simulation/lints/test_no_raw_asyncio_task.py`; the one unbounded `Event.wait` at `server/protocol/flow_control.py:17` is in `FlowControl.drain`, which has no caller). Swallow half closed by owner decision (2026-10-06: existing `except: pass` not worked unless Raft-related), held by `test_no_swallowed_exceptions.py` ratchet (228 functions, 26 under `distributed/`; the one cluster site, `cluster_membership.py:1876`, is the watch's intended timeout). Not "code does better" — a scope decision.
- P-AD52-1 Cluster formation/membership — Built under other names: `ClusterMembership` (`distributed/cluster/cluster_membership.py`, formation/join/leave/resize/mode) wired `nodes/manager/server.py:640`, `nodes/gate/server.py:1102`; joint consensus + learners in `RaftNode.change_membership`/`reconcile_membership`; CLI `join`/`membership`/`remove`/`resize`/`cluster` registered (`commands/root.py:12-20,72-80`); `serve.py` gone. Tests `tests/unit/simulation/sim/test_cluster_membership_vopr.py`, `test_raft_membership_vopr.py`, `tests/integration/cli/test_cli_cluster_resize.py`, `test_cli_cluster_departures.py`, `test_cli_seed_locators.py`. `ClusterRPCFence` per-RPC header: Doc-obsolete (AD_52.md Status "Fencing (decided 2026-10)": per-concern fencing; a uuid check would fence running jobs at every refounding).
- P-AD52-2 Phi-accrual / watch / disconnected mode — Built: `PhiAccrualDetector` in `datacenters/datacenter_health_manager.py` and `swim/detection/probe_budget.py`; watch `handle_watch` (`cluster_membership.py:1856`); `ClusterViewCache` + `ClusterWatchFollower` (`distributed/cluster/`); tombstone eviction (`CLUSTER_TOMBSTONE_RETENTION_SECONDS`). Tests `tests/unit/distributed/gate/test_gate_manager_phi_accrual.py`, `tests/unit/simulation/sim/test_swim_probe_budget.py`, `test_cluster_watch_follower.py`.
- P-AD52PLAN-2 Phase 1 (12 items) — Built: identity (`RaftStore` identity stamp, D1), seed locators (`commands/run/seed_locators.py`, test `tests/unit/commands/test_seed_locators.py`), formation/join, joint consensus, learners, ReadIndex (`raft/raft_node.py` `read_index`), node wiring; fence header Doc-obsolete (above).
- P-AD52PLAN-3 Phases 2–5 (except 5.2 above) — Built: drain/force-remove/freeze/read-only (`cluster_membership.py:1451-1640`), Raft group commit (D1 `raft/store/`, VOPR `test_raft_store_vopr.py`), leader leases (`RAFT_LEADER_LEASES_ENABLED`, `env.py:216`; `test_raft_leader_leases.py`), observability (`handle_metrics` :1975), worker join via locators (`commands/run/worker.py`), routing via cache, determinism (lint `tests/simulation/lints/test_no_direct_time_random.py` + byte-identical VOPR replay), chaos harness = membership/Raft VOPRs. Doc-obsolete: snapshot export/import (AD_52.md §13 "not the design"), pipelined AppendEntries (plan Phase 4, measured: nothing fills a window), `DatacenterCatalog` federation (plan Phase 4: no replicated DC catalog), AD-31 `granted_at_cluster_epoch` (fencing decision).
- delta #1 Per-job VSR — Doc-obsolete: per-job Raft is the design (plan header, "per your decision"). architecture.md still has 72 "VSR" mentions — Phase 9 doc sweep pending.
- delta #2 Merkle anti-entropy — Doc-obsolete: Raft log catch-up + ledger replicator (plan header). The phantom `AntiEntropyRequest/Response` priority entries are gone (zero hits in `hyperscale/`). architecture.md: 6 "Merkle" mentions unmarked.
- delta #3 Acknowledgment windows — Doc-obsolete: same plan decision; architecture.md 34 mentions unmarked.
- delta #4 Bootstrap module — Doc-obsolete: D3 (AD-52 seed locators/join/watch); 15 dead `DiscoveryConfig` fields deleted (plan Phase 6).
- delta #5 WAL buffer layer — Doc-obsolete and doc fixed: architecture.md:28590 "Superseded (2026-10) -- Parts 14-16".
- delta #6 AD-52 cluster creation / inert `serve.py` — Built (see P-AD52-1; `serve.py` deleted, `hyperscale run manager|gate|worker` + operator commands).
- delta #7 F_FULLFSYNC — Built: `core/runtime/real_filesystem.py:37-48,179-191` (`_sync_durably`, fallback on ENOTSUP/EINVAL only); every WAL/ledger/idempotency/logger sync goes through it. Test `tests/unit/core/test_real_filesystem_durable_sync.py`.
- delta #8a mTLS strict parse — Built: `RoleValidator.extract_peer_claims` threads the validator's `strict_mode` (`discovery/security/role_validator.py:294-323`); callers `nodes/manager/server.py:8275`, `nodes/gate/handlers/tcp_manager.py:356` (the worker-registration handler file is gone). Test `tests/unit/distributed/discovery/test_mtls_strict_claims.py`.
- delta #8b Timeout-tracker fence validation — Built: `_reject_superseded_report`/`_admitted_report_info` (`jobs/gates/gate_job_timeout_tracker.py:139-198`) gate `record_progress` (:200) before `dc_last_progress` is touched. Test `tests/unit/distributed/jobs/test_gate_job_timeout_tracker_fencing.py`.
- delta #9 Complexity lint — Built: `tests/simulation/lints/test_complexity_ceiling.py` (ceiling 3, D7). Burn-down is not done: 1,566 functions still snapshotted in `expected_complexity_violations.py`; god files grew (manager `server.py` 14,604, gate 10,102, `health_aware_server.py` 7,601 lines) — owned by the REFACTOR rows of the dev ledger.
- delta #10 CI running tests — Built: `.github/workflows/ci.yml` (lints/units/simulation on push+PR, nightly `vopr`), `release.yml:28-32` gates publish on it via `workflow_call`. Green-run status not verifiable read-only.

### New gaps found
- Doc contradiction, architecture.md:21085 Part 8 "**Not built (2026-10).** Reads are not leveled" — false since plan D2 (`ReadConsistency` EVENTUAL/SESSION/BOUNDED_STALENESS/STRONG; `tests/unit/distributed/manager/test_job_status_consistency.py`). Size S.
- Doc contradiction inside AD_52.md: Status says leader leases and the §10 soft-state cache are built and "every section of this AD is built", but the §9 paragraph (AD_52.md Status, ~line 190) still says "section 10, which is not built" and the §11 paragraph (~line 205) "Leader leases are not built"; §2 (:624) and the flag appendix (:1559) still promise `--max-seed-candidates`, which the Status calls unbuilt (a fixed `--cohort-size` cohort is never sampled). Size S.
- ~~`GatePeerCoordinator.on_peer_confirmed` dead duplicate (see P-COMPLIANCE-1).~~ Deleted 2026-10-07.
- (Closed, b56858c2) The cluster cookie syncs through `RealFilesystem` (F_FULLFSYNC on darwin) and fsyncs its directory after `os.link`.

## Ledger: architecture.md §1
Counts: old P/A 25/0 → Built 13 · Doc-obsolete 7 · Still Partial 5 · Still Absent 0

(Differs from REMAINING_WORK_PLAN's 2/15/8: several rows the plan left Partial were closed by Phase 3–6 work — systemic eviction hold, discovery selection wiring, per-destination bounds, security defaults.)

### Still Partial / Still Absent

#### A1-G-14 — Tiered cross-DC stats: periodic-tier cadence — CLOSED 2026-10-07 — size S
- Closed: Kept 0.25 s on merit: the Tier-2 push is the operator's only live aggregate, so the interval is its staleness bound (Nielsen's 1.0 s continuous-feedback limit rules out "1-5 s"); the floor is the 0.05 s worker flush (five flushes per push, 4 msg/s per job callback); equal to `MANAGER_BATCH_PUSH_INTERVAL` for gateless parity. Derivation at `GATE_BATCH_STATS_INTERVAL` (env.py); AD_15.md table and architecture.md AD-15 corrected.
- Doc: architecture.md:400 "Periodic | Workflow progress, aggregate rates | Every 1-5s | TCP batch"
- Exists: `GATE_BATCH_STATS_INTERVAL = 0.25` (hyperscale/distributed/env/env.py:607-609) drives the gate batch loop; immediate and on-demand tiers live.
- Missing: constant/doc drift only (0.25 s vs "1-5s"). Justify 0.25 s in the doc or change the default.

#### A1-G-56 — Client push Tier-2 interval — CLOSED 2026-10-07 — size S
- Closed: Closed with A1-G-14 (same `GATE_BATCH_STATS_INTERVAL`, same derivation); the push-notification diagram already names it.
- Doc: architecture.md:8294 "On Tier 2 interval (every 2s)"
- Exists: the same `GATE_BATCH_STATS_INTERVAL = 0.25` (env.py:607); push and callback cleanup are live.
- Missing: the same drift as A1-G-14 (one fix closes both).

#### A1-G-42 — Worker health state from resource thresholds — CLOSED 2026-10-07 — size S
- Closed: Doc changed, with one exception built. CPU, memory and queue depth get no thresholds because none is derivable: a load generator's intended operating point is saturated cores (its harm is measured directly by loop lag and LHM); per-workflow memory is AD-41's; workers never queue (`_pending_workflows` is never appended, depth is always 0). File descriptors do have a derivable ceiling (RLIMIT_NOFILE) and now drain the worker (D-66). architecture.md Worker States note and diagram state the degradation mapping (LHM 2/4/6/7, lag ratio 0.5/1.0/1.5/2.0).
- Doc: architecture.md:5260-5263 HEALTHY requires "CPU < 80% · Memory < 85% · Queue depth < soft_limit · LHM score < 4"
- Exists: `_get_worker_state` → `_worker_state_for_degradation` (hyperscale/distributed/nodes/worker/server.py:1424-1438) maps the GracefulDegradation level (inputs: LHM and event-loop lag only, swim/health/graceful_degradation.py:232-254) to DRAINING/DEGRADED/HEALTHY; DRAINING rejects dispatch.
- Missing: CPU, memory and queue-depth conditions in the state decision (server.py:1432-1438). Either feed them in (the worker already samples cpu/memory for heartbeats) or change the doc to "LHM + loop lag".

#### A1-G-48 — Zombie-detection check interval — CLOSED 2026-10-07 — size S
- Closed: Kept `JOB_CLEANUP_INTERVAL` 60 s on merit: it is the silence threshold for reconciling a job copy, and its leader re-syncs every `MANAGER_PEER_JOB_SYNC_INTERVAL` (15 s), so 60 s tolerates three consecutive lost syncs before asking; retention overshoots `COMPLETED_JOB_MAX_AGE` by at most 20%. Derivation at the Env field. architecture.md: the zombie-detection box described a `check_timeouts` age eviction that no longer exists (AD-44 retry budget + backoff replaced it) and is rewritten; the cleanup-loop box and the config table (`MERCURY_SYNC_CLEANUP_INTERVAL 30s`) now say `JOB_CLEANUP_INTERVAL` 60 s.
- Doc: architecture.md:6495 "Check interval: 30 seconds (via _job_cleanup_loop)"
- Exists: `default_timeout_seconds=300` and `max_dispatch_attempts=5` match; `_job_cleanup_loop` is live (nodes/manager/server.py:1415, :4749).
- Missing: `JOB_CLEANUP_INTERVAL = 60.0` (env/env.py:413) vs the documented 30 s. Fix the doc or the default.

#### A1-G-77 — Message protocol reference: gossip priority and message tables — CLOSED 2026-10-07 — size S
- Closed: Doc changed: fewest-transmits-first is SWIM's λ·log n dissemination (memberlist's TransmitLimitedQueue); a type priority would starve the lowest class (ALIVE refutations among them) past its dissemination deadline under churn. Gossip Buffer box corrected; Provision* already pruned; the failure table's "Lease transfer"/DC-lease-expiry lines rewritten for per-job takeover and AD-44 settlement.
- Doc: architecture.md:4739 "Priority: JOIN > LEAVE > ALIVE > SUSPECT > DEAD"; :7993 "Priority ensures important updates propagate first when space limited"; the message tables list Provision*, DatacenterLease, LeaseTransfer.
- Exists: `GossipBuffer.get_updates_to_piggyback` picks the fewest-broadcast updates (swim/gossip/gossip_buffer.py:159-179, memberlist-style); a same-incarnation conflict resolves dead/leave > suspect > alive/join (:144).
- Missing: no update-type transmission priority (gossip_buffer.py:179 orders by `broadcast_count` only). The doc also still lists messages deleted in Phase 6 (D5 Provision*, DC lease/LeaseTransfer 2026-10-06). The likely resolution is a doc change to the transmit-count ordering plus pruning the deleted messages.

### Now Built / Doc-obsolete
- A1-G-4 Leader rebuilds state from workers AND peers — Built: `_on_manager_become_leader` runs `_state_sync.sync_state_from_workers` + `sync_full_state_from_manager_peers` (nodes/manager/server.py:2098-2101), both through RetryExecutor (nodes/manager/sync.py:128, :243); test tests/unit/distributed/manager/test_state_sync_retries.py
- A1-G-11 State-sync retries with exponential backoff — Built: `RetryConfig(max_attempts=retries+1, base=timeout/(2^r-1), FULL jitter)` (nodes/manager/sync.py:91-98) (Plan Phase 3 AD-11); test tests/unit/distributed/manager/test_state_sync_retries.py
- A1-G-8 Optimistic core freeing from progress — Built (through worker-reported availability instead of manager arithmetic on `cores_completed`): `WorkerPool.update_worker_cores_from_progress` (jobs/worker_pool.py:1216), called from the progress and result paths at nodes/manager/server.py:8962 and :10113, versioned per dispatch (Plan Phase 3 G-8); test tests/unit/simulation/sim/test_worker_core_accounting.py
- A1-G-18 Three-signal health with a >50% systemic eviction hold — Built: `is_systemic_failure` (health/systemic_failure.py:4) gates `_enforce_worker_deadlines` (nodes/manager/server.py:5237-5250); test tests/unit/distributed/manager/test_systemic_eviction_hold.py
- A1-G-27 AD-28 discovery selection — Built: rendezvous ranking in client targets (nodes/client/targets.py:141-148 `select_peers`), worker `select_best_manager` → `select_peer_with_filter` (nodes/worker/discovery.py:39-59, wired nodes/worker/server.py:184); SRV deadlock fixed (Plan Phase 1 #5); test tests/unit/distributed/discovery/test_select_peers_fill.py. Pool, sticky binding and eviction→promotion are Doc-obsolete (D4: discovery/pool/ deleted; only `__pycache__` remains).
- A1-G-33 Per-destination client queue (AD-32) — Built (the D10 mechanism): per-destination semaphores bounded by `OUTGOING_QUEUE_SIZE`, taken before the node-wide slot and forgotten when the last request settles (server/server/mercury_sync_base_server.py:305-307, 1193-1309); RobustMessageQueue FIFO fixed (Plan Phase 1 #4); test tests/unit/simulation/sim/test_send_destination_isolation.py. The doc's throttle/batch/reject thresholds on outgoing sends are doc-obsolete.
- A1-G-54 Quorum circuit breaker (3 failures/30 s, recover 10 s) — Built: `CIRCUIT_BREAKER_MAX_ERRORS=3/WINDOW=30.0/HALF_OPEN_AFTER=10.0` (env/env.py:236-238) feed the gate `_quorum_circuit` (nodes/gate/server.py:637-642); `QuorumCircuitOpenError` is raised and handled in gate submission (nodes/gate/handlers/tcp_job.py:466, :824); the manager's unused circuit was deleted (Plan Phase 3 G-55); test tests/unit/distributed/reliability/test_circuit_breaker_manager.py. Nit: the doc's `get_quorum_status` does not exist.
- A1-G-55 Per-link retries and breakers on node-to-node links — Built: every breaker reads `CIRCUIT_BREAKER_*` (env.py:1481-1483 `get_circuit_breaker_config`; worker registry, manager worker circuits, gate `_peer_gate_circuit_breaker` nodes/gate/server.py:411, 6318, 6670, 7020); dead `_gate_circuit` deleted (Plan Phase 3 G-55). Gate DC dispatch retry is bounded by `derive_datacenter_leader_failover_seconds`, not 2×@0.3 s (Plan Phase 1 #3, deliberate). Test tests/unit/distributed/reliability/test_circuit_breaker_manager.py
- A1-G-63 Dependency-graph, layer-based execution — Built: the DAG advances on completion (`set_on_workflow_completed`, nodes/manager/server.py:1183), and each dispatch first replicates job state to a quorum (`on_dispatch_state_registered=_replicate_job_state_for_dispatch`, server.py:1171 → :5677, checked at jobs/workflow_dispatcher.py:1048-1049); tests tests/unit/distributed/jobs/test_workflow_context_propagation.py, tests/unit/simulation/sim/test_multiprocess_l2_dag.py. `dependency_context` no longer exists (dependents get the full context), so the doc's subset is doc-obsolete.
- A1-G-64 Final-results flow, global-timeout push — Built: a single `_push_global_job_result(result)` (nodes/gate/server.py:4485) is called by completion (:3866) and by `handle_global_timeout` (:4068→:4095); test tests/simulation/lints/test_no_duplicate_method_definitions.py, tests/unit/distributed/gate/test_gate_best_effort_result_order.py
- A1-G-65 Context consistency: layer-boundary quorum — Built: dispatch is refused until job state (including context and layer version) syncs to a quorum (`_sync_job_state_to_peers(require_quorum=True)`, nodes/manager/server.py:5677-5687; Plan Phase 3 G-65 "already met"). The unused ContextForward/ContextLayerSync models and handlers are gone (grep: 0 hits). Tests as A1-G-63.
- A1-G-72 Per-workflow result streaming — Built: `HyperscaleClient.stream_workflow_results` (nodes/client/client.py:693 → tracking.py:161) (D13); test tests/unit/distributed/client/test_client_stream_workflow_results.py. Doc nit: `wait_for_completion` (architecture.md:12181, :13156) is `wait_for_job` (client.py:685).
- A1-G-76 Config surface (secret required, TLS hostname verification on by default) — Built: `MERCURY_SYNC_TLS_VERIFY_HOSTNAME = "true"` (env/env.py:73); `MERCURY_SYNC_AUTH_SECRET = None` → per-user cluster cookie, weak secrets refused everywhere (env.py:49-51; hyperscale/commands/run/cluster_cookie.py; Plan "Security defaults (P8)"); test tests/unit/commands/test_cluster_cookie.py
- A1-G-7 Worker failover reports active workflows — Doc-obsolete: the dead push-after-failover was deleted; pull recovery (`sync_state_from_workers` on leadership, nodes/manager/server.py:2100) replaces it (Plan Phase 3 "AD-7"; doc fix in Phase 9). grep `_report_active_workflows`/`send_progress_to_all_managers`: 0 hits.
- A1-G-24 Token-bucket rate limiting — Doc-obsolete: the authoritative limiter is `SlidingWindowCounter` (reliability/sliding_window_counter.py:11, used at reliability/adaptive_rate_limiter.py:62); 429 + Retry-After are live; legacy `TokenBucket` deleted (Plan Phase 6). The doc should name the sliding window.
- A1-G-34 TCP framing for "~4 GB" payloads — Doc-obsolete: frames are deliberately bounded, `MAX_FRAME_LENGTH = MAX_MESSAGE_SIZE + CIPHERTEXT_OVERHEAD` (server/protocol/receive_buffer.py:26-30; Plan Phase 9 "frame cap derived"); test tests/unit/distributed/protocol/test_frame_decoding_vopr.py
- A1-G-39 Dispatch: crypto-random selection + per-dispatch quorum confirm — Doc-obsolete: D5 deleted the Provision* handlers; per-dispatch quorum is the AD-3 leader plus quorum job-state replication (A1-G-65). Workers are chosen by WorkerPool health-bucket allocation, not `SystemRandom().choice()`. The doc (architecture.md:4947, :5572, :5768) should drop ProvisionRequest.
- A1-G-47 Manager rejects when ALL workers are at capacity — Doc-obsolete: submissions are shed under overload with a retry hint (`_load_shedder.should_shed_handler("job_submission")`, nodes/manager/server.py:11106; Plan Phase 3 "Gate backpressure → client"), while a merely full datacenter queues work and reports BUSY (`available_cores <= 0`, server.py:6478) so gates spill over (AD-43). Test tests/unit/distributed/manager/test_manager_load_shedding.py. The doc's flowchart (architecture.md:7185-7205) should show this.
- A1-G-67 Gate per-job leadership by hash ring + lease import/export — Doc-obsolete: per the 2026-10-06 decisions, lease import/export is deleted, takeover commits a Raft `GateJobReplica` with a strictly higher fence, and the ring (jobs/gates/consistent_hash_ring.py:32) only routes forwarding; the job lease is local (leases/job_lease_manager.py:83 renew). Tests tests/integration/cli/test_cli_leader_leases.py, test_consistent_hashing.py.
- A1-G-69 Health probe classes and config — Doc-obsolete: `health/probes.py` and its 12 Env fields were deleted as dead (Plan Phase 6); health is the three-signal state model (A1-G-18). The doc's `LIVENESS_PROBE_*`/`STARTUP_PROBE_*` table should go.

### New gaps found
- The message reference in architecture.md still documents deleted wire types: Provision* (D5), DatacenterLease/LeaseTransfer (removed 2026-10-06), ContextForward/ContextLayerSync. This is doc-only, folded into A1-G-77.
- AD-24 (related to A1-G-24): nodes don't consume Env `RATE_LIMIT_*`/`get_rate_limit_config()`; the per-operation table is `AdaptiveRateLimitConfig`'s built-in default (REMAINING_WORK_PLAN "Needs you"). Still open; owned by the AD-19–36 ledger.
- Four of the five Still-Partial rows are constant drift (doc vs Env default) with no recorded decision. A "defaults on merit" pass should settle each with a cited justification.

## Ledger: architecture.md §2
Counts: old P/A 25/10 → Built 14 · Doc-obsolete 17 · Still Partial 4 · Still Absent 0

### Still Partial / Still Absent

#### A2-G-247 — AD-36 success criteria 3 (failover speed) unverified, and not met by the tested bound — STILL PARTIAL — size M
- Doc: `docs/architecture/AD_36.md:279-285` Part 12: "1. 50% lower median RTT than random … 2. load variation coefficient < 0.3 … 3. **< 10 seconds from DC failure to routing around it** … 4. switch rate < 1% … 5. zero configuration".
- Exists: router counters `routing/gate_job_router.py:64-67,196-221` exported by `commands/cluster.py:212-234`; criteria 1 and 2 tested against the real router (`tests/unit/distributed/gate/test_gate_job_routing.py:521-560`); criterion 4 doc-obsolete (stateless router, rendezvous tie-break; plan G-247).
- Missing: criterion 3 has no test that asserts < 10 s. The only DC-loss bound is `tests/unit/simulation/sim/test_multiprocess_dc_loss.py:68-69` (`_DEATH_CLASSIFY_MIN = PHI_ACCRUAL_ACCEPTABLE_HEARTBEAT_PAUSE_SECONDS`, `_DEATH_CLASSIFY_MAX = 30.0`), i.e. the system is only held to ≤ 30 s. Either measure and prove < 10 s or change the criterion to the derived detection bound. Also AD_36 Part 11 (`:270-276`) still lists `RoutingSwitch`/switch metrics (Phase 9 doc fix).

#### A2-G-258 — Coalesced stats: "5000x cross-DC reduction" unmeasured — STILL PARTIAL — size S
- Doc: architecture.md AD-38 data plane, `StatsAggregator` (worker 100 ms batch, manager 500 ms, sample_rate 0.1) "5000x reduction" (`docs/architecture.md:22446-22480`).
- Exists: different, deliberate mechanism — worker latest-wins progress flush, manager windowed collection (`jobs/windowed_stats_collector.py`), per-worker windows to origin gate (`nodes/manager/server.py:4577-4595`), gate cross-DC aggregation (`nodes/gate/stats_coordinator.py:507-558`).
- Missing: no measurement of the reduction factor (no stats-ingest probe in the tree: `git ls-files | grep -i probe` finds none; D16 says "measure them with probe scripts"). The `StatsAggregator` text (`architecture.md:22446-22480, 23163, 23191-23246`) is still unbannered.

#### A2-G-264 — AD-38 control-plane numeric criteria — STILL PARTIAL — size M
- Doc: "LOCAL <1ms REGIONAL <10ms GLOBAL <300ms … <30s recovery … WAL bounded to 2x active job state … >1M progress events/s".
- Exists: all three legs live (see G-255/G-259 below); WAL bound by checkpoint cut (`ledger/job_ledger.py:1138` → `ledger/wal/node_wal.py:478 discard_through`), tested `tests/unit/distributed/ledger/test_wal_reclamation.py` ("log bounded after quiesce").
- Missing: the latency, recovery-time and throughput numbers are unmeasured — D16 calls for probe scripts the user runs; none exist in the tree (no probe/bench file tracked outside `helm/`). "Zero job loss under any single failure" has no single test asserting it across tiers.

#### A2-G-266 — Gate job replica: "durable before PrepareAck" — BUILT (see the top list, item 2)
- Doc: VSR write safety "SINGLE WRITER … SEQUENCED … FENCED … DURABLE: persisted before PrepareAck". (VSR itself is doc-obsolete — `docs/architecture.md:23278`; the properties still apply to the gate's takeover replica.)
- Exists: 2PC quorum `nodes/gate/replication_coordinator.py:167-330`; versions are `(fence_token, sequence)`, older epochs REJECTED (`:21-29`, `:921-926`); leader prepares its own vote first (`:194-198`); prepared reaper wired (`nodes/gate/server.py:9324` → `:1328`); tests `tests/unit/distributed/gate/test_gate_replica_versions.py`, `test_gate_reaps_expired_prepared_replicas.py`.
- Missing: prepared and committed replicas are in-memory only (`_prepared`, `_committed_replicas`; no WAL/store write in `replication_coordinator.py`), so "durably replicated to a quorum" (comment at `nodes/gate/handlers/tcp_job.py:1017-1019`) holds only while a quorum of gates stays up. `docs/architecture.md:148` says "every takeover commits a `GateJobReplica` **through Raft**" — false: it is the 2PC above (`raft/` has no reference to `GateJobReplica`). Either persist/route the replica through the gate Raft group or correct both comment and doc.

### Now Built / Doc-obsolete
- A2-G-204 Orphan workflow scanner — Doc-obsolete: orphans are reassigned (retried, charged to the retry budget, or failed for good by the job leader), not unconditionally failed; ASSESSMENT §2.4 "orphans requeued instead of failed" (code more correct). Loop `nodes/manager/server.py:4339`, handling `:4267`; Env `env/env.py:831-835`. Doc still says mark failed (Phase 9 sweep).
- A2-G-206 Per-workflow result streaming — Built (D13): `nodes/client/client.py:693` → `nodes/client/tracking.py:161`; test `tests/unit/distributed/client/test_client_stream_workflow_results.py`.
- A2-G-215 Windowed-stats push topology — Built: gate-routed jobs per worker to origin gate (`nodes/manager/server.py:4577-4595`, `aggregate=False`); gateless jobs aggregated to the client callback (`server.py:4558` → `nodes/manager/manager_stats_coordinator.py:201-234` → `jobs/windowed_stats_collector.py:343` `aggregate=True`); test `tests/unit/distributed/manager/test_manager_windowed_stats_routing.py`.
- A2-G-216 Client progress rate limiting — Doc-obsolete: dead `CLIENT_PROGRESS_*` deleted (plan Phase 6); live limiter is AD-24 op `progress_update` (`nodes/client/handlers/tcp_windowed_stats.py:42-51`, `rate_limited`). Doc's 20/s burst-5 needs the sweep.
- A2-G-217 Bootstrap design goals — Doc-obsolete (D3): AD-52 seed locators/join/watch (`commands/run/seed_locators.py:137`, used by `commands/run/{manager,gate,worker}.py`; `distributed/cluster/cluster_membership.py`); tests `tests/unit/commands/test_seed_locators.py`, `tests/integration/cli/test_cli_seed_locators.py`, `test_cli_node_join.py`.
- A2-G-218 Parallel probe first-success — Doc-obsolete (D3); `probe_timeout` deleted, `max_concurrent_probes` → `max_concurrent_dns_resolutions` (`discovery/models/discovery_config.py:162`).
- A2-G-219 Bootstrap backoff with jitter — Doc-obsolete (D3); the 15 unread `DiscoveryConfig` fields deleted (plan Phase 6); seed resolution retries within the boot timeout.
- A2-G-220 4-byte PING/PONG — Doc-obsolete (D3): join rides the AES-GCM-authenticated, replay-checked frame protocol.
- A2-G-221 Health-aware peer cache — Doc-obsolete (D3/D4): `DiscoveryService.get_healthy_peers` (`discovery/discovery_service.py:824`) + `PeerInfo` health; candidates from AD-52's `cluster/joined_peer_store.py`.
- A2-G-222 Bootstrap module structure — Doc-obsolete (D3). Doc text `docs/architecture.md:14700-14950` (bootstrap tree, `BootstrapConfig`, `ParallelProber`) still has no supersession banner.
- A2-G-224 Federated constants + `NoHealthyDatacentersError` — Doc-obsolete: constants exact (`env/env.py:224-231`, `:1516-1519`); an exception cannot cross the wire, so the gate answers `JobAck(accepted=False, error="No available datacenters - all unhealthy")` (`nodes/gate/handlers/tcp_job.py:905-911`). Doc snippet `architecture.md:15222-15227` still raises.
- A2-G-226 AD-33 lifecycle state machine — Built (now AD-54): `JobManager.workflow_lifecycle` (`jobs/job_manager.py:248`); every transition via `apply_transition` (`job_manager.py:1685, 1741-1750, 2082, 2559-2564, 2689, 2818-2872, 3026, 3080`; manager `server.py:7933`); invalid edges rejected and counted (`workflow/workflow_lifecycle_state_machine.py:171-187`); tests `tests/unit/simulation/sim/test_multiprocess_workflow_lifecycle.py`, oracle `tests/simulation/oracle/workflow_lifecycle_oracle.py`.
- A2-G-227 State-driven failure recovery — Built: `JobManager.return_workflow_to_pending` (`jobs/job_manager.py:1715-1757`) walks FAILED→FAILED_CANCELING_DEPENDENTS→FAILED_READY_FOR_RETRY→PENDING; dependents wait on completion, so none started (no topological cancel needed).
- A2-G-228 State machine gates dispatch/completion/cancel — Built: claim PENDING→DISPATCHED is a validated transition (`job_manager.py:1685`), completion `:2559-2564`, cancel `:2818-2872`.
- A2-G-229 State ↔ `WorkflowProgress.status` — Built: one projection `WORKFLOW_STATUS_BY_WORKFLOW_STATE` (`workflow/workflow_state.py:89`) applied on every accepted transition (`job_manager.py:1706-1713`; manager `server.py:7942`); no direct `status = WorkflowStatus.*` writes in `jobs/` or `nodes/manager/`.
- A2-G-248 AD-37 message classes / manager shedding — Built: manager `LoadShedder` with an externally sampled detector (`nodes/manager/server.py:610`, sampled `:5806`, consulted `:11106`); transport priorities `server/protocol/message_priority.py:173-195` (agree with `reliability/message_class.py` on all 88 named handlers, checked); dead AD-37 predicates deleted (plan Phase 6); test `tests/unit/distributed/manager/test_manager_load_shedding.py`.
- A2-G-251 Control vs data plane — Built: the CLI always passes a node directory (`commands/run/manager.py:154`, `commands/run/gate.py:141`), so the fsync'd ledger is on for every real node; data plane is the windowed-stats pipeline (Logger-based `StatsAggregator` doc-obsolete, see G-258).
- A2-G-252 Event-sourced job events — Built: all 11 types applied (`ledger/job_event_applier.py:42-52`); emitters `report_progress` (manager `server.py:9746`), `acknowledge_cancellation` (`nodes/manager/cancellation.py:1248`, gate `server.py:3277`), `fail_job` (manager `server.py:11931,13631,14027`; gate `:3482`), `time_out_job` (manager `:7897`, gate `:3458`); test `tests/unit/distributed/ledger/test_job_event_log_completeness.py`.
- A2-G-253 HLC invariants — Built: real HLC with max-offset refusal `distributed/hlc/hybrid_logical_clock.py:58-98` (wall-first, `ClockOffsetExceededError`), wired manager `server.py:618`, gate `server.py:526`, Raft apply (`raft/raft_node.py:1262,2073`); tests `tests/unit/distributed/hlc/test_hybrid_logical_clock.py`, `test_clock_fencing.py`, `tests/unit/distributed/raft/test_raft_clock_offset.py`.
- A2-G-254 WAL format/segments/batching — Doc-obsolete: 34-byte header exact (`ledger/wal/wal_entry.py:15-16`); segments/mmap replaced by an atomic cut (`node_wal.py:478 discard_through` → `wal_writer.py:549 rewrite`, plan Phase 2); batch timeout deleted as an idle poll (plan Phase 4 D1-B), group commit by queue drain (`wal_writer_config.py:11-12`); test `tests/unit/distributed/ledger/test_wal_reclamation.py`.
- A2-G-255 Per-operation durability — Built: create/accept/fail/cancel REGIONAL (manager `server.py:11770,11784,11941,13641,14034,14048`; `cancellation.py:1238-1264`), progress LOCAL (`:9751`), gate GLOBAL when the tier spans regions else REGIONAL (`nodes/gate/server.py:3503-3512`, D14).
- A2-G-256 Acknowledgment windows — Doc-obsolete: AD-54 DISPATCHED state + orphan scan + AD-30 suspicion; banner `docs/architecture.md:20524`.
- A2-G-257 Cross-DC circuit breakers queue-and-replay — Doc-obsolete (D6: reject-or-reroute). The un-awaited coroutine bug is fixed (`health/circuit_breaker_manager.py:124-136`). Doc still shows queue/replay config (`architecture.md:20630-20655`, `22873-22992`) with no banner.
- A2-G-259 Three-stage commit pipeline — Built: replicators injected (manager `server.py:694,1144`; gate `server.py:1167,1423-1424`, `_replicate_ledger_regional/global` `:3493-3501`) over per-job Raft (`raft/ledger_replicator.py`); GLOBAL = copies in ≥2 regions (D14); tests `tests/integration/raft/test_ledger_replication.py`, `test_ledger_region_span.py`, `tests/unit/distributed/ledger/test_commit_durability_honesty.py`.
- A2-G-260 Region-coded IDs + conflict resolution — Doc-obsolete: `JobIdGenerator` live (`ledger/job_ledger.py:275`); conflict rules moot under a single Raft-ordered log ("Nothing to merge", `docs/architecture.md:20900`).
- A2-G-261 Merkle anti-entropy — Doc-obsolete: Raft log repair (banner `docs/architecture.md:20887`).
- A2-G-262 Checkpoint + compaction — Built: `maybe_checkpoint` manager `server.py:4165`, gate `server.py:9634`; cut `job_ledger.py:1138`; tests `tests/unit/distributed/ledger/test_checkpoint_cadence.py`, `test_wal_reclamation.py`.
- A2-G-263 Read consistency levels — Built (D2): `models/read_consistency.py`; manager `server.py:12051-12190`; gate `server.py:4574-4594`; CLI `commands/job/status.py`; tests `tests/unit/distributed/manager/test_job_status_consistency.py`, `tests/unit/distributed/gate/test_gate_job_status_consistency.py`, `tests/unit/distributed/client/test_client_job_status_session.py`.
- A2-G-265 Per-job VSR — Doc-obsolete (per-job Raft; banner `docs/architecture.md:23278`).
- A2-G-267 VSR view change — Doc-obsolete (Raft elections; same banner).
- A2-G-268 VSR performance — Doc-obsolete (same banner).

### New gaps found
- `docs/architecture.md:21085-21091` banner says read consistency is "Not built … An unused `ConsistencyLevel` parameter … was removed" — stale; D2 built it (G-263).
- `docs/architecture.md:148` says takeovers commit `GateJobReplica` "through Raft"; the code is an in-memory 2PC (`nodes/gate/replication_coordinator.py:167`) — see G-266.
- No supersession banners yet on: bootstrap (`architecture.md:14700-14950`), cross-DC circuit-breaker queue/replay (`:20630-20655`, `:22873-22992`), `StatsAggregator`/`AckWindowManager` code listings (`:22446-22700`, `:23156-23246`), `NoHealthyDatacentersError` snippet (`:15222-15227`), AD_36 `RoutingSwitch` metrics (`AD_36.md:270-276`).
- AD-37 handler classification exists twice (`reliability/message_class.py` and `server/protocol/message_priority.py:173`, the latter's docstring: "duplicates the logic … to avoid circular imports"). They agree today on all 88 handlers, but nothing pins the agreement.

## Ledger: architecture.md §3 (WAL internals, idempotency, AD-41..45)
Counts: old P/A 28/7 → Built 18 · Doc-obsolete 15 · Still Partial 2 · Still Absent 0

### Still Partial / Still Absent

#### A3-G-9 — LoggerStream batch timeout is a buried constant — CLOSED 2026-10-07 — size S
- Closed: `LoggerStream(batch_timeout_ms=...)` is a constructor parameter; default `DEFAULT_BATCH_TIMEOUT_MS = 10.0`, derived where set (added batching delay of about one flush: PostgreSQL commit_delay guidance, Kafka linger.ms; 10 ms ≈ one rotational/networked-volume flush). Test `tests/unit/logging/test_batch_fsync.py::test_configured_batch_timeout_bounds_a_lone_entry_durability` (mutation: ignoring the parameter fails it). Part 12/13 notes updated.
- Doc: architecture.md Part 13 "LoggerStream gains `enable_coalescing`, `batch_timeout_ms`, `batch_max_size`".
- Exists: `batch_max_size` constructor param (`hyperscale/logging/streams/logger_stream.py:110`); FSYNC_BATCH timer (`:1290-1295`). The `enable_coalescing`/per-path WALWriter half is Doc-obsolete (see A3-G-6).
- Missing: `self._batch_timeout_ms: int = 10` is hardcoded (`logger_stream.py:179`), not a constructor/config parameter.

#### A3-G-50 — AD-44 best-effort: late-result policy and observability — BUILT (2026-10-06)
- Late-result policy as P-AD44-1. Metrics `retry_budget_consumed_total`, `retry_budget_exhausted_total` (manager), `best_effort_completions_total{reason}`, `best_effort_completion_ratio{job_id}`, `best_effort_late_results_total{outcome}` (gate) via `ClusterMetricsReply` / `hyperscale cluster --metrics`; logs `RetryBudgetExhausted` (`RetryBudgetManager.check_and_consume`), `BestEffortCompletion`, `LateDatacenterResult` (gate). AD_44.md Part 7.

### Now Built / Doc-obsolete
- A3-G-12 F_FULLFSYNC on darwin — Built: `hyperscale/core/runtime/real_filesystem.py:37-49,179-191` (`_sync_durably`, ENOTSUP-family fallback to fsync), used by `fsync`/`write_flush`/`append_fsync`/`atomic_write`; test `tests/unit/core/test_real_filesystem_durable_sync.py`.
- A3-G-20 AD-40 requirements (cross-DC leg) — Built: `GateJobReplica.idempotency_key` committed through gate Raft; prepare refuses another job holding the key (`nodes/gate/replication_coordinator.py:850-877`); replica kept until job retention cleanup (`gate/server.py:9263`); test `tests/unit/distributed/gate/test_gate_cross_gate_idempotency.py`.
- A3-G-25 AD-40 JobAck fields — Built: `models/job_ack.py:37-38`; set on gate (`nodes/gate/handlers/tcp_job.py:746-762`) and manager (`nodes/manager/server.py:11242-11266`); tests `test_gate_cross_gate_idempotency.py`, `test_gate_job_handler.py`.
- A3-G-26 Cross-DC idempotency via per-job VSR — Doc-obsolete: per-job VSR → Raft (plan header + D-decisions); the key rides the Raft-committed `GateJobReplica` (Phase 4 "G-26, G-20 and G-27"). Note: `IdempotencyReservedEvent`/`IdempotencyCommittedEvent` remain dead (see new gaps).
- A3-G-27 AD-40 correctness argument — Built: the three layers are gate cache, manager WAL ledger, and the cross-gate Raft replica key (as G-20).
- A3-G-30 Manager resource gossip / cluster view — Built: `ManagerResourceGossip` constructed `nodes/manager/server.py:731`, loop `:1433,5724-5761`, receiver `manager_resource_gossip` `:8729-8750`, view served in ping `:10425`; test `tests/unit/distributed/resources/test_resource_views.py`.
- A3-G-31 Gate vector-clock reconciliation — Doc-obsolete: age-tagged freshest-report-wins replaces vector clocks, and gates aggregate manager reports directly with no gate gossip (`resources/datacenter_resource_aggregator.py:18-60`; rationale in AD_41.md Part 4 §2 and §4). architecture.md's sketch (~:34794) is stale.
- A3-G-32 Uncertainty-aware enforcement — Built: `resources/resource_enforcer.py` (WARN→THROTTLE→KILL, `KILL_CONFIDENCE_SIGMAS = 2.0` :16, uncertainty-stretched graces :166-173); wired `nodes/manager/server.py:708-715`, checked on progress `:9075-9097`, released `:14364`; tests `tests/unit/distributed/resources/test_resource_enforcer.py`, E2E `tests/integration/cli/test_cli_resource_guard.py`.
- A3-G-33 AD-41 wire messages — Doc-obsolete: the shipped names are `ManagerResourceGossipMessage`, `ManagerResourceReport`, `WorkerResourceReport`, `WorkflowThrottleRequest/Response` and `DatacenterResourceView`; kill goes through the cancel path and gates need no gossip message (AD_41.md Part 4 §4 and §6). `ResourceBudget` rides `JobSubmission.resource_budget` (`models/job_submission.py:83`).
- A3-G-34 AD-41 node integration — Built: manager enforcement (above), worker throttle receiver `nodes/worker/server.py:2639` + `handlers/tcp_throttle.py`, gate resource view feeds routing (`nodes/gate/health_coordinator.py:699-723`). The fixed `cpu_pressure > 0.95` skip is Doc-obsolete per AD_41.md Part 4 §5.
- A3-G-36 SLO over SWIM hierarchy — Doc-obsolete (code-justified, not a plan decision): the manager measures workflow latency from the results it receives, and every manager heartbeats every gate, so no worker `latency_samples` or gate `dc_slo_summaries` gossip is needed (same topology argument as AD_41.md Part 4 §4). AD_42.md:67-75 is stale, but it is already in the Phase 9 doc sweep.
- A3-G-39 SLO in composite health — Built: `SLOHealthClassifier` `nodes/gate/server.py:1014` → `health_coordinator.py:481-483` (worse of manager view and SLO, plan Phase 3 G-39); test `tests/unit/distributed/gate/test_gate_slo_health.py`.
- A3-G-40 Resource-pressure → SLO prediction — Built: `ResourceAwareSLOPredictor` `gate/server.py:1040` → `health_coordinator.py:704-723`; test `tests/unit/distributed/resources/test_resource_aware_slo_prediction.py`.
- A3-G-41 SLO-aware routing score — Doc-obsolete: resource pressure enters through the predictor-adjusted SLO routing factor (`health_coordinator.py:704`) instead of a separate `resource_factor` (AD_41.md Part 4 §5). There is no `SLOAwareRoutingScorer` class; `DatacenterRoutingScore` carries the score.
- A3-G-43 Duration → wait estimation — Built: `nodes/manager/capacity_reporter.py:29-110` runs `ExecutionTimeEstimator` over `ActiveDispatch`; test `tests/unit/distributed/manager/test_manager_capacity_reporter.py`.
- A3-G-44 ManagerHeartbeat capacity fields — Built: populated `nodes/manager/server.py:6322-6350`. `estimated_cores_free_at`/`_freeing` are replaced by `cores_freeing_schedule` (`models/manager_heartbeat.py:116-120`).
- A3-G-45 Gate DC capacity aggregation — Built: with honest inputs now (G-44); `capacity/datacenter_capacity.py:108-121` walks the schedule; aggregator `gate/server.py:577`; test `tests/unit/distributed/gate/test_gate_datacenter_capacity.py`.
- A3-G-46 Spillover decision — Built: `dispatch_coordinator.py:537-600,698` on real capacity; test `tests/unit/distributed/capacity/test_spillover_properties.py`. Spillover is applied in dispatch after `route_job` (`routing/gate_job_router.py:69`, which has no `cores_required`): doc drift only.
- A3-G-47 AD-43 Env + fallback — Built: `SPILLOVER_*` fields + stale/disabled fallback. `CAPACITY_AGGREGATION_INTERVAL_SECONDS` is Doc-obsolete, because capacity is computed on read from recorded heartbeats (`capacity/capacity_aggregator.py:22-37`).
- A3-G-51 AD-44 JobSubmission fields — Built: `models/job_submission.py:79-89`; client `nodes/client/client.py:370-428`; dispatcher `jobs/workflow_dispatcher.py:207-210`; clamp `reliability/best_effort_manager.py:154,160`.
- A3-G-55 Blended-latency scorer integration — Built: `adaptive_routing_enabled` gate (`gate/server.py:584-591`, `BlendedScoringConfig`); dead `DatacenterRoutingScoreExtended` deleted; test `tests/unit/distributed/gate/test_gate_route_learning.py`.
- A3-G-56 AD-45 Env + outlier cap — Built: `ADAPTIVE_ROUTING_*` read via `routing/blended_scoring_config.py:22-34`; cap applied `observed_latency_tracker.py:39`; alpha 0.125 (`env/env.py:550`, RFC 6298, plan G-56).
- A3-G-57 AD-45 observability — Built: `ObservedLatencyRecorded` (`dispatch_coordinator.py:806`), `StaleObservationsDecayed` (`gate/server.py:9494`), `route_learning_*` metrics (`gate/server.py:10007`); test `tests/unit/distributed/cluster/test_route_learning_metrics_text.py`. `routing_latency_source`/`BlendedLatencyComputed` are Doc-obsolete (plan Phase 3 G-57).
- A3-G-5 `_write_to_file` sync — Doc-obsolete: async over the Filesystem seam (`logger_stream.py:1025`) is the better design. Doc drift.
- A3-G-6 WALWriter coalescing in logging — Doc-obsolete: group commit lives in the ledger `WALWriter` (`distributed/ledger/wal/wal_writer.py`); plan header ("WAL buffer layer … ledger WAL stack provides group commit") and D11 (keep LoggerStream modes).
- A3-G-8 WALReader — Doc-obsolete: `LoggerStream.read_entries`/`get_last_lsn` (`logger_stream.py:1149,1234`) always verify CRC; `NodeWAL` recovery uses torn-tail/set-aside rules (`node_wal.py:140-268`). There is no `verify_crc` toggle or `count_entries` (buffer layer, plan header).
- A3-G-10 Blocking backpressure — Doc-obsolete: explicit refusal, never a drop of an accepted write (plan Phase 1 #4; `WALBackpressureError`, `WALBatchOverflowError` `logger_stream.py:1280`).
- A3-G-13 Buffered 64KB reads — Doc-obsolete (buffer layer, plan header). NodeWAL reads its file whole (`node_wal.py:143,566`), and Phase 2 checkpoint cuts bound that file.
- A3-G-14 BufferPool/DoubleBuffer, A3-G-15 single-writer buffer, A3-G-16 `WriteStatus` enum, A3-G-18 SingleReaderBuffer, A3-G-19 ReaderPool/IndexedReader — Doc-obsolete: WAL buffer layer superseded by the ledger WAL stack (plan header; Phase 9 doc sweep "buffer layer"). The single writer is the ledger `WALWriter` queue + one drain task.

### New gaps found
- (Closed, b56858c2) The cluster cookie syncs through `RealFilesystem` (F_FULLFSYNC on darwin) and fsyncs its directory after `os.link`.
- `IdempotencyReservedEvent`/`IdempotencyCommittedEvent` (`distributed/idempotency/idempotency_reserved_event.py`, `idempotency_committed_event.py`, re-exported in `idempotency/__init__.py:4`) are still dead: no producer or consumer outside the package. Delete them (S).
- Cross-gate idempotency check is a linear scan of every committed and prepared replica per prepare (`replication_coordinator.py:869-871`). AD-40 requires "O(1) dedup"; a key→job index is missing (S).
- architecture.md §3 sketches for AD-41 (~:34794 `cpu_pressure > 0.95`), AD-42 Part 9 `resource_factor` (~:35741-35842), and AD-42 dissemination are stale against AD_41.md and the code; they belong in the Phase 9 doc sweep.

## Ledger: root docs (README, TODO, FIX, SCAN family, WAL.md, SCENARIOS, AGENTS) + delta partials
Counts: old P/A 37/8 (root-docs 29/8 + 8 delta Partial findings) → Built 24 · Doc-obsolete 4 · Still Partial 17 · Still Absent 0 (all 8 old Absents moved: G-21/52/53 Built; G-51/59/60/69 Still Partial; G-65 Doc-obsolete by owner decision)

Items: root-docs.md G-rows (R-G*) plus delta-partials/delta-changed-code findings not already a root row (R-D*).

### Still Partial / Still Absent

#### R-G3 — "Millions of requests/minute without excessive memory" has no measurement — STILL PARTIAL — size M
- Doc: README "capable of generating millions of requests or interactions per minute ... without consuming excessive memory".
- Exists: hot-path work (pooled connections, pre-encoded args); plan Phase 9 lists "Probes you run: throughput+RSS" (REMAINING_WORK_PLAN.md:340).
- Missing: no throughput/RSS probe script or benchmark anywhere under tests/ or a probes dir (grep `ru_maxrss|requests_per_second|benchmark` over tests/ → only lint snapshots). The number stays unverified until the probe exists and is run.

#### R-G11 — README points at a schema doc that does not exist — CLOSED 2026-10-07 — size S
- Closed: README.md now points at the schema's source of truth, the spec classes' `from_dict` validators in `tests/framework/specs/`, and at worked examples (`tests/end_to_end/gate_manager/`); a prose README.txt would duplicate and drift from them.
- Doc: README.md:255 "See `tests/framework/README.txt` for the full schema and examples."
- Exists: tests/framework/{actions,results,runner,runtime,specs}.
- Missing: tests/framework/README.txt (no file). Write it, or drop README.md:255.

#### R-G35 — Load-tier storm / flood / avalanche at the promised scale — STILL PARTIAL — size M
- Doc: SCENARIOS §21-23 "verify manager handles 100K stats/s ingest"; "10K workflows complete simultaneously".
- Exists: semantics covered (tests/end_to_end/gate_manager/section_21..23.py; StatsBuffer/RobustMessageQueue unit tests; SIM fanout fixes, plan :324-330).
- Missing: no 100K/s ingest or 10K-burst harness under tests/ (grep `storm|avalanche|flood|100_000` → none at that scale). Plan :340 "stats ingest, spike" probes not written.

#### R-G38 — Reporter/results aggregation math under load untested — STILL PARTIAL — size M
- Doc: SCENARIOS §34-35 "Counter overflow - Stats counter exceeds int64"; "Reporter failure isolation"; "Buffer replayed on reconnect".
- Exists: isolation now tested (tests/unit/distributed/{gate,client}/test_*_reporter_isolation.py); t-digest (tests/unit/distributed/slo/test_tdigest_properties.py).
- Missing: no test imports hyperscale/reporting/results.py, time_aligned_results.py or timings_aggregate.py (merge/percentile math, int64/float precision); no backend has a unit test; no replay-on-reconnect test.

#### R-G39 — 24h soak, 50K-VU spike — STILL PARTIAL — size M
- Doc: SCENARIOS §36-40/§42.1 "24-hour soak"; "Spike pattern - 10K → 50K → 10K over 1 minute".
- Exists: tests/simulation/soak/test_soak.py (1800 virtual s, opt-in `HYPERSCALE_SIM_SOAK=1`, :37-42; seed 902 un-skipped, :110-124); gate `stop()` cancels every loop (plan Phase 1 #10).
- Missing: no horizon beyond 1800 virtual s; no spike-profile harness; nightly VOPR/soak counts not yet timed into ci.yml (plan :131).

#### R-G51 — FIX.md status claims are stale — CLOSED 2026-10-07 — size S
- Closed: FIX.md marks §1.1/§1.2 FIXED with their tests, cites symbols instead of drifting line numbers, and records that it is a dated trace: later fixes live here and in REMAINING_WORK_PLAN.md, not copied into it.
- Doc: FIX.md:14 "| **High Priority** | 0 | 🟢 None found |"; §1.1/§1.2 listed as open.
- Exists: §1.1 fixed (role_validator.py:294-323 `extract_peer_claims`; callers manager/server.py:8275, gate/handlers/tcp_manager.py:356); §1.2 fixed (gate_job_timeout_tracker.py:207-212). The bugs that falsified "0 high" are fixed (delta-changed-code A1-A7, B).
- Missing: FIX.md still describes §1.1/§1.2 as open (FIX.md:22-42) and lists none of the ~19 Phase 9 / Phase 1 fixes; rewrite in the Phase 9 doc sweep (plan :345).

#### R-G55 — SCAN.md "ZERO remaining violations" — STILL PARTIAL — size L
- Doc: SCAN.md "All violations FIXED ... ZERO remaining violations".
- Exists: phantom attributes/member calls and unreceived actions at zero (tests/simulation/lints/expected_phantom_*.py, expected_unreceived_action_violations.py all empty).
- Missing: aggregate of R-G56/58/59/60/63/69 below.

#### R-G56 — No inline imports — STILL PARTIAL — size S
- Doc: SCAN.md "**BLOCKING**: Do not proceed ... if ANY inline imports exist (except TYPE_CHECKING)".
- Exists: lint tests/simulation/lints/test_no_inline_imports.py; snapshot 56 functions / 61 imports; node servers clean.
- Missing (non-engine, non-optional-dependency): hyperscale/distributed/env/env.py 8 config getters (`get_*_config`) and reliability/rate_limiting.py::execute_with_rate_limit_retry, server_rate_limiter.py::ServerRateLimiter.check — cycle-breakers (plan :221); break the cycle by moving the config types off the modules that import Env. core/engines 15 and core/jobs local_server_pool.py::run_thread 4 need owner OK. Reporting backends' lazy imports are optional deps (justified).

#### R-G58 — No `Any` — STILL PARTIAL — size M
- Doc: SCAN.md "PROBLEM 4: Any/object escape hatches".
- Exists: distributed/ down to 2 justified (ledger/checkpoint/checkpoint_model.py:16 and restricted_loads; plan :226-227).
- Missing: `Any` annotations remain in logging/ (12), ui/ (30), reporting/ (13), commands/ outside cli/ (17), core/jobs (32, peer-owned), core/engines (115, needs OK). No lint holds `Any` at its count.

#### R-G59 — Thin servers — STILL PARTIAL (was ABSENT) — size L
- Doc: SCAN.md "Verify Server is 'Thin'"; duplicates consolidated; dead code removed.
- Exists: dormant coordinators wired or deleted — manager `_state_sync` (server.py:545, used :2100,:3236,:10135), `_version_skew` (:568, used :10671,:11134), `_dispatch` (:828, used :1167); workflow_lifecycle/rate_limiting coordinators deleted; no `if coordinator else inline` fallbacks left (grep empty); `_job_dc_managers` single store (gate/state.py via `get_job_dc_managers`).
- Missing: the servers grew while being decomposed — nodes/manager/server.py 14,604 lines (~770 methods), nodes/gate/server.py 10,102, swim/health_aware_server.py 7,601. Plan Phase 8 "the remaining manager, gate and health_aware_server domains move into composed classes" (plan :299) not started. Gate server still reaches into `self._modular_state._progress_callbacks` directly (gate/server.py:2846,5046,6060,6077,6237,6696).

#### R-G60 — Cyclomatic complexity ≤3 — STILL PARTIAL (was ABSENT) — size L
- Doc: SCAN.md "Step 5.9f: Complexity Limits (MANDATORY - NO EXCEPTIONS)"; CLAUDE.md "beyond three".
- Exists: ratchet tests/simulation/lints/test_complexity_ceiling.py (D7); snapshot down to 1,567 functions (plan said 2,697 at Phase 5); manager `job_submission`/`cancel_job` and gate `handle_submission` no longer in the snapshot.
- Missing: 1,567 functions over 3 — core 967 (engines need OK; jobs peer-owned), ui 153, commands 106 (cli/ off-limits), logging 53, reporting 35, distributed 245 (swim 84, reliability 36, nodes 32, server 23, raft 22). Per-message hot paths (transport, SWIM per-datagram, Raft per-heartbeat — e.g. raft_node.py::handle_append_entries_response CC 24, health_aware_server.py::_extract_embedded_state CC 27) stay inlined by owner decision 2026-10-06; the control-plane remainder is the work.

#### R-G62 — AD-9..AD-50 compliance artifacts — STILL PARTIAL — size M
- Doc: SCAN.md "Compliance reports stored in `docs/architecture/compliance/`".
- Exists: docs/architecture/compliance/gate_compliance_2026_01_13.md (only file).
- Missing: no manager/worker/client compliance report; an untracked copy sits at docs/architecture/gate_compliance_2026_01_13.md; the report still claims "Action Items: None".

#### R-G63 — Gate/modular "server is pure delegation" — STILL PARTIAL — size L
- Doc: GATE_SCAN "Server wrapper is pure delegation (no business logic); No duplicate logic between layers".
- Exists: coordinators at nodes/gate/{dispatch,health,leadership,peer,stats,replication,orphan_job,job_failover}_coordinator.py, all constructed (gate/server.py:871-1072) and used; drifted inline fallbacks gone.
- Missing: gate/server.py still holds business logic in 10,102 lines (e.g. result fencing gate/server.py:6127-6169, best-effort tracking :8097-8110); same Phase 8 move as R-G59.

#### R-G66 — No raw tasks outside TaskRunner — CLOSED (2026-10-07) — size M
- Doc: AGENTS.md "We *never* create asyncio orphaned tasks or futures. Use the TaskRunner instead".
- Exists: lint tests/simulation/lints/test_no_raw_asyncio_task.py — scope is hyperscale/distributed only (:72 `PRODUCTION_ROOT = ... "distributed"`).
- Closed: the lint scans all of `hyperscale/` (`PRODUCTION_ROOT = REPO_ROOT / "hyperscale"`); `hyperscale/core/jobs/` is a path exemption (peer-owned). Every other remaining site is a snapshot entry with a one-line reason in tests/simulation/lints/expected_asyncio_task_violations.py (each holds its handle; the logging layer sits below the TaskRunner, so it keeps raw tasks). The SIGWINCH/SIGINT lambdas are gone: terminal.py `_on_resize_signal` holds each resize in `_resize_tasks` (stop()/abort() cancel and await them via `_cancel_resize_tasks`), and `_on_keyboard_interrupt_signal` holds the abort in `_keyboard_interrupt_task`, ignoring a repeat while it runs. Test: tests/unit/ui/test_terminal_signal_tasks.py.
- Found, engine-owned, not fixed: core/engines/client/playwright/mercury_sync_playwright_connection.py:153-154 `close()` calls `set_result(None)` on the tasks it just created; `Task.set_result` always raises RuntimeError.

#### R-G67 — Structured async Logger everywhere — CLOSED (2026-10-07) — size S
- Doc: AGENTS.md "We *always* use the Logger in hyperscale/Logger".
- Exists: leases' prints are gone (lease subsystem removed); ping writes errors to stderr deliberately (commands/ping.py:414-416).
- Closed: `DiscoveryService` takes the owning node's Logger as a required constructor field (`logger`, no fallback) and logs a failed DNS lookup as `DiscoveryDnsLookupFailed` (hyperscale_logging_models.py) beside the metric. The gate and client pass their loggers; the worker builds its DiscoveryService after the parent init that creates `_udp_logger`. Test: tests/unit/distributed/discovery/test_dns_peer_retirement.py `test_a_failed_lookup_retires_nothing`.

#### R-G69 — One class per file — STILL PARTIAL (was ABSENT) — size L
- Doc: AGENTS.md "One class per file. Period." (D8: no exemptions).
- Exists: lint tests/simulation/lints/test_one_class_per_file.py; models/ and all of distributed/ now one class per file (no distributed entry in expected_multi_class_files.py; models/distributed.py is a 266-line wire namespace with 1 class).
- Missing: 73 files in the snapshot — 60 in core/engines (456 classes; need owner OK, plan :304), core/jobs/protocols/{encryption,replay_guard,restricted_unpickler}.py (peer-owned). Decided to stay (not work): logging/hyperscale_logging_models.py, vendored plotille/tabulate, tools/filesystem (owner's WIP), commands/cli.

#### R-G71 — Integration tests "as in tests/integration" — STILL PARTIAL — size M
- Doc: AGENTS.md "Write integration style tests as in tests/integration"; "ONLY use uv, NEVER pip".
- Exists: uv-only is done (Dockerfile/release.yml/devcontainer, plan :235-239); 25 of 46 integration files are pytest.
- Missing: 17 integration files have zero collectable tests (script `__main__` + `sys.path` hacks), e.g. tests/integration/gates/test_gate_job_submission.py, tests/integration/manager/test_manager_cluster.py, tests/integration/worker/test_single_worker.py, tests/integration/swim/test_failure_scenarios.py; 3 more (ui/test_node_dashboard_live_cluster.py, extensions/test_extension_dissemination.py, slo/test_slo_dissemination.py) mix test functions with sys.path hacks.

### Now Built / Doc-obsolete
- R-G4 Identical local/distributed, CLI front door — Built: `hyperscale run manager|gate|worker` (commands/run/{manager,gate,worker}.py; boot in run/node_lifecycle.py:20-73); Helm templates gate/managers/workers.yaml; test tests/integration/cli/test_cli_cluster_workflow_run.py.
- R-G7 CustomResult.successful — Built: core/engines/client/custom/custom_result.py returns (note at :42); delta-changed-code A4.
- R-G14 EXECUTION_WORKFLOW P1 except:pass list — Doc-obsolete by owner decision (plan :220; 2026-10-05/06): gate/manager servers have 0; what remains in the named areas is abort/shutdown (worker/lifecycle.py abort_*, mercury_sync_base_server.py abort/_close_*, taskex Run.cancel/abort) plus the aes-gcm key-rotation ladder (not a swallow, ASSESSMENT §0); ratchet test_no_swallowed_exceptions.py stops new ones.
- R-G21 WAL.md Raft hard constraints — Built: Phase 6 artifacts integrated or deleted (RaftStore wired via commands/run/shared.py:218 `opened_raft_store`, used run/manager.py:143, run/gate.py:132; RaftWAL/ReplicatedStatsStore/ReplicatedMembershipLog deleted; SnapshotManager live raft_node.py:459 + cluster_raft_install_snapshot manager/server.py:14593); tests tests/unit/distributed/raft/test_raft_store_vopr.py, tests/integration/cli/test_cli_datacenter_restart_resumes.py. CC<5 on Raft per-heartbeat paths: owner decision (hot paths stay inlined, 2026-10-06); 14 raft functions ≥5 remain under R-G60.
- R-G24 "Replace ALL direct mutations with Raft-routed equivalents" — Doc-obsolete: Raft replicates the event-sourced job ledger (one `LedgerAppendCommand`, raft/models/ledger_append_command.py:10; raft/ledger_replicator.py:99,134), not each JobManager mutation; plan Phase 6 Raft cascade (:203).
- R-G25 Raft Phase 6 persistence — Built: D1 stages A-C (raft/store/raft_store.py; run/shared.py:218); tests test_raft_store_vopr.py, test_cli_datacenter_restart_resumes.py. Stats store / membership log deleted as dead (plan :203).
- R-G30 Spillover unit coverage — Built: tests/unit/distributed/capacity/test_spillover_properties.py (10 property tests incl. env thresholds :374), tests/unit/distributed/gate/test_gate_datacenter_capacity.py (staleness :227-249).
- R-G40 §41 catalogue named gaps — Built: best-effort (gate/server.py:865,8097; tests/unit/distributed/gate/test_gate_best_effort_result_order.py, tests/unit/simulation/sim/test_multiprocess_best_effort.py); mTLS strict (tests/unit/distributed/discovery/test_mtls_strict_claims.py); spillover (above); resource guard (manager/server.py:709 ResourceEnforcer, :9195 throttle; tests/unit/distributed/resources/test_resource_enforcer.py, tests/integration/cli/test_cli_resource_guard.py); t-digest (test_tdigest_properties.py).
- R-G42 TODO "64/64" contradictions — Built: task 37 (single `_push_global_job_result`, duplicate-method lint empty), task 53 (R-G47), split-store fixed (plan Phase 1 #12). Task 19 is Doc-obsolete (R-G44); TODO.md:58 still names it — doc sweep.
- R-G44 Task 19 client leader-query fallback — Doc-obsolete: the dead tracker paths were deleted (plan :199); a client asking any gate is forwarded to the job's leader (plan Phase 4 D2, :136-139) and pushes relay through peer gates.
- R-G46 Global-timeout completion leg — Built: duplicate `_push_global_job_result` removed (9422b810); lint tests/simulation/lints/test_no_duplicate_method_definitions.py.
- R-G47 Task 53 partition callbacks — Built: registered nodes/gate/health_coordinator.py:188-192; detection sets `_partitioned_datacenters` (:674) which demotes the routing bucket to DEGRADED (:785); server hooks gate/server.py:1032-1033.
- R-G48 Fence tokens on WorkflowResultPush — Built (as split-fence domains): models/workflow_result_push.py `manager_fence_token`/`gate_fence_token`; producer stamps manager/server.py:7543; receiver rejects stale gate/server.py:6127-6169; test tests/unit/distributed/client/test_client_leadership_transfer.py.
- R-G52 FIX.md §1.1 mTLS strict — Built: discovery/security/role_validator.py:294-323 `extract_peer_claims` (strict from config); callers manager/server.py:8275, gate/handlers/tcp_manager.py:356; test tests/unit/distributed/discovery/test_mtls_strict_claims.py.
- R-G53 FIX.md §1.2 timeout-tracker fencing — Built: jobs/gates/gate_job_timeout_tracker.py:207-212 (`_admitted_report_info` before any write); test tests/unit/distributed/jobs/test_gate_job_timeout_tracker_fencing.py.
- R-G57 No phantom methods/attributes — Built: test_no_phantom_attributes.py, test_no_phantom_member_calls.py, test_every_sent_action_has_a_receiver.py, all with empty snapshots.
- R-G61 Runtime-correctness scan — Built for its named defects: checkpoint cadence (manager/server.py:4165, gate/server.py:9634; tests/unit/distributed/ledger/test_checkpoint_cadence.py, test_wal_reclamation.py), `reap_expired_prepared` (gate/server.py:9324), all 8 event types (R-D2), gate stop cancels every loop (plan Phase 1 #10). Existing except-pass: owner decision (R-G65).
- R-G65 No swallowed errors — Doc-obsolete by owner decision (2026-10-06, plan :185/:220): existing sites (357 in 228 functions; 133 engines, 110 core/jobs) are not worked unless Raft-related (none in raft/); test_no_swallowed_exceptions.py blocks new ones.
- R-G68 Memory leaks / cleanup — Built: WAL compaction + disk reclamation (plan Phase 2), prepared reaping, stop() loop cancellation, SWIM per-peer lock leases, WorkerHealthManager.on_worker_removed (plan :94, :260).
- R-G70 No threading — Built: only vendored ssh/protocol/ssh/tuntap.py imports threading; lint tests/simulation/lints/test_no_threading.py (:22 allowlist). Executor offload (`run_in_executor`) is the asyncio counterpart.
- R-D1 Checkpoint never called (delta 1) — Built: see R-G61.
- R-D2 4 of 8 job event types never emitted (delta 2) — Built: ledger/job_ledger.py report_progress :666, acknowledge_cancellation :708, fail_job :774, time_out_job :804; emitted manager/server.py:9746,7897,11931 and gate/server.py:3277,3458,3482; applied ledger/job_event_applier.py:44-49; test tests/unit/distributed/ledger/test_job_event_log_completeness.py.
- R-D4 ManagerHeartbeat capacity fields shipped as zeros (delta 4) — Built: manager/server.py heartbeat builder (~:6319-6350) sets pending/remaining/cores_freeing_schedule from `_capacity_reporter.snapshot()`; test tests/unit/distributed/manager/test_manager_capacity_reporter.py.
- R-D5 AD-44 wire fields (delta 5) — Built: models/job_submission.py:79-89; client sends (nodes/client/submission.py:190,369); dispatcher reads jobs/workflow_dispatcher.py:209-210; BestEffortManager live (R-G40).
- R-D6 AD-41 enforcement tier (delta 6) — Built: resources/resource_enforcer.py wired manager/server.py:709; tests above.
- R-D7 Dormant inventory ~6,611 LOC (delta 7) — Built/deleted: manager coordinators wired (R-G59); discovery pool deleted (D4; discovery/pool/ holds only a stale __pycache__), selection live (discovery_service.py:162 AdaptiveEWMASelector; callers gate/datacenter_manager_selector.py:130, client/targets.py:148); BOCPD witness wired (manager/server.py:847); serve.py replaced by `hyperscale run`.
- R-D8 Security defaults (delta 8) — Built: `MERCURY_SYNC_TLS_VERIFY_HOSTNAME = "true"` (distributed/env/env.py:73, core/jobs/models/env.py:41); no published auth secret (env.py:51, per-user cookie commands/run/cluster_cookie.py); engine TLS verify default (setup_clients.py:44-47); tests tests/unit/core/test_encryptor_secret_refusal.py, tests/unit/core/test_engine_tls_verification.py.
- R-DC Commit-pipeline honesty leftovers (delta-changed-code C(ii)/C(iv)) — Built: checkpoint stamps `last_regional_lsn`/`last_global_lsn` (job_ledger.py:1118-1119); REGIONAL commits now in use at 10 call sites.

### New gaps found
- R-N1 (S) Seven idempotency WAL files are committed at the repo root (`git ls-files`: manager-idempotency-DC-DASH-50-*.wal ×7); dozens of untracked `*_results*.json` / `hyperscale.worker.*.log.json` run artifacts sit in the root too (ASSESSMENT §4 #8 asked for this sweep).
- R-N2 (S) AD-25 negotiated gate capabilities are write-only: manager/version_skew.py stores them (:121) but `gate_supports_feature`, `get_gate_capabilities`, `remove_gate`, `get_common_features_with_all_gates`, `get_version_metrics`, `is_version_compatible`, `get_local_capabilities` have 0 callers — no behavior is gated on a negotiated feature. (Cleanup on gate removal does happen in manager/state.py:391, so no leak.)
- R-N3 (S, engines — owner OK needed) core/engines/client/udp/protocols/dtls/prebuilt/win32-*/ ships OpenSSL 1.1 DLLs (6.9 MB, EOL) on every platform.
- R-N4 (S) An untracked duplicate docs/architecture/gate_compliance_2026_01_13.md shadows the tracked docs/architecture/compliance/ copy.

## Ledger: dev docs (simulation framework, rigor checklist, session mapping, improvements, REFACTOR)
Counts: old P/A 25/7 → Built 11 · Doc-obsolete 1 · Still Partial 15 · Still Absent 5

### Still Partial / Still Absent

#### D-5 — Windowed per-(target, latency type) LatencyDigestTracker — CLOSED 2026-10-07 — size M
- Closed: decision recorded at the top of docs/dev/slo.md — one latency type (the dispatch round trip to the worker's answer), keyed per datacenter (SLOSummary → gate health classification and AD-36 routing factor) and per worker (`ManagerState._worker_dispatch_latency_digests`, opened in `ManagerRegistry.register_worker`, closed in `unregister_worker`; reported as `dispatch_latency` by `cluster --metrics`). DISPATCH, E2E and NETWORK dropped from slo.md: nothing would read them (one sample per job never reaches `SLO_MIN_SAMPLE_COUNT`; run time is the workload's design; Vivaldi RTT is read directly by routing). `TimeWindowedTDigest.add` prunes when a window opens: per record 464 ns before, 561-574 ns recording both digests. Tests `tests/unit/distributed/manager/test_manager_worker_dispatch_latency.py`, `tests/unit/distributed/slo/test_time_windowed_digest_pruning.py`, SIM `tests/unit/simulation/sim/test_multiprocess_cluster_metrics.py` (one worker's frames delayed; mutations: every sample into every worker's digest, no per-worker record, digest kept past unregister, late sample reopening a digest, add never pruning).
- Doc: docs/dev/slo.md:645 `class LatencyDigestTracker` with `record_latency(target, latency_type, ...)` for dispatch/response/e2e/network.
- Exists: one `TimeWindowedTDigest` per manager, `nodes/manager/state.py:239`, fed only dispatch→response (`record_dispatch_latency` state.py:522); tests `tests/unit/distributed/manager/test_manager_slo_digest_times.py`.
- Missing: per-target keying and the other latency types (`LatencyType` absent). The comment at state.py:234-238 argues workflow run time says nothing about DC health — a deliberate narrowing, but no plan decision records it; either key per target or change slo.md.

#### D-9 — Harness CleanupReport — CLOSED 2026-10-07 — size S
- Closed: `CleanupReport` (`tests/simulation/harness/cleanup_report.py`) built and wired in `ClusterHarness.__aexit__`: a failed body carries every cleanup error and the pending invariant violation as PEP 678 notes on its own exception; a passing body fails with the violation (cleanup errors as notes) or a RuntimeError listing the errors. Test `tests/unit/simulation/test_harness_cleanup_report.py` (mutation: not attaching fails two).
- Doc: simulation_framework.md:284 "Errors collect into a `CleanupReport` attached to the test failure."
- Exists: `tests/simulation/harness/cluster_harness.py:159-170` collects `cleanup_errors` and raises them when the test passed.
- Missing: no `CleanupReport`; when the test body already failed (`exc_type is not None`) both the cleanup errors and a pending invariant violation are dropped (cluster_harness.py:166-170) instead of attached to the failure.

#### D-11 — Scenario retries with declared retryable exceptions — CLOSED 2026-10-07 — size S
- Closed: Doc-obsolete (decision 2026-10-07): SIM runs are a pure function of the seed, so a retry replays the failure or hides it behind another seed; REAL-mode election/quorum misses are the bugs the scenarios exist to catch. simulation_framework.md §8.2 retired with the reasons; the decorator is not built.
- Doc: simulation_framework.md `@scenario(retries=3, retry_on=(...))`.
- Exists: nothing (`retry_on|retries=` zero hits in `tests/simulation/harness`, `tests/simulation/scenarios`).
- Missing: the decorator. Candidate for Doc-obsolete (SIM determinism makes retries a flake-mask), but no decision recorded.

#### D-13 — Continuous safety/liveness invariant catalog — CLOSED 2026-10-07 — size M
- Doc: simulation_framework.md P-13/P-14 catalog: AtMostOneJobLeaderPerJob, MonotonicFenceTokens, WorkerSubprocessAttribution, NoOrphanWorkflows, LeakedLocksBounded, JobMakesProgress.
- Closed: all six run in every `ClusterHarness` scenario (`continuous_catalog()`, `tests/simulation/harness/invariants.py`; checks in `tests/simulation/harness/invariant_checks/`). Three as written are wrong for a correct cluster and were replaced, recorded in simulation_framework.md §12-13 and each check's module: WorkerSubprocessAttribution (pool vs its own 1 s snapshot) became PID disjointness across workers; LeakedLocksBounded (`<= active + 1`; a dead peer keeps its lock until reaped) became set membership; JobMakesProgress (completion count every N s; one long workflow completes nothing) became AD-34's stuck bound.
- Proof: mutation checks `tests/unit/simulation/harness/test_continuous_invariants.py` (each invariant holds on real state objects, then fails on the injected violation; the checker loop catches one injected mid-run); live evaluation `tests/simulation/scenarios/l2_single_dc/test_continuous_invariant_catalog.py`.

#### D-42 — F2 standing 500-seed swarm — CLOSED 2026-10-07 — size S
- Doc: simulation_rigor_checklist.md:424-433 "nightly/weekly soak invocation (`--sim-vopr-count=500`) ... the missing piece is purely the standing job and a place to record failing seeds."
- Closed: `.github/workflows/ci.yml` `vopr-swarm-nightly` and `vopr-swarm-weekly` run `tests/simulation/soak/run_swarm.py` (one `--sim-replay` per seed, so one failure no longer ends a sweep). Measured cost per seed: vopr ~6 s, gates ~16 s, mdc ~13 s, chaos ~21 s, soak ~34 s; 500 of each (~7.5 h) exceeds a hosted job, so nightly is time-budgeted per suite (180-min cap less 10 min setup; count measured on the runner) over a fresh window from the run id, and weekly runs seeds 1-500 per suite in five shards of 100 (`--require-all`). The nightly `vopr` job sets `HYPERSCALE_SIM_SOAK=1`. Failing seeds: ledger (seed, first/last commit) + seed log with replay commands, in the job summary, a 90-day artifact, and (nightly) the Actions cache, replayed first the next night until they pass.
- Proof: `tests/unit/simulation/harness/test_swarm_ledger.py`.

#### D-62 — Policy-driven placement — STILL PARTIAL — size M
- Doc: improvements.md:8 "explicit constraints (region affinity, min capacity, cost, latency budget) with pluggable policy."
- Exists: hard `datacenters=[...]` constraint in `GateJobRouter`, AD-43 spillover, AD-36 scoring, storage-aware exclusion (`datacenters/datacenter_health_manager.py:234`).
- Missing: a pluggable policy object, cost and latency-budget constraints (zero hits `PlacementPolicy|placement_policy|cost`).

#### D-63 — Pre-warm pools — STILL PARTIAL — size M
- Doc: improvements.md:9 "reserved workers for bursty tests; spillover logic to nearest DC."
- Exists: spillover (`capacity/` SpilloverEvaluator, gate-wired).
- Missing: reserved/pre-warmed worker pool (zero hits `prewarm|pre_warm|reserved_worker|warm_pool` under `hyperscale/distributed`).

#### D-65 — Concurrency caps per DC / job class — STILL PARTIAL — size M
- Doc: improvements.md:14 "hard limits per worker, per manager, per DC; configurable by job class."
- Exists: `MAX_WORKERS_PER_MANAGER` (`env/env.py:388`, default None), core allocation, `MERCURY_SYNC_MAX_CONCURRENCY`.
- Missing: per-DC cap and job-class vocabulary (zero hits `job_class|per_job_class|MAX_.*PER_DC`).

#### D-66 — Resource guards: FD ceiling — CLOSED 2026-10-07 — size S
- Closed: `ResourceViolationType.FILE_DESCRIPTORS_EXCEEDED` and `resources/file_descriptor_ceiling.py`: the ceiling is the RLIMIT_NOFILE soft limit read at runtime (none on Windows or when unlimited), against the largest single process's count (`ResourceMetrics.largest_process_file_descriptor_count`); at the AD-41 kill fraction the worker logs the violation and drains, resuming under the warning fraction. Worker-wide, not per workflow: executor processes are not attributable from the worker, and per-workflow counts would have to come from the executors' `WorkflowStatusUpdate` (`hyperscale/core/jobs`). Test `tests/unit/distributed/resources/test_file_descriptor_ceiling.py` (real pipes; mutations: no hysteresis, no worker floor). AD_41.md, improvements.md updated.
- Doc: improvements.md:15 "enforce CPU/mem/FD ceilings per workflow; kill/evict on violation."
- Exists: AD-41 `ResourceEnforcer` (`resources/resource_enforcer.py`) wired at `nodes/manager/server.py:709` (on by default, `env.py:418`), checked per progress report `server.py:9097`, kill via cancel path `server.py:9208`, evict `on_evict_worker`; test `tests/unit/distributed/resources/test_resource_enforcer.py`.
- Missing: FD budget — `ResourceViolationType` has only CPU/MEMORY (`resources/resource_violation_type.py`) although FDs are sampled (`process_resource_monitor.py:120-137`).

#### D-67 — Circuit breaker for noisy jobs — STILL ABSENT — size M
- Doc: improvements.md:16 "auto-throttle or quarantine high-impact tests."
- Exists: per-peer circuit breakers only; AD-41 throttles a workflow over its own budget (closest).
- Missing: any job/test-keyed breaker or quarantine (zero `quarantine|noisy` hits in nodes/jobs).

#### D-68 — Unified telemetry schema — CLOSED 2026-10-07 — size M
- Closed: `ClusterMetricsReply` is the schema every role answers `cluster_metrics` with (new defaulted fields `role`, `node_state`, `capacity`, `workload`, `resources`, `dispatch_throughput`, `dispatch_outcomes`, `dispatch_latency`, `slo`; sections shared across roles built once in `cluster/telemetry_sections.py`). Workers answer it (`nodes/worker/server.py::cluster_metrics`, from their heartbeat; no membership, empty `formation`); the manager adds its heartbeat's state/capacity/workload/AD-19 throughput, its own datacenter's AD-42 SLO, sends by `DispatchOutcome` (`ManagerDispatchCoordinator.dispatch_outcome_counts`) and per-worker round trips (D-5); the gate adds its state, held jobs and each datacenter's SLO as its classifier reads it (`GateRuntimeState.get_dc_slo_heartbeat`). `hyperscale cluster --metrics` prints the shared section for every role and the membership metrics only for members. Tests `tests/unit/distributed/cluster/test_node_telemetry_metrics.py` (incl. an older build's reply reading every new field as its default), `tests/unit/distributed/manager/test_manager_dispatch_send.py`, SIM `tests/unit/simulation/sim/test_multiprocess_cluster_metrics.py` (mutations: no manager telemetry, no worker handler, wrong worker capacity, no gate telemetry, outcome uncounted or double-counted, worker printing membership).
- Doc: improvements.md:19 "single event contract for client/gate/manager/worker."
- Exists: logger models in `hyperscale/logging/hyperscale_logging_models.py`; `cluster --metrics` on manager (`nodes/manager/server.py:14549`) and gate (`nodes/gate/server.py:9994`), client reader `nodes/client/client.py:664`.
- Missing: one schema across roles — workers expose no `cluster_metrics`; manager's handler is membership-only (`server.py:14557` delegates to `_cluster_membership.handle_metrics`); no shared event contract module.

#### D-70 — End-to-end backpressure: client honors gate shed hint — CLOSED 2026-10-06 (b56858c2, 10a82097): every hinted refusal is waited out, the final one included
- Doc: improvements.md:21 "client also adapts to gate backpressure."
- Exists: gate shed returns `JobAck(retry_after_seconds=OVERLOAD_SAMPLE_INTERVAL_SECONDS)` (`nodes/gate/handlers/tcp_job.py:393-400`); client honors `RateLimitResponse.retry_after_seconds` (`nodes/client/submission.py:522-524`).
- Missing: the client never reads `JobAck.retry_after_seconds` — a shed ack goes through `_rejection_outcome` (`submission.py:630`) and the retry loop sleeps its own exponential backoff from a literal `retry_base_delay = 0.5` (`submission.py:401`, `:433-444`). The gate's hint (and the replication-quorum hint) is dropped. NEW gap (Plan Phase 3 "Gate backpressure → client" built only the sending half).

#### D-74 — Per-tenant quotas — STILL ABSENT — size L
- Doc: improvements.md:29 "CPU/mem/connection budgets with enforcement."
- Exists: per-client rate limiting (AD-24) and per-job AD-41 budgets.
- Missing: tenant identity and quota model (zero `tenant|quota` hits under `hyperscale/distributed`).

#### D-75 — Job sandboxing (cgroups/containers) — STILL ABSENT — size L
- Doc: improvements.md:30.
- Missing: any isolation (zero `cgroup|sandbox` hits under `hyperscale/`); trust model is the authenticated frame (Plan "Unpickler and by-value workflows" decision), which does not cover runtime isolation.

#### D-83 — Cyclomatic complexity caps — STILL PARTIAL — size L
- Doc: REFACTOR.md:17 "Maximum cyclic complexity of 5 for classes and 4 for functions."
- Exists: ratchet `tests/simulation/lints/test_complexity_ceiling.py:27` at 3 (stricter, Plan D7); batches 1–3D took manager `job_submission` from CC 68 to 6.
- Missing: 1,567 functions still over the ceiling (`expected_complexity_violations.py`): 814 engines, 139 ui, 113 core/jobs, 245 distributed (32 under `nodes/`, e.g. `gate/health_coordinator.py::_classify_datacenter_reachability` 13; hot SWIM/transport paths like `health_aware_server.py::_extract_embedded_state` 27 stay inlined by the 2026-10-06 path-heat decision — those are effectively exempt but not marked so in the snapshot).

#### D-84 — LOC reduction — STILL ABSENT — size L
- Doc: REFACTOR.md:9 "Reduce the number of lines of code significantly."
- Exists: dead-code deletions (Plan Phase 6).
- Missing: the god files grew: `nodes/manager/server.py` 10,810 → 14,604 lines, `nodes/gate/server.py` 6,901 → 10,102, `swim/health_aware_server.py` 6,547 → 7,601 (complexity splits added methods in place instead of moving domains into composed classes — Plan Phase 8 "remaining manager, gate and health_aware_server domains move into composed classes" is still open).

#### D-81 — Node dataclasses in models/ with slots — CLOSED 2026-10-07 — size S
- Closed: The six moved into their node's `models/` with `slots=True`: `ClientConfig`, `ManagerConfig`, `WorkerConfig` (its derivations to `nodes/worker/worker_config_derivation.py`), `ExtensionTriggerConfig`, `_PerWorkflowTriggerState`, `PendingResult` (slots added; in-memory only). Pickle namespaces unchanged; every importer updated; dataclass ratchet lowered by 7.
- Doc: REFACTOR.md:5,14 "Dataclasses must be defined in `models/` submodules and declared with `slots=True`."
- Exists: ratchet `tests/simulation/lints/test_dataclass_conventions.py`; `models/` fully one-class-per-file.
- Missing: 270 snapshot entries repo-wide (230 under distributed); within REFACTOR's scope 6 node dataclasses sit outside `models/` — `nodes/client/config.py::ClientConfig`, `nodes/manager/config.py::ManagerConfig`, `nodes/worker/config.py::WorkerConfig`, `nodes/worker/extension_trigger_config.py::ExtensionTriggerConfig`, `nodes/worker/_per_workflow_trigger_state.py`, +1. Plan Phase 8 "move the 30 dataclasses that sit outside models/" open.

#### D-92 — SCENARIOS §7 workload patterns — STILL PARTIAL — size M
- Doc: docs/SCENARIOS.md §7: burst 100/s, sustained 10/s×60s, staggered, long-running, cancel, DAG, cross-DC dependencies, idempotent resubmit, submit-during-election/partition, adversarial workflows (panic/loop/OOM).
- Exists: 100-job instant burst `tests/unit/simulation/sim/test_multiprocess_fanout.py`; DAG + dispatch exhaustion `test_multiprocess_workflow_lifecycle.py`; cancel `test_multiprocess_job_cancellation.py`; blackout `test_multiprocess_l2_submission_blackout.py`; long-running `test_multiprocess_l2_extension.py`.
- Missing: sustained-rate and staggered-start scenarios; cross-DC dependency chains; adversarial workflows (a step that raises, loops forever, or exhausts memory) — none in `tests/simulation/harness/sim/multiprocess/`.

#### D-95 — SCENARIOS §10 WAL corruption refusal — CLOSED (AD-38 Part 3.2; `test_node_wal_damage_vopr.py`)
- Doc: docs/SCENARIOS.md §10 "WAL corruption. Detected at startup; node refuses to come up."
- Exists: Raft store applies a torn-last-frame-only rule and sets aside an untrustworthy disk (Plan D1 stage A); restart/idempotency/incarnation/snapshot-install covered (`tests/unit/distributed/raft/test_raft_snapshot_install.py`, `test_wal_reclamation.py`).
- Closed: `NodeWAL` accepts only a torn tail. It cuts the tail after a stable second read, and refuses any other damage with `WALUntrustworthyError` and a CRITICAL `WALUntrustworthy` log, leaving the file as found. Refusal was chosen over set-aside because nothing rebuilds a node's ledger from peers and LOCAL entries are never replicated (AD-38 Part 3.2). Tested in `tests/unit/distributed/ledger/wal/test_node_wal_damage_vopr.py`.

#### D-13b (G-96) — SCENARIOS §11 continuous 9-invariant checker — CLOSED 2026-10-07 — size M
- Doc: SCENARIOS.md §11, nine invariants every 100 ms.
- Closed: monotone fence tokens, unique sub-workflow tokens, terminal reach (JobMakesProgress), cancelled cores freed within the worker's cancellation bound, resource counters, member-count convergence (within one gossip dissemination) and cluster-ID isolation run continuously beside job-leader exclusivity; SCENARIOS §11 names each check and every bound's derivation. `available + reserved <= total` is an identity on the worker and `[0, total]` per counter on a manager (reserved cores are still inside the reported available count there). "At most one leader per DC" stays post-hoc (VOPR oracle).

### Now Built / Doc-obsolete
- D-6 One harness REAL/SIM — Doc-obsolete: the doc's own amendment (simulation_framework.md:885-906) moved SIM to SIM-native multiprocess suites; `tests/simulation/harness/execution_mode.py`.
- D-42-F1/F3 Soak + seed-drawn topology — Built: `tests/simulation/soak/`; chaos draws topology per seed `tests/simulation/vopr_chaos/chaos_plan.py:380-386` (F2 closed 2026-10-07, above).
- D-48 L-section client/edge faults — Built: chaos `client_partition` links `chaos_plan.py:588,639`; L5 `tests/unit/simulation/sim/test_multiprocess_rejection_storm.py`; L2/L4 as before. (Base VOPR `vopr/fault_plan.py:31` still manager↔worker only; the doc's recipe targets chaos.)
- D-49 Four queued production gaps — Built: completion obligation (`jobs/completion_notice_obligation.py`); mid-flight AD-36 failover `nodes/gate/job_failover_coordinator.py`, run at `gate/server.py:1552`, test `tests/unit/distributed/gate/test_gate_mid_flight_failover.py`; storage-aware placement `datacenters/datacenter_health_manager.py:234` (`storage_writable` in heartbeat), test `tests/unit/distributed/jobs/test_storage_aware_datacenter_health.py`; SIM duration workflows (Plan SIM17).
- D-59 Global ledger quorum replication — Built: manager REGIONAL replicator `nodes/manager/server.py:1144`, gate regional+global `gate/server.py:1423-1424`, GLOBAL when spanning regions `gate/server.py:3512`; tests `tests/integration/raft/test_ledger_replication.py`, `test_ledger_region_span.py`.
- D-73 Best-effort mode — Built: `BestEffortManager` at `nodes/gate/server.py:865`, deadline loop `:1388`, `record_result` `:8292`; tests `tests/unit/simulation/sim/test_multiprocess_best_effort.py`, `tests/unit/distributed/gate/test_gate_best_effort_result_order.py`.
- D-76 Audit trails — Built: all 8 job events emitted (`ledger/job_ledger.py:692,732,787,820` via manager `server.py:9746,11931,7897`, `cancellation.py:1248`, gate `server.py:3277,3458`) plus `JobLeadershipAcquired` (`job_ledger.py:999`; manager `server.py:3543`, gate `server.py:5355`); tests `tests/unit/distributed/ledger/test_job_event_log_completeness.py`, `test_leadership_acquired.py`.
- D-78 Synthetic large-scale — Built: 10×/100× fanout `tests/unit/simulation/sim/test_multiprocess_fanout.py`; backpressure under storm `test_multiprocess_rejection_storm.py`.
- D-79 Version skew + rolling upgrade — Built: `tests/unit/distributed/models/test_rolling_upgrade_wire_compatibility.py` (104 messages, both directions) + `tests/unit/distributed/protocol/test_version_skew*.py`. (No mixed-version multi-process scenario; wire-level is the contract.)
- D-80 One class per file across nodes — Built: zero multi-class files under `hyperscale/distributed` in `tests/simulation/lints/expected_multi_class_files.py` (73 left: 60 engines, 5 ui, 3 core/jobs, 3 tools, cli, logging models — all decided exemptions/owner-gated per Plan Phase 8).
- D-82 Behavior preservation + compliance — Built: the drift points are gone (no `coordinator else inline` fallbacks or `self._state` in `gate/server.py`; single `_job_dc_managers` store `gate/state.py:110,366`; manager coordinators called — `_state_sync`, `_workflow_dispatcher`, `_version_skew`, `_cancellation` in `manager/server.py`); held by phantom/duplicate/unreceived-action lints.
- D-90 SCENARIOS §5 clock anomalies — Built: backward/forward step at lease boundary `tests/unit/simulation/sim/test_multiprocess_lease_clock_step.py`; skew fencing `test_multiprocess_clock_fence.py`; VM pause `test_multiprocess_pause.py`; monotonic drift is a documented skip.
- D-94 SCENARIOS §9 adversarial messages — Built: replay `tests/unit/distributed/protocol/test_frame_replay_protection.py`; malformed pickle `tests/unit/distributed/messaging/test_restricted_unpickler_vopr.py`; mTLS claims `tests/unit/distributed/discovery/test_mtls_strict_claims.py`; oversized/malformed frames `tests/unit/distributed/protocol/test_frame_decoding_vopr.py`; wrong cluster `tests/unit/simulation/sim/test_cluster_mismatch_vopr.py`; versions (D-79).

### New gaps found
- (Closed) Client honors `JobAck.retry_after_seconds`; see D-70.
- (Closed) Job `NodeWAL` mid-file corruption now refuses to start; see D-95.
- Harness drops cleanup errors and a pending invariant violation when the test body already raised (`tests/simulation/harness/cluster_harness.py:166-170`) — see D-9.
- (Closed) Nightly CI ran only the default 4-seed sweep and never the soak; see D-42.
- God files grew ~35–46% since 2026-08 despite the complexity program — see D-84.
