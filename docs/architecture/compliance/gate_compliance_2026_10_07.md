# Gate Module AD Compliance Report

**Date**: 2026-10-07
**Commit**: 700b9aac (branch `AL-rework-commands`)
**Scope**: AD-9 through AD-50 (excluding AD-27), graded for the GATE role only
**Module**: `hyperscale/distributed/nodes/gate/` and the modules it calls: `swim/`, `jobs/gates/`, `ledger/`, `routing/`, `datacenters/`, `capacity/`, `reliability/`, `health/`, `discovery/`, `idempotency/`, `resources/`, `slo/`, `server/server/`
**Supersedes**: `gate_compliance_2026_01_13.md`

**Method**: graded from the first statements of each handler, not from which classes exist. For every requirement we found where the gate actually calls it, counted the callers with grep, and read the first statements of the receiving function. A requirement counts as met only if that function does the work. It does not count if the function is a stub, returns early on every call, writes a store nothing reads, or is constructed and never called. The AD names come from the AD docs themselves. The names in the SCAN.md Phase 12 matrix are stale for most rows (for example, AD-9 there is "Gate State Embedding" but `AD_9.md` is "Retry Requeues the Pending Workflow").

All file paths are relative to the repo root. To keep the tables readable, `D/` stands for `hyperscale/distributed/`, so `D/nodes/gate/server.py` is `hyperscale/distributed/nodes/gate/server.py`. Line numbers are as of 700b9aac.

---

## Summary

| Status | Count |
|--------|-------|
| COMPLIANT | 8 |
| PARTIAL | 23 |
| DIVERGENT | 0 (divergent details are noted inside PARTIAL rows) |
| MISSING | 1 |
| SUPERSEDED | 1 |
| N/A (role) | 8 |
| **Total** | **41** |

**Overall**: the gate's core control plane works end to end:
- split-brain prevention (AD-13)
- gossip-informed failure callbacks (AD-31)
- federated DC health probing (AD-33)
- the job ledger (AD-38)
- idempotent admission (AD-40)
- spillover (AD-43)
- route learning (AD-45)

The common failure is still "built but not wired" (write-only stores) along with several bugs that only show up between hosts:
- Five gate-side stores are written and never read: manager backpressure, `ManagerHealthState`, negotiated capabilities, the peer-gate `DiscoveryService`, and `JobStatsCRDT`.
- The AD-34 stuck check compares a manager host's `monotonic()` clock with the gate's.
- AD-35 Vivaldi coordinates are looked up under a key they are never stored under, so AD-36 routing never uses Vivaldi.
- An overloaded gate refuses every client cancel.

---

## Per-AD Findings

| AD | Name | Status | Evidence (file:line) | Notes |
|----|------|--------|----------------------|-------|
| AD-9 | Retry Requeues the Pending Workflow | N/A (role) | `D/jobs/workflow_dispatcher.py` (manager) | Retrying a workflow is the manager's job. The gate only fails a whole datacenter over (`D/nodes/gate/job_failover_coordinator.py:283-293`). |
| AD-10 | Per-Job Fencing Tokens | N/A (role) | `D/jobs/gates/gate_job_manager.py:99-101,431-449` | The dispatch fence runs manager→worker. The gate keeps its own per-job leadership fence (`D/nodes/gate/server.py:6551-6570`). Latent flaw: `D/nodes/gate/handlers/tcp_job.py:1296-1303` folds the manager's lease fence into the gate fence, mixing the two fence domains (`D/models/workflow_result_push.py:28-80`). That code is on the dead `receive_job_progress` path. |
| AD-11 | State Sync Retries with Exponential Backoff | PARTIAL | `D/nodes/gate/server.py:9005-9056`, `:1887-1891` | The manager side is built (`D/nodes/manager/sync.py:91-98`). The gate's startup sync makes one attempt with no backoff, then goes ACTIVE anyway (`:9007`). It skips silently when no leader is known (`:9011-9013`). A peer that is not ready answers `b"error"`, with no retryable "not ready" signal. Mitigated by quorum replicas and ledger recovery. |
| AD-12 | Manager Peer State Sync on Leadership | N/A (role) | `D/nodes/gate/server.py:5858-5870` | Manager-only. The gate's become-leader hook only scans for orphans; per-job quorum replication replaces a sync at election. |
| AD-13 | Gate Split-Brain Prevention | COMPLIANT | `D/nodes/gate/state.py:79-80`; `D/nodes/gate/server.py:703-704,5253,5285,6373-6395,10136-10151`; `D/swim/leadership/local_leader_election.py:217-219,677-729`; `D/swim/health_aware_server.py:5553-5556` | Pre-vote by floor(n/2)+1 over the configured cohort, which does not shrink when peers die. `is_leader()` requires a held quorum lease, and the gate steps down when it loses quorum. The doc is stale: the map and peer set now live in `GateRuntimeState`. |
| AD-14 | CRDT-Based Cross-DC Statistics | MISSING (in effect) | `D/nodes/gate/server.py:557,6749-6766,1793`; `D/nodes/gate/handlers/tcp_job.py:1332` | `JobStatsCRDT` is written only by `receive_job_progress`, and no manager sends that (the only sender is the gate's own peer forwarder at `D/nodes/gate/server.py:6688`). Nothing reads or merges it. `record_completed` also passes cumulative totals into `GCounter.increment`, which adds them, so every report would overcount. Cross-DC aggregation actually runs through windowed stats plus a sum of final results (`:4801,8015`). |
| AD-15 | Tiered Update Strategy for Cross-DC Stats | PARTIAL | `D/nodes/gate/stats_coordinator.py:99-173,349-376,466-473`; `D/nodes/gate/server.py:1583,3981,4228` | The immediate and on-demand tiers work. The 0.25 s batch tier runs, but `_build_job_batch_push` reads totals that are only filled in on the dead `receive_job_progress` path. So every in-run `JobBatchPush` carries zeros, and the client overwrites its real rate with 0.0 every tick (`D/nodes/client/status_application.py:80-85`). This undercuts the closed items A1-G-14 and A1-G-56. |
| AD-16 | Datacenter Health Classification | PARTIAL | `D/datacenters/datacenter_health_manager.py:152-156,196-272,443-463,554`; `D/nodes/gate/health_coordinator.py:317,510-594`; `D/routing/candidate_filter.py:25-26` | Classification order, the BUSY-on-zero-workers rule, INITIALIZING, and the merge with probes are all correct, and AD_16.md has been updated. But `DatacenterHealthManager.remove_manager`, `mark_manager_dead` and `cleanup_stale_managers` have 0 callers, so reaped managers stay in `total_count` for good (see bugs). The transitions to stale→UNHEALTHY (`:228-229`) and zero-workers→BUSY (`:259-267`) are never recorded, so no alert fires. The docstrings at `:7-11` and `:178-183` are still stale. |
| AD-17 | Smart Dispatch with Fallback Chain | COMPLIANT | `D/routing/candidate_filter.py:25-30`; `D/routing/constrained_placement_policy.py:22,95-104`; `D/routing/gate_job_router.py:68-80`; `D/nodes/gate/dispatch_coordinator.py:580-600,775-892,1033-1063` | Datacenters are tried HEALTHY > BUSY > DEGRADED, and a failed primary falls back to the next. If every datacenter is unhealthy the job is refused at submit (`D/nodes/gate/handlers/tcp_job.py:909-915`). The D-62 budget excess sorts ahead of the health bucket by design. |
| AD-18 | Hybrid Overload Detection | PARTIAL | `D/nodes/gate/server.py:439,446,457-461,694,6282,6745,9758-9793`; `D/reliability/hybrid_overload_detector.py:94-104,276-287` | The detector is sampled from outside and read by the shedder, the limiter, the heartbeat and readiness. Gaps: latency is fed only from `job_status` and progress, never from submissions, and shed replies record near-zero latencies. DIVERGENT detail: trend detection is now fast/slow EMA drift rather than linear regression. The code is better; the doc is stale. |
| AD-19 | Three-Signal Health Model | PARTIAL | `D/nodes/gate/health_coordinator.py:401-422,628-637`; `D/nodes/gate/state.py:92,347`; `D/nodes/gate/server.py:6094-6102,6270-6306` | The gate's own readiness and progress are published and gate leadership. Its per-manager `ManagerHealthState` store has no reader: liveness is only ever set with `success=True` and progress never. DC health ignores manager readiness and progress. Four gate model classes are never instantiated (`D/nodes/gate/models/gate_peer_state.py`, `dc_health_state.py`, `gate_peer_tracking.py`, `manager_tracking.py`). |
| AD-20 | Cancellation Propagation | PARTIAL | `D/nodes/gate/handlers/tcp_cancellation.py:215,276-310,338-344,363-376,799-806`; `D/nodes/gate/server.py:1825-1843,3284-3321,6244-6253` | The fence check, idempotent repeats, leader-redirect forwarding and the durable ledger record all work. Gaps: one attempt per datacenter (`max_attempts=1`). CANCELLED is set on the first datacenter's confirmation and re-issues stop at `:298`, so unconfirmed datacenters are never cancelled. Completions are relayed per datacenter, not once all have confirmed. The cancel is rate-limited at NORMAL priority (see bugs). |
| AD-21 | Unified Retry Framework with Jitter | PARTIAL | `D/nodes/gate/dispatch_coordinator.py:131-160,336,1139`; `D/nodes/gate/orphan_job_coordinator.py:413-420`; `D/nodes/gate/stats_coordinator.py:274-281`; `D/nodes/gate/server.py:3173-3180` | `RetryExecutor` with full jitter is used for dispatch and the room-wait. There are three hand-rolled exponential backoffs with no jitter, and the periodic loops sleep fixed intervals. |
| AD-22 | Load Shedding with Priority Queues | PARTIAL | `D/server/server/mercury_sync_base_server.py:1966-1985,2076-2088`; `D/reliability/load_shedder.py:186-195`; `D/nodes/gate/handlers/tcp_job.py:399-409,1125,1185` | Transport admission is health-gated with AD-37 priorities. Handler-level shedding of submission and status cannot be reached, because the transport refuses first. Cancel is CRITICAL at the transport but limited at NORMAL in the handler. DIVERGENT detail (code is defensible): final results are HIGH rather than CRITICAL, and the manager's resend obligation covers them. |
| AD-23 | Backpressure for Stats Updates | PARTIAL (gate piece) | `D/nodes/gate/handlers/tcp_manager.py:181-190,453-458`; `D/nodes/gate/server.py:2963-2986,6966-6993`; `D/nodes/gate/state.py:95-97,459-467,489-512` | The tiered `StatsBuffer` belongs to the manager. On the gate, received manager backpressure is written and never read: `get_dc_backpressure_level` and `get_max_backpressure_level` have 0 callers, and `_backpressure_delay_ms` only ever grows. The gate never signals backpressure upstream: `windowed_stats_push` answers `b"ok"`, and shed progress gets a normal ack. |
| AD-24 | Rate Limiting | PARTIAL | `D/nodes/gate/server.py:457-461,9705-9717`; `D/reliability/server_rate_limiter.py:104-160,176`; `D/reliability/adaptive_rate_limiter.py:126-140` | Per-client, per-operation sliding windows, health gating, 429 with Retry-After, and idle-client cleanup all work (AD24-1 closed). Gaps: CONTROL operations are limited at NORMAL inside the handler. UDP `check_sync` only reads a `"default"` counter that nothing creates, so it never limits anything. |
| AD-25 | Version Skew Handling | PARTIAL | `D/nodes/gate/handlers/tcp_manager.py:402-430,449`; `D/nodes/gate/handlers/tcp_job.py:514-561`; `D/swim/message_handling/membership/join_handler.py:64-70`; `D/nodes/gate/server.py:9515-9520,9549-9553` | The major version is checked for managers, clients and gate JOINs, and features are intersected for clients. `_manager_negotiated_caps` is written and only ever popped, so no gate behaviour depends on a negotiated feature. The gate does not check the manager's reply version. |
| AD-26 | Adaptive Healthcheck Extensions | N/A (role) | `D/nodes/gate/orphan_job_coordinator.py:541-571,632-659` | The protocol runs between workers and managers. The gate reuses `ExtensionTracker` for orphan grace periods, and that works. |
| AD-28 | Enhanced DNS Discovery with Peer Selection | PARTIAL | `D/nodes/gate/dispatch_coordinator.py:922,965,1002`; `D/nodes/gate/datacenter_manager_selector.py:77-139`; `D/nodes/gate/server.py:760,848-869,9832-9859`; `D/nodes/gate/peer_coordinator.py:142,220,266-268` | Manager selection by rendezvous plus EWMA, with outcome feedback and decay, works. Isolation and mTLS checks run on registration. The peer-gate `DiscoveryService` is written and never selected from; this is the gate analogue of AD28-1. The `_dc_manager_discovery` alias has no readers. The DNS layer is superseded by AD-52 seed locators. |
| AD-29 | Protocol-Level Peer Confirmation | PARTIAL | `D/swim/health_aware_server.py:998-1040,2548,4193,6364-6402`; `D/nodes/gate/server.py:707,5174,5247-5251,5285-5291,10055-10103` | Unconfirmed peers cannot be suspected; peers are confirmed on real contact; only confirmed peers become active. Gaps: the gate never calls `add_unconfirmed_peer`, so the 60 s unconfirmed warning never covers gates. A gate added at runtime is never counted as an active peer (see bugs). `peer_coordinator._confirm_peer` is a dead injected dependency. |
| AD-30 | Hierarchical Failure Detection | PARTIAL | `D/nodes/gate/server.py:716-725,5936-5994`; `D/nodes/gate/dispatch_coordinator.py:965-968,1028-1030`; `D/nodes/gate/health_coordinator.py:312-316`; `D/swim/detection/job_suspicion.py:41-42` | A failed dispatch does start a per-DC suspicion. But `_confirm_manager_for_dc` calls `confirm_job` from the originator, which is a no-op. Nothing ever refutes the suspicion, and the incarnation is always 0. Every suspicion therefore expires and charges a circuit failure. Routing never reads per-DC suspicion. Suspicions are keyed by TCP address, but cleanup and death checks look them up by UDP address. |
| AD-31 | Gossip-Informed Callbacks | COMPLIANT | `D/swim/health_aware_server.py:3703-3729,3775,3814-3850,865-889`; `D/nodes/gate/server.py:5253,5485-5508,5634-5640`; `D/nodes/gate/peer_coordinator.py:120-162` | A real NOT-DEAD→DEAD change fires the callbacks, which run the epoch bump, ring removal, orphan marking, takeover commit and manager notification. Minor: `_on_node_dead` removes a circuit by UDP address (`:5276-5279`), which does nothing; the reaper cleans up later. |
| AD-32 | Hybrid Bounded Execution with Priority Load Shedding | PARTIAL | `D/server/server/mercury_sync_base_server.py:286-311,1438-1490,1666-1694,1811-1842` | In-flight counts and the per-destination outgoing bounds (AD32-2) work. Every inbound TCP server request is spawned as HIGH regardless of handler, so CRITICAL control messages can be shed at the HIGH limit. A shed TCP request is dropped silently, with no error or Retry-After. The gate-specific limits are not applied. `_pending_response_warn_threshold` is never read. |
| AD-33 | Federated Health Monitoring | COMPLIANT | `D/nodes/gate/server.py:728-735,1401-1412,1484-1493,1638-1670,7207,7272,7311-7345`; `D/swim/health/federated_health_monitor.py:417-455,504-658`; `D/nodes/gate/health_coordinator.py:493-594,732-776` | Only the DC leader is probed. Incarnation and term are tracked, and the REACHABLE/SUSPECTED/UNREACHABLE results feed routing, failover, latency correlation and leader broadcast. Doc diagram is stale: datacenters start UNKNOWN. |
| AD-34 | Adaptive Job Timeout with Multi-DC Coordination | PARTIAL | `D/jobs/gates/gate_job_timeout_tracker.py:139-215,396-531,558,585-624`; `D/jobs/gate_coordinated_timeout.py:41,452,466,490-501`; `D/nodes/gate/server.py:626-630,1418,1937-2048,4426-4454,9616-9642` | The tracker starts, its loop runs, reports are fence-validated, and a global timeout cancels every datacenter. Gaps: the stuck check compares the manager's `monotonic()` with the gate's. Tracking never stops on normal completion. The "all DCs timed out" case returns early (`:442-445`). `has_recent_progress` is ignored. After a takeover, managers keep reporting to the old gate. The global-timeout fence is a counter that is always 1 and is compared with the manager's leader fence. Managers never resend pending reports. |
| AD-35 | Vivaldi Coordinates with Role-Aware Failure Detection | PARTIAL | `D/swim/health_aware_server.py:942-956,2186-2201,2458-2461`; `D/nodes/gate/server.py:1466-1474,5294,5309-5316`; `D/swim/core/gate_state_embedder.py:217` | RTT updates the coordinate and role-aware confirmation is wired. RTT-scaled suspicion is superseded, as the AD's "As built" section says. Coordinates are stored under `"host:port"` but `_get_datacenter_coordinate` looks them up by manager `node_id`, so the lookup always returns None. `record_probe_rtt` has 0 callers, so `_on_peer_coordinate_update` is dead. |
| AD-36 | Vivaldi-Based Cross-DC Job Routing | PARTIAL | `D/nodes/gate/handlers/tcp_job.py:850-864`; `D/nodes/gate/server.py:1009,1070-1080,6154-6203`; `D/routing/constrained_placement_policy.py:62-105`; `D/routing/datacenter_latency_estimator.py:84-106` | The router alone decides placement, using buckets, score, cooldown, rendezvous tie-break and failover. The Vivaldi RTT input is never present (AD-35 key bug), so latency comes only from the AD-45 observations and the prior. Hysteresis is superseded per the doc. The D-62 latency budget is not in AD_36.md. The "<10 s failover" criterion is untested (A2-G-247). |
| AD-37 | Explicit Backpressure Policy | PARTIAL | `D/nodes/gate/handlers/tcp_job.py:399-405`; `D/server/protocol/message_priority.py:13+`; `D/nodes/gate/state.py:459-522`; `D/nodes/gate/server.py:6966-6993` | The gate sheds its own load and the message classes are defined once. It does not honour manager backpressure: the levels are stored and never read (same store as in AD-23). The ledger note that the classes "exist twice" (REMAINING_LEDGER §2 new gaps) is out of date. |
| AD-38 | Global Job Ledger with Per-Node WAL | COMPLIANT | `D/nodes/gate/server.py:1353,1447-1466,3245-3323,3531-3549,3652-3687,5692-5714,9929,10001-10012`; `D/nodes/gate/ledger_region_span.py`; `D/nodes/gate/handlers/tcp_cancellation.py:371-381` | The ledger is opened and recovered before Raft starts, and every event type is emitted. REGIONAL commits go through per-job Raft; GLOBAL waits until two regions hold the entry. Checkpoints run off the reap tick. JobCreated is written after the client's ack; durability before the ack comes from the persisted replica (A2-G-266). The numeric criteria are unmeasured (A2-G-264). AD_38.md's status still says replicas are not persisted. |
| AD-39 | Logger Extension for AD-38 WAL Compliance | SUPERSEDED | `D/nodes/gate/server.py:1447-1465`; `D/ledger/wal/node_wal.py:142,290,432` | Gate durability uses the ledger's own `NodeWAL` (CRC, fsync, backpressure, HLC LSNs). The gate logger stays in data-plane mode (§8.4). The provider-WAL parts are Doc-obsolete (A3-G-6, -8, -10, -13…-19). |
| AD-40 | Idempotent Job Submissions | PARTIAL | `D/nodes/gate/handlers/tcp_job.py:441-448,643-768,970-972,1018`; `D/idempotency/gate_cache.py:82-99,163,279,302-314,351,384-389`; `D/nodes/gate/replication_coordinator.py:239-240,904-931`; `D/nodes/gate/server.py:1358,1420,5366,5406-5413` | The cache is checked before admission, duplicates of a PENDING entry wait, committed acks are replayed, keys are adopted across gates through the replica, recovered after a crash, and memory is bounded. Gaps: a committed cache entry expires after 300 s while the replica keeps holding the key, so a later retry is refused and, after the replica is reaped, runs again (see bugs). A wakeup can be lost on a PENDING wait. The cross-gate key check is a linear scan. DIVERGENT detail (code right): REJECTED is never cached. `reject()` and `_insert_entry` are dead. |
| AD-41 | Resource Guards | COMPLIANT (gate side) | `D/nodes/gate/health_coordinator.py:283-291,699-723,774`; `D/resources/datacenter_resource_aggregator.py:80,126-135`; `D/nodes/gate/server.py:2590` | The DC resource view is read: it feeds the routing factor and is returned in ping. Stale reports are dropped. Enforcement is the manager's job (P-AD41-1). |
| AD-42 | SLO-Aware Health and Routing | PARTIAL | `D/nodes/gate/state.py:318-328,425-457`; `D/nodes/gate/health_coordinator.py:466-486,774-785`; `D/routing/routing_scorer.py:65`; `D/nodes/manager/server.py:6339,6403` | The SLO factor feeds the score, health, the predictor and the D-62 p95. The "freshest manager" choice compares each manager's `slo_updated_at`, which is that manager's own `monotonic()`, across hosts. The same pattern is in `D/nodes/gate/models/dc_health_state.py:94-135`, which is dead. There are no per-job SLO targets. SWIM-hierarchy dissemination is superseded (A3-G-36). |
| AD-43 | Capacity-Aware Spillover and Core Reservation | COMPLIANT | `D/nodes/gate/dispatch_coordinator.py:687-738,796-809,839-871`; `D/capacity/capacity_aggregator.py:36-63`; `D/capacity/spillover_evaluator.py:42-120`; `D/nodes/gate/server.py:959-966` | Spillover is evaluated on every primary dispatch, with the real core requirement and fresh capacity. Spillover runs at dispatch time, not at submit (doc drift, A3-G-46). |
| AD-44 | Retry Budgets and Best-Effort Completion | PARTIAL | `D/reliability/best_effort_manager.py:62-104,196-200`; `D/nodes/gate/server.py:1421,3878-4165,8273-8291,8525,8652-8670`; `D/nodes/gate/dispatch_coordinator.py:655,1169-1173` | The retry budget passes through to managers. `min_dcs`, the deadline, and the late-result policy (log/update) work (P-AD44-1, A3-G-50). Gaps: the deadline loop isolates no job's errors, so one raise ends it for the rest of the process. Tracking starts only after every primary is dispatched, so an early final result takes the non-best-effort path. |
| AD-45 | Adaptive Route Learning | COMPLIANT | `D/nodes/gate/dispatch_coordinator.py:928-935,1011-1016`; `D/routing/observed_latency_state.py:74,103`; `D/routing/observed_latency_tracker.py:39,86-100`; `D/routing/datacenter_latency_estimator.py:66-90`; `D/routing/routing_scorer.py:61-66`; `D/nodes/gate/server.py:9870,10409` | Samples are taken when a manager accepts, folded into an EWMA with a confidence ramp, and blended into the routing score. Stale entries are cleaned up and metrics exported. `DispatchTimeTracker` does not exist; the start time is stamped in the dispatch coordinator. |
| AD-46 | SWIM Node State via IncarnationTracker | PARTIAL | `D/swim/health_aware_server.py:258,872,1198,1392,2432,6248-6266`; `D/swim/detection/incarnation_tracker.py:70-71,204,236,343-446,739`; `D/swim/message_handling/server_adapter.py:370-377`; `D/nodes/gate/server.py:641,5280,9979` | There is one store, writes go through incarnation and priority resolution, and DEAD entries are evicted with a cap. Gaps: `_death_timestamps` and `_death_incarnations` are never pruned (`cleanup_death_records` has 0 callers). `GateServer._dead_gate_addrs` duplicates SWIM's DEAD state and is not pruned on reap. The `safe_queue_put` stub is not removed. The doc's "no locks" is stale. |
| AD-47 | Worker Event Log | N/A (role) | — | Worker only; no use under `D/nodes/gate`. |
| AD-48 | Cross-Manager Worker Visibility | N/A (role) | `D/swim/health_aware_server.py:1841-1867,2029-2039` | Manager only. The gate's base class strips the `#|x`/`#|w` channels, as the doc's Part 16 says. |
| AD-49 | Workflow Context Propagation | N/A (role) | `D/nodes/gate/job_failover_coordinator.py:630-652` | Manager only. On datacenter loss the gate sends `rerun_workflow_ids`, which is consistent with the decision. |
| AD-50 | Manager Health Aggregation and Alerting | N/A (role) | `D/datacenters/datacenter_health_manager.py:326-351`; `D/nodes/gate/server.py:6116-6146,9927` | The decision is the manager's. The gate's counterpart aggregates manager health into DC health and raises a leader-overload alert. |

---

## Behavioral Verification

### AD-13: Gate Split-Brain Prevention
- When SWIM marks a peer DEAD, `_on_node_dead` runs (`D/nodes/gate/server.py:5253`) and then `GatePeerCoordinator.handle_peer_failure` (`D/nodes/gate/peer_coordinator.py:134-149`). Its first statements take the peer lock, bump the epoch, remove the active peer and remove it from the hash ring.
- The election majority comes from `_configured_gate_count()` (`D/nodes/gate/server.py:6373-6395`), the max of the known, cohort and active counts. It does not shrink when peers die, so a minority side's pre-vote fails at `D/swim/leadership/local_leader_election.py:695`.
- `is_leader()` requires a held lease (`D/swim/health_aware_server.py:5553-5556`), and the gate steps down after quorum stays lost (`D/nodes/gate/server.py:10136-10151`). ✓

### AD-14 / AD-15: Cross-DC Statistics
- A manager talks to the gate through `windowed_stats_push` (`D/nodes/gate/server.py:3020-3039`), `job_final_result`, `job_status_push_forward` and `receive_job_progress_report` (timeout tracker only, `:1951-1958`).
- None of these write `GlobalJobStatus` totals or `_job_stats_crdt`. Only `receive_job_progress` (`:1793`) does, and a repo-wide grep finds no sender other than the gate's own peer forwarder (`:6688`).
- Result: the CRDT is never written in production, and `JobBatchPush` carries the initial zeros until `_apply_global_result_to_job_locked` (`:4273`) runs at job end. Live aggregates reach clients only through the windowed-stats push. ✗

### AD-16: DC Health Classification
- A manager heartbeat calls `DatacenterHealthManager.update_manager` (`D/datacenters/datacenter_health_manager.py:118-132`), whose first statements cache the info and feed the phi detector.
- Routing calls `build_datacenter_candidates` → `classify_datacenter_health` (`D/nodes/gate/health_coordinator.py:754`) → `get_datacenter_health`, which applies, in order: no managers → UNHEALTHY, INITIALIZING, stale → UNHEALTHY, storage → UNHEALTHY, zero workers → BUSY, then the overload classifier. `CandidateFilter` then hard-excludes UNHEALTHY and INITIALIZING. ✓
- But the denominator, `len(dc_managers)` (`:443-463`), is never pruned. `_cleanup_stale_manager` (`D/nodes/gate/server.py:9836-9842`) reaps the manager from `GateRuntimeState`, the selector, backpressure and circuits, but not from `DatacenterHealthManager`. ✗

### AD-20: Cancellation Propagation
1. `cancel_job` is CRITICAL at the transport, so admission always lets it through (`D/reliability/adaptive_rate_limiter.py:126`).
2. The handler's first step, `_cancel_job_for_client` (`D/nodes/gate/handlers/tcp_cancellation.py:212-216`), calls `_check_rate_limit(client_id, "cancel")`. That reaches `ServerRateLimiter.check_rate_limit`, which is fixed at `RequestPriority.NORMAL` (`D/reliability/server_rate_limiter.py:176-181`), so under OVERLOADED it returns a 429. ✗
3. `_cancel_job_across_datacenters` visits each datacenter once (`max_attempts=1`). On the first confirmation it records ledger events and sets CANCELLED (`:363-376`). A later re-issue short-circuits at `:298`. Datacenters that did not confirm are never re-driven. ✗

### AD-22 / AD-24: Load Shedding and Rate Limiting
- On every TCP request, `_admit_tcp_request` (`D/server/server/mercury_sync_base_server.py:2076`) calls `check_handler(peername, handler, priority)` with the AD-37 priority.
- That limiter reads `detector.current_state`, which the gate's sampler sets (`D/nodes/gate/server.py:9781-9793`):
  - CRITICAL always passes.
  - OVERLOADED gets a 429 with a retry hint.
  - STRESSED charges a per-client budget.
  - BUSY refuses LOW.
  - Otherwise the per-operation sliding window applies.
- Handler-level `should_shed_handler("job_submission")` (`D/nodes/gate/handlers/tcp_job.py:399`) sheds only at OVERLOADED, which the transport has already refused, so it cannot be reached. Overload shedding works. ✓ (with the CONTROL-at-NORMAL defect above)

### AD-29 / AD-31: Peer Confirmation and Gossip Callbacks
- **Startup:** `join_cluster` marks seeds UNCONFIRMED (`D/swim/health_aware_server.py:4193`). An ack goes to `confirm_peer` (`:998`), whose first statements move the peer to confirmed, update the tracker, add it to probing and fire the callbacks. Then `GateServer._on_peer_confirmed` (`D/nodes/gate/server.py:5247-5251`) maps UDP to TCP and adds the active peer through the TaskRunner. ✓
- **Gossip DEAD:** `process_piggyback_data` (`D/swim/health_aware_server.py:3731`) checks freshness and reads `was_dead`. On a real change it calls `notify_node_dead`. The callbacks then run the peer failure, orphan marking of the dead leader's jobs (`D/nodes/gate/server.py:5485-5506`), the takeover quorum commit, and notification to managers (`:5634-5640`). ✓
- **Runtime-added gate:** its UDP→TCP mapping is set only from its heartbeat (`:5174`). Confirmation fires once from the JOIN, before the mapping exists, and is dropped, so the peer never becomes active. ✗

### AD-30: Hierarchical Failure Detection
- A failed dispatch (`D/nodes/gate/dispatch_coordinator.py:965-968`) starts a suspicion of the manager scoped to that datacenter (`D/nodes/gate/server.py:5958-5975`). ✓
- A successful dispatch (`:1028-1030`) and every manager heartbeat (`D/nodes/gate/health_coordinator.py:312-316`) call `_confirm_manager_for_dc` (`D/nodes/gate/server.py:5977-5994`). That calls `confirm_job(from_node=self)`, and `JobSuspicion.add_confirmation` drops it because the gate is the originator (`D/swim/detection/job_suspicion.py:41-42`).
- Nothing refutes, so the suspicion always expires and charges a circuit failure (`D/nodes/gate/server.py:5936-5952`) against a manager that has since succeeded. ✗

### AD-33: Federated Health Monitoring
- The probe loop (`D/swim/health/federated_health_monitor.py:417-455`) calls `_send_xprobe` (`D/nodes/gate/server.py:7207`). The xack returns to `_handle_xack_response` (`:1638`), which records the leader and calls `handle_ack`, setting REACHABLE and reporting latency to `cross_dc_correlation` (`:7272`).
- `D/nodes/gate/health_coordinator.py:493-594` merges probe state into DC health, which feeds `build_datacenter_candidates`, the router and failover (`D/nodes/gate/server.py:989-992`). A probe failure changes where jobs are routed. ✓

### AD-34: Adaptive Job Timeout
1. The manager's timeout loop sends `receive_job_progress_report` to `timeout_tracking.gate_addr`, which is set once from the origin gate (`D/jobs/gate_coordinated_timeout.py:466`).
2. The gate handler (`D/nodes/gate/server.py:1951`) drops reports for terminal jobs. `record_progress` (`D/jobs/gates/gate_job_timeout_tracker.py:200-215`) admits a report only if its fence is current. It then stores `report.timestamp`, which is the manager's `_DEFAULT_CLOCK.monotonic()` (`D/jobs/gate_coordinated_timeout.py:452`), into `dc_last_progress`.
3. Every 15 s, `_check_global_timeout` compares that value with the gate's own `_DEFAULT_CLOCK.monotonic()` (`D/jobs/gates/gate_job_timeout_tracker.py:439,495-503`). Those are two hosts' boot-relative clocks. ✗
4. A verdict sends `job_global_timeout` and calls `handle_global_timeout` (`D/nodes/gate/server.py:4426-4454`), which marks TIMEOUT, cancels the datacenters, pushes the result and stops tracking. ✓ That is the only gate `stop_tracking` caller apart from `:4435`. Normal completion never stops tracking. ✗

### AD-36: Vivaldi Routing
- `_place_submission` (`D/nodes/gate/handlers/tcp_job.py:850`) calls `_select_datacenters_with_fallback` (`D/nodes/gate/server.py:6154`), then `GateJobRouter.route_job`, then `ConstrainedPlacementPolicy.place`. Its first statement is `DatacenterLatencyEstimator.estimate`.
- That calls `_get_datacenter_coordinate` (`D/nodes/gate/server.py:5309-5316`), which asks `get_peer_coordinate(heartbeat.node_id)`. SWIM records coordinates under `f"{host}:{port}"` (`D/swim/health_aware_server.py:2190-2201`), so the lookup always returns None. Placement falls back to observed latency or the prior, then filters, applies the budget, scores and sorts. Routing works; Vivaldi never contributes. ✗

### AD-37 / AD-23: Backpressure
- A manager heartbeat with `backpressure_level > 0` (`D/nodes/gate/handlers/tcp_manager.py:453`) calls `_handle_manager_backpressure_signal` (`D/nodes/gate/server.py:6966`), which calls `GateRuntimeState.update_backpressure` (`D/nodes/gate/state.py:489`). That writes `_manager_backpressure`, `_dc_backpressure` and `_backpressure_delay_ms`.
- Grep finds 0 readers of `get_dc_backpressure_level`, `get_max_backpressure_level` and `_backpressure_delay_ms`. Dispatch and stats ignore manager backpressure. ✗

### AD-38: Job Ledger
- After a dispatch succeeds (`D/nodes/gate/dispatch_coordinator.py:657-668`), `_persist_accepted_job_durable` (`D/nodes/gate/server.py:3245`) calls `JobLedger.create_job(durability=REGIONAL|GLOBAL)`.
- `_replicate_ledger_regional` appends through Raft. `_replicate_ledger_global` runs only when the tier spans regions, and waits for holders in two regions (`D/nodes/gate/ledger_region_span.py`). A shortfall is logged.
- A terminal outcome goes through `_finalize_terminal_job` (`:3323`), which writes the terminal event and then revises the replica. At start, the ledger is recovered before Raft and before the tracker (`:1447-1466`, `:3652-3687`). ✓

### AD-40: Idempotency
- `_admit_valid_submission` (`D/nodes/gate/handlers/tcp_job.py:643`) first calls `_claim_idempotency_key`, which calls `GateIdempotencyCache.check_or_insert` (`D/idempotency/gate_cache.py:82`). Under the lock, it does one of three things:
  - replays a committed entry;
  - waits on a PENDING one;
  - reserves PENDING.
- Admission then takes the lease, checks quorum and routes. `replicate_with_quorum` runs the leader's own prepare with the key check (`D/nodes/gate/replication_coordinator.py:239-240`), then applies the commit, which adopts the key on every gate. `_commit_idempotency_keys` (`:972`) runs before dispatch, and a `finally` (`:441-448`) releases the reservation on every other exit. ✓
- A committed cache entry expires after `IDEMPOTENCY_COMMITTED_TTL_SECONDS = 300` (`D/env/env.py:155`, `D/idempotency/gate_cache.py:384-389`), while the replica keeps holding the key until job-retention cleanup. ✗ (see bugs)

### AD-43 / AD-44: Spillover and Best-Effort Completion
- **Spillover:** `dispatch_job` → `_dispatch_job_with_fallback` (`D/nodes/gate/dispatch_coordinator.py:775`) computes the real `job_cores`. For each primary, `_spill_over_or_keep` (`:839`) fetches the policy-allowed candidates and calls `SpilloverEvaluator.evaluate` with fresh capacity (stale heartbeats pruned, `D/capacity/capacity_aggregator.py:36-63`). The result slot moves to the chosen datacenter. ✓
- **Best effort:** each datacenter's final result goes to `_decide_job_final_result_locked` (`D/nodes/gate/server.py:8652`). Its first statement is `best_effort_manager.record_result`, which returns None for an untracked job. The deadline loop (`D/reliability/best_effort_manager.py:196-200`) awaits `_completion_handler` per job with no isolation, so one raise ends the loop task. ✗

### AD-45 / AD-46: Route Learning and the SWIM Node Store
- **AD-45:** `_try_dispatch_to_dc` stamps the start time (`D/nodes/gate/dispatch_coordinator.py:928`). On acceptance, `_record_accepted_dispatch` awaits `record_job_latency` (`:1011-1015`), which caps the sample and folds it into the EWMA. `BlendedLatencyScorer.get_observed_latency` feeds the estimator blend `confidence*observed + (1-confidence)*predicted` (`D/routing/datacenter_latency_estimator.py:66-73`), which goes into `final_score` (`D/routing/routing_scorer.py:61-66`). ✓
- **AD-46:** `ack_handler` calls `update_node_state` (`D/swim/health_aware_server.py:6248-6266`), which awaits `IncarnationTracker.update_node` (`D/swim/detection/incarnation_tracker.py:204`) and then `NodeState.update`. The 30 s `cleanup()` evicts DEAD entries. `_death_timestamps` and `_death_incarnations` are cleared only on rejoin or on refutation of a suspicion (`:602,687`), never by age. ✗

---

## Production Bugs Found by Reading

Each bug below was checked against the code at 700b9aac. Ledger ids are given where one applies.

1. **An overloaded gate refuses every client cancel.**
   - Where: `D/nodes/gate/handlers/tcp_cancellation.py:215` (and `:890`) → `D/nodes/gate/server.py:6246-6253` → `D/reliability/server_rate_limiter.py:176-181`, which is fixed at `RequestPriority.NORMAL`. `D/reliability/adaptive_rate_limiter.py:131` rejects non-CRITICAL requests under OVERLOADED.
   - Effect: `cancel_job` and `receive_cancel_single_workflow` get a 429 exactly when the gate is overloaded.
2. **A partly confirmed cancel strands the other datacenters.**
   - Where: `D/nodes/gate/handlers/tcp_cancellation.py:363-376`. The job is marked CANCELLED on the first confirmation, a re-issue short-circuits at `:298`, and the stray datacenter's progress is dropped as terminal (`D/nodes/gate/handlers/tcp_job.py:1203-1216`).
   - Effect: the datacenter that did not confirm runs to its own end.
3. **Reaped managers are never removed from DC health tracking.**
   - Where: `D/nodes/gate/server.py:9836-9842` never calls `DatacenterHealthManager.remove_manager` (`D/datacenters/datacenter_health_manager.py:152`). That method, `mark_manager_dead` (`:146`) and `cleanup_stale_managers` (`:554`) all have 0 callers.
   - Effect: `_dc_manager_info` and `_manager_detectors` grow with every manager address change. The manager-unhealthy ratio's denominator only grows, so after enough manager replacements a datacenter is pinned at DEGRADED, then UNHEALTHY, and is excluded from routing.
4. **The AD-34 stuck check compares clocks from different hosts.**
   - Where: `D/jobs/gates/gate_job_timeout_tracker.py:215` stores the manager's `monotonic()` (`D/jobs/gate_coordinated_timeout.py:452`), and `:439,495-503` compare it with the gate's `monotonic()`.
   - Effect: if a manager host booted more than 180 s after the gate host, every job looks "all DCs stuck" and is timed out. In the other direction, stuck is never detected. The bug cannot be seen on one host.
5. **The gate timeout tracker is not stopped on normal completion.**
   - Where: no `stop_tracking` call in completion or cleanup (`D/nodes/gate/server.py:9616-9642`). The only callers are `:4435` and `:4454`.
   - Effect: about 180 s after its last report, a completed job gets a false "global timeout" WARNING and a `job_global_timeout` is sent to the managers. A datacenter marked "completed" leaves the tracker entry in place forever. Related: when every datacenter reports a local timeout, the check returns early (`D/jobs/gates/gate_job_timeout_tracker.py:442-445,465`).
6. **After a gate takeover, managers keep sending AD-34 reports to the old gate.**
   - Where: the manager's `job_leader_gate_transfer` (`D/nodes/manager/server.py:12878`) does not update `job.timeout_tracking.gate_addr` (`D/jobs/gate_coordinated_timeout.py:466,495,523,556`).
   - Effect: the new leader's tracker (`D/nodes/gate/server.py:5659`) never hears progress and times a healthy job out 180 s after the takeover.
7. **AD-35/36: Vivaldi coordinates are never found.**
   - Where: stored under `"host:port"` (`D/swim/health_aware_server.py:2190`) but looked up by `node_id` (`D/nodes/gate/server.py:5316`). `record_probe_rtt` (`D/swim/core/gate_state_embedder.py:217`) has 0 callers.
   - Effect: Vivaldi never contributes to routing.
8. **AD-30: a suspicion is never cleared by a later success.**
   - Where: `_confirm_manager_for_dc` (`D/nodes/gate/server.py:5977-5994`) confirms from the originator, which `D/swim/detection/job_suspicion.py:41-42` drops. The incarnation is always 0.
   - Effect: every dispatch failure ends in an extra circuit failure, charged against a manager that has since succeeded.
9. **A gate added at runtime never becomes an active peer.**
   - Where: confirmation fires before the UDP→TCP mapping exists (`D/nodes/gate/server.py:5174,5247-5251`) and does not fire again.
   - Effect: the submission quorum and leader checks (`D/nodes/gate/handlers/tcp_job.py:843`, `D/nodes/gate/server.py:6394,10119`) count too few peers.
10. **AD-40: retries after 300 s get a wrong refusal, and later a duplicate run.**
    - Where: the committed cache entry expires after 300 s (`D/idempotency/gate_cache.py:384-389`, `D/env/env.py:155`) while the replica holds the key. The leader's own prepare is then REJECTED (`D/nodes/gate/replication_coordinator.py:239-240`), and the client gets "quorum unavailable".
    - Effect: once the replica is reaped, the retry is admitted as a new job and runs a second time.
11. **AD-15: the batch tier pushes zeros.**
    - Where: `D/nodes/gate/stats_coordinator.py:349-376` reads totals that only the dead `receive_job_progress` path fills.
    - Effect: the client's `overall_rate` is reset to 0.0 every 0.25 s (`D/nodes/client/status_application.py:83-85`).
12. **AD-14: CRDT overcount.**
    - Where: `D/nodes/gate/server.py:6765-6766` passes cumulative totals to `GCounter.increment`.
    - Effect: none today, because the store has no reader and no live writer.
13. **The best-effort deadline loop dies on the first raise.**
    - Where: `D/reliability/best_effort_manager.py:196-200`.
    - Effect: no best-effort job completes by its deadline afterwards. Also, tracking starts only after every primary has been dispatched (`D/nodes/gate/dispatch_coordinator.py:655` → `D/nodes/gate/server.py:8291`), so an early final result skips best-effort rules.
14. **AD-42: SLO "freshest manager" compares monotonic clocks across hosts.**
    - Where: `D/nodes/gate/state.py:425-457`, fed by `D/nodes/manager/server.py:6339,6403`.
    - Effect: the per-DC SLO follows the manager on the longest-booted host, including a dead manager's frozen summary until it is reaped.
15. **AD-46: SWIM death records are never pruned.**
    - Where: `D/swim/detection/incarnation_tracker.py:70-71`. `cleanup_death_records` (`:739`) has 0 callers.
    - Effect: the maps grow with address churn. `GateServer._dead_gate_addrs` (`D/nodes/gate/server.py:641`) is also not pruned on reap (`:9979`).
16. **AD-32: inbound TCP requests are all spawned as HIGH, and a shed request is dropped silently.**
    - Where: `D/server/server/mercury_sync_base_server.py:1666-1694`.
    - Effect: CRITICAL control messages can be shed at the HIGH limit, and the client gets no reply or Retry-After.
17. **AD-34: the global-timeout fence is a decision counter that is always 1.**
    - Where: `D/jobs/gates/gate_job_timeout_tracker.py:558,582`. The manager compares it with its leader fence (`D/jobs/gate_coordinated_timeout.py:80,288`).
    - Effect: after two or more manager failovers, the gate's timeout decision is rejected as stale. The cancel path still stops the job.
18. **Low severity:**
    - The AD-40 PENDING wait can miss its wakeup and block for the full 30 s (`D/idempotency/gate_cache.py:96-99,309-314`).
    - UDP `check_sync` never limits anything (`D/reliability/server_rate_limiter.py:124-160`).
    - Three retry loops have no jitter (`D/nodes/gate/orphan_job_coordinator.py:413-420`, `D/nodes/gate/stats_coordinator.py:274-281`, `D/nodes/gate/server.py:3173-3180`).
    - Immediate-tier `JobStatusPush` is built without `fence_token` (`D/nodes/gate/stats_coordinator.py:186-196`).
    - `_on_node_dead` removes a manager circuit by UDP address (`D/nodes/gate/server.py:5276-5279`).
    - `GateJobTimeoutTracker`, `DatacenterHealthManager` and the SWIM handlers use the module-global `RealClock` rather than the injected clock (`D/jobs/gates/gate_job_timeout_tracker.py:37`, `D/datacenters/datacenter_health_manager.py:45`). This affects SIM only.

---

## SCENARIOS.md Coverage

Not regraded in this pass. Scenario counts from the 2026-01-13 report (AD-34: 41, AD-37: 21, AD-16: 13, AD-31: 18) do not show that bugs 4-6 above are covered. No scenario runs a manager and a gate on separate monotonic clocks, takes over a job mid-run while AD-34 reports are flowing, or replaces managers by address.

---

## Action Items

These are real gaps only. "new" means no `docs/REMAINING_LEDGER.md` id covers it.

| # | Item | AD | Ledger |
|---|------|----|--------|
| 1 | Cancel must not be rate-limited at NORMAL: pass the CONTROL priority through `check_rate_limit_with_priority` (bug 1) | AD-20/22/24 | new |
| 2 | Keep re-driving the cancel to unconfirmed datacenters, and set CANCELLED only after all confirm or when the client is told it was partial (bug 2) | AD-20 | new |
| 3 | Remove reaped managers from `DatacenterHealthManager` in `_cleanup_stale_manager`, and record the stale/BUSY transitions (bug 3) | AD-16 | new (the docstring half is in AD16, Doc-obsolete row) |
| 4 | AD-34 timestamps: use the gate's receive time, not the sender's `monotonic()`. Stop tracking on normal completion. Act on "all DCs timed out". Re-point `timeout_tracking.gate_addr` on gate transfer. Fix the global-timeout fence domain. Resend or drop `_pending_reports` (bugs 4, 5, 6, 17) | AD-34 | new (R-G53 / delta #8b cover fencing only) |
| 5 | Look Vivaldi coordinates up by the key SWIM stores them under, and wire or delete `record_probe_rtt` (bug 7) | AD-35/36 | new; A2-G-247 (failover speed) still open |
| 6 | Let positive evidence clear a per-DC job suspicion, or drop the suspicion layer, and fix the TCP/UDP key mismatch (bug 8) | AD-30 | new |
| 7 | Make a runtime-added gate an active peer once its TCP address is learned (bug 9) | AD-29 | new |
| 8 | Keep the committed idempotency entry for as long as the replica holds the key, or answer from the replica on a cache miss (bug 10). Add a key→job index for O(1) checks. Delete `reject()`, `_insert_entry` and the dead Idempotency events | AD-40 | new; the O(1) index and dead events are in the §3 "New gaps found" rows (no id) |
| 9 | Feed `JobBatchPush` from the windowed-stats aggregate, or stop sending rate and totals that were never filled (bug 11) | AD-15 | new (contradicts the closed A1-G-14 / A1-G-56) |
| 10 | Mark AD-14 superseded and delete `JobStatsCRDT` and `receive_job_progress`, or wire them with gauge (max) semantics (bug 12) | AD-14 | new (A2-G-258 covers the replacement only) |
| 11 | Read manager backpressure in dispatch or stats, or delete the store, and signal backpressure upstream on `windowed_stats_push` and progress acks | AD-23/37 | new (A2-G-248 covers manager shedding only) |
| 12 | Read `ManagerHealthState` (readiness/progress) in DC health, or delete it along with the four uninstantiated gate model classes | AD-19 | new |
| 13 | Isolate per-job errors in the best-effort deadline loop, and create best-effort state before the first dispatch (bug 13) | AD-44 | new (P-AD44-1 / A3-G-50 closed the policy only) |
| 14 | Choose the SLO-freshest manager by gate receive time (bug 14), and add per-job SLO targets or record the decision | AD-42 | new |
| 15 | Prune SWIM death records on DEAD eviction, prune `_dead_gate_addrs` on reap, and delete `safe_queue_put` (bug 15) | AD-46 | new |
| 16 | Classify inbound TCP by handler priority, return an error with Retry-After on shed, and read or delete `_pending_response_warn_threshold` (bug 16) | AD-32 | new |
| 17 | Gate state sync: retry with backoff and a retryable "not ready" reply | AD-11 | new |
| 18 | Gate-side negotiated capabilities are write-only; also check the manager's reply version | AD-25 | R-N2 (manager side; extend it to the gate) |
| 19 | Peer-gate `DiscoveryService` and the `_dc_manager_discovery` alias are write-only: delete them or select from them | AD-28 | new (gate analogue of AD28-1) |
| 20 | Jitter the three hand-rolled backoffs | AD-21 | new |
| 21 | Doc sweep: AD_13 (state location, cohort majority), AD_18 (EMA drift), AD_22 (final-result priority), AD_33 (UNKNOWN start state), AD_36 (D-62 budget), AD_38 (replica persisted), AD_40 (REJECTED not cached), AD_46 (locks), the stale `datacenter_health_manager.py` docstrings, and the SCAN.md Phase 12 matrix names | many | AD16, A2-G-247 (RoutingSwitch); the rest are new |
| 22 | Unmeasured numeric criteria (AD-36 failover <10 s, AD-38 latency/recovery/throughput) | AD-36/38 | A2-G-247, A2-G-264 |
| 23 | Thin server: `D/nodes/gate/server.py` is now 10,499 lines, one class, 596 defs | AD-27 (context) | R-G59, R-G63, AD27-1 |
| 24 | ~~No manager, worker or client compliance report exists~~ — closed: manager/worker/client_compliance_2026_10_07.md | — | R-G62 |

---

## Notes

- AD-27 is excluded per the scan parameters.
- N/A (role): AD-9, 10, 12, 26, 47, 48, 49, 50. Each was checked for a gate-side piece before being graded N/A; see the per-AD rows.
- The 2026-01-13 report graded 35 ADs COMPLIANT by checking that the artifacts existed. This report grades on what runs. The drop to 8 COMPLIANT reflects the method, not a regression.
