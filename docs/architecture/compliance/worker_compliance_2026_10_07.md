# Worker Module AD Compliance Report

**Date**: 2026-10-07
**Commit**: 700b9aac
**Scope**: AD-9 through AD-50 (excluding AD-27), graded for the WORKER role
**Module**: `hyperscale/distributed/nodes/worker/` (plus the shared modules it calls: `swim/health_aware_server.py`, `server/server/mercury_sync_base_server.py`, `reliability/`, `resources/`, `discovery/`)

**Method.** Each AD's requirements were taken from its `docs/architecture/AD_<n>.md` text (the doc names, not the older SCAN.md Phase 12 matrix names, which no longer match the numbering). For each requirement the worker's call site was found and the receiving code's first statements read, so that a class that exists but is never called, a store with no reader, or a handler that returns early is not credited. Caller counts are from `grep` over `hyperscale/`. Line numbers are relative to the repository root as of commit 700b9aac. No tests were run. "N/A (role)" means the AD is owned by another role; the worker's part, if any, is noted in one line.

---

## Summary

| Status | Count |
|--------|-------|
| COMPLIANT | 11 |
| PARTIAL | 10 |
| DIVERGENT | 1 |
| MISSING | 0 |
| SUPERSEDED | 0 |
| N/A (other role) | 19 |
| **Total** | **41** |

**Overall.** Fencing, cancellation, orphan handling, the extension protocol's wire path, resource guards, context propagation and the no-consensus rule all work as the ADs describe. Four problems change behaviour in production:
1. The AD-23/AD-37 backpressure state latches: the worker never applies a release signal from a manager. After one REJECT it drops all progress until it restarts.
2. The AD-47 event log can never be turned on.
3. The AD-26 H4 trigger loop stops for good after the first exception in a tick.
4. AD-29: managers that are only named in a registration response enter the healthy set before any contact with them.

Three per-job maps on the worker also grow without bound (see Action Items).

---

## Detailed Findings

| AD | Name | Status | Evidence (file:line) | Notes |
|----|------|--------|----------------------|-------|
| AD-9 | Retry Requeues the Pending Workflow | N/A (manager) | — | The requeue happens in the manager's dispatcher. On the worker, the fence check under AD-10 rejects the old attempt. |
| AD-10 | Per-Job Fencing Tokens | COMPLIANT | `nodes/worker/handlers/tcp_dispatch.py:140-156` → `nodes/worker/state.py:288-301`; transfer fence `nodes/worker/server.py:1681-1690`, `handlers/tcp_leader_transfer.py:93-101` | A dispatch is accepted only when its token is strictly greater than the stored one, under `_counter_lock`. A leader transfer is checked the same way against the per-job token. Memory leaks in these maps are listed under Action Items 6-8. |
| AD-11 | State Sync Retries with Exponential Backoff | N/A (manager) | — | The manager owns the retries (`nodes/manager/sync.py:91-98`). The worker is only the target of the sync. |
| AD-12 | Manager Peer State Sync on Leadership | COMPLIANT | `nodes/worker/server.py:2692-2697` → `handlers/tcp_state_sync.py:56-62` | The worker answers `state_sync_request` with a full `WorkerStateSnapshot` (`server.py:1467-1476`). If the request fails to parse, it returns `b""` (`tcp_state_sync.py:63-64`), which the manager treats as no answer. |
| AD-13 | Gate Split-Brain Prevention | N/A (gate) | — | |
| AD-14 | CRDT-Based Cross-DC Statistics | N/A (gate) | — | |
| AD-15 | Tiered Update Strategy | N/A (gate/manager) | — | The worker feeds the periodic tier through its progress flush (`WORKER_PROGRESS_FLUSH_INTERVAL`). |
| AD-16 | Datacenter Health Classification | N/A (gate) | — | |
| AD-17 | Smart Dispatch with Fallback Chain | N/A (manager/gate) | — | |
| AD-18 | Hybrid Overload Detection | PARTIAL | `nodes/worker/backpressure.py:56` (detector), `:92-108` (poll loop, started at `server.py:1047-1051`), `:120-130` | Only the resource arm (CPU/memory) is fed. `record_workflow_latency` (`backpressure.py:142`) has 0 callers, so the delta and absolute-latency arms never run. That is a recorded decision: workflow duration is not a latency signal (`workflow_executor.py:363-379`). `WORKER_OVERLOAD_POLL_INTERVAL` is read into `WorkerConfig.overload_poll_interval_seconds` (`models/worker_config.py:183`) but never passed on: the manager is built without `poll_interval` (`server.py:193-202`), so the hard-coded `0.25` (`backpressure.py:48`) applies. |
| AD-19 | Three-Signal Health Model | COMPLIANT | Embed `nodes/worker/server.py:350-368`; TCP heartbeat `:1525-1572`; throughput `state.py:554-582` fed by `workflow_executor.py:389` | Liveness comes from SWIM. Readiness is `accepting_work` plus available cores. Progress is completions per interval against an expected rate. The worker reports `lhm_score`. Note: the two copies of "accepting work" disagree. The SWIM embed (`:350-351`) counts HEALTHY or DEGRADED and ignores overload. The TCP heartbeat (`:1552-1555`) counts not-DRAINING and not overloaded or critical. |
| AD-20 | Cancellation Propagation | COMPLIANT | `handlers/tcp_cancel.py:77-88` (already-done is idempotent success, `:149-157`); `cancellation.py:109-149`; job-scoped `handlers/tcp_cancel_job.py:147` | The worker cancels the TaskRunner run, the RemoteGraphManager nodes and the event, then acks. The manager finalizes from that direct ack (`nodes/manager/cancellation.py:440-460`). Note: the backstop `send_cancellation_complete` push (`server.py:2537-2554`) looks up `_active_workflows` after `Run.cancel` has waited for the task (`taskex/run.py:299-309`). By then the task's `finally` has already removed the workflow (`workflow_executor.py:670-689`), so on the normal path the push is skipped. The manager does not depend on it. |
| AD-21 | Unified Retry Framework with Jitter | PARTIAL | `registration.py:148-161` (`RetryExecutor`, FULL jitter); `worker_progress_reporter.py:169-178` (FULL jitter); `worker_cluster_connection.py:492-502` (jittered rejoin) | Two retry paths have no jitter. Final-result retries back off as pure doubling (`worker_progress_reporter.py:1336-1338`). Seed recovery uses fixed steps of 1, 2, 4, 8, 10 s (`server.py:1902-1907`), and it runs exactly when every worker has lost every manager, so all workers retry in step. The registry's `recovery_jitter_*` and `recovery_semaphore` are stored (`registry.py:61-64`) and never read. |
| AD-22 | Load Shedding with Priority Queues | PARTIAL | `server/server/mercury_sync_base_server.py:276-278` (limiter), `:286-298` (AD-32 in-flight trackers) | AD-32's per-priority in-flight bounds apply, inherited from the base server. Shedding gated on health does not. The worker keeps the base `ServerRateLimiter`, whose own `HybridOverloadDetector` is never sampled, so it always reads HEALTHY. Manager and gate replace theirs with a sampled detector (`nodes/manager/server.py:760-764`, `nodes/gate/server.py:457`). The worker's sampled detector (`backpressure.py:56`) is a separate object. |
| AD-23 | Backpressure for Stats Updates | PARTIAL | `worker_progress_reporter.py:1165-1180`; `state.py:490-508`; flush `background_loops.py:695-737` | The worker applies a manager's level and delay on the way up, but never on the way down. `_apply_ack_backpressure` acts only when `ack.backpressure_level > 0` (`:1168`), and the delay is `max(current, suggested)` (`:1176`). Nothing else writes `_manager_backpressure` or `_backpressure_delay_ms`, so once a manager signals, the level and delay stay at their peak until the process restarts. The manager sends NONE (`nodes/manager/server.py:9041`, `:8958`), and the worker drops it. Production bug (Action Item 1). |
| AD-24 | Rate Limiting | PARTIAL | Inbound: `mercury_sync_base_server.py:276`, `:1973`; outbound: `worker_progress_reporter.py:1572-1582`, `:1326-1335` | Per-operation sliding windows apply to inbound traffic. Refused final results wait out `retry_after_seconds` without spending an attempt. The health gate is inert (see AD-22). Also, AD-24's STRESSED budget assumes a throttled worker adds `WORKER_BACKPRESSURE_THROTTLE_DELAY_MS` (500 ms) (`reliability/rate_limit_derivation.py:98-105`). In fact the worker adds the manager's suggested delay, which is fixed at 100 ms (`reliability/backpressure_signal.py:30`, `stats_buffer.py:201`). The Env setting's only reader, `get_throttle_delay_seconds` (`backpressure.py:227`), has 0 callers. With a 100 ms delay, a throttled worker can send 67 updates per workflow per 10 s window. The budget assumes 19. |
| AD-25 | Version Skew Handling | PARTIAL | Sent: `registration.py:132-145`; stored: `registration.py:357-376` | The worker sends its version and capabilities. The stored result has `compatible=True` hard-coded (`:375`), with no MAJOR check. `common_features` is the manager's own list, not the intersection with the worker's (`:365-374`). Nothing reads the result: the `negotiated_capabilities` property (`:88`) has 0 callers. Unknown and missing wire fields are handled by `Message` (`models/message.py`, shared), which complies. |
| AD-26 | Adaptive Healthcheck Extensions | PARTIAL | H4 trigger `server.py:295-304`, started `:1087-1094`; piggyback `:355-366`; dispatch-time request `:2477-2488`; response `:2731-2782` (clears the latch and stretches the local deadline, `state.py:312-331`); orphan grace extension `background_loops.py:430-476` | The wire path, the latch lifecycle and local deadline extension all work. Gaps: (a) `run_loop` re-raises any exception from `tick()` (`extension_trigger.py:310-317`), even though its own comment says a failed tick "must not kill the loop". The first failure ends autonomous extensions for the life of the process. (b) The H3 `step_transitions` dimension is always 0 (`server.py:1521`, `:2485`), so only two of the three progress dimensions ever move. |
| AD-28 | Enhanced DNS Discovery with Peer Selection | PARTIAL | Selection `discovery.py:39-63` ← `registry.py:382` ← `server.py:185-190`; EWMA feed `server.py:2079-2094` (from `:2061-2076`); DNS maintenance `background_loops.py:548-626` | Rendezvous selection plus Power of Two Choices picks the primary manager. The EWMA latency is fed only by registration round trips. Progress, result and heartbeat traffic never update it, so the ranking rests on a sample or two taken at boot or rejoin. mTLS role claims are enforced in the shared transport (N/A here). |
| AD-29 | Protocol-Level Peer Confirmation | PARTIAL | `server.py:495` → `:2269-2296`; heartbeat confirm `heartbeat.py:103`; unconfirmed add `registration.py:513-525` | The SWIM-confirmed callback is wired, and the AD-29 docstring says it is "the ONLY place" managers join `_healthy_manager_ids` (`server.py:2274`). That is not true. On every accepted registration, `_mark_managers_healthy(response.healthy_managers)` (`registration.py:282`, `:342-345`) adds every manager the responder lists, none of which this worker has contacted. They are also marked registered (`registration.py:280` → `register_peer`). Primary selection and the cluster-connection watchdog then treat unreached managers as active. |
| AD-30 | Hierarchical Failure Detection | N/A (manager) | — | The worker's result retry cap is set from `JOB_RESPONSIVENESS_THRESHOLD` (`models/worker_config.py:169`) so that results keep arriving within the manager's job-responsiveness window. |
| AD-31 | Gossip-Informed Callbacks | COMPLIANT | `server.py:488` (`register_on_node_dead`); gossip path `swim/health_aware_server.py:3838`; `health.py:63-75` → `server.py:2160-2214` (orphans); leadership from heartbeat `server.py:2334-2366`; transfer `handlers/tcp_leader_transfer.py:240-294` | When a manager dies, whether the worker learned it directly or by gossip, its workflows are marked orphaned. The orphan flag is cleared when a heartbeat claims the job's leadership or a transfer arrives. |
| AD-32 | Hybrid Bounded Execution | COMPLIANT | Inherited: `mercury_sync_base_server.py:286-298` (InFlightTracker), per-destination send semaphores (`:300-308`, `send_tcp`) | Applies through inheritance, as the AD's applicability matrix says. `OUTGOING_OVERFLOW_SIZE` and `OUTGOING_MAX_DESTINATIONS` are still dead Env fields (ledger, AD-1–36 "New gaps found"). |
| AD-33 | Federated Health Monitoring | N/A (gate) | — | |
| AD-34 | Adaptive Job Timeout | COMPLIANT | Deadline set `workflow_executor.py:207-208`; enforced `background_loops.py:478-488` ← `state.py:341-367`; grant stretches it `server.py:2784-2799` | The worker enforces its own deadline (the Phase H2 hierarchy on the wire) and extends it when an AD-26 grant backed by progress arrives. |
| AD-35 | Vivaldi Coordinates, Role-Aware Detection | N/A (SWIM base / manager) | — | Coordinates are carried by the shared SWIM layer. Passive detection of workers is the manager's side. |
| AD-36 | Vivaldi Cross-DC Routing | N/A (gate) | — | |
| AD-37 | Explicit Backpressure Policy | DIVERGENT | `background_loops.py:695-781`; `server.py:2590-2615`; ack `worker_progress_reporter.py:1165-1180` | (a) The AD's worker state diagram returns to NO_BACKPRESSURE when the level falls below THROTTLE. The worker never does (AD-23 bug). At REJECT, every flush clears the buffer (`background_loops.py:708-711`), so after one REJECT the manager never gets this worker's progress again. (b) BATCH is supposed to "aggregate buffer, flush less often". Instead it keeps the single update with the most `completed_count` per job and discards the rest (`server.py:2608-2615`, applied at `background_loops.py:767-768`). Under sustained BATCH, a job's other workflows may never report progress. |
| AD-38 | Global Job Ledger / Per-Node WAL | COMPLIANT | No Raft, WAL or ledger import under `nodes/worker/` (grep: 0 files) | Workers stay out of every consensus path, as required. Progress is fire-and-forget. A final result is pushed by the worker until acked, which is not a consensus ack. |
| AD-39 | Logger WAL Extension | N/A (logger) | — | |
| AD-40 | Idempotent Job Submissions | N/A (client/gate/manager) | — | |
| AD-41 | Resource Guards | COMPLIANT | Kalman per workflow `workflow_executor.py:915-933` (tracker built `server.py:440-443`, released `workflow_executor.py:682`); THROTTLE `server.py:2662-2667` → `handlers/tcp_throttle.py:59-75`; FD ceiling `server.py:1151-1171` → `:1448-1462`, drains via `:1441` | Per-workflow CPU and memory estimates with their uncertainty ride `WorkflowProgress`. The throttle scales executor concurrency. A process at its descriptor ceiling stops taking work. |
| AD-42 | SLO-Aware Health and Routing | N/A (manager) | — | AD_42.md says the worker-heartbeat digest tier is superseded. The manager measures dispatch latency itself. |
| AD-43 | Capacity-Aware Spillover | COMPLIANT | `handlers/tcp_dispatch.py:102-109` (rejects when cores cannot be allocated); `available_cores` in the heartbeat (`server.py:1542`) and the embed (`:337`) | One workflow per allocated core and no queue on the worker, as the AD says. Note that `_pending_workflows` (`state.py:87`) is never appended to (0 writers). The `MERCURY_SYNC_MAX_PENDING_WORKFLOWS` check (`tcp_dispatch.py:129-137`) and `queue_depth` are therefore always 0. |
| AD-44 | Retry Budgets and Best-Effort | N/A (manager/gate) | — | |
| AD-45 | Adaptive Route Learning | N/A (gate) | — | |
| AD-46 | SWIM Node State via IncarnationTracker | N/A (SWIM base) | — | The worker uses the inherited tracker to spot a restarted manager by its incarnation jump (`server.py:1234-1267`). |
| AD-47 | Worker Event Log | PARTIAL | Logger setup `server.py:741-761`; events `server.py:765`, `:885`, `:1638`, `:2304`, `:2828`; `workflow_executor.py:253`, `:425`, `:587`, `:626` | Built but never reachable. `_start_event_logger` returns at once when `event_log_dir is None` (`server.py:743`). `WorkerConfig.event_log_dir` defaults to None (`models/worker_config.py:110`), `from_env` never sets it (`:147-185`), and no Env field, CLI option or `WorkerServer` parameter supplies it (grep: one assignment site, the default). So no event is ever written in production. The `WorkerActionStarted/Completed/Failed` models (`hyperscale_logging_models.py:431-450`) are also never emitted (0 call sites). |
| AD-48 | Cross-Manager Worker Visibility | N/A (manager) | — | The worker registers with every manager it learns of, which this AD relies on. |
| AD-49 | Workflow Context Propagation | COMPLIANT | `workflow_executor.py:446` (`dispatch.load_context()`) → `:330-336` (`execute_workflow(..., context_dict, ...)`) → `:351` → `:656` (`WorkflowFinalResult.context_updates`) | The worker runs with the dispatched context and returns the updated context in its final result. |
| AD-50 | Manager Health Aggregation | N/A (manager) | — | |

---

## Behavioral Verification

### AD-10: Dispatch fencing
- ✓ `WorkflowDispatchHandler.handle` → `_admit_and_allocate` → `_stale_fence_rejection` (`tcp_dispatch.py:140-156`) runs before cores are allocated.
- ✓ `update_workflow_fence_token` holds `_counter_lock` and rejects `fence_token <= current` (`state.py:288-301`).
- ✓ A leader transfer is checked per job with a strict `>` (`server.py:1681-1690`) under a per-job lock (`tcp_leader_transfer.py:93-101`).
- ✗ The token is stored before allocation. If allocation fails (`tcp_dispatch.py:108-109`), the entry is never removed, and the workflow is requeued elsewhere (AD-9) (Action Item 8).

### AD-23 / AD-37: Progress backpressure
- ✓ The ack is parsed and applied per manager (`worker_progress_reporter.py:1137-1143`).
- ✓ The flush loop adds the delay, aggregates at BATCH and drops at REJECT (`background_loops.py:705-768`).
- ✗ A release is never applied: `if ack.backpressure_level > 0` (`worker_progress_reporter.py:1168`). The delay only ever ratchets up (`:1176-1179`). No other writer exists (grep: `set_manager_backpressure` is written only at `:1174`).
- ✗ BATCH discards every workflow's update except one per job (`server.py:2608-2615`).
- Test gap: `tests/unit/distributed/worker/test_worker_backpressure.py` calls `set_manager_backpressure(..., NONE)` directly and never sends an ack, so the latch is never exercised.

### AD-26: Extensions
- ✓ Dispatch-time request `server.py:2477-2488`; autonomous trigger `extension_trigger.py:156-294`; the heartbeat carries the latch (`server.py:355-366`).
- ✓ `extension_response` clears the latch and stretches the local deadline only for a grant backed by progress (`server.py:2778-2799`).
- ✓ Termination callbacks drop the trigger's per-workflow state on every exit path (`server.py:309-319`, `state.py:216-246`).
- ✗ A failed tick re-raises and the loop exits (`extension_trigger.py:310-317`). `_create_background_task` only logs it (`swim/health_aware_server.py:605-606`), and nothing restarts the loop.

### AD-29: Peer confirmation
- ✓ SWIM-confirmed managers are added through `_on_peer_confirmed` (`server.py:2269-2296`). Heartbeats confirm the peer (`heartbeat.py:103`).
- ✗ The registration response adds every listed manager to the healthy set directly (`registration.py:282`). The worker has not contacted them.

### AD-47: Event log
- ✗ No configuration path reaches `event_log_dir` (`models/worker_config.py:110`). Every `if self._event_logger is not None` guard is false in production.

---

## SCENARIOS.md Coverage

| AD | Matching lines in `docs/SCENARIOS.md` |
|----|----------------------------------------|
| AD-10 (fence) | 6 |
| AD-20 (cancel) | 9 |
| AD-26 (extension) | 4 |
| AD-23/AD-37 (backpressure) | 1 |
| AD-29 (unconfirmed) | 0 |
| AD-47 (event log) | 0 |

---

## Action Items

The first five items are production behaviour changes. Items 6-8 are memory leaks (CLAUDE.md: "Memory leaks are unacceptable").

1. **AD-23/AD-37: backpressure never releases.** `worker_progress_reporter.py:1168` ignores level 0, and `:1176` only ever raises the delay. Once one REJECT arrives, all of this worker's progress is dropped until restart (`background_loops.py:708-711`). Fix: on every parsed ack, write the ack's level for its manager, including NONE. Take the delay from the current per-manager signals instead of a running maximum. Clear a manager's entry when it is removed. *new (no ledger id)*
2. **AD-37: BATCH drops progress instead of batching it** (`server.py:2608-2615`). *new (no ledger id)*
3. **AD-47: the event log cannot be enabled** (`models/worker_config.py:110`, `from_env` `:147-185`). Also, `WorkerAction*` events are never emitted. *new (no ledger id)*
4. **AD-26: the H4 trigger loop dies on its first exception** (`extension_trigger.py:310-317`). Also, `step_transitions` is always 0 (`server.py:1521`). *new (no ledger id)*
5. **AD-29: managers known only from a registration response are marked healthy and registered** (`registration.py:280-282`). *new (no ledger id)*
6. **Leak: per-job transfer locks and tokens.** `WorkerState.remove_job_transfer_lock` (`state.py:588-592`) has 0 callers. `_job_leader_transfer_locks` and `_job_fence_tokens` gain an entry for every job that ever had a leader transfer, and none is removed. *new (no ledger id)*
7. **Leak: `_pending_transfers`.** An entry expires only when a workflow it lists is later dispatched to this worker (`server.py:1698-1715`). A transfer whose workflows already finished or never arrive (`tcp_leader_transfer.py:148-164`) stays forever. *new (no ledger id)*
8. **Leak: `_workflow_fence_tokens`.** An entry is written at admission (`tcp_dispatch.py:142-146`) and left behind when allocation then fails (`:108-109`). Only the executor's `finally` removes entries (`workflow_executor.py:683`). *new (no ledger id)*
9. **AD-22/AD-24: the worker's health gate is inert.** The base `ServerRateLimiter` detector is never sampled on workers (`mercury_sync_base_server.py:276-278`). The AD-24 STRESSED budget assumes a 500 ms worker throttle; the worker actually applies the manager's fixed 100 ms (`reliability/rate_limit_derivation.py:98-105` vs `reliability/backpressure_signal.py:30`). Related to AD24-1 (closed). *new (no ledger id)*
10. **AD-25: negotiated manager capabilities are write-only.** `compatible=True` is hard-coded and the feature set is not intersected (`registration.py:357-376`). Same shape as **R-N2** (manager side).
11. **AD-21: two retry paths have no jitter.** Final-result retries (`worker_progress_reporter.py:1336-1338`) and seed recovery (`server.py:1902-1907`). *new (no ledger id)*
12. **AD-18: `WORKER_OVERLOAD_POLL_INTERVAL` is never read** (`server.py:193-202`, `backpressure.py:48`). *new (no ledger id)*
13. **AD-28: manager EWMA is fed only by registration round trips** (`server.py:2079-2094`). *new (no ledger id)*
14. **Unwired worker code (0 callers).** `WorkerStateSync.generate_snapshot` (`sync.py:45`); `WorkerHealthIntegration.get_health_embedding`, `get_health_status` and `is_healthy` (`health.py:91-129`); registry `_recovery_jitter_*` and `_recovery_semaphore` (`registry.py:61-64`); `_pending_workflows` (`state.py:87`); `get_throttle_delay_seconds` together with `WORKER_BACKPRESSURE_*_DELAY_MS` (`backpressure.py:227-243`). Wire them or delete them. Part of **R-G62** / **D-84** cleanup.

This report, with the gate, manager and client reports of the same date, closes **R-G62**.

---

## Notes

- AD-27 was excluded per scan parameters.
- The SCAN.md Phase 12 matrix names (for example "AD-23 Backpressure (Worker)", "AD-26 Healthcheck Extensions", "AD-47 Event Logging") match the AD docs only in places. This report grades against the AD docs.
- `docs/architecture/compliance/gate_compliance_2026_01_13.md` grades against the matrix names, so its AD numbers do not line up with the AD docs for most rows.
