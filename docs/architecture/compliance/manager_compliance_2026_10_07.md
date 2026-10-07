# Manager Module AD Compliance Report

**Date**: 2026-10-07
**Commit**: 700b9aac (branch `AL-rework-commands`)
**Scope**: AD-9 through AD-50 (excluding AD-27), graded against the AD documents in `docs/architecture/AD_<n>.md`. Where a document has an "As built" or Status section, that section is the bar.
**Module**: `hyperscale/distributed/nodes/manager/` (`server.py` is 14,765 lines), plus the modules it drives: `jobs/`, `ledger/`, `raft/`, `swim/`, `reliability/`, `health/`, `resources/`, `capacity/`, `idempotency/`, `slo/`, and `server/server/mercury_sync_base_server.py`.

**Method note.** The AD names follow the AD documents. The SCAN.md Phase 12 matrix uses older titles, which no longer match the ADs. Each requirement was traced to the place the manager actually calls it. Callers were counted with grep, and the receiving function's first statements were read to rule out stubs, early returns that always fire, stores nobody reads, and objects that are built but never called. A class merely existing earns no credit. Each bug listed below was re-read in the code before it went into this report. No tests were run.

ADs whose primary role is the gate are graded on the duty they give the manager (heartbeat fields, xprobe answers). They are marked N/A (role) only when the manager has no duty at all.

Path shorthand: `server.py` = `hyperscale/distributed/nodes/manager/server.py`; `mgr/` = `hyperscale/distributed/nodes/manager/`; `dist/` = `hyperscale/distributed/`.

---

## Summary

| Status | Count | ADs |
|--------|-------|-----|
| COMPLIANT | 12 | 11, 12, 15, 16, 17, 29, 31, 36, 38, 44, 46, 49 |
| PARTIAL | 24 | 9, 10, 18, 19, 20, 21, 22, 23, 24, 25, 26, 28, 30, 32, 33, 34, 35, 37, 40, 41, 42, 43, 48, 50 |
| DIVERGENT | 0 | (divergences where the code is the better design are noted per row; these ADs owe doc edits) |
| MISSING | 1 | 14 (the manager→gate input the CRDT consumes is never produced) |
| SUPERSEDED | 1 | 39 (superseded by AD-38 §3.1/§3.3 `NodeWAL`) |
| N/A (role) | 3 | 13, 45, 47 |
| **Total** | **41** | |

**Overall.** Every manager-relevant AD is wired: its components are constructed, and some caller does reach them. The gaps are in the details: half of a signal never fed, a reset nobody calls, a term that is never passed in, a state that latches. Eight of these are behavioural bugs that can hurt production (listed first under Action Items).

---

## Per-AD Findings

| AD | Name | Status | Evidence (file:line) | Notes |
|----|------|--------|----------------------|-------|
| AD-9 | Retry Requeues the Pending Workflow | PARTIAL | `requeue_workflow` `dist/jobs/workflow_dispatcher.py:1811` (requeues only when PENDING, :1868-1873; records the exclusion :1897), called from the worker-loss path `server.py:2501-2508`; `excluded_worker_ids` read on allocation `workflow_dispatcher.py:797` → `dist/jobs/worker_pool.py:1101,1118,1145`; fresh fence per dispatch `workflow_dispatcher.py:972-975` | The dispatch-failure retry (`workflow_dispatcher.py:1217-1222` → `_return_to_pending_and_resume` :1086) does not exclude the failing worker. `excluded_worker_ids` only grows, so a worker falsely declared dead that rejoins under the same id is excluded for good. The retry budget is spent (`server.py:2469`) before `return_workflow_to_pending` (`:2501`) has succeeded. |
| AD-10 | Per-Job Fencing Tokens | PARTIAL | `(term<<32)\|counter` at `dist/jobs/job_manager.py:328-360`; stamped `workflow_dispatcher.py:988-1006`; a stale result is rejected `job_manager.py:172-200,2239` | **The term is never wired in.** `WorkflowDispatcher(...)` at `server.py:1188-1201` omits `get_leader_term`, so `workflow_dispatcher.py:972` always uses term 0. The per-job counter is in memory only, so a new leader restarts it at 1. |
| AD-11 | State Sync Retries with Exponential Backoff | COMPLIANT | `RetryConfig` `mgr/sync.py:91-98` (attempts = r+1, base = timeout/(2^r−1), FULL jitter); used `sync.py:126-128` (workers) and `:241-243` (peers); Env `mgr/config.py:115-116` | Only not-ready and non-timeout `OSError` are retried. When retries run out, the failure is logged. |
| AD-12 | Manager Peer State Sync on Leadership | COMPLIANT | callback registered `server.py:953`; `_on_manager_become_leader` `server.py:2120-2125` runs the worker sync and the full peer sync | The two syncs run concurrently, not peers first. Job takeover does sequence them (`server.py:3582-3583`). The doc's method names are stale. |
| AD-13 | Gate Split-Brain Prevention | N/A (role) | — | Gate only; the manager has no duty. |
| AD-14 | CRDT-Based Cross-DC Statistics | MISSING (manager input) | The CRDT is gate-only: `dist/nodes/gate/server.py:557,6749-6768`, fed by `gate/handlers/tcp_job.py:1332` from `JobProgress` | Nothing constructs `JobProgress` (grep `JobProgress(`: 0 producers). The manager sends `JobStatusPush` totals (`server.py:7639-7651`) and windowed stats, and neither feeds the CRDT. The CRDT is also never read (only `pop`, `gate/server.py:9634`). The gate's `GCounter.increment` with cumulative totals (`gate/server.py:6765-6766`) would inflate counts if this path were ever fed. |
| AD-15 | Tiered Update Strategy | COMPLIANT | Immediate tier: `_push_job_status_to_client` `server.py:7604-7625`, final-result obligation `:14300-14335`. Periodic tier: `_stats_push_loop` `:4566-4581` (`MANAGER_BATCH_PUSH_INTERVAL` 0.25 s) → `mgr/manager_stats_coordinator.py:199-300`; per-worker windows to the origin gate `server.py:4583-4617`. On-demand tier: `job_status` `:12102` | Windows for gate-routed jobs are removed before the send (`server.py:4613`), so a failed push loses them. The `ManagerConfig` dataclass defaults (1.0 s / 1000 ms, `mgr/models/manager_config.py:99,106,123`) disagree with Env. |
| AD-16 | Datacenter Health Classification | COMPLIANT (manager duty) | Heartbeat inputs: worker/healthy counts, available/total cores, overloaded/stressed/busy counts, `storage_writable` (`server.py:6363-6379,6405`) | The gate classifies on these. The xprobe answer contradicts AD-16 (see AD-33). |
| AD-17 | Smart Dispatch with Fallback Chain | COMPLIANT (manager duty) | Health inputs in the heartbeat (`server.py:6328-6390`); worker buckets HEALTHY > BUSY > DEGRADED, UNHEALTHY excluded, each sorted by free cores (`worker_pool.py:1099-1110,1160-1163`) | `total_cores` counts dead workers (`server.py:6003-6007`). |
| AD-18 | Hybrid Overload Detection | PARTIAL | Detector from Env `server.py:595`; CPU/memory sampled `server.py:5821-5839` (loop started `:1453`); state consumed by the SWIM piggyback `:1015`, the heartbeat `:6392`, xprobe `:6497`, the rate limiter `:760-764`, LoadShedder `:607` | **No latency is ever fed.** `record_latency` has no caller under `nodes/manager/` (only the gate at `gate/server.py:6747` and the worker), so the delta and absolute tiers (`dist/reliability/hybrid_overload_detector.py:126,204`) always return HEALTHY. The manager already measures dispatch latency (`mgr/state.py:527`). |
| AD-19 | Three-Signal Health Model | PARTIAL | Worker liveness and readiness `worker_pool.py:990,993,1341-1349`; systemic hold `server.py:5259-5273` (`dist/health/systemic_failure.py:4`); own signals `server.py:1011-1012,6388-6391` | **The worker progress signal is never fed.** `WorkerPool.update_worker_progress` (`worker_pool.py:755`) has 0 callers, so the STUCK→EVICT branch is unreachable, and `get_workers_to_evict/investigate/drain` (`:787-833`) have 0 callers. Expected throughput is a fixed 1/s per worker (`manager_stats_coordinator.py:116-121`). `_progress_state` is never written (`:74`). |
| AD-20 | Cancellation Propagation | PARTIAL | `cancel_job` `server.py:10175` → `mgr/cancellation.py:783`; fence `:1007-1025`; leader redirect `:899-1005`; worker RPC `:353-384`; completion push `:238-285,1407-1413` | Worker cancels are awaited **serially** per sub-workflow at 60 s each (`cancellation.py:588-602`; `CANCELLED_WORKFLOW_TIMEOUT` `dist/env/env.py:495`). There is no worker-cancel retry (`:371`) and no retry on the completion push (`:249-259`). A cancel on a FAILED or TIMEOUT job overwrites its status and records a second terminal event (only CANCELLED/COMPLETED are terminal, `:1033-1046`; overwrite at `:1061`). |
| AD-21 | Unified Retry Framework with Jitter | PARTIAL | `RetryExecutor` with FULL jitter only for state sync (`sync.py:128,243`) | Dispatch backoff is hand-rolled with no jitter (`workflow_dispatcher.py:81-84,1245-1254`); so are the eviction-notice resend (`server.py:2896-2911`) and the completion-notice resend (`:14411-14440`). `leader_election_jitter_max_seconds` is never read (`mgr/config.py:117`). |
| AD-22 | Load Shedding with Priority Queues | PARTIAL | Shedding at transport admission `dist/server/server/mercury_sync_base_server.py:1966-1987,2075`; `LoadShedder` consulted only for `job_submission` `server.py:11156` | STRESSED applies a per-client budget instead of shedding NORMAL (AD-24 as built); the doc owes an edit. **Bug:** cancel and extension are checked at NORMAL priority in the handlers (`server.py:6294-6311` → `dist/reliability/server_rate_limiter.py:175-177`; callers `cancellation.py:838-845`, `server.py:10233-10238`). Both are CONTROL (`dist/server/protocol/message_priority.py:16,44`), so an OVERLOADED manager refuses them. |
| AD-23 | Backpressure for Stats Updates (receiving side) | PARTIAL | `StatsBuffer` from Env `server.py:798-804`; recorded per report `manager_stats_coordinator.py:187`; level, delay and batch flag sent in `WorkflowProgressAck` `server.py:9029-9046` | **REJECT latches.** `StatsBuffer.record` returns at REJECT (`dist/reliability/stats_buffer.py:83-86`) before `_maybe_promote_tiers` (`:95`, the only caller), and nothing calls `clear()`. Once 95% of the hot tier is full, every ack says REJECT for the life of the process. The worker never clears a level (`dist/nodes/worker/worker_progress_reporter.py:1168`). `stats_buffer_*_watermark` is never read (`mgr/config.py:102-104`). |
| AD-24 | Rate Limiting | PARTIAL | Per-client, per-operation sliding window from Env `server.py:760-764`; every TCP request admitted through it, refusal answered with `RateLimitResponse(retry_after)` (`mercury_sync_base_server.py:2075-2087`); idle-client cleanup `server.py:4740-4769` | Compliant at the transport. The handler-level checks break the CONTROL exemption (see AD-22). The limiter uses `_DEFAULT_CLOCK`, not the injected clock (`dist/reliability/adaptive_rate_limiter.py:123,350`). |
| AD-25 | Version Skew Handling | PARTIAL | Gate negotiation `server.py:10710-10724` → `mgr/version_skew.py:83-119`; client MAJOR check `server.py:11180-11191`; tolerant decode `dist/models/message.py:184-200` | R-N2 is still open: `gate_supports_feature` and its siblings (`version_skew.py:134-209`) have 0 callers, and no behaviour is gated on a negotiated feature. **Worker registration negotiates nothing:** the worker's MAJOR version and capabilities are never read, and both `RegistrationResponse` builders send `capabilities=""` (`server.py:8374-8381,8509-8517`). |
| AD-26 | Adaptive Healthcheck Extensions | PARTIAL | TCP `extension_request` `server.py:10188→10263`; heartbeat piggyback `:3721→3755→3783`; logarithmic grants `dist/health/extension_tracker_impl.py:179-205`; deadline loop `server.py:5234-5363`, evicting at `:8166-8185` | **Trackers never reset.** `WorkerHealthManager.on_worker_healthy` (`dist/health/worker_health_manager.py:694`) has 0 callers (its only mention is in the docstring at :73). After 5 lifetime grants a worker is MAX_EXHAUSTED for every later workflow, and a healthy worker in a long workflow is then evicted. "Deny if suspect" is not implemented. |
| AD-28 | Enhanced DNS Discovery with Peer Selection | PARTIAL | Manager selection superseded (the doc's as-built note; `ManagerDiscoveryCoordinator` deleted). cluster/env checks `server.py:8482-8506,8857-8885,10662-10690`; mTLS claims `server.py:8310` | **The role matrix is not enforced.** `_validate_certificate_role_claims` (`server.py:8337-8348`) calls only `validate_claims` (cluster and environment); `claims.role` is never compared. A worker or client certificate can register as a gate or manager peer. The gate enforces this (`gate/handlers/tcp_manager.py:387`). |
| AD-29 | Protocol-Level Peer Confirmation | COMPLIANT | `_on_peer_confirmed` registered `server.py:957`, implemented `:2040-2053`; a gate starts unconfirmed `:10541`; heartbeats confirm `:3707,3949,3983` | SWIM core: a forwarded JOIN confirms a third party it has never contacted (`dist/swim/message_handling/membership/join_handler.py:195`). |
| AD-30 | Hierarchical Failure Detection | PARTIAL | Global layer `server.py:970-982`, `suspect_node_global` `:8153`; job layer is the manager's own `JobSuspicion` via `_job_responsiveness_iteration` `server.py:4442-4463` → `mgr/manager_health_monitor.py:385-414` | **No refutation.** `refute_job_suspicion` and `confirm_job_suspicion` (`manager_health_monitor.py:347,360`) have 0 callers, and `record_job_progress` (`:280-289`) does not clear an active suspicion. A worker that resumes progress is still declared dead for the job about 2 s later: its workflows are reassigned (run twice), and escalation to global death follows (`server.py:4460-4462`). The HFD's own job layer is never fed, and its `on_job_death` arity (3) does not match `_on_worker_dead_for_job` (2), which is latent. |
| AD-31 | Gossip-Informed Callbacks | COMPLIANT | `process_piggyback_data` → `notify_node_dead(..., "gossip")` `dist/swim/health_aware_server.py:3816-3857,865-889`; `_on_node_dead` registered `server.py:955`, dispatching to worker (`:2694`), manager-peer (`:2946` → takeover `:3207`) and gate (`:3168`) failure handlers | |
| AD-32 | Hybrid Bounded Execution | PARTIAL | Per-priority in-flight trackers `mercury_sync_base_server.py:286-298`; UDP per-handler classification `:1815-1843`; client per-destination semaphores `:309-311,1185-1210` | TCP has no per-handler class: every server request is HIGH (`:1673-1694`). **Bug:** a shed TCP request's coroutine is never closed and gets no reply (`:1456-1459`); UDP closes it (`:1531`). `_pending_response_warn_threshold` is never read (`:299`). |
| AD-33 | Federated Health Monitoring | PARTIAL (manager duty) | The leader answers xprobe `server.py:6408-6452` | **The xprobe ack reports UNHEALTHY when `worker_count == 0`** (`server.py:6461-6462`) and DEGRADED below a majority of healthy workers (`:6480-6493`). AD-16 as built says zero workers is BUSY. The gate lets an ack's UNHEALTHY override TCP's BUSY (`gate/health_coordinator.py:536-547`), so a warming datacenter reads UNHEALTHY. |
| AD-34 | Adaptive Job Timeout | PARTIAL | Strategy chosen by gate presence `server.py:7735-7751`, started `:11786-11796` / recovery `:13488-13502`; check loop `:5137-5171`; fence check `dist/jobs/gate_coordinated_timeout.py:276-300`; 5-minute fallback `:126-152`; AD-26 hook `server.py:10352` | Only the SWIM cluster leader checks timeouts (`server.py:5159`), but jobs are led per job. A demoted manager's jobs, and a taken-over job (no `set_job_timeout_strategy` in `server.py:3488-3519`), have no timeout. Losing leadership sends `stop_tracking("leadership_lost")` (`server.py:2130-2148`), which GateCoordinated reports as FAILED (`gate_coordinated_timeout.py:332-340`). `resume_tracking` never clears `locally_timed_out`. `_pending_reports` is never resent (`:41,490,501`). Progress reports go every 30 s, not 10 s. |
| AD-35 | Vivaldi Coordinates with Role-Aware Detection | PARTIAL | Coordinate updated on probe acks `health_aware_server.py:2152-2201`, piggybacked `:2348-2354`; manager role `server.py:416`, peers recorded as MANAGER `:1657` | A gate learned at registration is added with no role (`server.py:10541`) and defaults to WORKER (`health_aware_server.py:967-974`): 180 s passive instead of 120 s proactive. RTT samples are inflated: the probe start is read with `.get` and not consumed (`:2193-2201`). |
| AD-36 | Vivaldi-Based Cross-DC Routing | COMPLIANT (manager duty) | Heartbeat carries `term`, `is_leader`, cores and AD-43 fields `server.py:6354-6372` | The whole-pool view comes from workers registering with every seed manager plus state sync (`mgr/sync.py:203-275`), not from AD-48. |
| AD-37 | Explicit Backpressure Policy | PARTIAL | Level from StatsBuffer, sent per ack `server.py:9029-9043`; message classes feed AD-32 UDP priority; `LoadShedder` sampled `server.py:5828` | Inherits the AD-23 REJECT latch. `LoadShedder` is consulted for `job_submission` only (`server.py:11156`), and that branch is unreachable because the transport has already refused DISPATCH at OVERLOADED. Two `BackpressureLevel` enums (`mgr/backpressure_level.py`, `dist/reliability/backpressure_level.py`). Error acks hard-code level 0 (`server.py:8958`). |
| AD-38 | Global Job Ledger with Per-Node WAL | COMPLIANT | `JobLedger.open` + REGIONAL replicator `server.py:694-705,1164-1175`; events: create/accept `:11849-11870`, progress `:9795-9802`, cancel `cancellation.py:1230-1264`, timeout `server.py:7928-7940`, fail `:12037,13737,14144`, complete `:14161`, takeover `:3562-3565`; checkpoint `:4170-4187`; startup recovery `:1294,13520-13580` | A REGIONAL shortfall is logged and the job proceeds, by design. `self._node_wal = self._job_ledger._wal` reaches into a private attribute (`server.py:1175`). |
| AD-39 | Logger Extension for WAL Compliance | SUPERSEDED | The logger has `DurabilityMode`, binary CRC format and LSN (`hyperscale/logging/streams/logger_stream.py:1018-1271`) | None of the manager's durable stores uses it: the ledger has `NodeWAL`/`WALWriter`, the idempotency ledger frames its own records (`dist/idempotency/manager_ledger.py:237`), and Raft has its own store. AD-38 Part 3.3 chose a dedicated `NodeWAL`. The logger's WAL mode is unused in production. |
| AD-40 | Idempotent Job Submissions | PARTIAL | Ledger built and started `server.py:1177-1186`, closed `:1369-1370`; duplicate returns the original ack `:11194-11318`; reserve `:11586-11604`; commit before ack `:11908-11939`; reject `:12061-12096`; TTL, eviction and compaction `manager_ledger.py:396-469` | The ledger is per manager and not shared within the datacenter. A PENDING key lapses after its TTL (`manager_ledger.py:181-185`), so after a crash or failed commit (`server.py:11921-11926`) a same-key retry with a new job id runs the job twice. Eviction at `max_entries` can drop an in-flight PENDING entry, whose commit is then a no-op (`:147-150,158-160`). A PENDING duplicate is told to retry immediately, not made to wait (`IDEMPOTENCY_WAIT_FOR_PENDING` is unused). `IdempotencyReservedEvent`/`IdempotencyCommittedEvent` are dead. |
| AD-41 | Resource Guards | PARTIAL | Enforcer `server.py:708-720`, judged per progress `:8936-8939→9120-9157`; budget `:11784`; throttle RPC `:9177-9256`; kill `:9258-9288`; evict `:9290-9315`; resource gossip `:5746-5783,8764-8798` | **Leak:** `ResourceEnforcer.release_job` (`dist/resources/resource_enforcer.py:94-95`) pops only `_job_budgets`. `_violations` and `_throttled_resources` (keyed by workflow) are cleared only by a terminal progress report on the enforcement path (`server.py:9144-9145`). Job cleanup calls only `release_job` (`server.py:14485-14486`). |
| AD-42 | SLO-Aware Health and Routing | PARTIAL | Latency sampled `mgr/dispatch.py:100-113,150-161` into a T-Digest (`mgr/state.py:239-241,527-532`); seven `slo_*` fields on every heartbeat `server.py:6339,6396-6403`; read by the gate `gate/state.py:426-457` | `slo_updated_at` is a host-local monotonic `window_end` (`dist/slo/slo_summary.py:83`). The gate compares it across managers to pick the "freshest" (`gate/state.py:431-445`), which actually picks the longest-up host. Each manager samples only its own dispatches, so taking one manager's summary discards the rest. D-62 placement reads the same p95. |
| AD-43 | Capacity-Aware Spillover and Core Reservation | PARTIAL | `ManagerCapacityReporter` `server.py:871-877` fills the heartbeat `:6340-6345,6367-6371`; only led jobs counted `mgr/capacity_reporter.py:91-97` | **Overrun bound is wrong:** past its duration a dispatch is scheduled to free at `dispatched_at + timeout_seconds` (`dist/capacity/execution_time_estimator.py:46-49`). The engine runs it until duration + timeout (`hyperscale/core/jobs/graphs/remote_graph_manager.py:1083-1086`), so every overrunning workflow is reported as freeing now. Pending entries are not filtered by leadership (`capacity_reporter.py:62-70`). Per-class reserved cores (D-63) are not reflected in `available_cores`. |
| AD-44 | Retry Budgets and Best-Effort Completion | COMPLIANT | `RetryBudgetManager` `server.py:539-544`, shared `:1198`; per-job budget clamped `workflow_dispatcher.py:210-214`; checked before backoff `:1208-1244`; worker loss charged `server.py:2482-2490`; systemic hold exempts `:2232,2769,4332,11038`; cleanup `workflow_dispatcher.py:1756` | Best-effort is gate-only, as the doc says. The spent budget is neither durable nor replicated, so a new leader starts with a full budget. Dispatch failures during a systemic hold are still charged (`workflow_dispatcher.py:1217`). |
| AD-45 | Adaptive Route Learning | N/A (role) | — | The gate measures dispatch latency; the manager has no extra duty. |
| AD-46 | SWIM Node State via IncarnationTracker | COMPLIANT | All SWIM status reads go through `_incarnation_tracker.get_node_state` (`server.py:1570,2951,4531,5972,7279,8583,10594`); bounded and cleaned (`dist/swim/detection/incarnation_tracker.py:404-418`, `health_aware_server.py:2432`) | `safe_queue_put` is a no-op stub with no callers (`dist/swim/message_handling/server_adapter.py:370-376`). |
| AD-47 | Worker Event Log | N/A (role) | — | Worker only; the manager appears only as a field in worker events. |
| AD-48 | Cross-Manager Worker Visibility | PARTIAL | TCP broadcast on register `server.py:8672-8675`, on eviction `:8186-8187,9313-9314`, on drain `:8260-8261`; receive `:10845-10883` → `mgr/worker_dissemination.py:313-366`; UDP gossip `server.py:6580-6600` | **The join bootstrap never decodes:** `list_workers` replies `response.dump()` (`server.py:10898-10899`), but the reader uses the `|`-split `WorkerListResponse.from_bytes` (`worker_dissemination.py:434`). There is no "dead" broadcast on SWIM death and no "left" broadcast. The remote pool (`worker_pool.py:1536-1600`) has no reader, and `cleanup_remote_workers_for_manager` (`:1636`) has 0 callers. `_worker_incarnations` is never pruned. Real visibility comes from multi-manager registration plus state sync; the AD owes an "As built" section. |
| AD-49 | Workflow Context Propagation | COMPLIANT | Applied on the leader `server.py:10130,10154-10158` → `job_manager.py:3325-3346`; to dependents `workflow_dispatcher.py:913-993`; stored for requeue `:1015-1019`, `job_manager.py:3359-3416`; replicated `server.py:5421-5422,5515,5607-5608`; recovered `:12589,12703-12719` | Every dispatch gets the whole namespaced context, not a per-dependency subset. That is the better design; the doc should say so. Minor race: `layer_version` is checked outside `job.lock` and then assigned without taking the max (`server.py:12709-12719`). |
| AD-50 | Manager Health Aggregation and Alerting | PARTIAL | Aggregation and alert rules `mgr/manager_health_monitor.py:463-588`, fired from the peer heartbeat `server.py:3896,3958-3962`, fed real CPU/memory state `:5821-5840` | Alerts fire only on a transition from a known earlier state (`server.py:3957`). `_dc_leader_manager_id` is never cleared (`server.py:3936-3937`; `mgr/state.py:1227`), so a stale ex-leader causes false "DC leader overloaded" alerts that suppress the ratio alerts. All alerts are ServerWarning; recovery is logged at Debug (`server.py:5893`). |

---

## Behavioral Verification

### AD-10: Per-Job Fencing Tokens
- ✓ Token format `(term << 32) | counter`, under a lock (`dist/jobs/job_manager.py:328-360`).
- ✓ Results whose fence does not match are rejected as `stale_fence_token` (`job_manager.py:172-200`).
- ✗ The term is always 0: `WorkflowDispatcher` takes `get_leader_term` (`workflow_dispatcher.py:103,143,972`), but `server.py:1188-1201` does not pass it. The manager already has the term (`self._leader_election.state.current_term`, used at `server.py:6357,7066`).

### AD-23 / AD-37: Stats Backpressure
- ✓ The level is sent in every `WorkflowProgressAck` (`server.py:9029-9046`), and the worker reads it.
- ✗ Once the buffer reaches REJECT it stays there: `record()` returns before `_maybe_promote_tiers()` (`stats_buffer.py:83-95`). The fill ratio is `len(self._hot)` (`:122`), so it never falls, and no manager code calls `clear()`. Hot capacity defaults to 1000 (`MANAGER_STATS_HOT_MAX_ENTRIES`), so more than about 16 reports/s sustained for the 60 s promotion window trips it.

### AD-26 / AD-30: Extension and Job-Layer Detection
- ✓ Grants shrink logarithmically and require progress after the first; deadline enforcement runs with the systemic hold.
- ✗ Extension trackers are keyed per worker for its whole lifetime, and nothing calls the reset (`worker_health_manager.py:694`).
- ✗ A job suspicion, once raised, always expires into job-death and reassignment. `check_job_suspicion_expiry` (`manager_health_monitor.py:385-414`) never consults progress, and the refute path has no caller.

### AD-34: Adaptive Job Timeout
- ✓ Choosing between LocalAuthority and GateCoordinated, the check loop, fence validation, the 5-minute fallback and the AD-26 extension hook are all live.
- ✗ Leadership boundaries are wrong in three ways:
  - Checks run only on the SWIM cluster leader (`server.py:5159`), while jobs are led per job.
  - A takeover never installs a strategy (`server.py:3488-3519`).
  - Losing cluster leadership reports `leadership_lost` to the gate as FAILED (`server.py:2130-2148`; `gate_coordinated_timeout.py:332-340`).

### AD-38: Ledger
- ✓ Every job event type is emitted on the manager. REGIONAL commits are bounded, and checkpoints run on the reap tick.
- ✓ On startup the WAL replays, and ACTIVE jobs are settled by asking peers before they are resumed or failed.

### AD-48: Worker Visibility
- ✗ `WorkerListResponse.from_bytes(dump())` returns None: the encoder pickles, while the decoder splits on `|` (`server.py:10898-10899` vs `worker_dissemination.py:434`). The manager-registration push that follows (`server.py:1248`) therefore always runs over an empty remote pool.

---

## SCENARIOS.md Coverage

Keyword grep over SCENARIOS.md (1,714 lines). These are rough counts, not scenario-by-scenario mapping:

| AD | Keyword hits |
|----|--------------|
| AD-34 (timeout) | 65 |
| AD-37 (backpressure) | 45 |
| AD-40 (idempotency) | 23 |
| AD-26 (extension) | 15 |
| AD-38 (ledger/WAL) | 14 |
| AD-30 (suspicion) | 8 |
| AD-48 (dissemination) | 1 |

None of the bugs below has a scenario: leadership flap on timeouts, latched REJECT, unrefuted job suspicion, tracker exhaustion, and the join bootstrap decode.

---

## Action Items

Only real gaps are listed. Ledger ids refer to `docs/REMAINING_LEDGER.md`. Items 1-8 are production bugs.

### Production bugs
1. **AD-23/37: the StatsBuffer REJECT state latches for good** (`dist/reliability/stats_buffer.py:83-95`), and with it every progress ack. The worker drops progress at REJECT. Fix: promote and age entries before the level check. New (no ledger id).
2. **AD-30: a job suspicion is never refuted by progress** (`mgr/manager_health_monitor.py:280-289,385-414`; refute/confirm have 0 callers). A recovered worker's workflows are reassigned and run twice. New (no ledger id).
3. **AD-34: timeout coverage breaks at leadership boundaries:**
   - checks are gated on cluster leadership, not job leadership (`server.py:5159`);
   - a takeover installs no strategy (`server.py:3488-3519`);
   - `leadership_lost` is reported to the gate as FAILED (`server.py:2130-2148`, `dist/jobs/gate_coordinated_timeout.py:332-340`);
   - `resume_tracking` does not clear `locally_timed_out`.
   
   New (no ledger id).
4. **AD-26: extension trackers never reset** (`dist/health/worker_health_manager.py:694`, 0 callers). After 5 lifetime grants a healthy worker is evicted in a long workflow. New (no ledger id).
5. **AD-48: the join worker-list bootstrap cannot decode** (`server.py:10898-10899` vs `mgr/worker_dissemination.py:434`). New (no ledger id).
6. **AD-33/AD-16: the xprobe ack reports a zero-worker datacenter as UNHEALTHY** (`server.py:6461-6462`), and the gate's merge lets this beat TCP's BUSY (`gate/health_coordinator.py:536-547`). New (no ledger id). AD16 in the ledger closed only the TCP classifier.
7. **AD-22/24: cancel and extension are rate-checked as NORMAL** in the handlers (`server.py:6310` → `dist/reliability/server_rate_limiter.py:175-177`), so an OVERLOADED manager refuses CONTROL traffic. New (no ledger id). AD24-1 is closed and did not cover this.
8. **AD-20: a cancel on a FAILED or TIMEOUT job overwrites its terminal status** and records a second terminal event (`mgr/cancellation.py:1033-1046,1061`). Worker cancels are awaited serially at 60 s each (`:588-602`). New (no ledger id).

### Correctness and wiring gaps
9. **AD-10:** pass `get_leader_term` to `WorkflowDispatcher` (`server.py:1188-1201`). New (no ledger id).
10. **AD-41: `ResourceEnforcer` per-workflow state leaks** past job end (`dist/resources/resource_enforcer.py:94-95`; `server.py:14485-14486`). New (no ledger id). P-AD41-1 graded the enforcer Built and did not cover cleanup.
11. **AD-43: the release schedule uses `dispatched_at + timeout`** where it should use `+ duration + timeout` (`dist/capacity/execution_time_estimator.py:46-49`). Pending entries are not filtered by leadership. New (no ledger id). A3-G-43 is Built and did not cover this.
12. **AD-42: `slo_updated_at` is a host-local monotonic value** compared across hosts at the gate (`dist/slo/slo_summary.py:83`; `gate/state.py:431-445`). There is no cross-manager SLO merge. New (no ledger id). Related: A3-G-36 (Doc-obsolete rationale "every manager heartbeats every gate").
13. **AD-28: the manager does not enforce the role matrix** on mTLS claims (`server.py:8337-8348`). New (no ledger id). Related: delta #8a / R-G52 (strict parse, Built).
14. **AD-25:** no behaviour is gated on negotiated gate capabilities (R-N2). Worker registration does no version negotiation and sends empty capabilities (`server.py:8374-8381,8509-8517`); this half is new (no ledger id).
15. **AD-19: the worker progress signal is never fed** (`dist/jobs/worker_pool.py:755`, 0 callers), so STUCK→EVICT is dead. New (no ledger id).
16. **AD-18: the manager's overload detector gets no latency samples** (no `record_latency` under `nodes/manager/`). New (no ledger id).
17. **AD-14: the CRDT is dead end to end.** No `JobProgress` producer; the CRDT has no reader; cumulative totals are passed to `GCounter.increment` (`gate/server.py:6765-6766`). Either feed it or delete it. New (no ledger id). Related: A2-G-258.
18. **AD-48:**
    - no "dead"/"left" broadcast;
    - the remote pool has no reader;
    - `cleanup_remote_workers_for_manager` has 0 callers;
    - `_worker_incarnations` grows without bound (`mgr/worker_dissemination.py:76`);
    - incarnations are not persisted.
    
    Either wire these or retire the pool and write the AD's as-built section. New (no ledger id).
19. **AD-9:**
    - the dispatch-failure retry does not exclude the failing worker;
    - `excluded_worker_ids` is never cleared;
    - the budget is charged before the requeue succeeds (`server.py:2469` vs `:2501`).
    
    New (no ledger id).
20. **AD-40:**
    - a PENDING key lapses after its TTL, allowing a duplicate run;
    - eviction can drop an in-flight reservation (`dist/idempotency/manager_ledger.py:147-150,181-185`);
    - the ledger is not shared across the datacenter's managers.
    
    New (no ledger id). The dead `IdempotencyReservedEvent`/`IdempotencyCommittedEvent` are already listed, without an id, under REMAINING_LEDGER "Ledger: architecture.md §3 → New gaps found".
21. **AD-32: a shed TCP request leaks an unclosed coroutine and gets no reply** (`dist/server/server/mercury_sync_base_server.py:1456-1459`). TCP has no per-handler priority. New (no ledger id). AD32-2 (Built) covered only the client side.
22. **AD-35: a gate registered via TCP is tracked with the WORKER confirmation strategy** (`server.py:10541`). RTT samples are inflated (`dist/swim/health_aware_server.py:2193-2201`). New (no ledger id).
23. **AD-50:**
    - `_dc_leader_manager_id` is never cleared, causing false leader-overload alerts;
    - no alert on a peer's first-seen overloaded state;
    - severities differ from the doc.
    
    New (no ledger id).
24. **AD-34:** `_pending_reports` is never resent (`gate_coordinated_timeout.py:41,490,501`). Progress reports go every 30 s against the designed 10 s. Strategies use `_DEFAULT_CLOCK` instead of the injected clock. New (no ledger id).
25. **AD-21:** dispatch, eviction-notice and completion-notice retries have no jitter. `leader_election_jitter_max_seconds` is never read (`mgr/config.py:117`). New (no ledger id).
26. **Dead or never-read manager config:**
    - `stats_buffer_*_watermark` (`mgr/config.py:102-104`);
    - `ManagerStatsCoordinator._progress_state` (`mgr/manager_stats_coordinator.py:74`);
    - the dead `hasattr(heartbeat, "deadline")` branch (`mgr/manager_health_monitor.py:73-75`);
    - the unreachable `worker_discovery` handler, whose `_register_with_discovered_worker` would raise `TypeError` (`server.py:10762-10808,13284-13307`).
    
    New (no ledger id).

### Doc edits owed (the code is the better design)
27. **Doc edits owed:**
    - AD-39: mark it superseded by AD-38 §3.3.
    - AD-22: point to AD-24's STRESSED budget and AD-37's classes.
    - AD-49: whole-context dispatch.
    - AD-48: "As built" visibility via multi-registration plus state sync.
    - AD-40: the PENDING duplicate is told to retry, not made to wait.
    - AD-12: the stale method names.
    
    Fold into the Phase 9 doc sweep (ledger AD-1–36 note "every Doc-obsolete row … still owes its Phase 9 doc edit").

### Structural
28. `server.py` is 14,765 lines. Ledger R-G59 / D-84 cover this (still open).
29. This report, with the gate, worker and client reports of the same date, closes ledger R-G62.

---

## Notes
- AD-27 is excluded per scan parameters.
- Line numbers are as of commit 700b9aac and the working copy at `/private/tmp/claude-501/p54_integration`.
