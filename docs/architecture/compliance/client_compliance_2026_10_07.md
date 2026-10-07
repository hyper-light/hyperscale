# Client Module AD Compliance Report

**Date**: 2026-10-07
**Commit**: 700b9aac
**Scope**: AD-9 through AD-50 (excluding AD-27), graded for the CLIENT role
**Module**: `hyperscale/distributed/nodes/client/` (`HyperscaleClient` and its submodules and handlers, plus the shared modules it calls: `server/server/mercury_sync_base_server.py`, `reliability/`, `discovery/`, `idempotency/`, `protocol/`)

**Method.** Each AD's requirements were taken from its `docs/architecture/AD_<n>.md` text (the doc names, not the older SCAN.md Phase 12 matrix names). For each requirement the client's call site was found and the receiving code's first statements read, so that a class that exists but is never called, a store with no reader, or a handler that returns early is not credited. Caller counts are from `grep` over `hyperscale/`. Line numbers are relative to the repository root as of commit 700b9aac. No tests were run. "N/A (role)" means the AD is owned by another role; the client's part, if any, is noted in one line.

---

## Summary

| Status | Count |
|--------|-------|
| COMPLIANT | 11 |
| PARTIAL | 4 |
| DIVERGENT | 0 |
| MISSING | 0 |
| SUPERSEDED | 0 |
| N/A (other role) | 26 |
| **Total** | **41** |

**Overall.** What the client is responsible for works:
- Idempotent submission with one key per logical submission.
- Waiting out every hinted refusal (D-70), and transient INITIALIZING refusals are retried.
- AD-28 target ranking.
- Leveled status reads.
- Cancellation within a time budget, and ignoring cancellation reports it did not ask for (AD-36).
- AD-44 provisional and late results.

The gaps are smaller:
- Negotiated capabilities have no reader.
- Manager job-leader transfers are recorded but never used for routing.
- Cancellation requests are never fenced.
- AD-24 describes cooperative client-side pacing that the client does not do.

There is also one finding outside the ADs: the windowed-stats handler unpickles network bytes with `cloudpickle.loads` instead of the restricted `Message.load`.

---

## Detailed Findings

| AD | Name | Status | Evidence (file:line) | Notes |
|----|------|--------|----------------------|-------|
| AD-9 | Retry Requeues the Pending Workflow | N/A (manager) | — | |
| AD-10 | Per-Job Fencing Tokens | PARTIAL | Gate transfer `handlers/gate_leader_transfer_handler.py:44-73` → `leadership.py:37-59`; manager transfer `handlers/manager_leader_transfer_handler.py:54-75` → `leadership.py:61-87` | Both transfer handlers reject a fence token that is not strictly greater than the one held, under a per-job routing lock. A gate transfer also re-points the job's target (`gate_leader_transfer_handler.py:73`), and status polls and replay follow it (`targets.py:204-221` ← `client.py:916`, `:963`). A manager transfer is only recorded. `get_preferred_manager_for_job` (`targets.py:223`) has 0 callers, and the handler never calls `mark_job_target`. So in gateless mode, cancels go to the accepting manager first and rely on its redirect (`cancellation.py:314`, `targets.py:158-176`), and status polls go round-robin (`client.py:914-921`). |
| AD-11 | State Sync Retries | N/A (manager) | — | |
| AD-12 | Manager Peer State Sync | N/A (manager) | — | |
| AD-13 | Gate Split-Brain Prevention | N/A (gate) | — | |
| AD-14 | CRDT Cross-DC Statistics | N/A (gate) | — | |
| AD-15 | Tiered Update Strategy | COMPLIANT | Immediate: `client.py:1004-1012` → `handlers/job_status_push_handler.py`; periodic: `client.py:1014-1022` → `handlers/job_batch_push_handler.py`, `client.py:1064-1072` → `handlers/tcp_windowed_stats.py`; on demand: `client.py:846-882`, poll fallback `:885-921` used by `tracking.py:295` | All three tiers have a working receiver. |
| AD-16 | Datacenter Health Classification | COMPLIANT | `protocol/transient_errors.py:64-66` (`"initializing"`) via `submission.py:705-711` | Refusals while a datacenter warms up are retried, as the AD requires ("clients retry"). |
| AD-17 | Smart Dispatch with Fallback | N/A (gate/manager) | — | |
| AD-18 | Hybrid Overload Detection | N/A (servers) | — | The client builds an unfed `HybridOverloadDetector()` for its inbound stats limiter (`client.py:197-200`); see AD-24. |
| AD-19 | Three-Signal Health Model | N/A (servers) | — | |
| AD-20 | Cancellation Propagation | PARTIAL | `client.py:727-746` → `cancellation.py:243-310` (time-budget sweep), `:438-600` (redirects, rate limit, classification); completion `client.py:1074-1089` → `handlers/tcp_cancellation_complete.py:52-58`; wait `cancellation.py:602-668` | The request goes out, retries are idempotent and follow redirects, and `await_job_cancellation` waits for the confirmation push. However, `JobCancelRequest.fence_token` is always 0 (`cancellation.py:256`), and servers treat 0 as unfenced (`nodes/gate/handlers/tcp_cancellation.py:283-288`). The AD's message spec says the token "must match current job epoch". The client does hold the job's fence token from its status reads (`client.py:865`, `:882`) and from leader transfers, but never sends it. |
| AD-21 | Unified Retry Framework with Jitter | COMPLIANT | Submit `submission.py:446-487` (hinted wait × (1 + U), unhinted RFC 6298 base × 2^n × (0.5 + U)); cancel `cancellation.py:420-436` | Every retry is jittered and backs off exponentially. Note: neither path uses `RetryExecutor`. Each is written separately for a documented reason (the RFC 6298 base, and the time budget). |
| AD-22 | Load Shedding | N/A (servers) | — | The client sheds nothing. Inbound pushes pass through the inherited transport admission. |
| AD-23 | Backpressure for Stats | N/A (manager/worker) | — | |
| AD-24 | Rate Limiting (Client and Server) | PARTIAL | Submit honors `RateLimitResponse` and `JobAck.retry_after_seconds` (`submission.py:570-573`, `:584`, `:480-482`); cancel `cancellation.py:527-532`; inbound stats limiter `handlers/tcp_windowed_stats.py:41-50` | Hinted refusals are waited out on submit (D-70) and on cancel. AD_24.md still describes a cooperative client that does a "pre-flight check before sending" and "delays requests when approaching limit". That component, `CooperativeRateLimiter`, was deleted (AD24-1), and the client does no pre-flight pacing. The inbound `stats_update` limiter's detector is never sampled (`client.py:197-200`), so its health gate always reads HEALTHY. Status reads (`client.py:846-882`, `:923-937`) do not recognize a `RateLimitResponse` and try to parse it as `GlobalJobStatus`. |
| AD-25 | Version Skew Handling | PARTIAL | Sent `submission.py:369-372`; stored `submission.py:644-650` → `protocol.py:57-107` | Version and capabilities go on every submission, and the common set is computed as an intersection (`protocol.py:95`). The result is `compatible=True` hard-coded (`:102`), with no MAJOR check. Nothing reads it: `has_feature` and `validate_server_compatibility` (`protocol.py:125`, `:145`) have 0 callers outside `protocol.py`, so no client behavior depends on a negotiated feature. |
| AD-26 | Healthcheck Extensions | N/A (worker/manager) | — | |
| AD-28 | Enhanced DNS Discovery / Peer Selection | COMPLIANT | `client.py:204-216` (`DiscoveryService`); `targets.py:26-41` (configured peers), `:99-113` (per-job rendezvous order, gates before managers), EWMA feed `:115-128` ← `submission.py:564`, `:636-639` | Submissions are spread across targets per job and steer away from slow or failing ones. Redirect targets outside the configured set are deliberately left untracked, so the selector stays bounded. |
| AD-29 | Peer Confirmation | N/A (SWIM roles) | — | The client takes no part in SWIM. |
| AD-30 | Hierarchical Failure Detection | N/A (manager) | — | |
| AD-31 | Gossip-Informed Callbacks | N/A (SWIM roles) | — | |
| AD-32 | Hybrid Bounded Execution | COMPLIANT | Inherited per-destination send bound `mercury_sync_base_server.py:300-308` (`send_tcp`), in-flight trackers `:286-298` | The client's outbound requests get the per-destination semaphore through inheritance. |
| AD-33 | Federated Health Monitoring | N/A (gate) | — | |
| AD-34 | Adaptive Job Timeout | COMPLIANT | `submission.py:236-250`, `:359-365` | `timeout_seconds=None` sends 0 with `timeout_seconds_explicit=False`, which lets the manager apply the H2 hierarchy. A positive value is sent as explicit. |
| AD-35 | Vivaldi Coordinates | N/A (SWIM roles) | — | |
| AD-36 | Vivaldi Cross-DC Routing | COMPLIANT | `handlers/global_job_result_handler.py:132-166` (`rerun_of` carried into `ClientWorkflowDCResult`); `handlers/tcp_cancellation_complete.py:46-53` | Re-run results are marked as such. A cancellation report the client did not ask for (a datacenter the gate moved the job off) is acknowledged and not stored. |
| AD-37 | Explicit Backpressure Policy | N/A (gate/manager/worker) | — | |
| AD-38 | Global Job Ledger | COMPLIANT | `client.py:846-882` (`JobStatusQuery` carries `ReadConsistency`, `observed_fence_token`, `observed_view_time`; the view is recorded at `:882`) | The client implements its half of the Part 8 session guarantees (SESSION is never older than the last view, BOUNDED_STALENESS takes a bound). Views are released with the job (`state.py:155`). |
| AD-39 | Logger WAL Extension | N/A (logger) | — | |
| AD-40 | Idempotent Job Submissions | COMPLIANT | Key built once per `submit_job` (`submission.py:377`) and reused across every retry and redirect (`:415-434`); generator `idempotency/idempotency_key_generator.py` (client id, monotonic sequence, random nonce per process), wired at `client.py:236-238` | Follows the `{client_id}:{sequence}:{nonce}` model. Note: `client_id` is `host:port`, which only survives a restart if the client rebinds the same address. The per-process nonce keeps keys from colliding across restarts, as the AD requires. |
| AD-41 | Resource Guards | COMPLIANT | `submission.py:200`, `:381` (`resource_budget` on the wire); `discovery.py:86`, `:119` (ping responses carry `DatacenterResourceView`) | The client passes the job's budget through and can read the datacenter view (AD_41.md "Clients read it from ManagerPingResponse.resources"). |
| AD-42 | SLO-Aware Health and Routing | N/A (manager/gate) | — | |
| AD-43 | Capacity-Aware Spillover | N/A (gate/manager) | — | |
| AD-44 | Retry Budgets and Best-Effort | COMPLIANT | Submission fields `submission.py:198-204`, `:379-385`; provisional or late results `handlers/global_job_result_handler.py:258-307` (`_is_stale_provisional` `:287`) | A provisional push never overwrites a final or fuller result. Pushes may arrive out of order. |
| AD-45 | Adaptive Route Learning | N/A (gate) | — | |
| AD-46 | SWIM Node State | N/A (SWIM roles) | — | |
| AD-47 | Worker Event Log | N/A (worker) | — | |
| AD-48 | Cross-Manager Worker Visibility | N/A (manager) | — | |
| AD-49 | Workflow Context Propagation | N/A (manager/worker) | — | |
| AD-50 | Manager Health Aggregation | N/A (manager) | — | |

---

## Behavioral Verification

### AD-40: Idempotent submission
- ✓ `submit_job` → `_build_job_submission` creates one `IdempotencyKey` (`submission.py:377`). `_submit_with_retry` resends the same `JobSubmission` to each target and each redirect (`:415-434`, `:556-561`).
- ✓ The job id comes from a deterministic logical generator (`submission.py:172`). Protection against replay comes from the key.

### AD-24 / D-70: Hinted refusals
- ✓ A hinted refusal waits `hint × (1 + U[0,1))`, including the last refused attempt (`submission.py:480-482`).
- ✓ `RateLimitResponse` is told apart from `JobAck` with an `isinstance` check (`submission.py:604-610`).
- ✗ The cancel path's `_check_rate_limit` (`cancellation.py:594-600`) has no such check. It is safe today only because `JobCancelResponse` has no `retry_after_seconds` field, so the attribute read raises and is caught. Adding that field would make every cancel response read as "Rate limited".

### AD-20: Cancellation
- ✓ The sweep is bounded by a time budget, and each send is capped (`cancellation.py:33`, `:290-291`). Redirect cycles are detected (`:564-583`). Targets proven unreachable are passed to the next target (`:502`, `:512`).
- ✓ Completion events are recorded only for cancellations this client asked for (`handlers/tcp_cancellation_complete.py:52`).
- ✗ `fence_token=0` on every request (`cancellation.py:256`).

### AD-10: Leader transfers
- ✓ Fence tokens are validated strictly increasing per job (gate) and per job and datacenter (manager) (`leadership.py:37-87`).
- ✗ Manager leader records have no reader (`targets.py:223`, 0 callers).

---

## SCENARIOS.md Coverage

| AD | Matching lines in `docs/SCENARIOS.md` |
|----|----------------------------------------|
| AD-40 (idempotency) | 5 |
| AD-20 (cancel) | 9 |
| AD-28 (discovery) | 2 |
| AD-25 (capabilities) | 1 |
| AD-24 (rate limit / retry-after) | 0 |

---

## Action Items

1. **Security: unrestricted unpickling of wire data.** `handlers/tcp_windowed_stats.py:55` calls `cloudpickle.loads(data)` on inbound bytes. Every other client handler uses the restricted `Message.load` (`models/message.py`, "prevents arbitrary code execution"). Today only the cluster's transport encryption stands in front of it. *new (no ledger id)*
2. **AD-25: negotiated capabilities are write-only, and `compatible=True` is hard-coded** (`protocol.py:95-107`; `has_feature` and `validate_server_compatibility` have 0 callers). Either gate behaviour on them or delete the query surface. Same shape as **R-N2** (manager side); *new for the client*.
3. **AD-10: manager job-leader transfers do not change routing** (`handlers/manager_leader_transfer_handler.py:75`; `targets.py:223` has 0 callers). Use the recorded leader for cancel and status targets in gateless mode, or stop tracking it. *new (no ledger id)*
4. **AD-20: cancel requests are never fenced** (`cancellation.py:256`). Either send the fence token the client observed, or amend AD_20.md: job ids are unique logical ids, so an unfenced cancel cannot hit a different job. *new (no ledger id)*
5. **AD-24: doc and code disagree on client-side cooperative limiting.** AD_24.md's diagram still lists client pre-flight checks and pacing near the limit, but the component was deleted (follow-on to **AD24-1**, closed). Also, the inbound `stats_update` limiter's detector is never sampled (`client.py:197-200`), and status reads do not recognize `RateLimitResponse` (`client.py:880-882`, `:933-937`). *new (no ledger id)*
6. **Latent: `_check_rate_limit` lacks the `isinstance` check** that submission documents (`cancellation.py:594-600` vs `submission.py:604-610`). *new (no ledger id)*

This report, with the gate, manager and worker reports of the same date, closes **R-G62**.

---

## Notes

- AD-27 was excluded per scan parameters.
- The gateless path (client to manager, with no gate) is first-class in the code (`targets.py:99-113`, `client.py:914-921`). The connection matrix in AD-28 ("Client → Manager: No") predates that and is stale.
- No AD in AD-9..AD-50 specifies the client's leader-transfer handlers by name. `leadership.py` calls itself "AD-16 (Leadership Transfer)", but AD-16 is datacenter health classification. The handlers are graded here under AD-10, which owns the fence-token semantics they enforce.
