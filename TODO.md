# Hyperscale Distributed Bug Fixes TODO

**Generated**: 2026-01-14  
**Progress**: 64/64 completed (100%)

---

## Overview

Systematic bug fixes for the Hyperscale distributed performance testing framework across three node types: **Gate**, **Manager**, and **Worker**.

### Constraints
- Do NOT modify `RemoteGraphManager`, `LocalServerPool`, or any classes in `hyperscale/core/`
- Only modify files in `hyperscale/distributed/`
- Use `asyncio.Lock`, NEVER threading locks
- Follow modular delegation architecture - changes go in coordinator/handler classes, NOT directly in server.py
- Use TaskRunner for background tasks, never raw asyncio tasks

---

## Completed Tasks (64)

- [x] **Task 1**: Fix Gate parameter mismatch (handle_exception vs active_peer_count)
- [x] **Task 2**: Fix Gate idempotency race condition - check_or_insert not atomic, TOCTOU vulnerability
- [x] **Task 3**: Fix Gate _job_submissions memory leak
- [x] **Task 4**: Fix Gate WindowedStatsCollector memory leak
- [x] **Task 5**: Fix Gate WorkflowResultPush aggregation race - _cleanup_single_job has no lock
- [x] **Task 6**: Fix Worker final results - pending result retry loop NEVER INVOKED
- [x] **Task 7**: Fix Worker core leak on dispatch failure
- [x] **Task 11**: Implement circuit breaker for gate-to-gate peer forwarding
- [x] **Task 12**: Add CircuitBreakerManager.remove_circuit calls for dead managers and peers
- [x] **Task 15**: Add retry logic for client callback pushes instead of best-effort swallow
- [x] **Task 20**: Add GateJobLeaderTransfer emission from gate to client
- [x] **Task 21**: Add ManagerJobLeaderTransfer emission from gate to client
- [x] **Task 24**: Add guard against progress updates after job completion
- [x] **Task 25**: Add windowed_stats job existence check before recording
- [x] **Task 26**: Add timeout path for missing DC workflow results
- [x] **Task 27**: Add exactly-once completion guard for duplicate final results
- [x] **Task 28**: Add TCP handler for job_leader_gate_transfer in GateServer
- [x] **Task 35**: Add GlobalJobResult aggregation path in gate
- [x] **Task 37**: Global timeout trigger gate-side cancellation/completion
- [x] **Task 39**: Add orphan job timeout -> failed path
- [x] **Task 42**: Extend state sync to include workflow results, progress callbacks
- [x] **Task 44**: Manager: Implement _cancel_workflow to send WorkflowCancelRequest
- [x] **Task 46**: Manager: Wire stats backpressure to actual stats recording
- [x] **Task 47**: Manager: Add windowed stats flush/push loop
- [x] **Task 51**: Manager: Connect StatsBuffer recording to stats handling
- [x] **Task 52**: Cross-DC correlation - wire check_correlation to gate routing
- [x] **Task 53**: Partition callbacks - wire to routing changes in health coordinator
- [x] **Task 55**: WorkflowResultPush - add fence tokens for stale rejection
- [x] **Task 56**: Manager idempotency ledger - wire to job submission dedup
- [x] **Task 57**: Gate idempotency wait_for_pending timeout -> duplicate jobs fix
- [x] **Task 58**: Manager stats backpressure - wire to windowed stats
- [x] **Task 64**: Gate process resource sampling loop - add ProcessResourceMonitor
- [x] **Task 8**: Fix Manager health state race condition
- [x] **Task 9**: Fix Manager circuit breaker auto-transition bug (verified - already correct in ErrorStats)
- [x] **Task 10**: Fix Manager dispatch counter race
- [x] **Task 19**: Add client-side fallback to query gate for leader on missed transfers
- [x] **Task 22**: Fix dead peer reaping - remove from _gate_peer_unhealthy_since (verified - already handled)
- [x] **Task 23**: Fix peer cleanup to fully purge UDP-TCP mapping (verified - already handled)
- [x] **Task 13**: Add JobFinalResult peer-forwarding for gate resilience (verified - already implemented in tcp_state_sync.py)
- [x] **Task 14**: Add immediate status replay after client reconnect/register_callback (verified - already implemented)
- [x] **Task 16**: Add job_status_push retry/peer-forward on failure (verified - already implemented in stats_coordinator.py)
- [x] **Task 17**: Invoke progress callbacks on batch updates (verified - already implemented in stats_coordinator.py)
- [x] **Task 18**: Add client poll-on-reconnect or replay mechanism (verified - already implemented with last_sequence)
- [x] **Task 36**: Implement mixed final status resolution across DCs (verified - already implemented in _resolve_global_result_status)
- [x] **Task 40**: Integrate job lease acquisition/renewal in gate submission (verified - already implemented in tcp_job.py)
- [x] **Task 43**: Manager validate cluster/environment on registration (verified - already implemented in handle_register)
- [x] **Task 45**: WorkflowProgressAck structure compatibility (verified - structure matches producer/consumer)
- [x] **Task 48**: Workflow reassignment updates dispatch state (verified - already implemented in _apply_workflow_reassignment_state)
- [x] **Task 49**: Worker state sync applies to local state (verified - already implemented in sync.py _apply_worker_state)
- [x] **Task 50**: Manager job leader transfer notification to workers (verified - already implemented in _notify_workers_job_leader_transfer)
- [x] **Task 54**: Peer state sync reconciles fence tokens (verified - already implemented in update_fence_token_if_higher)
- [x] **Task 59**: Reporter results end-to-end path (implemented reporter_result_push handler in gate)
- [x] **Task 31**: Add ordering/dedup for JobProgress beyond fence token (added check_and_record_progress to state.py, integrated in tcp_job.py)
- [x] **Task 32**: Add explicit progress percentage calculation in gate (added _calculate_progress_percentage to tcp_job.py, added progress_percentage field to GlobalJobStatus)
- [x] **Task 33**: Add recovery path for manager dies with pending stats (added export_checkpoint/import_checkpoint to StatsBuffer, wired into ManagerStateSync and ManagerStateSnapshot)
- [x] **Task 34**: Add ReporterResultPush forwarding path in gate (verified - already implemented via Task 59)
- [x] **Task 38**: Add reporter task creation and result dispatch in gate (added _dispatch_to_reporters to server.py, called from _complete_job)
- [x] **Task 41**: Add LeaseTransfer sender in gate code (added _send_lease_transfer to leadership_coordinator.py, called during transfer_leadership)
- [x] **Task 62**: Connection storm mitigation - add explicit connection caps (added is_at_capacity to ServerState, reject in connection_made)
- [x] **Task 63**: Protocol size violations - send structured error response (added to_error_response to FrameTooLargeError, send before close)
- [x] **Task 60**: Routing SLO-constraint gating - filter by SLO targets (added SLO exclusion reasons, latency/throughput filtering to CandidateFilter)
- [x] **Task 61**: Latency handling - add percentile/jitter control (added p50/p95/p99 percentiles and jitter_ms to ObservedLatencyState)
- [x] **Task 29**: Integrate DatacenterCapacityAggregator into routing/dispatch (verified - already wired into health_coordinator.build_datacenter_candidates and fed by server.py heartbeat recording)
- [x] **Task 30**: Integrate SpilloverEvaluator into routing decisions (verified - already wired into dispatch_coordinator._evaluate_spillover and called during _dispatch_job_with_fallback)

---

## High Priority Tasks (0 remaining)

All HIGH priority tasks in Wave 3 have been verified as complete.

---

## Medium Priority Tasks (0 remaining)

All MEDIUM priority tasks have been verified as complete.

---

## Verification Checklist

After implementing fixes, verify:

### High Priority
- [x] All Manager race conditions fixed with asyncio.Lock
- [x] Circuit breaker state transitions are correct
- [x] JobFinalResult forwards to leader gate
- [x] Client reconnect replays missed status
- [x] Dead peer cleanup removes all tracking data
- [x] Multi-DC status resolution works correctly
- [x] Job leases are acquired and renewed
- [x] Manager validates cluster/environment
- [x] WorkflowProgressAck structure matches consumers
- [x] Workflow reassignment updates dispatch state
- [x] Worker state sync applies correctly
- [x] Job leader transfers notify workers
- [x] Peer sync reconciles fence tokens
- [x] Reporter results flow end-to-end

### Medium Priority
- [x] DatacenterCapacityAggregator influences routing
- [x] SpilloverEvaluator triggers when needed
- [x] JobProgress is ordered and deduplicated
- [x] Progress percentage is calculated correctly
- [x] Manager stats survive failure
- [x] ReporterResultPush reaches clients
- [x] Reporter tasks are created properly
- [x] LeaseTransfer happens on gate handoff
- [x] SLO constraints gate routing
- [x] Latency percentiles are tracked
- [x] Connection limits prevent storms
- [x] Protocol size errors are helpful

---

## Notes

- All changes must pass `lsp_diagnostics` before committing
- Run integration tests after completing related task groups
- Use TaskRunner for background tasks, never raw asyncio tasks
- Follow existing code patterns in each file
- One class per file rule applies
- Memory leaks are unacceptable - always clean up
