Scenarios to Test
1. Dead Peer Reaping
- Gate peer goes down and stays down - Verify that after dead_peer_reap_interval_seconds (120s), the peer is moved from _gate_peer_unhealthy_since to _dead_gate_peers and properly cleaned up
- Gate peer goes down then recovers before reap threshold - Verify mark_peer_healthy() clears the unhealthy timestamp and peer is not reaped
- Multiple peers fail simultaneously - Verify all are tracked independently and reaped correctly
2. Quorum Loss Stepdown
- Gradual quorum loss - Gates fail one by one until quorum is lost; verify leader steps down after 3 consecutive failures
- Quorum regained before stepdown threshold - Lose quorum momentarily but regain before 3 consecutive check intervals; verify _consecutive_quorum_failures resets to 0
- Non-leader during quorum loss - Verify non-leader gates don't attempt stepdown
- Split-brain scenario - Network partition where each partition has < quorum; both sides should step down
3. Recovery After Dead Peer Cleanup
- Dead peer rejoins after cleanup - Peer marked dead, cleaned up after 2x reap interval, then rejoins; verify it's treated as new peer
- Stale dead peer tracking cleanup - Verify cleanup_dead_peer() properly removes all associated state (locks, epochs, timestamps)
4. Integration Scenarios
- Job forwarding during peer reaping - Active job forwarding to a peer that gets reaped mid-operation
- Leadership handoff during quorum loss - Leader steps down with active jobs; verify jobs are not orphaned
- Circuit breaker interaction - Dead peers should be removed from circuit breaker tracking
5. Edge Cases
- Single gate cluster - No peers, quorum always satisfied (1/1)
- Two gate cluster - Quorum = 2, so any failure = quorum loss
- Rapid failure/recovery cycles - Peer flaps rapidly; epoch checking should prevent stale recovery handling
Do you want me to create integration tests for any of these scenarios, or should we commit the current changes first?
▣  Sisyphus · claude-opus-4-5 · 17.7s
Can we instead focus on gate <-> manager cases?
Yes, let's focus on gate <-> manager scenarios. Based on the distributed architecture, here are the key scenarios:
Gate <-> Manager Scenarios to Test
1. Manager Failure During Job Dispatch
- Manager dies mid-dispatch - Gate sends job to manager, manager dies before ACK; verify gate retries to another manager in same DC
- All managers in DC fail during dispatch - Gate should failover to next DC or return error to client
- Manager dies after ACK but before job completion - Verify gate detects via SWIM/heartbeat and handles job leadership transfer
2. Manager Registration/Discovery
- Manager registers with gate - Verify gate adds to _datacenter_manager_status, discovery service, and health tracking
- Manager re-registers after restart - Verify stale state is cleared and fresh registration is accepted
- Gate discovers new manager via peer gate broadcast - Verify ManagerDiscoveryBroadcast propagates correctly
3. Manager Health & Circuit Breakers
- Manager becomes unhealthy - Repeated failures trigger circuit breaker; verify gate stops routing to that manager
- Manager recovers from unhealthy - Circuit breaker half-open → closed; verify traffic resumes
- All managers in DC unhealthy - Verify DC-level backpressure is applied
4. Job Progress & Results Flow
- Manager sends job progress - Verify gate aggregates and forwards to client callback
- Manager sends workflow result - Verify gate waits for all DCs then aggregates
- Manager sends final result - Verify gate cleans up job state and notifies client
5. Backpressure Propagation
- Manager signals backpressure - Verify gate reduces dispatch rate to that manager/DC
- DC-wide backpressure - All managers in DC overloaded; verify gate routes to other DCs
6. Leadership Transfers
- Manager leader dies, gate coordinates takeover - Verify JobLeaderManagerTransfer is handled
- Gate receives JobLeaderGateTransfer - Verify gate accepts job leadership from dying peer
Which scenarios would you like to start with? I'd suggest starting with Manager Failure During Job Dispatch since that's the most critical path.
▣  Sisyphus · claude-opus-4-5 · 13.2s
Let's extend this to include stats reporting and results reporting.
Good call. Let me extend the scenarios to include the full job lifecycle including stats and results:
Gate <-> Manager Scenarios (Extended)
1. Manager Failure During Job Dispatch
- Manager dies mid-dispatch - Gate sends job to manager, manager dies before ACK; verify gate retries to another manager in same DC
- All managers in DC fail during dispatch - Gate should failover to next DC or return error to client
- Manager dies after ACK but before job completion - Verify gate detects via SWIM/heartbeat and handles job leadership transfer
2. Manager Registration/Discovery
- Manager registers with gate - Verify gate adds to _datacenter_manager_status, discovery service, and health tracking
- Manager re-registers after restart - Verify stale state is cleared and fresh registration is accepted
- Gate discovers new manager via peer gate broadcast - Verify ManagerDiscoveryBroadcast propagates correctly
3. Manager Health & Circuit Breakers
- Manager becomes unhealthy - Repeated failures trigger circuit breaker; verify gate stops routing to that manager
- Manager recovers from unhealthy - Circuit breaker half-open → closed; verify traffic resumes
- All managers in DC unhealthy - Verify DC-level backpressure is applied
4. Job Progress Flow
- Manager sends job progress - Verify gate aggregates and forwards to client callback
- Progress from multiple DCs - Verify gate merges progress correctly
- Manager dies mid-progress-stream - Verify gate handles partial progress gracefully
- Out-of-order progress updates - Verify gate handles versioning/ordering correctly
5. Stats Reporting
- Manager sends WindowedStatsPush - Verify gate aggregates into _windowed_stats
- Stats from multiple DCs for same job - Verify CRDT merge via JobStatsCRDT
- Manager dies with pending stats - Verify partial stats are not lost
- Stats push to client callback - Verify gate forwards aggregated stats at _stats_push_interval_ms
- Windowed stats drift - Stats arrive outside tolerance window; verify handled per stats_drift_tolerance_ms
- Stats for completed job - Verify late stats are discarded or handled appropriately
6. Workflow Results Reporting
- Manager sends WorkflowResultPush - Verify gate stores in _workflow_dc_results
- Results from all target DCs received - Verify gate calls _aggregate_and_forward_workflow_result
- Partial DC results (some DCs fail) - Verify gate handles incomplete results with timeout
- Manager dies before sending workflow result - Verify gate detects and marks workflow as failed/incomplete
- Duplicate workflow results - Manager retries; verify idempotency
- Results for unknown job - Verify gate forwards to peer gates or discards
7. Final Job Results
- Manager sends JobFinalResult - Verify gate records route learning latency, calls _complete_job
- Final results from all DCs - Verify gate aggregates into GlobalJobResult
- Manager dies before final result - Verify job timeout tracker detects and handles
- Conflicting final results - DC1 says COMPLETED, DC2 says FAILED; verify resolution logic
8. Reporter Integration
- Gate spawns reporter tasks - Verify _job_reporter_tasks tracks per-job reporters
- Reporter receives aggregated results - Verify ReporterResultPush sent correctly
- Reporter task fails - Verify error handling, job not marked failed due to reporter
- Job cleanup cleans reporter tasks - Verify _job_cleanup_loop cancels reporter tasks
9. Backpressure Propagation
- Manager signals backpressure - Verify gate reduces dispatch rate to that manager/DC
- DC-wide backpressure - All managers in DC overloaded; verify gate routes to other DCs
- Backpressure affects stats/results flow - Verify stats/results still processed under backpressure
10. Multi-DC Coordination
- Job spans multiple DCs - Verify gate tracks target_dcs and waits for all
- One DC completes, others timeout - Verify JobTimeoutReport handling
- DC leader transfer mid-job - Verify JobLeaderTransfer updates gate tracking
- Cross-DC result aggregation - Verify WorkflowDCResult merged correctly
Which area would you like to start implementing tests for? I'd suggest we prioritize:
1. Stats Reporting (5) - Core observability path
2. Workflow Results Reporting (6) - Core job completion path
3. Final Job Results (7) - End-to-end completion
▣  Sisyphus · claude-opus-4-5 · 19.6s
Also extend this to include our job routing, health detection, overload/health reporting, capacity reporting and more. Be exceptionally thorough.
Let me be exceptionally thorough and cover the entire gate <-> manager interaction surface:
Gate <-> Manager Scenarios (Comprehensive)
---
1. Job Dispatch & Routing
1.1 Basic Dispatch
- Single DC dispatch - Job submitted, gate selects manager, dispatches, receives ACK
- Multi-DC dispatch - Job targets multiple DCs, gate dispatches to each, tracks per-DC managers in _job_dc_managers
- Dispatch with client callback - Verify _progress_callbacks registered, callback address stored
1.2 Routing Decisions (AD-36 GateJobRouter)
- Vivaldi coordinate-based routing - Gate selects DC with lowest latency via _coordinate_tracker
- Blended latency scoring - Verify BlendedLatencyScorer combines Vivaldi + observed latency
- Route learning (AD-45) - Verify _dispatch_time_tracker.record_start() called on dispatch
- Route learning completion - Verify _observed_latency_tracker.record_job_latency() on result
- Stale route data - Route data older than max_staleness_seconds; verify fallback behavior
- Insufficient samples - Fewer than min_samples_for_confidence; verify Vivaldi-only routing
- DC candidate building - Verify _build_datacenter_candidates() includes health, capacity, latency
1.3 Dispatch Failures
- Manager dies mid-dispatch - Gate sends job, manager dies before ACK; verify retry to another manager
- All managers in DC fail - Verify failover to next DC in routing order
- Dispatch timeout - Manager doesn't respond within manager_dispatch_timeout_seconds
- Dispatch rejected (rate limited) - Manager returns rate limit response
- Dispatch rejected (backpressure) - Manager signals overload, gate backs off
1.4 Job Forwarding (Cross-Gate)
- Job forwarded to owner gate - Hash ring says different gate owns job; verify forward via _job_forwarding_tracker
- Forward timeout - Owner gate doesn't respond within forward_timeout_seconds
- Max forward attempts exceeded - Verify job rejected after max_forward_attempts
- Forward loop detection - Verify forwarding doesn't create infinite loops
1.5 Idempotency (AD-40)
- Duplicate job submission - Same idempotency key; verify _idempotency_cache returns cached response
- Idempotency key expiry - Key older than TTL; verify treated as new submission
- Concurrent duplicate submissions - Race condition; verify only one dispatch occurs
---
2. Manager Registration & Discovery
2.1 Registration Flow
- Manager registers with gate - Verify added to _datacenter_manager_status, _dc_manager_discovery, _manager_health
- Registration with capabilities - Verify _manager_negotiated_caps stores negotiated protocol version
- Registration from unknown DC - Manager claims DC not in _datacenter_managers; verify handling
- Re-registration after restart - Verify stale state cleared, fresh registration accepted
- Registration with role validation (AD-28) - Verify _role_validator checks mTLS claims
2.2 Discovery Propagation
- Gate broadcasts manager discovery - Verify ManagerDiscoveryBroadcast sent to peer gates
- Gate receives manager discovery - Verify manager added to local tracking
- Discovery of already-known manager - Verify no duplicate state created
- Discovery failure decay - Verify _discovery_maintenance_loop decays failure counts
2.3 Manager Heartbeats
- Manager heartbeat received - Verify _manager_last_status updated
- Heartbeat with state changes - Manager reports new job count, capacity; verify state updated
- Stale heartbeat rejection - Heartbeat older than _versioned_clock; verify rejected
- Heartbeat timeout - No heartbeat within heartbeat_timeout_seconds; verify manager marked unhealthy
---
3. Health Detection & Monitoring
3.1 Manager Health State (AD-19)
- Liveness probe success - Verify ManagerHealthState.update_liveness(success=True)
- Liveness probe failure - Verify failure count incremented, threshold checking
- Liveness failure threshold exceeded - Verify manager marked not-live
- Readiness probe - Manager has workers, not overloaded; verify ready state
- Readiness failure - Manager has no workers or is overloaded; verify not-ready
- Startup probe - New manager registering; verify startup grace period
3.2 Gate Health State
- Gate peer liveness - Verify GateHealthState tracking for peer gates
- Gate peer readiness - Verify has_dc_connectivity, connected_dc_count tracked
- Gate health aggregation - Verify _get_healthy_gates() filters by health state
3.3 Circuit Breaker (Per-Manager)
- Error threshold reached - Verify circuit opens after circuit_breaker_max_errors
- Circuit open behavior - Verify requests to that manager are rejected
- Half-open transition - After circuit_breaker_half_open_after_seconds; verify probe request sent
- Circuit close on success - Probe succeeds; verify circuit closes
- Circuit stays open on failure - Probe fails; verify circuit remains open
- Circuit breaker per-manager isolation - One manager's circuit doesn't affect others
3.4 Datacenter Health Manager (AD-16)
- DC marked healthy - All managers healthy; verify _dc_health_manager state
- DC marked degraded - Some managers unhealthy; verify degraded state
- DC marked unhealthy - All managers unhealthy; verify DC-level unhealthy
- DC health affects routing - Unhealthy DC deprioritized in routing decisions
- Manager added to DC - Verify _dc_health_manager.add_manager()
- Manager removed from DC - Verify proper cleanup
3.5 Federated Health Monitor
- Cross-DC probe sent - Verify _dc_health_monitor sends probes via _send_xprobe
- Cross-DC probe response - Verify latency recorded, health updated
- Cross-DC probe timeout - Verify failure recorded, suspicion incremented
- DC leader change detected - Verify _on_dc_leader_change callback
- DC health change detected - Verify _on_dc_health_change callback
- DC latency recorded - Verify _on_dc_latency callback updates routing
3.6 Hierarchical Failure Detector (AD-30)
- Global death detected - Manager unresponsive globally; verify _on_manager_globally_dead
- Job-level death detected - Manager unresponsive for specific DC; verify _on_manager_dead_for_dc
- Timeout adaptation - Verify timeouts adjust based on _get_dc_manager_count
3.7 Cross-DC Correlation Detector
- Correlated failures detected - Multiple DCs fail simultaneously; verify CorrelationSeverity
- Network partition suspected - Verify appropriate logging/alerting
- Independent failures - Failures not correlated; verify normal handling
---
4. Overload Detection & Load Shedding
4.1 Hybrid Overload Detector (AD-18)
- Delta-based detection - Latency rises above baseline; verify state transition
- Absolute threshold detection - Latency exceeds OVERLOAD_ABSOLUTE_*_MS; verify detection
- CPU-based detection - CPU exceeds OVERLOAD_CPU_* thresholds
- Memory-based detection - Memory exceeds OVERLOAD_MEMORY_* thresholds
- State transitions - HEALTHY → BUSY → STRESSED → OVERLOADED; verify smooth transitions
- Recovery detection - Load decreases; verify state transitions back
4.2 Load Shedding (AD-22)
- Shed request when overloaded - Verify _load_shedder.should_shed() returns true
- Shed percentage by state - BUSY sheds less than STRESSED sheds less than OVERLOADED
- Priority-based shedding - High-priority requests shed less often
- Shed response to client - Verify appropriate error returned with retry-after
4.3 Rate Limiting (AD-24)
- Per-client rate limiting - Verify _rate_limiter tracks per-client request counts
- Rate limit exceeded - Verify RateLimitResponse returned
- Rate limit cleanup - Verify _rate_limit_cleanup_loop removes inactive clients
- Rate limit with backpressure - Verify rate limits adjust based on backpressure
---
5. Backpressure Propagation (AD-37)
5.1 Manager Backpressure Signals
- Manager signals NONE - Verify _manager_backpressure[addr] = BackpressureLevel.NONE
- Manager signals LOW - Verify gate reduces dispatch rate slightly
- Manager signals MEDIUM - Verify gate reduces dispatch rate more
- Manager signals HIGH - Verify gate significantly reduces dispatch rate
- Manager signals CRITICAL - Verify gate stops dispatching to that manager
5.2 DC-Level Backpressure
- Aggregate manager backpressure - Verify _dc_backpressure reflects worst manager
- DC backpressure affects routing - High backpressure DC deprioritized
- Backpressure delay calculation - Verify _backpressure_delay_ms computed correctly
5.3 Backpressure Recovery
- Manager backpressure decreases - Verify gate increases dispatch rate
- DC backpressure clears - All managers report NONE; verify DC-level clears
---
6. Capacity Reporting & Spillover
6.1 Datacenter Capacity Aggregator
- Manager reports capacity - Verify _capacity_aggregator updates DC capacity
- Capacity staleness - Data older than CAPACITY_STALENESS_THRESHOLD_SECONDS; verify marked stale
- Aggregate DC capacity - Multiple managers; verify correct aggregation
6.2 Spillover Evaluator
- Spillover enabled - Verify SPILLOVER_ENABLED controls behavior
- DC at capacity - Primary DC full; verify spillover to secondary
- Spillover latency penalty - Verify SPILLOVER_MAX_LATENCY_PENALTY_MS considered
- Spillover improvement ratio - Verify SPILLOVER_MIN_IMPROVEMENT_RATIO threshold
- Spillover wait timeout - Verify SPILLOVER_MAX_WAIT_SECONDS honored
- No spillover target available - All DCs at capacity; verify behavior
---
7. Job Progress Flow
7.1 Progress Updates
- Manager sends JobProgress - Verify gate updates job state
- Manager sends JobProgressReport (AD-34) - Verify _job_timeout_tracker.record_progress()
- Progress from multiple DCs - Verify gate merges progress correctly
- Progress with workflow details - Verify per-workflow progress tracked
- Progress callback forwarding - Verify gate forwards to _progress_callbacks[job_id]
7.2 Progress Edge Cases
- Out-of-order progress - Later update arrives before earlier; verify ordering
- Duplicate progress - Same progress sent twice; verify idempotent handling
- Progress for unknown job - Verify graceful handling (forward to peers or discard)
- Progress after job complete - Late progress for finished job; verify discarded
- Manager dies mid-progress-stream - Verify partial progress preserved
7.3 Progress Aggregation
- Aggregate progress across DCs - Verify consistent global view
- Progress percentage calculation - Verify correct math across DCs/workflows
---
8. Stats Reporting
8.1 Windowed Stats Collection
- Manager sends WindowedStatsPush - Verify _windowed_stats updated
- Stats within window - Verify stats aggregated correctly
- Stats outside drift tolerance - Verify stats_drift_tolerance_ms enforced
- Stats window age limit - Verify stats_max_window_age_ms cleanup
8.2 Stats CRDT Merge (AD-14)
- Single DC stats - Verify JobStatsCRDT created for job
- Multi-DC stats merge - Verify CRDT merge produces correct totals
- Concurrent stats updates - Verify no race conditions
- Stats conflict resolution - Different DCs report different values; verify CRDT semantics
8.3 Stats Push to Client
- Batch stats loop - Verify _batch_stats_loop runs at _batch_stats_interval
- Windowed stats push loop - Verify runs at _stats_push_interval_ms
- Stats coordinator aggregation - Verify GateStatsCoordinator.batch_stats_update()
- Client callback delivery - Verify stats sent to registered callback
8.4 Stats Edge Cases
- Manager dies with pending stats - Verify partial stats not lost
- Stats for completed job - Verify late stats handled (discarded or logged)
- Stats for unknown job - Verify graceful handling
- High-volume stats - Many jobs, high frequency; verify no memory leak
---
9. Workflow Results Reporting
9.1 Workflow Result Flow
- Manager sends WorkflowResultPush - Verify stored in _workflow_dc_results[job_id][workflow_id][dc]
- Track expected workflows - Verify _job_workflow_ids[job_id] populated
- Result from unknown job - Verify _forward_workflow_result_to_peers() called
- Result logging - Verify debug logging includes job_id, workflow_id, dc
9.2 Multi-DC Result Aggregation
- All DCs report results - Verify _aggregate_and_forward_workflow_result() called
- Partial DC results - Some DCs haven't reported; verify waiting behavior
- DC result timeout - DC never reports; verify timeout handling
- Aggregation logic - Verify correct merge of per-DC results
9.3 Result Forwarding
- Forward to client - Verify aggregated result sent to client callback
- Forward to reporter - Verify ReporterResultPush generated
- Forward to peer gates - Job leader on different gate; verify forwarding
9.4 Result Edge Cases
- Duplicate workflow results - Manager retries; verify idempotency
- Out-of-order workflow results - Later workflow completes before earlier
- Workflow result for cancelled job - Verify appropriate handling
- Large result payload - Verify no serialization issues
---
10. Final Job Results
10.1 Final Result Flow
- Manager sends JobFinalResult - Verify JobFinalResult.load(data) succeeds
- Route learning update - Verify _dispatch_time_tracker.record_completion()
- Observed latency recording - Verify _observed_latency_tracker.record_job_latency()
- Job completion - Verify _complete_job() called via state sync handler
10.2 Final Result Aggregation
- All DCs report final - Verify GlobalJobResult constructed
- Mixed final statuses - DC1=COMPLETED, DC2=FAILED; verify resolution
- Final result with errors - Verify error aggregation
10.3 Job Completion Cleanup
- Job state cleanup - Verify _job_manager.delete_job() eventually called
- Workflow results cleanup - Verify _workflow_dc_results.pop(job_id)
- Workflow IDs cleanup - Verify _job_workflow_ids.pop(job_id)
- Progress callbacks cleanup - Verify _progress_callbacks.pop(job_id)
- Leadership cleanup - Verify _job_leadership_tracker.release_leadership(job_id)
- DC managers cleanup - Verify _job_dc_managers.pop(job_id)
- Reporter tasks cleanup - Verify tasks cancelled, _job_reporter_tasks.pop(job_id)
- CRDT stats cleanup - Verify _job_stats_crdt.pop(job_id)
- Router state cleanup - Verify _job_router.cleanup_job_state(job_id)
10.4 Final Result Edge Cases
- Manager dies before final result - Verify _job_timeout_tracker detects
- Duplicate final result - Verify idempotent handling
- Final result for unknown job - Verify graceful handling
- Route learning failure - Verify error logged, doesn't block completion
---
11. Job Timeout Tracking (AD-34)
11.1 Timeout Detection
- Progress timeout - No progress within threshold; verify detection
- DC-local timeout - Manager sends JobTimeoutReport; verify recorded
- All-DC stuck detection - All DCs stuck for all_dc_stuck_threshold_seconds
- Global timeout - Verify JobGlobalTimeout generated
11.2 Timeout Handling
- Timeout triggers cancellation - Verify job cancelled on global timeout
- Timeout with partial completion - Some workflows done, others stuck
- Leader transfer on timeout - Verify JobLeaderTransfer handling
11.3 Timeout Tracker Lifecycle
- Start tracker - Verify _job_timeout_tracker.start() in gate startup
- Stop tracker - Verify _job_timeout_tracker.stop() in gate shutdown
- Job registration - Verify jobs registered with timeout tracker
- Job cleanup - Verify completed/cancelled jobs removed from tracker
---
12. Reporter Integration
12.1 Reporter Task Management
- Reporter task creation - Verify _job_reporter_tasks[job_id] populated
- Multiple reporters per job - Verify all tracked
- Reporter task execution - Verify reporter receives data
12.2 Reporter Data Flow
- Workflow stats to reporter - Verify WorkflowStats sent
- Final results to reporter - Verify Results sent
- Reporter push - Verify ReporterResultPush message format
12.3 Reporter Error Handling
- Reporter task fails - Verify error logged, job not affected
- Reporter timeout - Verify timeout handling
- Reporter connection lost - Verify reconnection or graceful failure
12.4 Reporter Cleanup
- Job cleanup cancels reporters - Verify tasks cancelled in _job_cleanup_loop
- Reporter cleanup on gate shutdown - Verify all reporters stopped
---
13. Job Leadership & Coordination
13.1 Job Leadership Tracking
- Gate assumes leadership - Verify _job_leadership_tracker.assume_leadership()
- Leadership broadcast - Verify _broadcast_job_leadership() notifies peers
- Leadership notification received - Verify JobLeadershipNotification handling
- Leadership query - Verify _job_leadership_tracker.is_leader(job_id)
13.2 Leadership Transfers (Gate-to-Gate)
- Gate leader dies - Verify _handle_job_leader_failure() triggered
- Leadership takeover - Verify new gate assumes leadership
- Transfer acknowledgment - Verify JobLeaderGateTransferAck
13.3 Leadership Transfers (Manager-Level)
- Manager leader transfer - Verify JobLeaderManagerTransfer handling
- Manager leader ack - Verify JobLeaderManagerTransferAck
- Manager leader notification - Verify manager notified of new leader
13.4 Orphan Job Handling
- Job leader gate dies - Verify _orphan_job_coordinator detects
- Orphan grace period - Verify _orphan_grace_period honored
- Orphan job takeover - Verify orphan adopted by new gate
- Orphan job timeout - No takeover within grace; verify job failed
---
14. Lease Management
14.1 Job Leases
- Lease acquisition - Verify _job_lease_manager grants lease
- Lease renewal - Verify lease extended before expiry
- Lease expiry - Verify on_lease_expired callback
- Lease cleanup - Verify _lease_cleanup_loop removes expired
14.2 Datacenter Leases
- DC lease acquisition - Verify _dc_lease_manager grants lease
- Lease transfer - Gate transfers lease to peer; verify LeaseTransfer handling
- Lease transfer ack - Verify LeaseTransferAck
- Fence token increment - Verify next_fence_token() on operations
---
15. Quorum & Consistency
15.1 Quorum Checking
- Quorum available - Verify _has_quorum_available() returns true
- Quorum unavailable - Verify appropriate error returned
- Quorum size calculation - Verify _quorum_size() correct
15.2 Quorum Circuit Breaker
- Quorum errors tracked - Verify _quorum_circuit records errors
- Quorum circuit opens - Too many errors; verify circuit opens
- Quorum circuit recovery - Verify half-open and close transitions
15.3 Consistency Guarantees
- At-most-once dispatch - Verify idempotency prevents duplicates
- Exactly-once completion - Verify job completes exactly once
- Ordered operations - Verify versioned clock prevents stale updates
---
16. State Synchronization
16.1 Gate State Sync
- State sync request - Peer gate requests state; verify GateStateSyncRequest handling
- State sync response - Verify GateStateSyncResponse with snapshot
- State snapshot application - Verify _apply_gate_state_snapshot()
- Versioned state clock - Verify stale updates rejected
16.2 Startup Sync
- New gate joins - Verify _complete_startup_sync() syncs state
- Sync from leader - Verify state obtained from current leader
- Sync completion - Verify gate transitions to ACTIVE state
---
17. Protocol Negotiation (AD-25)
17.1 Capability Negotiation
- Manager advertises capabilities - Verify NodeCapabilities received
- Negotiate common capabilities - Verify negotiate_capabilities() called
- Store negotiated caps - Verify _manager_negotiated_caps[addr] updated
17.2 Version Compatibility
- Same version - Verify full feature set available
- Older manager - Verify graceful degradation
- Newer manager - Verify forward compatibility
- Feature checking - Verify get_features_for_version() used
---
18. Cancellation Flow
18.1 Job Cancellation
- Client requests cancellation - Verify CancelJob handling
- Cancellation to managers - Verify gate forwards to all DCs
- Cancellation acknowledgment - Verify CancelAck handling
- Cancellation completion - Verify JobCancellationComplete aggregation
18.2 Workflow Cancellation
- Single workflow cancel - Verify SingleWorkflowCancelRequest handling
- Workflow cancel response - Verify SingleWorkflowCancelResponse
- Workflow cancellation status - Verify WorkflowCancellationStatus tracking
18.3 Cancellation Coordination
- Cancellation coordinator - Verify GateCancellationCoordinator logic
- Cancellation errors - Verify _cancellation_errors[job_id] tracked
- Cancellation event - Verify _cancellation_completion_events[job_id] signaled
---
19. Throughput & Metrics
19.1 Throughput Tracking
- Forward throughput - Verify _forward_throughput_count incremented
- Throughput calculation - Verify calculate_throughput() correct
- Throughput interval - Verify _forward_throughput_interval_seconds honored
19.2 Latency Tracking
- Per-manager latency - Verify LatencyTracker samples stored
- Latency sample age - Verify latency_sample_max_age_seconds cleanup
- Latency sample count - Verify latency_sample_max_count limit
---
20. Error Handling & Recovery
20.1 Exception Handling
- Handler exceptions - Verify handle_exception() called
- Background loop exceptions - Verify loops continue after exception
- Coordinator exceptions - Verify graceful degradation
20.2 Connection Failures
- TCP send failure - Verify retry logic, circuit breaker update
- UDP send failure - Verify SWIM handles gracefully
- Connection timeout - Verify appropriate timeout handling
20.3 Serialization Failures
- Invalid message format - Verify error logged, connection not crashed
- Partial message - Verify handled gracefully
- Large message - Verify size limits enforced
---

Manager <-> Worker Scenarios (Comprehensive)
---
1. Worker Registration & Discovery
1.1 Registration Flow
- Worker registers with manager - Verify ManagerRegistry.register_worker() adds to _workers, _worker_addr_to_id, initializes circuit breaker
- Registration with core count - Verify registration.node.total_cores stored correctly
- Registration with health state - Verify initial health state tracked
- Re-registration after restart - Verify old state cleared, fresh registration accepted
- Registration from unknown worker - Verify appropriate logging/tracking
1.2 Worker Pool Integration
- Worker added to pool - Verify WorkerPool receives registration
- Worker health state in pool - Verify get_worker_health_state() returns correct state
- Worker health state counts - Verify get_worker_health_state_counts() aggregates correctly
1.3 Worker Unregistration
- Worker disconnects gracefully - Verify unregister_worker() cleans up all state
- Worker dies unexpectedly - Verify detected via SWIM, state cleaned up
- Cleanup includes - _workers, _worker_addr_to_id, _worker_circuits, _dispatch_semaphores, _worker_deadlines, _worker_unhealthy_since
---
2. Core Allocation
2.1 Basic Allocation
- Allocate cores to workflow - Verify CoreAllocator.allocate() returns correct indices
- Allocation atomicity - Verify check-and-allocate is atomic (no TOCTOU)
- Allocation tracking - Verify _core_assignments and _workflow_cores updated
- Available cores count - Verify available_cores property updated
2.2 Allocation Constraints
- Request exceeds total - Verify error returned if cores_needed > total_cores
- Request exceeds available - Verify error returned if insufficient free cores
- Zero/negative cores - Verify validation error for invalid requests
- Duplicate allocation - Verify error if workflow already has cores
2.3 Core Release
- Free all cores - Verify CoreAllocator.free() releases all cores for workflow
- Free subset - Verify CoreAllocator.free_subset() releases partial cores
- Cores available event - Verify _cores_available event set when cores freed
2.4 Streaming Workflows
- Partial core release - Workflow releases cores as parts complete
- Core tracking during release - Verify _workflow_cores[workflow_id] shrinks correctly
- Final cleanup - Verify empty list removes workflow from tracking
2.5 Core Contention
- Multiple workflows compete - First-come-first-served allocation
- Wait for cores - Verify wait_for_cores() with timeout
- Core starvation - Large workflow waiting while small ones complete
---
3. Workflow Dispatch
3.1 Dispatch Coordination
- Manager dispatches to worker - Verify ManagerDispatchCoordinator.dispatch_workflow()
- Worker selection - Verify AD-17 health bucket selection (HEALTHY > BUSY > DEGRADED)
- Dispatch semaphore - Verify _dispatch_semaphores limits concurrent dispatches per worker
- Fence token - Verify fence token incremented and sent with dispatch
3.2 Worker Selection (AD-17)
- Healthy workers preferred - Verify healthy bucket checked first
- Fallback to busy - No healthy workers; verify busy bucket used
- Fallback to degraded - No healthy/busy; verify degraded bucket used
- Overloaded excluded - Verify overloaded workers never selected
- Capacity check - Verify worker has total_cores >= cores_required
- Circuit breaker check - Verify workers with open circuits excluded
- Sorting by capacity - Within bucket, workers sorted by total_cores descending
3.3 Dispatch Message
- WorkflowDispatch construction - Verify all fields populated correctly
- Workflow data serialization - Verify workflow_data bytes included
- Context serialization - Verify context passed for dependent workflows
- VUs and cores - Verify vus and cores from workflow priority
3.4 Dispatch Response
- WorkflowDispatchAck received - Verify ACK parsed correctly
- Accepted dispatch - Verify ack.accepted == True, cores assigned
- Rejected dispatch - Verify ack.accepted == False, error reason
- Throughput counter - Verify _dispatch_throughput_count incremented on success
3.5 Dispatch Failures
- Worker unreachable - Verify timeout handling, circuit breaker updated
- Worker rejects dispatch - Verify error recorded, retry logic
- Dispatch exception - Verify exception logged, circuit breaker records error
---
4. Workflow Priority & Scheduling
4.1 Priority Classification
- Explicit priority - Workflow has priority = StagePriority.HIGH
- AUTO priority - Default priority, cores split equally
- EXCLUSIVE priority - Workflow gets dedicated resources
4.2 Priority-Based Allocation
- Explicit priority first - Explicit priority workflows allocated before AUTO
- Priority ordering - Higher priority value = higher priority allocation
- VUs tiebreaker - Same priority, more VUs = earlier allocation
4.3 Core Distribution
- Proportional by VUs - Cores allocated proportionally to VU count
- Minimum cores - Each workflow gets at least 1 core
- Remaining cores to AUTO - After explicit, remaining cores split among AUTO
4.4 EXCLUSIVE Handling
- EXCLUSIVE detection - Verify EXCLUSIVE workflows identified
- EXCLUSIVE isolation - EXCLUSIVE workflows run alone or sequentially
- EXCLUSIVE completion - Verify resources released for next workflow
---
5. Worker Health & Circuit Breakers
5.1 Worker Health States
- HEALTHY - Normal operation, preferred for dispatch
- BUSY - Moderate load, second preference
- STRESSED/DEGRADED - High load, last resort
- OVERLOADED - Excluded from dispatch entirely
5.2 Health State Transitions
- HEALTHY → BUSY - Load increases
- BUSY → STRESSED - Load continues increasing
- STRESSED → OVERLOADED - Critical load level
- Recovery path - OVERLOADED → STRESSED → BUSY → HEALTHY
5.3 Circuit Breaker Per-Worker
- Error threshold - Circuit opens after N consecutive errors
- Circuit open - Dispatch attempts rejected
- Half-open - After timeout, single test request allowed
- Circuit close - Test succeeds, normal operation resumes
5.4 Unhealthy Worker Tracking
- Mark unhealthy - Verify _worker_unhealthy_since[worker_id] set
- Dead worker reaping - Verify _dead_node_reap_loop removes after interval
- Recovery detection - Worker heartbeat clears unhealthy status
---
6. Worker Failure Scenarios
6.1 Worker Dies Mid-Workflow
- Detection - SWIM detects worker death
- Workflow orphaned - Manager marks workflow as orphaned
- Grace period - Wait for potential recovery
- Reschedule - After grace period, reschedule to another worker
6.2 Worker Dies Before ACK
- Dispatch timeout - No ACK received within timeout
- Retry to another worker - Select different worker
- All workers fail - Report dispatch failure to gate
6.3 Worker Dies After Completion
- Result not received - Workflow completed but result lost
- Timeout detection - Manager detects missing result
- Status reconciliation - Check worker state on recovery
6.4 Partial Failure
- Some cores fail - Multi-core workflow has partial failure
- Partial results - Handle incomplete results appropriately
- Core cleanup - Ensure all allocated cores freed
---
7. Workflow Execution Lifecycle (AD-33)
7.1 State Machine Transitions
- PENDING → DISPATCHED - Workflow dispatched to worker
- DISPATCHED → RUNNING - Worker starts execution
- RUNNING → COMPLETED - Successful completion
- RUNNING → FAILED - Execution error
- Any → CANCELLED - Cancellation received
7.2 Invalid Transitions
- COMPLETED → anything - Terminal state, no transitions
- FAILED → anything - Terminal state, no transitions
- CANCELLED → anything - Terminal state, no transitions
7.3 Transition Logging
- Successful transitions - Debug log with old → new state
- Failed transitions - Warning log with attempted transition
7.4 Completion Events
- Event signaling - _workflow_completion_events[workflow_id] set
- Waiting on completion - Other code can await completion
- Cleanup after completion - Events cleaned up
---
8. Workflow Execution on Worker
8.1 Dispatch Handling
- WorkflowDispatch received - Verify parsing and validation
- Core allocation - Request cores from CoreAllocator
- State tracking - Add to _active_workflows
- Cancel event creation - Create asyncio.Event for cancellation
8.2 Workflow Deserialization
- Load workflow - dispatch.load_workflow() deserializes workflow
- Load context - dispatch.load_context() deserializes context
- Workflow name - Extract and track workflow name
8.3 Execution via RemoteGraphManager
- Manager available - Verify RemoteGraphManager initialized
- Execute workflow - Call remote_manager.execute_workflow()
- Monitor progress - Background task monitors execution
8.4 Execution Completion
- Success path - Status = COMPLETED, results collected
- Failure path - Status = FAILED, error captured
- Cancellation path - Status = CANCELLED
8.5 Cleanup
- Free cores - Release allocated cores
- Remove from tracking - Clean up _active_workflows
- Send final result - WorkflowFinalResult to manager
---
9. Progress Reporting
9.1 Progress Collection
- WorkflowProgress updates - Collected during execution
- Step stats - Per-step completed/failed counts
- Rate calculation - Completions per second
9.2 Progress Buffering (AD-37)
- Buffer updates - Store in _progress_buffer
- Flush interval - Send at _progress_flush_interval
- Backpressure handling - Adjust flush behavior based on level
9.3 Backpressure Effects on Progress
- NONE - Normal flush interval
- THROTTLE - Add delay between flushes
- BATCH - Accumulate, flush less often (every 4 cycles)
- REJECT - Drop non-critical updates entirely
9.4 Progress to Manager
- WorkflowProgress message - Sent to job leader manager
- Manager aggregation - Manager aggregates progress across workers
- Forward to gate - Manager forwards aggregated progress
---
10. Resource Contention
10.1 Core Contention
- Multiple dispatches arrive - Race for limited cores
- Atomic allocation - Lock prevents race conditions
- Waiters queue - Workflows wait for cores to free
10.2 Memory Contention
- Large workflow payloads - Memory pressure during deserialization
- Result serialization - Memory for results/context
- Buffer accumulation - Progress buffer growth
10.3 CPU Contention
- Workflow execution - Actual workflow work
- Progress monitoring - Background monitoring tasks
- Heartbeat/health - SWIM protocol overhead
10.4 Network Contention
- Progress updates - Frequent small messages
- Final results - Large result payloads
- Heartbeats - Constant background traffic
---
11. Backpressure (AD-23, AD-37)
11.1 Manager → Worker Backpressure
- Backpressure signal - Manager signals backpressure level
- Worker receives - Verify _manager_backpressure updated
- Behavior adjustment - Worker adjusts progress flush rate
11.2 Worker Backpressure Response
- NONE - Normal operation
- THROTTLE - Slow down progress updates
- BATCH - Batch progress updates
- REJECT - Drop non-critical updates
11.3 Latency Recording
- Workflow latency - Record completion latency for backpressure calc
- Latency digest - TimeWindowedTDigest for SLO tracking
---
12. Orphan Workflow Handling
12.1 Orphan Detection
- Manager dies - Worker detects via SWIM
- Mark orphaned - Workflow marked in _orphaned_workflows
- Orphaned timestamp - Record when orphaned
12.2 Grace Period
- Wait for takeover - Grace period for new manager
- Manager recovery - If same manager recovers, clear orphan status
- New manager takes over - Leadership transfer message
12.3 Orphan Expiry
- Grace period exceeded - get_orphaned_workflows_expired()
- Workflow handling - Complete locally or fail
- Cleanup - Remove from orphan tracking
---
13. Job Leadership Transfer
13.1 Transfer Protocol
- Transfer message received - JobLeaderTransfer from manager
- Fence token check - Verify token is newer
- Accept transfer - Update job leader for affected workflows
13.2 Transfer Validation
- Stale token rejection - Old fence token rejected
- Unknown manager rejection - Transfer from unknown source
- Duplicate transfer - Handle idempotently
13.3 Pending Transfers
- Store pending - If workflows not yet dispatched
- Apply on dispatch - Apply when workflow arrives
- Cleanup - Remove after application
13.4 Transfer Metrics
- Received count - Total transfers received
- Accepted count - Successfully accepted
- Rejected counts - By rejection reason
---
14. Cancellation Flow
14.1 Cancel Request
- CancelJob received - Manager receives from gate
- Pending workflows - Track workflows to cancel
- Send to workers - Forward cancel to workers with workflows
14.2 Worker Cancellation
- Cancel event set - Signal _workflow_cancel_events[workflow_id]
- Execution interruption - Workflow observes cancellation
- Status update - Set status = CANCELLED
14.3 Cancellation Completion
- All workflows cancelled - All pending marked complete
- Completion event - Signal _cancellation_completion_events
- Error collection - Aggregate cancellation errors
14.4 Partial Cancellation
- Some workers unreachable - Cancellation fails for subset
- Timeout handling - Don't wait forever for all
- Error reporting - Report partial cancellation
---
15. Quorum Protocol
15.1 Provision Quorum
- Request provision - Manager requests quorum for workflow
- Peer confirmation - Peers confirm resource reservation
- Quorum achieved - Proceed with dispatch
- Quorum failed - Reject dispatch
15.2 Quorum Calculation
- Quorum size - (peers + 1) // 2 + 1
- Confirmation tracking - Track confirming nodes
- Timeout handling - Don't wait forever for quorum
15.3 Provision Cleanup
- Clear pending - Remove from _pending_provisions
- Clear confirmations - Remove from _provision_confirmations
---
16. Stats & Metrics
16.1 Dispatch Throughput
- Throughput counter - _dispatch_throughput_count
- Interval calculation - Calculate throughput over interval
- Reset on interval - Reset counter after calculation
16.2 Latency Tracking
- Per-worker latency - Track dispatch latency per worker
- Latency samples - Bounded deque of samples
- Sample cleanup - Remove old samples
16.3 Worker Metrics
- Worker count - Total registered workers
- Unhealthy count - Workers marked unhealthy
- Circuit state - Per-worker circuit breaker state
16.4 SLO Tracking
- Workflow latency digest - TimeWindowedTDigest
- Latency observations - Aggregate for reporting
- Percentile calculation - P50, P95, P99 latencies
---
17. Version Skew Handling
17.1 Protocol Negotiation
- Capability advertisement - Manager advertises capabilities
- Worker capabilities - Worker responds with its capabilities
- Negotiated version - Agree on common feature set
17.2 Feature Gating
- Check feature support - Before using feature
- Fallback behavior - Use older protocol if needed
---
18. Event Logging (AD-47)
18.1 Workflow Events
- WorkerJobReceived - Workflow dispatch received
- WorkerJobStarted - Execution started
- WorkerJobCompleted - Successful completion
- WorkerJobFailed - Execution failed
18.2 Event Fields
- Timing - Timestamps for forensics
- Identifiers - job_id, workflow_id, worker_id
- Metrics - VUs, cores, elapsed time
- Errors - Error message and type for failures
---
19. Extension Requests (AD-26)
19.1 Extension State
- Extension requested - _extension_requested flag
- Extension reason - Why extension needed
- Progress tracking - Current progress, estimated completion
19.2 Extension Metrics
- Active workflow count - Workflows that need more time
- Completed items - Work done so far
- Total items - Total work expected
---
20. Error Handling & Recovery
20.1 Dispatch Errors
- Timeout - Worker doesn't respond
- Rejection - Worker rejects dispatch
- Exception - Unexpected error during dispatch
20.2 Execution Errors
- Workflow exception - Error during workflow execution
- Serialization error - Context/result serialization fails
- Resource error - Out of memory, cores unavailable
20.3 Recovery Actions
- Retry dispatch - Retry to same or different worker
- Mark worker unhealthy - After repeated failures
- Escalate to gate - Report failure for job-level handling
---
