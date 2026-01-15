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
Manager <-> Worker Scenarios (Comprehensive)
---


High-Throughput Load Test Scenarios
---

21. Stats Update Storm (Workers → Manager)
21.1 Burst Stats Traffic
- 1000 VUs generating stats - Each VU completes ~100 req/s; verify manager handles 100K stats/s ingest
- Stats batching under load - Verify WindowedStatsBatch aggregates before send
- Stats queue overflow - Stats arrive faster than processing; verify bounded queue, oldest dropped
- Stats memory pressure - Large stats payloads accumulate; verify memory limits enforced
- Stats flush backpressure - Manager signals BATCH level; verify workers reduce flush rate

21.2 Stats Ordering and Deduplication
- Out-of-order stats batches - Network reordering delivers batch 5 before batch 4
- Duplicate stats batch - Worker retry sends same batch twice; verify deduplication
- Stats from dead worker - Worker dies, stats arrive after death detection; verify discarded
- Stats version conflict - Concurrent updates from same workflow; verify CRDT merge

21.3 Stats Aggregation Under Load
- Parallel stats merging - Multiple workers send concurrently; verify thread-safe aggregation
- Partial aggregation windows - Some workers report, others delayed; verify window handling
- Stats window boundary - Stats span window boundary; verify correct bucketing
- Stats compression - Large stats payloads; verify compression reduces network load

21.4 Stats Pipeline Backpressure
- Manager overloaded - Can't process stats fast enough; verify backpressure to workers
- Gate overloaded - Can't forward stats; verify backpressure to manager
- Client callback slow - Stats backing up; verify bounded buffer, oldest dropped
- End-to-end latency spike - Stats delayed > 5s; verify staleness detection
---

22. Results Flood (Workers → Manager → Gate)
22.1 High-Volume Result Handling
- 10K workflows complete simultaneously - Burst of WorkflowFinalResult messages
- Result serialization bottleneck - Large result payloads serialize slowly
- Result queue depth - Results queue faster than forward rate
- Result memory accumulation - Results buffered waiting for aggregation

22.2 Result Ordering Edge Cases
- Results arrive before dispatch ACK - Worker fast, network slow
- Results from workflow not in tracking - Race with dispatch registration
- Duplicate results - Network retry delivers twice; verify idempotent
- Partial result set - 9/10 workflows complete, 1 times out; verify partial aggregation

22.3 Cross-DC Result Aggregation
- DC latency asymmetry - DC-west reports in 10ms, DC-asia in 300ms
- DC result conflict - Same workflow, different results from different DCs
- DC result timeout - One DC never reports; verify timeout and partial completion
- Result aggregation race - Gate aggregating while new results arrive
---

23. Progress Update Avalanche
23.1 High-Frequency Progress
- Sub-second progress updates - VUs report progress every 100ms
- Progress batching efficiency - Verify batch size vs network overhead tradeoff
- Progress ordering - Updates reordered by network; verify monotonic progress
- Progress memory churn - Rapid progress creates garbage; verify GC pressure acceptable

23.2 Progress Fan-Out
- Multi-DC progress merge - Progress from 5 DCs for same job; verify merge correctness
- Progress to multiple callbacks - Job has 3 progress callbacks; verify all receive
- Progress callback latency - Slow callback; verify doesn't block other jobs
- Progress callback failure - Callback unreachable; verify retry then give up

23.3 Progress Under Partition
- DC becomes unreachable - Progress from 4/5 DCs; verify partial progress shown
- DC reconnects - Backlog of progress arrives; verify catch-up handling
- Progress gap detection - Missing progress sequence numbers; verify gap handling
---

Global Distribution Scenarios
---

24. Cross-Region Latency Challenges
24.1 Latency Asymmetry
- US-to-Europe dispatch - 100ms RTT; verify timeouts account for latency
- US-to-Asia dispatch - 200ms RTT; verify Vivaldi coordinates accurate
- Latency spike - Transient 500ms spike; verify not mistaken for failure
- Latency variance - 50-200ms jitter; verify median vs P99 handling

24.2 Clock Skew
- DC clocks differ by 100ms - Verify versioned clocks handle skew
- Clock jump - NTP correction jumps clock 500ms; verify no message rejection
- Clock drift - Slow drift over hours; verify periodic sync
- Timestamp comparison - Events from different DCs; verify logical ordering

24.3 Continent-Scale Partitions
- Trans-Atlantic partition - US and Europe isolated; verify both sides handle gracefully
- Trans-Pacific partition - US and Asia isolated; verify partition detection
- Partial partition - US can reach Europe, Europe can't reach US; verify asymmetric handling
- Partition heals - Connectivity restored; verify state reconciliation

24.4 Regional Failure Cascades
- US-West region fails - 3 DCs in region go dark; verify not mistaken for partition
- Gradual regional degradation - DCs fail one by one; verify correct correlation
- Regional recovery - Region comes back online; verify reintegration
---

25. Multi-Region Consistency
25.1 Job State Consistency
- Job created in US, dispatched to Asia - Verify state propagates before dispatch arrives
- Job cancelled in Europe, running in US - Verify cancellation reaches running workers
- Job completes in Asia, gate in US - Verify result reaches correct gate

25.2 Membership Consistency
- New gate joins in Europe - Verify US gates learn about it via gossip
- Worker joins in Asia - Verify US gate includes in routing decisions
- Manager dies in US - Verify Europe gates detect and update routing

25.3 Configuration Consistency
- Rate limit change - New limit deployed; verify all regions converge
- DC capacity update - Capacity increased; verify routing adjusts
- Feature flag change - Verify all regions see change consistently
---

26. Federated Health Across Regions
26.1 Cross-Region Health Probes
- Health probe latency - 200ms probe to Asia; verify timeout > RTT
- Probe packet loss - 5% packet loss; verify doesn't trigger false failure
- Probe batching - Multiple probes to same DC; verify efficient batching
- Probe prioritization - Probe critical DCs more frequently

26.2 Health State Propagation
- DC health change - Asia DC becomes unhealthy; verify US gates learn within 5s
- Health flapping - DC oscillates healthy/unhealthy; verify damping
- Health disagreement - US says Asia healthy, Europe says unhealthy; verify resolution
- Health state cache - Verify health state cached to reduce probe frequency

26.3 Regional Health Aggregation
- Region health rollup - 3 DCs in region; verify region-level health state
- Regional load balancing - Route away from degraded region
- Regional failover - Primary region fails; verify secondary takes over
---

27. Globally Distributed Job Routing
27.1 Latency-Aware Routing
- Route to nearest DC - Job from Europe routes to Europe DC
- Route with capacity constraint - Nearest DC full; verify spillover to next nearest
- Route with SLO constraint - Job requires <100ms; verify only low-latency DCs considered
- Route preference override - Client specifies DC; verify honored if healthy

27.2 Load Distribution
- Global load balancing - Distribute jobs across regions proportionally
- Hotspot detection - One DC receiving disproportionate load
- Load shedding by region - Overloaded region sheds to others
- Capacity-aware distribution - Route more to higher-capacity regions

27.3 Routing During Failures
- Primary DC fails - Verify automatic failover to secondary
- All DCs in region fail - Verify cross-region failover
- Partial DC failure - DC degraded but not dead; verify reduced routing
- Routing oscillation - Avoid rapid routing changes (hysteresis)
---

Race Conditions Under Load
---

28. Dispatch Race Conditions
28.1 Concurrent Dispatch to Same Worker
- Two dispatches hit same worker - Only one should succeed for capacity
- Dispatch + failure simultaneous - Dispatch in flight when worker dies
- Dispatch + cancellation race - Cancellation sent while dispatch pending
- Dispatch + completion race - Workflow completes before dispatch ACK

28.2 Leadership Race Conditions
- Two gates claim job leadership - Fencing token must resolve
- Leadership transfer during dispatch - Transfer arrives mid-dispatch
- Leadership + cancellation race - Transfer and cancel arrive together
- Leadership timeout race - Grace period expires as transfer arrives

28.3 State Update Race Conditions
- Concurrent health state updates - Two sources update same manager health
- Concurrent stats merge - Two DCs send stats simultaneously
- Concurrent result submission - Same workflow result from retry
- Concurrent cleanup - Job cleanup races with late result
---

29. High-Load Memory and Resource Scenarios
29.1 Memory Pressure
- Stats buffer growth - 10K jobs, each buffering stats
- Result accumulation - Slow aggregation causes result buildup
- Progress callback backlog - Slow callbacks cause progress accumulation
- Hash ring memory - Large cluster with 1000 nodes

29.2 Connection Exhaustion
- TCP connection storm - 1000 workers connect simultaneously
- Connection per manager - Many managers exhaust file descriptors
- UDP socket buffer overflow - High probe rate fills buffer
- Connection leak detection - Verify all connections eventually cleaned

29.3 CPU Pressure
- Stats aggregation CPU - CRDT merge is CPU intensive
- Serialization CPU - Large payloads serialize slowly
- Routing calculation CPU - Complex routing decisions
- Event loop saturation - Too many concurrent operations
---

30. Failure During High Load
30.1 Component Failure Under Load
- Manager dies with 1000 active workflows - Verify all rescheduled
- Gate dies with 500 jobs in progress - Verify peer takeover
- Worker dies with 100 VUs running - Verify stats not lost
- Network partition during burst - Verify recovery after partition heals

30.2 Cascading Failures
- One manager fails, others overloaded - Load redistribution causes cascade
- Worker death spiral - Deaths trigger rescheduling, triggering more deaths
- Gate quorum loss under load - Jobs in flight during quorum loss
- Circuit breaker cascade - One circuit opens, others follow

30.3 Recovery Under Load
- Manager recovers during high load - Verify gradual reintegration
- Worker recovers with pending results - Verify results delivered
- Gate recovers with jobs in flight - Verify state sync under load
- Network heals with message backlog - Verify backlog processed correctly
---

31. Timeout and Deadline Scenarios Under Load
31.1 Timeout Racing
- Response arrives as timeout fires - Verify no duplicate handling
- Multiple timeouts fire together - Verify serialized handling
- Timeout + success race - Success arrives just after timeout
- Cascading timeouts - One timeout triggers others

31.2 Deadline Pressure
- Job approaching deadline - 90% of deadline elapsed
- Worker extension request - Worker needs more time
- Extension denied under load - System too loaded to grant extension
- Deadline during partition - Deadline expires while partitioned

31.3 Timeout Configuration
- Aggressive timeouts - Short timeouts cause false failures under load
- Conservative timeouts - Long timeouts delay failure detection
- Adaptive timeouts - Timeouts adjust based on load
- Timeout jitter - Prevent thundering herd on timeout
---

32. Idempotency Under Extreme Conditions
32.1 Retry Storm
- Network hiccup causes mass retry - 1000 retries hit simultaneously
- Idempotency cache pressure - Cache size exceeded
- Idempotency key collision - Hash collision in high volume
- Idempotency expiry during retry - Key expires between retries

32.2 Duplicate Detection
- Near-simultaneous duplicates - Two requests 1ms apart
- Cross-gate duplicates - Same request to different gates
- Duplicate with different payload - Same key, different data
- Duplicate after completion - Retry after job finished
---

33. Split-Brain Scenarios During Load Test
33.1 Gate Cluster Split
- 3/5 gates partitioned - Minority and majority partitions
- Jobs in both partitions - Same job owned by different gates
- Partition heals - Verify state reconciliation
- Fencing token resolution - Higher token wins

33.2 Manager Cluster Split
- Manager cluster splits - Verify quorum prevents dual writes
- Worker dispatches to wrong partition - Verify rejection
- Partition detection - Verify correlation detector identifies
- Partition recovery - Verify gradual reintegration

33.3 DC Isolation
- Entire DC isolated - DC can't reach any other DC
- Isolated DC continues running - Jobs in DC continue
- Isolation detected - Gates mark DC unreachable
- Isolation ends - DC reintegrates, state reconciled
---

34. Stats-Specific Edge Cases for Load Tests
34.1 Action Timing Stats
- Sub-millisecond actions - HTTP requests completing in <1ms
- Very long actions - Actions taking >30s
- Action timeout stats - Timed-out actions still counted
- Action retry stats - Retried actions counted once or multiple?

34.2 VU Lifecycle Stats
- VU ramp-up stats - Stats during VU scaling up
- VU ramp-down stats - Stats during VU scaling down
- VU iteration stats - Stats per VU iteration
- VU error rate - Errors per VU tracked

34.3 Workflow-Level Stats
- Workflow duration histogram - Distribution of workflow durations
- Workflow throughput - Workflows per second
- Workflow failure rate - Failed workflows percentage
- Workflow retry rate - Retried workflows

34.4 Stats Accuracy
- Floating point precision - Stats aggregation precision
- Counter overflow - Stats counter exceeds int64
- Rate calculation accuracy - Throughput calculation over time
- Percentile accuracy - P99 with limited samples
---

35. Reporter Integration Under Load
35.1 Reporter Throughput
- High-volume reporter - Reporter receives 10K events/s
- Reporter batching - Events batched for efficiency
- Reporter backlog - Reporter slower than event rate
- Reporter memory - Event buffer memory pressure

35.2 Multiple Reporter Types
- Concurrent reporters - JSON, Prometheus, Datadog simultaneously
- Reporter priority - Critical reporters get priority
- Reporter failure isolation - One reporter fail doesn't affect others
- Reporter resource limits - Per-reporter resource quotas

35.3 Reporter During Failure
- Reporter unreachable - Events buffered or dropped
- Reporter reconnection - Buffer replayed on reconnect
- Reporter timeout - Slow reporter times out
- Reporter crash recovery - Reporter restarts mid-test
---

36. End-to-End Load Test Scenarios
36.1 Realistic Load Profile
- Ramp-up pattern - 0 → 10K VUs over 5 minutes
- Steady state - 10K VUs for 30 minutes
- Spike pattern - 10K → 50K → 10K over 1 minute
- Ramp-down pattern - 10K → 0 VUs over 5 minutes

36.2 Multi-Region Load Test
- Load from US - 5K VUs targeting US endpoints
- Load from Europe - 3K VUs targeting Europe endpoints
- Load from Asia - 2K VUs targeting Asia endpoints
- Cross-region load - US VUs targeting Asia endpoints

36.3 Mixed Workflow Types
- HTTP workflows - Simple HTTP request workflows
- GraphQL workflows - GraphQL query workflows
- Playwright workflows - Browser automation workflows
- Mixed workload - All workflow types simultaneously

36.4 Failure Injection During Load
- Kill random worker - During steady state
- Kill random manager - During steady state
- Network partition - During ramp-up
- DC failure - During spike

36.5 Resource Monitoring During Load
- Memory growth - Memory usage over time
- CPU utilization - CPU usage over time
- Network throughput - Bytes sent/received over time
- Connection count - Open connections over time
- Goroutine/task count - Concurrent operations over time
---

37. Zombie and Stale State Under Load
37.1 Zombie Detection Under Load
- Node restart under load - Node restarts, rejoins during high load
- Incarnation validation - Verify incarnation checked despite load
- Stale message rejection - Old messages rejected
- Death record cleanup - Verify cleanup happens under load

37.2 Stale State Cleanup
- Completed job cleanup - 10K jobs complete; verify timely cleanup
- Orphaned workflow cleanup - Worker dies; verify orphans detected
- Dead peer cleanup - Peer dies; verify state cleaned
- Result cache cleanup - Old results cleaned

37.3 State Accumulation
- Long-running test - 24-hour load test
- State growth monitoring - Verify bounded state growth
- Memory leak detection - No memory leaks over time
- File descriptor monitoring - No FD leaks
---

38. Protocol Edge Cases Under Load
38.1 Message Size Limits
- Large workflow payload - Workflow near size limit
- Large result payload - Result near size limit
- Large stats batch - Stats batch near size limit
- Size limit exceeded - Verify graceful rejection

38.2 Message Fragmentation
- Fragmented TCP messages - Message split across packets
- Reassembly under load - Correct reassembly despite high load
- Incomplete messages - Connection closed mid-message
- Message corruption detection - CRC or checksum validation

38.3 Protocol Version Negotiation
- Mixed version cluster - Old and new nodes
- Feature degradation - Graceful degradation for old nodes
- Version upgrade during test - Rolling upgrade
- Version rollback - Rollback during test
---

39. Observability Under Load
39.1 Logging Under Load
- Log volume - High log rate during load
- Log sampling - Sample logs during overload
- Structured logging - JSON logging performance
- Log buffer overflow - Log buffer exceeded

39.2 Metrics Under Load
- Metrics cardinality - Many labels under load
- Metrics sampling - Sample metrics during overload
- Metrics push latency - Delay in metrics push
- Metrics memory - Memory for metrics buffers

39.3 Tracing Under Load
- Trace sampling rate - Appropriate sampling under load
- Trace propagation - Context propagated correctly
- Trace storage - Traces stored correctly
- Trace analysis - Traces analyzable post-test
---

40. Graceful Shutdown Under Load
40.1 Gate Shutdown
- Gate shutdown with jobs - Jobs in progress during shutdown
- Leadership transfer during shutdown - Transfer leadership before exit
- Stats flush on shutdown - Final stats sent
- Connection draining - Existing connections complete

40.2 Manager Shutdown
- Manager shutdown with workflows - Workflows rescheduled
- Worker notification - Workers notified of shutdown
- Result forwarding - Pending results forwarded
- State handoff - State transferred to peers

40.3 Worker Shutdown
- Worker shutdown mid-workflow - Graceful workflow completion
- Core release on shutdown - Cores released
- Result submission - Final results sent
- Health state update - Marked unhealthy before shutdown
---

41. Multi-Gate Multi-DC Job Submission Simulation (3 Gates, 3 DCs)
41.1 Topology Bootstrap and Peer Confirmation (AD-29, AD-46)
- All 3 gates start concurrently - Unconfirmed peers not suspected during startup
- Managers start before gates - Confirmed on first successful heartbeat
- Unconfirmed peer never responds - Removed without DEAD transition
- Gossip about unconfirmed peer - NodeState remains UNCONFIRMED until direct ACK
- NodeState memory bound - Updates for same node remain O(1)

41.2 Dispatch Retry Data Preservation (AD-9)
- Retry dispatch uses original bytes - VUs/timeouts/context identical across retries
- Failed worker exclusion - Retry avoids failed worker set
- Retry after partial ACK - No double execution, one workflow instance
- Corrupted original bytes - Retry rejected with validation error
- Concurrent retries - Only one active dispatch per workflow

41.3 Fencing Tokens and Leadership Safety (AD-10, AD-13)
- Leader gate dispatches with current term - Worker accepts
- Stale leader dispatch - Worker rejects stale fencing token
- Leadership transfer mid-dispatch - New leader increments token and takes over
- Split-brain partition - Both leaders step down, no duplicate job completion
- Cancellation from stale leader - Rejected by manager/worker

41.4 State Sync Retries and Leadership Recovery (AD-11, AD-12)
- Leader change - Sync from workers and peer managers with backoff
- Peer manager unreachable - Sync continues with remaining peers
- Backoff jitter - No thundering herd when peers recover
- Sync race with shutdown - No deadlock between sync and stop
- Sync after partial state - Missing peers logged but job continues

41.5 Idempotent Job Submission Across Gates (AD-40)
- Same idempotency key to two gates - One job created, duplicate returns cached
- Pending entry wait - Second request blocks until first resolves
- Key expiry during retry - Treated as new submission after TTL
- Same key, different payload - Rejected or returns cached original response
- Idempotency cache cleanup - Entries evicted without memory growth

41.6 Capacity-Aware Spillover (AD-43)
- Primary DC lacks cores - Spillover to DC with immediate capacity
- Primary wait time below threshold - Queue at primary, no spillover
- Spillover latency penalty too high - Reject spillover despite capacity
- Stale capacity heartbeat - Gate degrades confidence, avoids spillover
- Core freeing schedule - Estimated wait time matches dispatch order

41.7 Adaptive Route Learning (AD-45, AD-36)
- Initial routing uses RTT UCB - No observed samples yet
- Observed latency samples accumulate - Confidence increases, blended score shifts
- Stale observations - Confidence decays to 0 after max staleness
- Late latency sample - Does not override newer sample ordering
- Routing hysteresis - Avoids oscillation under mixed scores

41.8 Retry Budgets and Best-Effort Completion (AD-44)
- Job retry budget shared - Total retries capped across workflows
- Per-workflow cap enforced - One workflow cannot consume entire budget
- Budget exhausted - Workflow marked failed without further retries
- Best-effort min_dcs met - Job completes with partial results
- Best-effort deadline hit - Completion with available results only

41.9 Explicit Backpressure and Load Shedding (AD-23, AD-37, AD-22, AD-32)
- Manager signals THROTTLE - Worker increases progress flush interval
- Manager signals BATCH - Worker batches progress updates
- Manager signals REJECT - Non-critical updates dropped, control unaffected
- CRITICAL messages under overload - Never shed by InFlightTracker
- Stats buffer bounds - Hot/Warm/Cold retention prevents memory growth

41.10 Durability and WAL Boundaries (AD-38, AD-39)
- Job create/cancel committed globally - Survives gate crash
- Workflow dispatch committed regionally - Survives manager crash
- WAL backpressure - Producer blocked or error surfaced
- WAL recovery - Replayed entries yield consistent state
- Data-plane stats - Fire-and-forget, no durability requirement

41.11 Workflow Context Propagation and Recovery (AD-49)
- Context from workflow A to B across DCs - Dependent receives correct context
- Worker dies mid-workflow - Re-dispatch uses stored dispatched_context
- Context update arrives late - Dependent dispatch waits or retries
- Context snapshot during leader transfer - New leader resumes with version
- Empty context - Dispatch still proceeds with defaults

41.12 Cross-Manager Worker Visibility (AD-48)
- Worker registers with Manager A - B/C learn via TCP broadcast
- Missed broadcast - Gossip piggyback eventually converges
- Stale incarnation update - Rejected by remote manager
- Owner manager down - Remote workers marked unusable for scheduling
- Manager joins late - Full worker list requested and applied

41.13 Resource Guards and Leak Prevention (AD-41)
- CPU exceeds warn threshold - Warning emitted, no throttle
- CPU exceeds throttle threshold - Throughput reduced
- Memory exceeds kill threshold - Workflow terminated gracefully
- Process tree monitoring - Child processes included in totals
- High uncertainty - Enforcement delayed until confidence improves

41.14 SLO-Aware Health and Routing (AD-42)
- p95 exceeds threshold - DC health shifts to DEGRADED
- T-Digest merge across managers - Percentiles stable across merges
- Sparse samples - Routing falls back to RTT-based scoring
- SLO data stale - Excluded from routing score contribution
- SLO violation with good RTT - Routing avoids violating DC

41.15 Manager Health Aggregation Alerts (AD-50)
- Leader manager overloaded - ALERT fired once per transition
- Majority overloaded - ALERT fired with peer counts
- High non-healthy ratio - WARNING emitted
- Peer recovery - INFO emitted, alert clears
- No peers - Aggregation skipped without error

41.16 Worker Event Logging (AD-47)
- Worker job lifecycle events logged - Start/complete/fail captured
- Action events under load - Logging does not block execution
- Event log overflow - Drops events without worker slowdown
- Log rotation - Old logs archived, retention enforced
- Crash forensics - Last events show active job and action

41.17 Hierarchical Failure Detection and Gossip Callbacks (AD-30, AD-31)
- Gossip-informed death - _on_node_dead_callbacks invoked on gossip update
- Timer starvation case - Suspicion expires despite frequent confirmations
- Job-layer suspicion - Node dead for one job, alive globally
- Refutation race - Higher incarnation clears suspicion
- Global death clears job suspicions - All per-job states removed

41.18 Rate Limiting and Version Skew (AD-24, AD-25)
- Client rate limit exceeded - 429 with Retry-After returned
- Server-side limit enforced - Per-client token bucket honored
- Mixed protocol versions - Feature negotiation uses min version
- Unknown fields ignored - Forward compatibility maintained
- Major version mismatch - Connection rejected

41.19 Deadlock and Lock Ordering
- Gate leadership transfer + state sync - No lock inversion deadlock
- Manager job lock + context update - Avoids lock ordering cycles
- Retry budget update + cleanup loop - No deadlock under contention
- WAL backpressure + shutdown - Shutdown completes without blocking
- Cancellation + timeout loops - No deadlock when both fire

41.20 Federated Health Monitoring (AD-33)
- Cross-DC probe timeout scaled - High RTT does not trigger false suspect
- DC leader change mid-probe - New leader accepted, old leader ignored
- Stale cross-DC incarnation - Rejected, no health downgrade
- Probe jitter distribution - No synchronized bursts across gates
- Correlation detector gating - Multiple DC failures treated as network issue

41.21 Pre-Voting and Quorum Safeguards (AD-5, AD-3)
- Pre-vote prevents split-brain - No dual leaders during partition
- Quorum size from config - Ignores transient membership count
- Quorum circuit breaker - Opens after repeated quorum failures
- Quorum recovery - Half-open allows probe, closes on success
- Minority partition - Leadership denied without quorum

41.22 Adaptive Healthcheck Extensions (AD-26)
- Extension granted with progress - Deadline extended per logarithmic rule
- Extension denied without progress - Worker marked suspect after deadline
- Extension cap reached - Further requests rejected
- Extension + global timeout - Timeout accounts for extensions granted
- Extension during overload - Manager denies under high load

41.23 Enhanced DNS Discovery and Role Validation (AD-28)
- Cluster/env mismatch - Registration rejected with error
- Role-based connection matrix - Worker cannot contact gate directly
- Rendezvous hash stability - Candidate set minimal churn on peer change
- Power-of-two choice - Load distributed across similar peers
- Sticky pool eviction - Evict on error rate or latency threshold

41.24 Retry Framework Jitter (AD-21)
- Full jitter distribution - Retry timings spread across nodes
- Decorrelated jitter - No periodic retry alignment
- Jitter + backoff cap - Max delay enforced
- Retryable exception filter - Non-retryable errors fail fast
- Backoff under recovery - Avoids thundering herd

41.25 Global Job Ledger Consistency (AD-38)
- Cancellation beats completion - Conflict resolution honors cancel
- Higher fence token wins - Later operation dominates
- HLC ordering - Causal sequence preserved across gates
- Regional vs global durability - Workflow dispatch not blocked by ledger
- Ledger repair - Merkle mismatch triggers anti-entropy

41.26 Logger WAL Extensions (AD-39)
- FSYNC batch overflow - Error surfaced in WAL mode
- Read-back recovery - WAL entries decoded with CRC validation
- File lock cleanup - No lock/FD leaks after close
- Sequence number monotonic - LSN order preserved across batches
- Data-plane mode - Errors logged, caller not blocked

41.27 Worker Event Log Fidelity (AD-47)
- Healthcheck events - Probe received logged at TRACE
- Action failure logging - Error type captured without crash
- Log buffer saturation - Events dropped without blocking
- Log retention - Old archives pruned by age/size
- Shutdown event ordering - WorkerStopping logged before exit

41.28 Context Consistency Under Multi-DC (AD-49)
- Context update on completion - JobInfo.context updated with LWW semantics
- Concurrent providers - Conflicting context keys resolved by version
- Re-dispatch with stored context - No recompute on recovery
- Context snapshot during state sync - Peer manager applies snapshot
- Context for unknown workflow - Ignored with warning

41.29 SLO and Resource Correlation (AD-42, AD-41)
- SLO violation with low RTT - Routing penalizes SLO-offending DC
- CPU pressure predicts latency - Routing reduces DC score proactively
- Memory pressure spikes - Health degraded before failure
- Percentile window rotation - Old samples aged out correctly
- T-Digest merge ordering - Merge produces stable p95/p99

41.30 Bounded Execution and Load Shedding (AD-32, AD-22)
- Global in-flight limit reached - LOW/NORMAL shed, HIGH/CRITICAL accepted
- Per-priority limits enforced - No starvation of CRITICAL
- Destination queue overflow - Oldest dropped, newest preserved
- Slow destination isolation - Fast destinations continue unaffected
- Queue state recovery - Transition back to HEALTHY after drain

---
42. Extended Chaos and Soak Scenarios
42.1 Long-Running Soak (24h)
- Memory growth over time - No unbounded job/worker state
- Retry budget drift - Budgets do not leak across jobs
- Idempotency cache churn - TTL eviction remains stable
- Stats buffer retention - Hot/Warm/Cold tiers bounded
- Event log rotation - Rotations do not stall workers

42.2 Targeted Chaos Injection
- Random manager restarts - Gate routing adapts without job loss
- Random gate restarts - Leadership transfers preserve job state
- Random worker restarts - Orphans requeued without duplicate results
- Network delay injection - Vivaldi coordinates adapt gradually
- Packet loss injection - SWIM suspicion does not spike

42.3 Backpressure + Rate Limiting Interaction
- Rate limit + backpressure - Both signals applied correctly
- Retry after headers - Client respects server guidance
- Throttle escalation - NONE -> THROTTLE -> BATCH -> REJECT
- Control-plane immunity - SWIM/cancel unaffected by backpressure
- Recovery ramp - Backpressure relaxes without oscillation

42.4 Multi-Gate Submit Storm
- 3 gates accept 10K submits - No duplicate job IDs
- Idempotency across gates - Same key returns same job
- Spillover under storm - Capacity-aware routing still works
- Observed latency learning - Score adjusts under load
- Quorum loss mid-storm - Leaders step down cleanly

42.5 Multi-DC Partial Failure Matrix
- DC-A unhealthy, DC-B busy, DC-C healthy - Routing chooses DC-C
- DC leader down - Federated health marks DC unreachable
- Manager majority unhealthy - DC classified DEGRADED
- Worker majority unhealthy - DC health changes propagate
- Recovery sequence - Health transitions stable and monotonic

---
43. Additional Manager/Worker Scenarios II
43.1 Worker affinity vs rebalancing - Sticky assignment vs fairness under churn
43.2 Dispatch gating on slow heartbeats - Avoid routing to slow-but-healthy workers
43.3 Cancellation storms with partial completion - Cancel vs finalize race
43.4 Manager failover mid-dispatch - Avoid double-dispatch
43.5 Per-tenant quotas under mixed load - No cross-tenant starvation
43.6 Clock drift on progress timestamps - Ordering and dedupe stability
43.7 Compression negotiation for progress/results - Fallback when unsupported
43.8 Cold-start throttling - Ramp first workflow after restart
43.9 Heartbeat loss burst then recovery - No false mass-eviction
43.10 Worker capability downgrade mid-run - Feature negotiation fallback
---
44. Additional Manager/Worker Scenarios III
44.1 Worker lease expiry - Lease expires during long action
44.2 Dispatch list staleness - Manager dispatches using stale worker list
44.3 Retry token mismatch - Worker reports mismatched retry token
44.4 Progress flush on shutdown - Worker flushes progress before exit
44.5 Result ack retry loop - Manager retries ack for flaky worker
44.6 Cancel vs retry race - Cancellation races with retry dispatch
44.7 Worker metadata eviction - Evict stale worker metadata safely
44.8 Backpressure recovery ramp - Backpressure relaxes without spikes
44.9 Manager queue fairness - Mixed retry/cancel fairness enforced
44.10 Worker health debounce - Avoid flapping health states
---
45. Additional Manager/Worker Scenarios
45.1 Stats batching drift - Worker stats batching windows vs flush interval drift
45.2 Priority fairness under contention - Manager fairness with mixed priorities and core contention
45.3 Retry budget exhaustion - Worker retry budget exhaustion escalates to manager/gate
45.4 Progress idempotency - Duplicate progress frames and stale progress replay
45.5 Late dispatch ACK reconciliation - Timeout fires then late ACK arrives
45.6 Worker state sync after restart - Pending workflows and cancel events restored
45.7 Circuit breaker oscillation - Manager circuit breaker flaps under intermittent worker failures
45.8 Result integrity on restart - Partial workflow completion across worker restarts
46. Scheduling and Fairness
46.1 Starvation prevention - Mixed workflow sizes avoid starvation
46.2 Uneven core fairness - Fairness across workers with uneven cores
46.3 Priority inversion - Low-priority holds scarce cores
47. Dispatch and Acks
47.1 Duplicate dispatch ACKs - Idempotent handling of ACKs
47.2 ACK without execution - Worker crashes after ACK, before run
47.3 Re-dispatch after partial execution - Resume with partial metadata
48. Progress and Backpressure
48.1 Progress buffer overflow recovery - Recover after overflow
48.2 Progress jitter smoothing - Smooth bursty update timing
48.3 Backpressure de-escalation hysteresis - Avoid flapping
49. Retry and Timeout Semantics
49.1 Retry budget reset on failover - Manager failover resets budget safely
49.2 Extension early completion - Extension granted but worker finishes early
49.3 Overlapping retry windows - Multiple retry windows per workflow
50. Worker Health and Recovery
50.1 Health restored mid-dispatch - Avoid double scheduling
50.2 Zombie late progress - Late progress ignored safely
50.3 GC pause false positive - Health monitor tolerates GC pause
51. Result Integrity and Validation
51.1 Result dedupe across restarts - Avoid duplicate final results
51.2 Result merge after retries - Merge partial outputs safely
51.3 Result schema change - Validation handles schema changes
52. State Sync and Consistency
52.1 Snapshot with in-flight dispatches - State snapshot applied safely
52.2 Restore pending cancellations - Worker restores cancel events
52.3 Stale state version rejection - Reject stale state on reconnect
---
53. Additional Manager/Worker Scenarios IV
53.1 Worker lease renewal jitter - Renewal jitter does not cause false expiry
53.2 Dispatch retry collapse - Burst of retries collapses to single enqueue
53.3 Progress snapshot batching - Snapshot batching avoids duplication
53.4 Result forwarding timeout - Retry with backoff to gate
53.5 Manager load shed on dispatch - Load shed avoids overload spiral
53.6 Worker queue overflow - Oldest workflow dropped safely
53.7 Health probe priority inversion - Probes not starved by dispatch
53.8 Worker clock skew - Manager tolerates skew in timestamps
53.9 Retry budget global cap - Per-job retries respect global cap
53.10 Cancel propagation lag - Cancel reaches all workers within SLA
---
