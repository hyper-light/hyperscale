# AD-10 through AD-34 Edge-Case Analysis (Distributed Rewrite)

This document summarizes edge cases for each AD (10–34), cross-AD interactions, and the most robust fixes given our
Gate → Manager → Worker load-testing architecture (high CPU/memory workers, frequent progress updates).

## Per-AD Edge Cases and Status

### AD-10 (Fencing Tokens from Terms)
- **Edge case**: Leader transfer during in-flight dispatch; stale token acceptance by workers.
- **Status**: Fencing tokens include leader term + per-job counter; workers validate.
- **Refs**: `hyperscale/distributed_rewrite/jobs/job_manager.py:160`, `hyperscale/distributed_rewrite/jobs/workflow_dispatcher.py:567`
- **Robust fix**: None required.

### AD-11 (State Sync Retries with Exponential Backoff)
- **Edge case**: Partial sync success → inconsistent metadata across workers/managers.
- **Status**: RetryExecutor-based sync; continues on partial failure.
- **Refs**: `hyperscale/distributed_rewrite/nodes/manager.py:1752`
- **Robust fix**: None required.

### AD-12 (Manager Peer State Sync on Leadership)
- **Edge case**: New leader races with ongoing worker state updates.
- **Status**: New leader syncs from workers and peers.
- **Refs**: `hyperscale/distributed_rewrite/nodes/manager.py:1648`
- **Robust fix**: None required.

### AD-13 (Gate Split-Brain Prevention)
- **Edge case**: Concurrent gate startup causes competing leaders.
- **Status**: SWIM + pre-vote election prevents split-brain.
- **Refs**: `hyperscale/distributed_rewrite/swim/leadership/local_leader_election.py:1`, `hyperscale/distributed_rewrite/nodes/gate.py:623`
- **Robust fix**: None required.

### AD-14 (CRDT Cross-DC Stats)
- **Edge case**: Out-of-order/duplicate stat merges across gates.
- **Status**: GCounter/CRDT merges are commutative/idempotent.
- **Refs**: `hyperscale/distributed_rewrite/models/crdt.py:17`, `hyperscale/distributed_rewrite/nodes/gate.py:6474`
- **Robust fix**: None required.

### AD-15 (Tiered Update Strategy)
- **Edge case**: Immediate updates overwhelm gate during spikes.
- **Status**: Tiered strategy exists; load shedding and backpressure mitigate.
- **Refs**: `hyperscale/distributed_rewrite/nodes/gate.py:2851`, `hyperscale/distributed_rewrite/reliability/load_shedding.py:1`
- **Robust fix**: None required.

### AD-16 (Datacenter Health Classification)
- **Edge case**: UDP probe failure vs TCP heartbeat mismatch.
- **Status**: Gate combines TCP and federated health monitor signals.
- **Refs**: `hyperscale/distributed_rewrite/nodes/gate.py:2087`, `hyperscale/distributed_rewrite/datacenters/datacenter_health_manager.py:1`
- **Robust fix**: None required.

### AD-17 (Dispatch Fallback Chain)
- **Edge case**: All DCs in BUSY/DEGRADED but not UNHEALTHY.
- **Status**: Bucket ordering preserved; fallback chain constructed.
- **Refs**: `hyperscale/distributed_rewrite/nodes/gate.py:2532`
- **Robust fix**: None required.

### AD-18 (Hybrid Overload Detection)
- **Edge case**: High latency but low resource usage (false negatives).
- **Status**: Delta + absolute thresholds; worker latency recorded.
- **Refs**: `hyperscale/distributed_rewrite/reliability/overload.py:1`, `hyperscale/distributed_rewrite/nodes/worker.py:2516`
- **Robust fix**: None required.

### AD-19 (Three-Signal Health Model)
- **Edge case**: Progress stalls but readiness remains OK.
- **Status**: Throughput/expected throughput tracked for gates/managers/workers.
- **Refs**: `hyperscale/distributed_rewrite/nodes/worker.py:1573`, `hyperscale/distributed_rewrite/nodes/manager.py:2678`, `hyperscale/distributed_rewrite/nodes/gate.py:1908`
- **Robust fix**: None required.

### AD-20 (Cancellation Propagation)
- **Edge case**: Cancellation during leader transfer.
- **Status**: Idempotent cancellation with push acknowledgements.
- **Refs**: `hyperscale/distributed_rewrite/nodes/manager.py:10775`, `hyperscale/distributed_rewrite/nodes/worker.py:3634`
- **Robust fix**: None required.

### AD-21 (Unified Retry Framework)
- **Edge case**: Retries without jitter causing herd effects.
- **Status**: RetryExecutor with jitter used across nodes.
- **Refs**: `hyperscale/distributed_rewrite/reliability/retry.py:1`
- **Robust fix**: None required.

### AD-22 (Load Shedding)
- **Edge case**: Overload drops critical health/cancel traffic.
- **Status**: CRITICAL never shed; priority-based thresholds.
- **Refs**: `hyperscale/distributed_rewrite/reliability/load_shedding.py:1`, `hyperscale/distributed_rewrite/server/protocol/in_flight_tracker.py:1`
- **Robust fix**: None required.

### AD-23 (Backpressure for Stats Updates)
- **Edge case**: Manager overload but workers keep flushing.
- **Status**: Manager emits backpressure in progress acks; worker throttles.
- **Refs**: `hyperscale/distributed_rewrite/nodes/manager.py:6066`, `hyperscale/distributed_rewrite/nodes/worker.py:3320`
- **Robust fix**: Ensure Gate respects manager backpressure for forwarded updates (see AD-37 fix).

### AD-24 (Rate Limiting)
- **Edge case**: Burst traffic from clients overwhelms gate before rate limit checks.
- **Status**: Gate/manager/worker check rate limits prior to handling.
- **Refs**: `hyperscale/distributed_rewrite/reliability/rate_limiting.py:1`, `hyperscale/distributed_rewrite/nodes/gate.py:4746`
- **Robust fix**: None required.

### AD-25 (Version Skew)
- **Edge case**: Mixed protocol versions during rolling upgrades.
- **Status**: Version negotiation and capability fields present.
- **Refs**: `hyperscale/distributed_rewrite/protocol/version.py:1`, `hyperscale/distributed_rewrite/nodes/manager.py:5128`
- **Robust fix**: None required.

### AD-26 (Adaptive Healthcheck Extensions)
- **Edge case**: Extension granted but timeout ignores extension.
- **Status**: AD-34 integrates extension tracking into timeouts.
- **Refs**: `hyperscale/distributed_rewrite/health/extension_tracker.py:1`, `hyperscale/distributed_rewrite/jobs/timeout_strategy.py:138`
- **Robust fix**: None required.

### AD-27 (Gate Module Reorganization)
- **Edge case**: Not assessed per request (ignored).

### AD-28 (Enhanced DNS Discovery)
- **Edge case**: Peer selection using stale health metrics.
- **Status**: Adaptive selection and role validation present.
- **Refs**: `hyperscale/distributed_rewrite/discovery/__init__.py:1`, `hyperscale/distributed_rewrite/discovery/security/role_validator.py:86`
- **Robust fix**: None required.

### AD-29 (Peer Confirmation)
- **Edge case**: Gossip-discovered peers falsely suspected before confirmation.
- **Status**: UNCONFIRMED state gating suspicion; stale unconfirmed logged.
- **Refs**: `hyperscale/distributed_rewrite/swim/health_aware_server.py:273`, `hyperscale/distributed_rewrite/swim/detection/incarnation_tracker.py:443`
- **Robust fix**: None required.

### AD-30 (Hierarchical Failure Detection)
- **Edge case**: Job-layer suspicion conflicts with healthy node-level status.
- **Status**: Separate job-layer tracking with responsiveness thresholds.
- **Refs**: `hyperscale/distributed_rewrite/swim/detection/hierarchical_failure_detector.py:544`, `hyperscale/distributed_rewrite/nodes/manager.py:9587`
- **Robust fix**: None required.

### AD-31 (Gossip-Informed Callbacks)
- **Edge case**: Lost gossip leading to stale leadership transfer.
- **Status**: Health gossip buffer + explicit leader transfer messages.
- **Refs**: `hyperscale/distributed_rewrite/swim/gossip/health_gossip_buffer.py:1`, `hyperscale/distributed_rewrite/nodes/manager.py:1349`
- **Robust fix**: None required.

### AD-32 (Bounded Execution)
- **Edge case**: CRITICAL messages dropped under load.
- **Status**: CRITICAL never shed; bounded queues for other priorities.
- **Refs**: `hyperscale/distributed_rewrite/server/protocol/in_flight_tracker.py:1`, `hyperscale/distributed_rewrite/server/server/mercury_sync_base_server.py:182`
- **Robust fix**: None required.

### AD-33 (Workflow State Machine)
- **Edge case**: Timeout logic depends on progress events; state machine doesn’t emit callbacks.
- **Status**: Manager manually reports progress to timeout strategy.
- **Refs**: `hyperscale/distributed_rewrite/workflow/state_machine.py:1`, `hyperscale/distributed_rewrite/nodes/manager.py:9586`
- **Robust fix**: Add optional callbacks to WorkflowStateMachine so timeout strategy is notified directly.

### AD-34 (Adaptive Job Timeout, Multi‑DC)
- **Edge case**: Leader transfer while timeout loop running; stale decisions.
- **Status**: Fence tokens and resume_tracking guard stale decisions; gate aggregation works.
- **Refs**: `hyperscale/distributed_rewrite/jobs/timeout_strategy.py:35`, `hyperscale/distributed_rewrite/nodes/manager.py:9331`, `hyperscale/distributed_rewrite/jobs/gates/gate_job_timeout_tracker.py:89`
- **Robust fix**: Move timeout check interval to `env.py` for configuration (`manager.py:9369` TODO).

## Cross‑AD Interactions (Selected)

- **AD-23 + AD-22 + AD-32**: Backpressure throttles stats while load shedding/bounded execution protect control‑plane.
- **AD-26 + AD-34**: Extensions add to effective timeout; progress and extension grants update last_progress_at.
- **AD-29 + AD-30**: Peer confirmation gating prevents false suspicion at job layer.
- **AD-31 + AD-33**: Leadership transfer + state machine ensures consistent workflow lifecycle after failures.

## Most Robust Fixes for Our Use Case

1. **AD‑37 (Backpressure Policy) – missing gate integration and unified message classification**
   - Add gate-side backpressure consumption for forwarded updates.
   - Centralize message class → priority mapping used by both load shedding and in‑flight tracker.

2. **AD‑34 (Timeout check interval configuration)**
   - Move `check_interval` from manager hardcoded constant to `env.py`.

3. **AD‑33 (Optional timeout callbacks)**
   - Add optional progress callbacks in WorkflowStateMachine to improve timeout observability and reduce coupling.
