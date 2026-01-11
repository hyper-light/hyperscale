# Improvements

## Control Plane Robustness
- Global job ledger: durable job/leader state with quorum replication to eliminate split‑brain after regional outages.
- Cross‑DC leadership quorum: explicit leader leases with renewal + fencing at gate/manager layers.
- Idempotent submissions: client‑side request IDs + gate/manager dedupe cache.

## Routing & Placement
- Policy‑driven placement: explicit constraints (region affinity, min capacity, cost, latency budget) with pluggable policy.
- Pre‑warm pools: reserved workers for bursty tests; spillover logic to nearest DC.
- Adaptive route learning: feed real test latency into gate routing (beyond RTT UCB).

## Execution Safety
- Max concurrency caps: hard limits per worker, per manager, per DC; configurable by job class.
- Resource guards: enforce CPU/mem/FD ceilings per workflow; kill/evict on violation.
- Circuit‑breaker for noisy jobs: auto‑throttle or quarantine high‑impact tests.

## Progress & Metrics
- Unified telemetry schema: single event contract for client/gate/manager/worker.
- SLO‑aware health: gate routing reacts to latency percentile SLOs, not only throughput.
- Backpressure propagation end‑to‑end: client also adapts to gate backpressure.

## Reliability
- Retry budgets: cap retries per job to avoid retry storms.
- Safe resumption: WAL for in‑flight workflows so managers can recover without re‑dispatching.
- Partial completion: explicit “best‑effort” mode for tests when a DC is lost.

## Security & Isolation
- Per‑tenant quotas: CPU/mem/connection budgets with enforcement.
- Job sandboxing: runtime isolation for load generators (cgroups/containers).
- Audit trails: immutable log of job lifecycle transitions and leadership changes.

## Testing & Validation
- Chaos suite: automated kill/restart of gates/managers/workers to verify recovery.
- Synthetic large‑scale tests: simulate 10–100× fanout jobs with backpressure validation.
- Compatibility tests: version skew + rolling upgrade scenarios.
