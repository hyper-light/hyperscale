# Improvements

*Status of each item checked against the code 2026-10-06 (italic notes; details in `docs/REMAINING_LEDGER.md`, dev-docs ledger D-59 to D-78).*

## Control Plane Robustness
- Global job ledger: durable job/leader state with quorum replication to eliminate split‑brain after regional outages. *Built: event-sourced job ledger replicated by per-job Raft; manager REGIONAL replicator, gate REGIONAL + GLOBAL (GLOBAL = copies in 2+ regions, D14).*
- Cross‑DC leadership quorum: explicit leader leases with renewal + fencing at gate/manager layers. *Built: per-job fence tokens at gate and manager; Raft leader leases exist behind `RAFT_LEADER_LEASES_ENABLED` (default off, `env/env.py:216`).*
- Idempotent submissions: client‑side request IDs + gate/manager dedupe cache. *Built (AD-40): gate cache, manager WAL-backed ledger, cross-gate key on `GateJobReplica`.*

## Routing & Placement
- Policy‑driven placement: explicit constraints (region affinity, min capacity, cost, latency budget) with pluggable policy. *Partial: hard `datacenters=[...]` constraint, AD-43 spillover, AD-36 scoring and storage-aware exclusion exist; no pluggable policy object, no cost or latency-budget constraint.*
- Pre‑warm pools: reserved workers for bursty tests; spillover logic to nearest DC. *Partial: spillover is built (AD-43); no reserved/pre-warmed worker pool.*
- Adaptive route learning: feed real test latency into gate routing (beyond RTT UCB). *Built (AD-45).*

## Execution Safety
- Max concurrency caps: hard limits per worker, per manager, per DC; configurable by job class. *Built 2026-10-07 (D-65): the DC leader admits a job only while its datacenter has room -- `JOB_CONCURRENCY_CAP_PER_DC` (unset: unfinished work plus the new job's within registered cores x the job's timeout) and `JOB_CLASS_CONCURRENCY_CAPS` per job class (its workflow names); a capped job is refused with `retry_after_seconds` (`jobs/job_admission_control.py`). Per-worker/per-manager: `MAX_WORKERS_PER_MANAGER`, core allocation, `MERCURY_SYNC_MAX_CONCURRENCY`.*
- Resource guards: enforce CPU/mem/FD ceilings per workflow; kill/evict on violation. *Built (2026-10-07): AD-41 `ResourceEnforcer` enforces CPU and memory per workflow (WARN→THROTTLE→KILL→EVICT, on by default); FDs are guarded per worker by `FileDescriptorCeiling` (RLIMIT_NOFILE soft limit detected at runtime, largest process's count; drains the worker at the kill line, resumes under the warning line) -- see AD_41.md.*
- Circuit‑breaker for noisy jobs: auto‑throttle or quarantine high‑impact tests. *Built 2026-10-07 (D-67): a job ending with a retry refused for a spent AD-44 budget quarantines its job class at the DC leader for that job's lifetime, then one probe job is admitted; a clean probe closes the breaker (`jobs/job_class_circuit_breaker.py`).*

## Progress & Metrics
- Unified telemetry schema: single event contract for client/gate/manager/worker. *Built (2026-10-07, D-68): every role answers `cluster_metrics` with one `ClusterMetricsReply` -- role and state, capacity, workload, resources, a manager's dispatch throughput, outcomes and per-worker dispatch round trips, the AD-42 latency SLO per datacenter (a manager's own, a gate's view of each) -- read by `HyperscaleClient.cluster_metrics` and printed by `hyperscale cluster --metrics` for a gate, manager or worker. Structured events stay the Logger's models (`hyperscale/logging/hyperscale_logging_models.py`), one module for every role.*
- SLO‑aware health: gate routing reacts to latency percentile SLOs, not only throughput. *Built: gate DC health is the worse of the managers' view and SLO compliance (`SLOHealthClassifier`).*
- Backpressure propagation end‑to‑end: client also adapts to gate backpressure. *Built 2026-10-06: a refusal carrying `retry_after_seconds` (gate shed, replication quorum, rate limit) is waited out, never below the hint (`nodes/client/submission.py:435-453`); without a hint the base delay is `ClientConfig.retry_base_delay_seconds` = `OVERLOAD_SAMPLE_INTERVAL_SECONDS`.*

## Reliability
- Retry budgets: cap retries per job to avoid retry storms. *Built (AD-44): `retry_budget` / `retry_budget_per_workflow` on `JobSubmission`; `RETRY_BUDGET_DEFAULT` is 10.*
- Safe resumption: WAL for in‑flight workflows so managers can recover without re‑dispatching. *Built: the manager's job WAL/ledger recovers job state on restart; dispatch is fenced.*
- Partial completion: explicit “best‑effort” mode for tests when a DC is lost. *Built (AD-44 `BestEffortManager`).*

## Security & Isolation
- Per‑tenant quotas: CPU/mem/connection budgets with enforcement. *Not built: no tenant identity or quota model.*
- Job sandboxing: runtime isolation for load generators (cgroups/containers). *Not built: the trust boundary is the authenticated, replay-checked frame; there is no runtime isolation.*
- Audit trails: immutable log of job lifecycle transitions and leadership changes. *Built: all job events plus `JobLeadershipAcquired` are journaled in the job ledger.*

## Testing & Validation
- Chaos suite: automated kill/restart of gates/managers/workers to verify recovery. *Built: VOPR, chaos and multiprocess SIM suites under `tests/simulation/` and `tests/unit/simulation/`.*
- Synthetic large‑scale tests: simulate 10–100× fanout jobs with backpressure validation. *Built: `tests/unit/simulation/sim/test_multiprocess_fanout.py`, `test_multiprocess_rejection_storm.py`.*
- Compatibility tests: version skew + rolling upgrade scenarios. *Built at the wire level: `tests/unit/distributed/models/test_rolling_upgrade_wire_compatibility.py` (104 messages, both directions); no mixed-version multi-process scenario.*
