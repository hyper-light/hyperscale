# Simulation Scenario Taxonomy

Phase-2 closure handed us a working harness that exercises lifecycle (start
→ stabilize → teardown) and a single happy-path workload (submit → dispatch
→ execute → result → client callback). This document enumerates the
scenarios the harness must grow to cover, organised by the subsystem each
scenario actually exercises.

Every scenario is a place a real bug can hide in production. The taxonomy
is exhaustive on purpose — partially-implemented coverage is worse than
none, because it hides which failure modes are still untested.

The phases referenced below are from
[`docs/dev/simulation_framework.md`](dev/simulation_framework.md). Some
scenarios need fault primitives that ship in later phases; those are
flagged with `(Phase N)` in their headings.

**Coverage status (checked 2026-10-06).** This is a taxonomy of what must be
tested, not a list of what is. Where a section's coverage or the code's
behavior is known, a *Status* line says what exists; the full regrade is
`docs/REMAINING_LEDGER.md` (dev-docs ledger, D-90 to D-95).

---

## 1. Leadership / Election

Hyperscale runs SWIM-tier leader election (`LocalLeaderElection`) on
managers and gates. Some clusters also run Raft for log consensus. These
scenarios exercise both layers.

- **Graceful step-down.** Leader.stop() while followers up. Assert: at
  most one leader at any moment (safety); new leader elected within one
  election cycle (liveness).
- **Hard kill of leader (Phase 3).** SIGKILL — no graceful handoff.
  Lease expires, peers detect via SWIM, election after suspicion timeout.
  Assert: no split-brain in the dead window.
- **Pause / resume of leader (Phase 3).** SIGSTOP. Peers elect new
  leader; on resume, old leader observes higher term and steps down.
  Assert: old leader never re-acquires leadership without re-election.
- **LHM-driven step-down.** Pump `_local_health.score` past
  `max_leader_lhm`. Assert: steps down only when `member_count > 1`
  (regression for the guard added in commit `df16ffbc`).
- **Concurrent candidates.** Two managers start `_run_election` at the
  same term. Exactly one becomes leader.
- **Pre-vote during stable lease.** Follower starts pre-vote while a
  healthy leader holds the lease. Peers reject. No term bump.
- **Flapping detection.** Force repeated election failures; assert the
  flapping detector backs off.
- **Term exhaustion.** Synthetic, near `MAX_TERM`. Election bails cleanly.
- **Election during dispatch.** Submit a job, kill the leader before
  dispatch completes. Workflow completes via failover dispatch — no
  duplicate, no loss.
- **Job-leadership transfer (gate tier, L3).** Gate that owns a job
  dies; peer takes over. Callback addresses preserved, fence token
  continuity maintained.
- **SWIM-leader vs Raft-leader divergence.** Induce state where SWIM
  and Raft pick different nodes. Assert reconciliation, or document why
  the two layers intentionally differ.

## 2. Node failure / rejoin (Phase 3)

- **Worker dies mid-dispatch (TCP RST mid-`workflow_dispatch`).** Manager
  redispatches to a peer worker.
- **Worker dies post-ack, pre-execute.** Orphan reclaim path.
- **Worker dies mid-execute.** Cancellation propagation, sub-workflow
  re-dispatched or job marked failed.
- **Worker dies post-execute, pre-result-push.** Result lost; manager
  job-leader detects and either redispatches or surfaces failure (the
  recovery path the worker WAL is supposed to backstop).
- **Worker rejoins with same incarnation.** Stale state on manager.
  Worker refutes via incarnation bump.
- **Worker rejoins with new incarnation.** Manager clears suspicion,
  accepts re-registration cleanly.
- **Worker disappears permanently.** Assigned workflows fail-over or
  fail with reason; resources released.
- **Manager (follower) dies.** Leader continues; new follower joins,
  catches up via `manager_state_sync_request`.
- **Manager (leader) dies.** Re-election + workflow leadership transfer.
- **Quorum loss.** Kill 2 of 3 managers: writes blocked, no split-brain
  accepts. Liveness regained when one returns.
- **All managers die.** Workers and gates degrade gracefully; new
  cluster forms when a manager returns.
- **Gate dies (L3).** DC routing fails over to peer gate; client retries
  with `leader_hint`.
- **Cascade.** Kill manager A → first action of B-after-promotion makes
  C crash. Assert the failure detector doesn't shed too aggressively.

## 3. Network conditions (Phase 4)

Need transport injection (`FaultInjectingTransport`) wrapping
`send_tcp` / `send_udp`.

- **Fixed latency** per link (e.g., 50 ms cross-DC).
- **Latency drift.** Gradually increase RTT; assert Vivaldi tracks and
  probe timeouts adapt via LHM.
- **Asymmetric latency** (A → B fast, B → A slow). Probe ack window
  must be sized correctly.
- **Jitter** around a mean.
- **Packet drop** (uniform 1 % / 5 % / 20 %). SWIM still converges;
  Lifeguard retransmits compensate.
- **Drop bursts.** Full loss for 200 ms, then resume. No false-positive
  DEAD declarations.
- **Bandwidth cap.** Saturation under heartbeat + gossip + state-sync.
- **Reordering** (especially UDP). `_replay_guard` / `message_id`
  dedup must hold.
- **Duplicate delivery.** No double-counted ACK, no double-execute on
  retried dispatch.
- **TCP mid-stream RST.** Server re-establishes or fails the request
  cleanly.

## 4. Partitions (Phase 4)

- **Symmetric two-way.** A ↔ B blocked. Quorum side keeps writing;
  minority must not.
- **Asymmetric.** A → B works, B → A doesn't. Tests one-way SWIM probe
  + indirect-probe ack path.
- **Three-way.** True multi-way split.
- **Flapping.** Heal then break repeatedly. Audit log captures every
  transition; no stuck-suspect.
- **Cross-DC partition (L3).** One DC isolated. Surviving DCs continue;
  isolated DC marks itself disconnected.
- **Gate-tier partition.** Gates split; both halves may have client
  connections.
- **Healing.** Partition resolves: membership reconverges, suspicions
  clear, work resumes.
- **Quorum-isolating partition.** Minority side rejects job submits
  with a clear error, never silently queues.

## 5. Time / clock anomalies (Phase 5)

Need `Clock` injection.

- **Per-node clock skew.** Each at slightly different real time. Lease
  comparisons still work.
- **Clock jump forward.** Lease appears expired prematurely.
- **Clock jump backward.** Lease appears valid longer than it should —
  fence-token security risk.
- **VM pause.** Long sleep mid-RPC; on resume, all timers fire at once.
- **Lease boundary races.** Lease expires *exactly* as heartbeat lands.

*Status:* covered — forward/backward step at a lease boundary
(`tests/unit/simulation/sim/test_multiprocess_lease_clock_step.py`), skew
fencing (`test_multiprocess_clock_fence.py`), VM pause
(`test_multiprocess_pause.py`). Monotonic drift is a documented skip.

## 6. Membership churn

- **Registration storm.** 50 workers register within 1 s. Manager
  handles without dropping.
- **Graceful scale-down.** 50 workers leave over 10 s. Orphan workflows
  reassigned.
- **Mass crash.** 50 workers die at once (Phase 3). Pool degrades; jobs
  fail with reason.
- **Slow churn.** 1 worker every 10 s for 5 min. Tests aggregate gossip
  load.
- **Beyond cap.** Register past `MAX_WORKERS_PER_MANAGER` (if a cap
  exists). Clean rejection.

## 7. Workload patterns

- **Burst.** 100 jobs in 1 s. Tests load shedder, idempotency, dispatch
  fairness.
- **Sustained.** 10 jobs/s for 60 s. Steady-state queueing.
- **Staggered.** Submissions at offsets across multiple gates
  concurrently.
- **Long-running** (workflow runs minutes). Tests progress reporting,
  mid-flight recovery, AD-26 deadline extension.
- **Mid-flight cancel.** Client `cancel_job` while workflow executing.
  Worker cancels, releases cores, manager records.
- **Cancel during election.** Cancel arrives at old leader after
  step-down. Must redirect to new leader.
- **Dependency chains.** A → B → C. Kill mid-chain. B's failure mode
  propagates or recovers cleanly.
- **Cross-DC dependencies (L3).** Workflow B in DC-east depends on A
  in DC-west.
- **Idempotent resubmit.** Same `idempotency_key` twice; collapses to
  one execution.
- **Submit during election.** Should get retry hint, not silent loss.
- **Submit during partition.** Minority manager refuses with
  `leader_hint=unknown`.
- **Adversarial workflows.** Panic, infinite loop (timeout-killed),
  giant memory allocation (OOM-killed).

*Status:* built. 100-job instant burst
(`tests/unit/simulation/sim/test_multiprocess_fanout.py`), dependency chains
and dispatch exhaustion (`test_multiprocess_workflow_lifecycle.py`), mid-flight
cancel (`test_multiprocess_job_cancellation.py`), submit during a blackout
(`test_multiprocess_l2_submission_blackout.py`), long-running with AD-26
extension (`test_multiprocess_l2_extension.py`). Sustained 10 jobs/s for 60 s
(`test_multiprocess_sustained_submission.py`: paced from the first acceptance,
every sojourn within two rounds, in-flight within Little's bound, drained
tables independent of the job count). Staggered starts through three gates
(`test_multiprocess_staggered_submission.py`: offsets anchored on the first
acceptance, all gates in flight at once, exactly once). Cross-DC chain
(`test_multiprocess_cross_dc_chain.py`): a job's workflows are placed
together, so B runs in another datacenter than its A only when A's
datacenter is lost between them -- AD-36 moves B to the replacement and
re-runs A there for context alone; A's counted result stays the lost
datacenter's. Adversarial workflows (`test_multiprocess_adversarial_workflows.py`):
a raising step fails its job; a step that swallows every cancellation is
ended by the job's AD-34 timeout; a hog is killed by AD-41 at its memory
budget (SIM scripts the hog's memory at the worker: executor monitors do
not run on the SimulationLoop) -- each FAILED with its cause named, and the
next job completes within the cancellation windows. Open (core/jobs): a
raised step's error reaches the client only as "No results returned"
(strict xfail in that file).

## 8. Resource pressure / pool fidelity

- **CPU saturation.** Pump synthetic CPU. LHM rises. Leader steps down
  (where applicable). Probes adapt.
- **Memory pressure.** Trigger graceful degradation; load-shedder
  rejects with backpressure level.
- **Worker subprocess crash.** PoolExecutor child dies. Worker reaps;
  redispatches or fails sub-workflow.
- **Worker subprocess hang.** Child alive but unresponsive. Worker
  enforces timeout.
- **Event-loop lag injection.** Synchronous CPU work in main loop;
  `EventLoopHealthMonitor` LHM penalty fires.

## 9. Adversarial messages

- **Replay.** Resend an old message; `ReplayGuard.validate_frame` drops
  it (every TCP/UDP frame carries a Snowflake frame id inside the AES-GCM
  body; duplicates are keyed on the nonce, 2026-10-06).
- **Wrong cluster_id / environment_id.** `WorkerRegistration` rejected
  (AD-28).
- **Wrong mTLS claims.** `RoleValidator.validate_claims` rejects.
- **Stale fence_token.** Worker rejects; manager re-dispatches with
  fresh token.
- **Oversized message.** `MAX_UDP_PAYLOAD` enforcement.
- **Malformed pickle.** `RestrictedUnpickler` rejects with
  `SecurityError`.
- **Mixed protocol versions.** Older worker, newer manager. Capability
  negotiation does the right thing.

*Status:* covered — replay `tests/unit/distributed/protocol/test_frame_replay_protection.py`;
malformed pickle `tests/unit/distributed/messaging/test_restricted_unpickler_vopr.py`;
mTLS claims `tests/unit/distributed/discovery/test_mtls_strict_claims.py`;
oversized/malformed frames `tests/unit/distributed/protocol/test_frame_decoding_vopr.py`;
wrong cluster `tests/unit/simulation/sim/test_cluster_mismatch_vopr.py`;
versions `tests/unit/distributed/models/test_rolling_upgrade_wire_compatibility.py`
(all 104 wire messages, both directions) and `tests/unit/distributed/protocol/test_version_skew*.py`.

## 10. Persistence / recovery

- **Manager restart with WAL replay.** In-flight job state recovered.
- **Idempotency ledger across restart.** Dedup survives.
- **Incarnation persistence across restart.** Node returns with
  monotonically-greater incarnation (prevents zombie peers).
- **Raft snapshot + log truncation.** Follower behind > snapshot
  threshold catches up via snapshot, not log.
- **WAL corruption.** Detected at startup; node refuses to come up.

*Status:* the first four are covered (`tests/unit/distributed/ledger/test_wal_reclamation.py`,
`tests/unit/distributed/idempotency/test_manager_ledger_recovery.py`,
`tests/unit/distributed/swim/test_incarnation_persistence_degraded.py`,
`tests/unit/distributed/raft/test_raft_snapshot_install.py`). WAL corruption
is met (2026-10-06, b75eb7ea): `NodeWAL` cuts only a torn tail (nothing
written after the damaged frame) and refuses anything else, logging
`WALUntrustworthy` and raising `WALUntrustworthyError` out of node start
with the file left as found (`hyperscale/distributed/ledger/wal/node_wal.py`,
AD-38 Part 3.2); the Raft store applies the same torn-last-frame rule and
sets an untrustworthy disk aside (D1). Test:
`tests/unit/distributed/ledger/wal/test_node_wal_damage_vopr.py`.

## 11. Continuous safety invariants

These run every `invariant_poll_interval` (default 100 ms) for the entire
lifetime of any scenario above. Any violation fails the scenario
immediately, regardless of which fault path is being exercised.

*Status (2026-10-07):* every `ClusterHarness` scenario runs the catalog
(`continuous_catalog()` in `tests/simulation/harness/invariants.py`; checks in
`tests/simulation/harness/invariant_checks/`) every
`HarnessTimeouts.invariant_poll_interval` (0.1 s). Each item below names its
check; where the item as written is wrong for a correct cluster, the check's
module records why and what it checks instead. Mutation checks:
`tests/unit/simulation/harness/test_continuous_invariants.py`; live
evaluation: `tests/simulation/scenarios/l2_single_dc/test_continuous_invariant_catalog.py`.
"At most one leader per DC" is not continuous (the VOPR oracles judge leader
exclusivity post-hoc, `tests/simulation/oracle/cluster_trace_oracle.py`).

- Job leaders: `AtMostOneJobLeaderPerJob`.
- Fence tokens: `MonotonicFenceTokens` -- manager lease and dispatch tokens,
  worker accepted tokens, gate tokens, against a per-instance high-water mark.
- Sub-workflow tokens: `UniqueSubWorkflowTokens` -- no token runs on two
  workers, the worker running it is the one it names, no job lists it twice.
- Terminal reach: `JobMakesProgress` -- a job with work in flight progresses
  within AD-34's stuck bound (`stuck_threshold` + AD-26 extension seconds +
  `JOB_TIMEOUT_CHECK_INTERVAL`), past which its leader must time it out.
- Cancelled cores: `CancelledJobsFreeCores` -- within the worker's own
  cancellation bound (poll interval + query timeout + cancel wait + one
  execution-update wait, from `WorkerConfig`), counted only while no network
  fault or pause is in force and the job's leader is live.
- Resource counters: `ResourceCounterConsistency` -- on the worker the bound
  is an identity (free + assigned cores = total; `available_cores` caches
  the free count); on a manager, reserved cores are still inside the
  reported available count, so `available + reserved` may exceed the total
  there and the check is that each stays within `[0, total]`.
- Member counts: `MemberCountConvergence` -- per datacenter's managers and
  across gates, once stabilized and with no view-splitting fault in force,
  within one gossip dissemination: `(max(1, int(lambda * ln(n + 1))) + 1)`
  protocol periods plus one probe timeout.
- Cluster isolation: `ClusterIdIsolation` -- one `CLUSTER_ID` across the
  nodes, and every SWIM member any node holds is a node of the cluster.


- **At most one leader per DC** at any moment.
- **At most one job-leader per job** at any moment.
- **Fence tokens per job monotonically increase.**
- **Sub-workflow tokens are unique** per `(job, workflow)` pair.
- **Acknowledged jobs reach a terminal state** (`completed`, `failed`,
  `cancelled`) — no indefinite `RUNNING`.
- **Cancelled jobs free worker cores within budget.**
- **Resource counter consistency.** `available + reserved ≤ total`
  for every worker.
- **Member-count convergence.** All observers agree within bounded
  gossip rounds of any membership change.
- **Cluster-ID isolation.** No node ever sees membership from a
  different `cluster_id`.

## 12. Observability hooks the harness must expose

- **Per-event audit.** Election, leadership change, partition, workflow
  assignment — so post-hoc you can answer *what happened*.
- **On-demand snapshot dumper.** Capture cluster state at any point,
  not just on timeout.
- **Per-scenario seed.** Same seed → same fault sequence. Lays
  groundwork for SIM-mode replay (Phase 6).

---

## Roadmap

| Phase | Unblocks |
|-------|----------|
| 3 | kill / restart / pause / resume primitives → categories 1, 2, 6, 8 |
| 4 | transport injection → categories 3, 4 |
| 5–6 | clock / random injection + SIM mode → categories 5, 12, replayable runs |

Phases 3 and 4 alone get roughly 70 % of the bug-finding value. Phases 5
and 6 are for replayability and exhaustive determinism.

## Where to start (first 5 high-value scenarios)

1. **Leader killed mid-dispatch** → workflow completes via failover
   (covers 1, 2, 11).
2. **Worker crash mid-execute** → sub-workflow re-dispatched or
   job-fails-with-reason (2, 7).
3. **Symmetric two-way partition with ongoing workload** (4, 11).
4. **Latency injection on cross-DC links during multi-DC submit**
   (3, 4, 7).
5. **Burst submit storm with idempotency keys** (7, 11).

Each exercises 3+ subsystems and stresses multiple invariants
simultaneously.
