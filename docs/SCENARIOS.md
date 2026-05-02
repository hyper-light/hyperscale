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

- **Replay.** Resend an old message; `_replay_guard` drops.
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

## 10. Persistence / recovery

- **Manager restart with WAL replay.** In-flight job state recovered.
- **Idempotency ledger across restart.** Dedup survives.
- **Incarnation persistence across restart.** Node returns with
  monotonically-greater incarnation (prevents zombie peers).
- **Raft snapshot + log truncation.** Follower behind > snapshot
  threshold catches up via snapshot, not log.
- **WAL corruption.** Detected at startup; node refuses to come up.

## 11. Continuous safety invariants

These run every `invariant_poll_interval` (default 100 ms) for the entire
lifetime of any scenario above. Any violation fails the scenario
immediately, regardless of which fault path is being exercised.

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
