# TigerBeetle-Grade Rigor Checklist — hyperscale VOPR/SIM harness

Gap analysis of this repo's deterministic simulation harness against the
TigerBeetle VOPR methodology. Every item is mapped onto the repo:

- **(a) COVERED** — already exercised; the file/test is named.
- **(b) COVERABLE** — existing coordinator/entry primitives suffice; the exact
  recipe is given.
- **(c) NEW CAPABILITY** — a minimal harness addition is described.

Priorities: **P0** = required for the gate/multi-DC rigor program to make its
claims; **P1** = closes a real fault-class hole; **P2** = completeness.

(Restored into the repo after the original scratchpad copy was lost to a
tmp wipe — program-specification documents live in version control.)

Repo anchors used throughout:

- Coordinator: `tests/simulation/harness/sim/multiprocess/simulation_coordinator.py`
  (`schedule_kill/partition/drop_rate/delay/duplicate/restart`, per-datagram
  fault chokepoint `_enqueue`, seeded fault RNG, `PYTHONHASHSEED` pin,
  restart generations under `{pid}.gen{n}`).
- Child runtime: `.../multiprocess/child_runtime.py` (seam swaps, SNAPSHOT
  power-loss protocol, `determinism-audit-unswapped` emission).
- Disk model: `tests/simulation/harness/sim/sim_filesystem.py`
  (`set_slow_disk`, `set_disk_full`, `set_fsync_reorder`, `crash()`,
  `dump_durable`/`restore_durable`).
- VOPR: `tests/simulation/vopr/{fault_plan,vopr_runner,test_vopr}.py`
  (seed → plan → run → invariants → byte-identical replay; the shared
  `--sim-replay` lives in `tests/simulation/conftest.py`).
- Oracle: `tests/simulation/oracle/job_status_oracle.py` (client-observed
  history linearization; unit-tested in
  `tests/unit/simulation/oracle/test_job_status_oracle.py`).
- Scenario pins: `tests/unit/simulation/sim/test_multiprocess_*.py`.
- Design doc: `docs/dev/simulation_framework.md` §11–13, Phase 7.

---

## 0. TigerBeetle VOPR methodology, distilled

What the bar actually is (from their simulator design, restated as testable
properties):

1. **Fault taxonomy, saturating**: network (partition incl. asymmetric,
   loss, reorder, duplication, delay), storage (torn writes, MISDIRECTED
   reads/writes, CORRUPTED reads/writes a.k.a. bitrot, sector faults, disk
   latency), process (crash, restart-from-surviving-disk, PAUSE/RESUME).
   Faults are drawn with high probability and in COMBINATION — the simulator
   saturates the schedule with overlapping faults rather than injecting one
   calibrated event at a time.
2. **Safety vs liveness split**: safety (strict serializability of the
   client-observed history, replica state-machine equality) is checked
   CONTINUOUSLY, under any fault density — it must hold even mid-chaos.
   Liveness is only demanded AFTER faults cease: heal partitions, restart
   crashed replicas, stop injecting — then the cluster must CONVERGE
   (all replicas identical, all pending work resolved) within a bounded
   number of ticks. This split is what lets fault density exceed "survivable
   by design" without unfair test failures.
3. **State-checker oracle**: every replica's state machine is compared
   (commit-by-commit hash equality), not just the client's view. Divergence
   is caught at the transition, with the seed as reproducer.
4. **Swarm testing**: enormous numbers of seed-randomized schedules
   (fleet-scale, continuous), each seed byte-reproducible forever; failing
   seeds are automatically filed. Cluster parameters and workload shape are
   ALSO seed-drawn, not fixed.
5. **Long horizons**: hours-equivalent of simulated time per seed, at
   hundreds-of-times real-time speedup — enough for repair, view changes,
   and slow leaks to manifest.
6. **Completeness philosophy**: "if it can happen in production, the
   simulator must be able to produce it." Every exclusion is a documented,
   justified impossibility (not an inconvenience).
7. **Checker canaries**: the harness proves it can FAIL — deliberately
   broken invariants/mutations must be caught by the oracles.
8. **Client faults are first-class**: clients crash/restart and their
   sessions are part of the replicated state machine (request numbers +
   reply cache = exactly-once per session); the request/reply history is
   verified strictly serializable across client faults.

---

## A. Network fault taxonomy

**A1. Symmetric partition + heal — (a) COVERED — P0 (keep)**
- Invariant: no false-DEAD inside detection bound; membership converges
  post-heal; jobs ride out sub-detection cuts.
- Evidence: `test_multiprocess_network_faults.py::test_partition_detected_and_membership_recovers_after_heal`
  (cut 20→110s, witness-less design bound `25 ≤ latency ≤ 85` asserted, not a
  wide window); VOPR draws survivable partitions (6–16s heals).

**A2. Asymmetric (one-way) partition — (b) COVERABLE — P0**
- Invariant: one-way silence must not split-brain leadership; SWIM
  suspicion/refutation handles hearing-but-unheard peers; job path survives
  or fails loudly.
- Gap: `schedule_partition(..., bidirectional=False)` exists in the
  coordinator but NO committed test and NO VOPR plan ever draws it.
- Recipe: gate-tier scenario cutting only `gate-a → gate-b` while
  `b → a` flows; chaos-plan generator (E1) draws a direction bit per
  partition event.

**A3. Partial partitions / islands (3+ node topologies) — (b) COVERABLE — P1**
- Invariant: majority island retains/regains leadership; minority island
  never acts on stale leases; healing merges membership without zombie
  state.
- Recipe: pairwise `schedule_partition` calls among `sim-gate-a/b/c`
  (entries: `gate_cluster_demo.gate_tier_entry`) — e.g. isolate one gate
  from both peers but not from its DC managers. Constraint from the traced
  SWIM decomposition: 2-node islands run WITNESS-LESS detection (~64s+ —
  see memory note + `test_multiprocess_network_faults.py` docstring), so
  partition windows must be sized against the witness-count of the island,
  not one global number.

**A4. Probabilistic packet loss — (a) COVERED / (b) density — P1**
- Covered: `test_multiprocess_network_faults.py::test_job_completes_through_packet_loss`
  (10% both directions); VOPR draws 5–25%.
- Gap vs TB: they saturate (up to ~30–50%+ on links). Recipe: chaos-window
  plans (E2) draw 30–90% loss DURING chaos only; safety oracles still hold;
  completion demanded only post-quiesce.

**A5. Added delay + jitter — (a) COVERED**
- `test_multiprocess_network_faults.py::test_job_completes_through_delay_and_duplication`;
  VOPR draws 20–80ms + jitter. Coordinator guarantees additivity (lookahead
  preserved).

**A6. Reordering — (b) COVERABLE — P1**
- Invariant: no protocol step depends on datagram arrival order (gossip,
  probe acks, status pushes); the client's `JobStatusApplier` ordering guard
  absorbs out-of-order pushes.
- Today: reorder only arises implicitly when delay-jitter exceeds inter-send
  gaps; nothing asserts it happened.
- Recipe: `schedule_delay(src, dst, extra_seconds=0.0, jitter_seconds=0.2)`
  over a busy window reorders same-link datagrams deterministically (seeded
  jitter draws). Pair with the status-seen oracle (G1) which catches any
  order-sensitivity as a rank regression. Optional sugar: a
  `schedule_reorder` alias — not required.

**A7. Duplication — (a) COVERED**
- Same test as A5; VOPR draws 20–60%. Dedup taxonomy (gossip dedup cache,
  identity-idempotent control messages) exercised end to end.

**A8. On-wire corruption (bit flips) — (c) NEW CAPABILITY — P2**
- Invariant: corrupt frames are REJECTED (auth/parse), never half-applied;
  a flipped datagram is indistinguishable from loss at the protocol level.
- Mechanism: a `schedule_corrupt(src, dst, probability, ...)` rule in the
  coordinator's `_enqueue` chokepoint flipping seeded bytes of `("dgram", …)`
  payloads (streams exempt — TCP checksums make delivered-corrupt frames a
  non-production schedule). Small, fully deterministic (fault RNG already
  exists). Low priority: loss (A4) already covers the drop-equivalent
  outcome; this only adds coverage of the reject-path itself.

**A9. Fault scoping honesty — (a) COVERED (design)**
- Executor-pool pipe IPC is exempt from WAN faults (same scoping as REAL
  mode); stream frames are never probabilistically dropped (TCP masks loss;
  partitions/delay do apply to streams); dial-to-nowhere hangs and surfaces
  via production connect timeouts. All documented at the chokepoints
  (`simulation_coordinator.py` docstrings, `child_context.py`). This is the
  "documented, justified exclusions" bar — keep it enforced in review.

---

## B. Storage fault taxonomy

**B1. Slow disk (latency) — (a) COVERED — P1 to extend placement**
- `SimFilesystem.set_slow_disk` charges VIRTUAL time per op;
  `test_storage_faults.py`; VOPR draws windows (5–40ms ops). Invariant:
  bounded storage delay must be ridden THROUGH to completion (slow_disk is
  NOT stranding-capable in `vopr_runner.check_invariants`).
- Extension: today only the MANAGER arms storage schedules
  (`worker_manager_demo.apply_storage_fault_schedule` + `manager_entry`
  arg). That currently equals the whole durable surface (gates have no
  durable tier; workers run no WAL) — revisit at Phase 8.

**B2. Disk full (ENOSPC) — (a) COVERED**
- `set_disk_full` byte budget; VOPR draws 256–2048-byte budgets armed in
  [8, 20); invariant: explicit client-observed rejection or loud terminal —
  silence is always a violation (`vopr_runner.check_invariants`, the
  `submit-rejected` acceptance path). This half found the dormant-NodeWAL
  bug — the pattern works.

**B3. Torn/reordered writes on power loss — (a) COVERED**
- `set_fsync_reorder(seed)` keeps a seeded SUBSET of un-fsynced segments and
  tears the last survivor; exercised through `schedule_restart(...,
  fsync_reorder_seed=…)`:
  `test_multiprocess_manager_restart.py::test_job_resumes_through_fsync_reordered_crash_debris`
  + VOPR restart events (half carry a reorder seed). WAL/ledger recovery is
  truncation-safe past CRC-fail debris (`test_storage_faults.py`).

**B4. Corrupted READS (bitrot detected at read time) — (c) NEW — P1**
- Invariant: a CRC-failing read is a LOUD failure or a clean
  recovery-truncation — never silently-applied wrong state. TB treats this
  as core: a replica must detect local corruption and (in their world)
  repair from peers; our single-durable-node analog is detect-and-fail-loud.
- Gap: CRC paths are only exercised against CRASH debris (recovery-time).
  Nothing corrupts bytes returned by `read_bytes`/`read` at RUNTIME
  (checkpoint loads, ledger reads, incarnation store reads).
- Mechanism: `SimFilesystem.set_read_corruption(seed, probability,
  path_glob=None)` — flip seeded bytes in read results (not stored state) —
  armed via the existing `storage_fault_schedule` entry-arg pattern
  (extend the schedule vocabulary in a NEW demo entry file, coordinator
  untouched). Invariant wiring: same loud-outcome rule as disk_full.

**B5. Misdirected reads/writes — (c) NEW — P2**
- Invariant: a write landing in the wrong file / a read returning a
  neighbor's bytes is caught by record framing + CRC + file-format headers,
  never interpreted as valid foreign state.
- Mechanism: `SimFilesystem.set_misdirect(seed, probability)` — on a seeded
  draw, apply a write's segment to a seeded SIBLING path (same parent dir),
  or satisfy a read from one. Justification for P2: our layout is
  file-per-purpose (WAL segments, submissions dir, incarnation file), so the
  class is narrower than TB's raw-sector world — but WAL-segment confusion
  is still production-possible (kernel/FS bugs) and cheap to model.

**B6. Transient I/O errors (EIO on read/write) — (c) NEW — P2**
- Invariant: an EIO is either retried or escalates loudly; never a silent
  skip. Mechanism: `set_io_error(seed, probability, window)` raising
  `OSError(5)` from `_charge_operation`. Same entry-arg arming pattern.

**B7. Gate durable tier — BLOCKED (Phase 8) — P1 to pin**
- Gates have no WAL/ledger/resume; a restarted gate forgets its jobs.
  Scenarios must assert today's LOUD outcome (client observes
  terminal/rejection or a documented gap) and pin aspirational invariants
  with `@pytest.mark.skip(reason="Phase 8: gate durable tier")`. Do NOT
  write failing tests.

**B8. Storage fault DURING recovery — (b) COVERABLE — P1**
- Invariant: gen-2 recovery (WAL replay + submission resume) completes
  through a slow disk; disk_full during recovery degrades to the durable-
  FAILED truth-telling path, never a wedged `start()`.
- Recipe: `schedule_restart("manager", R, down_seconds=D)` plus a
  `storage_fault_schedule` entry with `at_time < R` and `until_time > R+D`:
  gen-2 replays the SAME entry args, `loop.call_at` with a past `at_time`
  fires at boot, so the knob is armed for the whole recovery. Probe first
  to confirm the slow-disk window straddles replay; note the disk_full byte
  budget re-arms FRESH per generation — pick budgets accordingly.

---

## C. Process fault taxonomy

**C1. Crash (SIGKILL, incl. whole-host death) — (a) COVERED**
- `test_multiprocess_worker_kill.py` (worker + both executors at one
  instant; detection bound `20 ≤ latency ≤ 70` asserted; job fails LOUDLY),
  `test_multiprocess_executor_kill.py` (pool child dies; production
  exit-code path fails the workflow), `test_multiprocess_worker_retry.py`
  (kill + late-joining worker → retry → completion). VOPR draws executor
  kills.

**C2. Crash + restart from surviving disk (power loss) — (a) COVERED (manager)**
- `schedule_restart` (power loss, volatile writes lost, reboot from durable
  disk, generations under `{pid}.gen{n}`);
  `test_multiprocess_manager_restart.py` (resume + completion across
  restart, both fsync-reorder flavors, byte-identical replay); VOPR draws
  restarts (manager only). Client outcome exactly-once, execution
  at-least-once — asserted.

**C3. Gate restart — (b) COVERABLE — P0 (program mission)**
- Invariant TODAY (no durable tier): a restarted gate's in-flight jobs end
  in a client-observed terminal/rejection or a DOCUMENTED gap — never
  silence. Aspirational (skip-pinned): job survives gate restart (Phase 8).
- Recipe: gates are leaf processes (no spawned children) —
  `schedule_restart("sim-gate-b", at, down_seconds=…)` works mechanically
  with `gate_tier_entry`. Also assert peer gates re-admit the rebooted gate
  (peer-count milestones) and leadership re-converges (stability window,
  never a t=X snapshot).

**C4. Worker restart — (b) COVERABLE with care — P1**
- The coordinator refuses restarting a process with live spawned children.
  But kills apply BEFORE restarts at the same instant
  (`_drive`: `remaining_kills` drains, then `remaining_restarts`), so:
  `schedule_kill("executor-…-9009", t)`, `schedule_kill("executor-…-9011",
  t)`, `schedule_restart("worker", t)` — the live-children check passes and
  gen-2 respawns its pool (dead executors left `connections`, so re-used
  executor ids don't collide). PROBE FIRST; if any wrinkle appears, this
  becomes (c) "cascade restart" — but restarting gates/managers is the
  sanctioned surface, so treat worker restart as optional depth.

**C5. Pause/resume (SIGSTOP-style freeze) — (c) NEW CAPABILITY — P0**
- Invariants protected — the classic ones this fault class exists for:
  - a paused node is declared dead within the detection design bound
    (same brackets as kill);
  - a RESUMED former leader (gate leader / manager Raft leader) must NOT
    act on stale leadership: fencing tokens / monotonic leases reject its
    writes; no double-dispatch, no split-brain — this is the sharpest test
    of the dedup-class-separation + monotonic-lease fixes the gate stack
    just hardened;
  - the resumed node rejoins via incarnation bump; membership converges.
- Today: REAL-mode `tests/simulation/harness/fault_matrix.py::pause/resume`
  exists (cancels loops), but the multiprocess SIM coordinator has NOTHING —
  and SIM is where the schedule is deterministic enough to catch the race.
- Minimal mechanism (coordinator-only; children untouched):
  `schedule_pause(process_id, at_time, resume_time)` — during
  `[at, resume)`, the coordinator (1) excludes the child from GRANTs (it
  blocks at its barrier — a real freeze), (2) buffers the child's due
  deliveries in a per-victim queue instead of delivering, (3) EXCLUDES the
  victim's `next_times` entry from the global `min()` so time advances
  without it, then at `resume_time` grants a window at the current global
  time with ALL buffered deliveries — the child's timers fire "late" in a
  burst, exactly the thawed-process semantics. Determinism is free (the
  schedule is data). Datagram-buffer realism knob (drop past N queued
  datagrams, keep stream frames) can come later.

**C6. Repeated crash/restart cycles (crash DURING recovery) — (b) COVERABLE — P1**
- Invariant: recovery is idempotent — a second power loss mid-replay/mid-
  resume never double-delivers a client result and never loses the durable
  truth (ledger record vs submission payload ordering already has a loud
  degraded path).
- Recipe: two `schedule_restart("manager", …)` events with
  `R2 ≥ R1 + down1` (a restart landing INSIDE the down window targets an
  unknown pid and raises — generators must sequence). Draw `R2` shortly
  after gen-2 boot so the second crash hits recovery itself. VOPR today
  caps at ONE restart per plan (`fault_plan.py` dedups `restart`) — the
  chaos generator (E1) lifts this.

---

## D. Clock faults

**D1. Per-node wall-clock skew (time() offset) — (c) NEW — P1**
- Invariant: HLC (`HybridLamportClock`) tolerates inter-node wall skew —
  causality never inverts in the ledger/WAL; deadline arithmetic never mixes
  skewed `time()` with `monotonic()` incorrectly; leases don't extend/expire
  on another node's clock.
- Today: `VirtualClock.time()` == `monotonic()` == loop time, and its
  docstring explicitly reserves skew as "a deliberate FaultMatrix capability
  addition, not a free-floating offset"
  (`tests/simulation/harness/sim/virtual_clock.py`).
- Minimal mechanism: `VirtualClock.set_wall_offset(delta_seconds)` — only
  `time()` (and a wall-`now` if added) returns `loop.time() + offset`;
  `monotonic()`/timers untouched (matching real NTP steps, which move the
  wall clock but not `CLOCK_MONOTONIC`). Armed per node at virtual instants
  via the entry-arg schedule pattern (`context.loop.call_at(at,
  clock.set_wall_offset, delta)` in a NEW demo entry). Lockstep coherence is
  unaffected — the loop's virtual time stays global.

**D2. Clock jumps (NTP step, backwards wall time) — (c) same knob — P1**
- Invariant: HLC never regresses (logical component absorbs the step);
  `LogicalIdGenerator` ids stay unique/monotone; nothing panics on a
  backwards wall read. Mechanism: D1's knob with negative/step deltas
  mid-run. Chaos plans can then draw `("clock_skew", node, at, delta)`.

**D3. Monotonic rate drift (fast/slow timers on one node) — SKIP (P2, documented)**
- Would require per-child time scaling inside `SimulationLoop`, breaking the
  lockstep window contract — high cost. C5 (pause) plus A5 (delay) cover the
  schedule-visible consequences of a slow node; document the exclusion.

---

## E. Fault density, combination, and the chaos-window pattern

**E1. Fault DENSITY (saturating, overlapping schedules) — (b) COVERABLE — P0**
- The single biggest philosophical gap. `fault_plan.generate_fault_plan`
  draws **0–3 events**, each dedup-limited (≤1 kill, ≤1 restart, ≤1
  slow_disk, ≤1 disk_full) and each individually calibrated survivable.
  TigerBeetle saturates: many simultaneous faults, kinds combined, on
  purpose.
- Recipe: a NEW generator (`chaos_plan.py` + runner + test; do not modify
  the existing calibrated corpus, it is the regression baseline): draw 5–15
  events across ALL links and kinds with overlapping windows. Two mechanics
  to respect:
  - same-kind rules on the same link are FIRST-MATCH-WINS in the
    coordinator (declaration order), so per-link windows of one kind must
    be disjoint (or accept first-match) — density comes from stacking
    KINDS and LINKS, which fully compose (partition + drop + delay +
    duplicate all evaluate per datagram);
  - kills/restarts interact with the topology rules (no restart with live
    children; restart-during-down-window raises) — the generator sequences
    them.

**E2. Chaos window → quiesce → verify convergence — (b) COVERABLE — P0**
- The TB pattern that makes E1 sound: constrain every generated fault to
  `[0, T_chaos]` (all `until_time`/`heal_time ≤ T_chaos`, restarts respawn
  by `T_chaos`), then a fault-free convergence budget until the ceiling.
  Invariant split:
  - SAFETY (whole run, any density): JobStatusOracle per job (G1), no
    unknown-vocabulary statuses, exactly-once results, audit-absence (G4),
    cross-node agreement (G3).
  - LIVENESS (post-quiesce only): every submitted job reaches a
    client-observed terminal state within the convergence budget;
    membership converges (survivor registries match live topology); gate
    DC-health returns `healthy`; a new job submitted AFTER quiesce
    completes (the "cluster is actually alive" probe — `two_job_client_entry`
    in `eviction_recovery_demo.py` is the pattern for post-fault
    submission).
- Everything needed exists: windows are parameters on every scheduling
  primitive; ceilings are free; late-joining capacity via
  `worker_entry(start_at=…)`.

**E3. Beyond-survivable faults during chaos — (b) COVERABLE — P0 (with E2)**
- Today every VOPR range is calibrated survivable BY DESIGN (partitions heal
  inside the ~38s detection window, drop ≤25%…). Under the E2 split this
  restriction lifts DURING chaos: partitions longer than detection bounds
  (death is then the intended outcome), 50–90% loss, kill+partition+
  disk_full stacked. Safety oracles must hold anyway; liveness is only
  demanded after quiesce. This is precisely how TB gets density without
  unfair failures.

**E4. Keep a viable core (fair liveness) — (b) design rule inside E2 — P0**
- TB's liveness checker only demands convergence when a sufficient "core"
  of replicas survives. Analog here, encoded as generator constraints:
  post-quiesce there must exist ≥1 live manager (restarts complete before
  `T_chaos`), ≥1 registered-able worker (never kill the LAST worker without
  a `start_at` late joiner before quiesce), and — for L3 — a gate quorum.
  Also honor the witness-count rule: islands of 2 run witness-less
  detection (~64s+), so the convergence budget must exceed the worst
  detection leg the topology can produce ([25, 85]s witness-less).

---

## F. Horizons and swarm scale

**F1. Long horizons — (b) COVERABLE — P1**
- Today: ceilings are 90–220s virtual (VOPR: 100s; restart test: 220s).
  TB runs hours-equivalent per seed. Long horizons catch what short ones
  cannot: slow leaks (memory/task/lock tables), lease/incarnation
  wraparound behavior, repeated detection/rejoin cycles, WAL growth and
  checkpoint interaction.
- Recipe: a soak variant of the VOPR (new test file, opt-in marker or
  `--sim-soak` option following the shared conftest pattern) with
  `max_virtual_time=1000–3600` and a multi-job client submitting work every
  ~30–60s so the horizon is OCCUPIED, plus faults recurring per E1. Wall
  cost scales with event count (0.25–0.5s watcher cadences dominate) —
  probe scenarios/minute first. Respect the 300s per-barrier wall deadman;
  never run concurrent heavy sims.
- *Status 2026-10-06: built* — `tests/simulation/soak/test_soak.py`, an
  1800-virtual-second horizon of sequential jobs with recurring faults,
  opt-in via `HYPERSCALE_SIM_SOAK=1`. Nothing longer than 1800 virtual
  seconds exists.

**F2. Swarm scale (seed count, continuous) — (b) COVERABLE — P0**
- Today: the default sweep is **4 seeds** (101–104), widened only manually
  via `--sim-vopr-count`. TB's methodology is thousands of seeds,
  continuously, failures auto-filed.
- Recipe: no new harness code — a nightly/weekly soak invocation
  (`uv run pytest tests/simulation/vopr -q --sim-vopr-count=500`) run
  SERIALLY (deadman constraint), plus the same knob on the chaos/gate
  corpora once they exist. Every failure is already a permanent reproducer
  (`--sim-replay=<seed>`); the missing piece is purely the standing job and
  a place to record failing seeds. Cheapest rigor purchase on this list.
- *Status 2026-10-06: partial.* The standing job exists: the nightly
  `vopr` job in `.github/workflows/ci.yml` (cron `0 7 * * *`, 180-minute
  cap) runs `uv run pytest tests/simulation --ignore=tests/simulation/lints`
  and uploads `tests/simulation/_artifacts/` for 14 days. It passes neither
  `--sim-vopr-count` nor `HYPERSCALE_SIM_SOAK=1`, so it runs the default
  4-seed sweep and skips the soak; failing seeds are kept only as those
  14-day artifacts.

**F3. Seed-randomized topology/workload parameters — (b) COVERABLE — P1**
- Today: the VOPR topology is FIXED (manager + 2-core worker + client) and
  the workload is one fixed 2s workflow. TB randomizes cluster size,
  request mix, etc. per seed.
- Recipe: the chaos generator draws from a topology menu using EXISTING
  entries — worker count (1–2), late-join `start_at`s, gate tier present
  (via `gate_tier_entry`/`multi_gate_manager_entry`), DC count (1–2) — and
  workload shape (K1/K2 parameters). All value-tuple plan events, replay
  contract unchanged.

---

## G. Safety oracles (the state checker)

**G1. Client-history linearization — (a) COVERED**
- `JobStatusOracle.check_client_log`: rank monotonicity (forward skips
  legal), absorbing terminals, finished/observed agreement, exactly-once
  result delivery. Wired into EVERY VOPR schedule and unit-tested.

**G2. Loud-outcome rule (no silent stranding) — (a) COVERED**
- Every schedule must produce a client-observed terminal or explicit
  rejection; silence is a violation regardless of faults
  (`vopr_runner.check_invariants`, including the disk_full-rejection
  acceptance path).

**G3. Cross-node state comparison — (b)+(c) — P0**
- THE oracle gap. TB compares every replica's state machine; we judge ONLY
  the client's view. Silent wrongness that never reaches the client —
  manager ledger recording `completed` while the client saw `failed`, a job
  executing in TWO DCs, two managers claiming the same job's leadership, a
  gate's job record disagreeing with its DC — is invisible to every current
  VOPR invariant. (One pinned test does a bespoke slice of this:
  `test_multiprocess_multi_dc.py` asserts exactly-one-DC execution.)
- Mechanism, no coordinator changes: all children share ONE coherent
  virtual timeline, so per-node milestone logs merge into a global trace.
  1. NEW demo entry files that also record node-side milestones: manager
     `("job-accepted", t)`, `("job-terminal", status, t)`,
     `("job-leader-acquired"/"job-leader-lost", t)`; worker
     `("workflow-executed"/"workflow-failed", workflow_name, t)`; gate
     `("gate-job-terminal", status, t)`. Values only — no snowflakes/node
     ids (replay contract).
  2. NEW oracle class (one class per file, `tests/simulation/oracle/`), e.g.
     `ClusterTraceOracle`: merges result rows by virtual time and checks —
     manager terminal == client terminal per job; at most one job-leader
     interval holder at any instant; workflow executions ≤ retries+1 and
     ≥1 for completed jobs; exactly one DC executes a single-DC job; gate
     and manager agree on the job's terminal.
  3. Wire into VOPR/chaos invariants exactly like `JobStatusOracle`.
- Given determinism, post-run trace checking is EQUIVALENT to TB's
  continuous checking: the trace is complete and replayable — a violation
  at any instant is in the log at that instant.

**G4. Determinism-audit absence — (a) COVERED (landed during the program)**
- `vopr_runner.check_invariants` now checks every process result row for
  `("determinism-audit-unswapped", ...)` entries. Replicate in every new
  suite's invariants.

**G5. Continuous checking cadence — (a) COVERED by design**
- Watcher tasks sample at 0.25–0.5s cadence into milestone logs; with G3's
  merged trace this is the continuous record. Document the equivalence
  argument (determinism ⇒ post-hoc == online) in the chaos runner docstring.

**G6. Checker canaries (the oracle can actually fail) — (b) COVERABLE — P1**
- TB deliberately verifies its checkers catch injected wrongness. Here:
  unit tests feeding synthetic violating histories exist for
  `JobStatusOracle`; extend the same style to `ClusterTraceOracle`
  (regressed manager/client disagreement, overlapping leadership intervals)
  and to the audit-absence helper (a synthetic result dict carrying the
  tag). Never canary via production-code mutation in committed tests.

---

## H. Liveness invariants

**H1. Convergence-after-quiesce as an EXPLICIT invariant — (b) — P0**
- Missing today as a named, reusable check; it exists only as ad-hoc
  assertions. Mechanism: E2's liveness set, expressed as a helper the chaos
  runner applies: for every job submitted before `T_chaos`, a terminal
  observation with `t ≤ T_chaos + budget`; membership milestones show
  survivors converged; post-quiesce probe job completes. Budgets derived
  from the traced detection bounds (H2), never wide windows.

**H2. Design-bound latency assertions — (a) COVERED, extend — P1**
- The house style is already TB-grade here: assert the MECHANISM's bounds —
  `[20, 70]`s evidence-accelerated, `[25, 85]`s witness-less — with the
  traced decomposition in the docstring. Extend the same discipline to
  gate-tier legs: leader re-election stability window after a gate
  pause/restart, DC-health reclassification bounds (measured: unhealthy at
  kill+[20,31]s via 30s heartbeat staleness / 10s period; recovery at
  heal+~8.5s), rejoin-after-heal bounds. Probe-then-pin.

**H3. Hang detection — (a) COVERED**
- Virtual runaway: `max_virtual_time` ceiling. Wall runaway: per-child
  300s barrier deadman naming the wedged child. Silent job stall surfaces
  as a missing terminal (G2).

---

## I. Determinism and reproducibility

**I1. Byte-identical replay — (a) COVERED**
- Every scenario runs twice and asserts full result-dict equality
  (including `.genN` generations and every virtual timestamp); the VOPR
  does it per seed. Seeded per-child RNG streams (monotone admission
  counter), coordinator-owned fault RNG, `PYTHONHASHSEED=0` pin,
  `SimSystemResources` constant telemetry, float time quantum.

**I2. Seed = permanent reproducer — (a) COVERED**
- `pytest tests/simulation/<suite> --sim-replay=<seed>` expands, prints,
  runs twice, judges. The flag is owned by `tests/simulation/conftest.py`
  for all suites.

**I3. Generator stability + coverage guard — (a) COVERED**
- `test_vopr.py::test_plan_generation_is_deterministic_and_covers_fault_kinds`
  pins that the seed space exercises every fault kind AND the fault-free
  baseline. Replicate for every new generator (chaos, gate-tier, mdc).

**I4. Cross-commit seed validity — (a) design position, document**
- Seeds reproduce against a fixed tree (schedules shift when production
  timing changes). Same stance as TB (seeds are per-commit). Record failing
  seed + commit hash together in the soak log (F2).

---

## J. Schedule-space completeness ("any production-possible schedule")

**J1. Justified exclusions ledger — (a) COVERED, keep ratcheted**
- Pipe-IPC scoping, stream-loss exemption, dgram-only duplication — each
  documented at the chokepoint with its production justification. The lint
  ratchets (`tests/simulation/lints/`) keep new production code inside the
  seams. Add the new knobs (B4/B6, D1) to the seam lint expectations when
  built.

**J2. Node-kill coverage of EVERY role — (b) COVERABLE — P1**
- Generated plans kill only EXECUTORS; pinned scenarios kill workers.
  NOTHING ever kills the manager without reboot (permanent loss) or a gate.
  Manager-forever-dead: define the loud outcome (client `wait_for_job`
  timeout → the entry must record a terminal milestone rather than an
  unhandled task exception — new client entry variant logging
  `("job-wait-timeout", t)`), then let chaos plans draw it. Gate kill: with
  3 gates, killing one must leave dispatch working through survivors.

**J3. Gate-tier VOPR (L3 fault space) — (b) COVERABLE — P0 (program mission)**
- In progress: `tests/simulation/vopr_gates/` (7 gate-tier kinds + baseline,
  gate kills/partitions/restarts, client-link faults, leadership chaos).
  Invariants: G1/G2/G3/G4 + H1 + gate-leader stability windows.

---

## K. WORKLOAD REALISM

**K1. Faults intersecting LIVE execution — (b) COVERABLE — P0**
- The old workload (`SimPingWorkflow`: 2s, one 0.5s step) occupies ~2
  virtual seconds of a 100s ceiling — generated faults almost always hit
  IDLE cluster time. TB's workload runs continuously.
- KNOWN CONSTRAINT (found by this program): duration-governed (TEST-hook)
  workflows CANNOT run under SIM — `WorkflowRunner._generate` busy-waits
  `sleep(0)` with virtual time frozen at the duration boundary →
  `SimulationConstraintError` in executors. Until the runner fix lands
  (performance-sensitive hot loop), long-lived SIM workloads use chained
  ACTION steps with parameterized virtual sleeps (`soak_job_demo.py`,
  `gate_fault_client_demo.py`).
- Recipe: long multi-step action workflows (30–60s) + generator rules
  placing ≥1 fault inside the probed dispatch-to-drain window.

**K2. Concurrent multi-job interleaving — (b) COVERABLE — P1**
- At most two SEQUENTIAL jobs exist today (`two_job_client_entry`). No
  scenario has two jobs ALIVE at once contending for the same worker.
- Recipe: new client entry submitting N jobs with overlapping lifetimes,
  milestones prefixed `("job<k>-…", …)`; a per-job log-splitter adapter
  feeds each stream to `JobStatusOracle`. Invariants: each job
  independently linearizes; all reach terminals; a fault mid-first-job
  must not corrupt the second.

**K3. Dependent workflow DAGs across fault boundaries — (b) COVERABLE — P1**
- The submission API carries DAGs; every SIM scenario submits a single
  dependency-free workflow. Order-under-fault is unexercised.
- Recipe: submit `[([], LongA), (["LongA"], ShortB)]`; worker milestone
  `("workflow-executed", name, t)`. Invariants: B never starts before A's
  terminal instant (G3 merged trace); A fails ⇒ B's documented outcome
  observed LOUDLY; whole-job terminal agrees.

**K4. Progress-push streams under faults — (b) COVERABLE — P1**
- No scenario stresses the push stream itself: pushes dropped/duplicated/
  delayed while the poll path races them.
- Recipe: `schedule_drop_rate`/`duplicate`/`delay` on `manager→client`
  (and `gate→client`) during a long workflow; the status-seen oracle
  catches regressions; assert final observed stats equal the workflow's
  deterministic totals.

**K5. AD-26 extension requests under contention — (b) COVERABLE — P1**
- The worker's autonomous extension trigger never fires for a 2s workflow
  under a 30s timeout — the whole AD-26 grant/refuse surface is dead code
  in SIM today.
- Recipe: long workflow with `timeout_seconds` chosen so elapsed crosses
  the lookahead fraction mid-run (probe-pin); combine with faults
  (partition across the extension window; slow_disk during grant
  persistence). Invariants: extension outcome always LOUD; no workflow
  survives past hard timeout + max_extensions; client history linearizes.

**K6. Retry storms — (b) COVERABLE — P1**
- Single-retry recovery is pinned. Not covered: REPEATED loss, retry-cap
  exhaustion, retries contending with concurrent jobs.
- Recipe: chaos plans drawing multiple host-kills with staggered
  `worker_entry(start_at=…)` replacements; a no-replacement variant pins
  the retry-cap terminal (loud failure within bound, executions ≤ cap+1
  in the G3 trace).

---

## L. CLIENT/EDGE FAULTS

TB treats clients as replicas-of-a-kind: sessions in the replicated state
machine, monotone request numbers, reply cache per session (exactly-once),
client crash/restart + link faults verified against strict
serializability. Our analog: AD-40 idempotency key = request number,
manager idempotency ledger = reply cache, `JobStatusApplier` = client-side
ordering guard, gateless `job_status` query = poll fallback.

**L1. Client↔manager / client↔gate link faults — (b) COVERABLE — P0**
- `fault_plan._NODE_LINKS` is ONLY `manager↔worker` — no generated schedule
  has ever faulted the client's links. Yet the client edge is where
  exactly-once claims meet the network: a submission whose ACCEPT is lost
  (client retries → ledger must dedup on the SAME key), a completion push
  that never arrives (poll fallback must converge).
- Recipe: extend chaos link vocabulary with `client↔manager` and
  `client↔gate` (all four kinds; partitions sized against the 1s
  submission-retry cadence). Invariants: G1/G2 + submission accepted at
  most once on the manager (needs G3's accept milestone — duplicate ACCEPT
  of one idempotency key is a violation) + terminal convergence after heal.

**L2. Push loss vs poll fallback convergence — (b) COVERABLE — P0**
- Invariant: when every completion push is lost (client link cut across the
  completion instant), the client still reaches the terminal via its
  poll/gateless-`job_status` fallback after heal — bounded, never silence.
- Recipe: probe a seed to pin the completion instant `T_c`; cut
  `manager↔client` at `T_c - ε` healing at `T_c + W`; the one-way
  (`bidirectional=False`) variant is sharper. Assert `job-finished` lands
  in `(T_c + W, T_c + W + budget]`.
- MEASURED CAVEAT (multi-DC probes): in the GATE topology the manager's
  completion notify to the gate is a SINGLE 5s send followed by job-state
  cleanup (`manager/server.py:~10379`) — a partition covering that instant
  loses the completion permanently and the job ends `timeout` despite
  successful execution. Production fix queued (notice-backoff pattern);
  until it lands, gate-topology scenarios assert the documented loud
  outcome.

**L3. Late pushes racing polls (ordering guard under duplication) — (a)/(b) — P1**
- The guard exists (`status_application.py`); the G1 oracle catches
  regressions. Recipe to make it deliberate: heavy duplication (0.5–0.9) +
  jittered delay on `manager→client` during a long workflow; assert
  status-seen linearizes AND final stats are never wound back.

**L4. Client restart semantics — (b) scenario + documented guarantee — P1**
- The client holds NO durable state. `schedule_restart("client", at)` works
  today (leaf process); gen-2 re-runs the entry and submits a NEW job
  (fresh idempotency key).
- Assert TODAY's guarantees loudly: (1) gen-1's orphaned job still reaches
  its durable terminal on the manager (G3 milestone, no client observer);
  (2) gen-2's submission is a NEW logical job completing independently;
  (3) nothing wedges — the manager's best-effort push to the dead client
  is dropped internally, manager stays healthy.
- Aspirational skip-pin: a client persisting its idempotency key could
  resume exactly-once (same key ⇒ ledger dedup returns the SAME job;
  gateless `job_status` recovers the outcome).

**L5. Submission retry under sustained rejection — (b) COVERABLE — P2**
- Not covered: a LONG rejection storm — client partitioned from the whole
  submission surface 30–60s while retrying every 1s. Assert bounded
  `submit-rejected` cadence (~1/s — no hot-spin, no give-up), acceptance
  after heal, completion.

**L6. TB client-session mapping summary — documentation row — P2**
- request number ↔ AD-40 idempotency key (per-submission, not per-session —
  the L4 delta); reply cache ↔ manager idempotency ledger + durable
  JobLedger; session eviction ↔ none (clients stateless to the cluster;
  assert dead-client push state is cleaned up); strict serializability ↔
  G1 + G3 agreement.

---

## Priority roll-up

**P0 (the program's spine):**
| ID | Gap | Mechanism class |
|----|-----|-----------------|
| E1–E4 | Fault density + chaos-window→quiesce→converge | (b) new chaos generator/runner |
| G3 | Cross-node state oracle | (b) node milestones + trace oracle |
| G4 | Determinism-audit assertion | LANDED |
| J3 | Gate-tier VOPR | IN PROGRESS (agent) |
| C5 | Pause/resume | (c) coordinator `schedule_pause` |
| C3 | Gate restart scenarios | IN PROGRESS (agent) |
| K1 | Faults intersecting live execution | IN PROGRESS (agents; runner fix queued) |
| L1/L2 | Client-edge link faults; push-loss/poll-fallback | IN PROGRESS (agents) + chaos vocab |
| F2 | Swarm scale (standing soak) | (b) invocation + seed log |
| A2 | Asymmetric partitions (zero usage) | (b) direction bit in generators |

**P1:** A3, A4 density, A6 reorder, B4 corrupted reads, B7 Phase-8 pins,
B8 fault-during-recovery, C4 worker restart, C6 crash-during-recovery,
D1/D2 clock skew/jump, F1 long horizons, F3 randomized topology, G6
canaries, H1 convergence helper, H2 gate-tier bounds, J2 permanent kills,
K2 concurrent jobs, K3 DAGs, K4 push streams, K5 AD-26, K6 retry storms,
L3 ordering under duplication, L4 client restart.

**P2:** A8 wire corruption, B5 misdirected IO, B6 EIO, D3 rate drift
(documented skip), L5 rejection storms, L6 mapping doc.

**Measured production gaps queued from this program:** completion-push
single-send loss (manager→gate, `server.py:~10379` — fix: notice-backoff
obligation pattern); no mid-flight AD-36 failover (dispatch-time only);
storage-blind placement (disk-full manager classifies healthy);
duration-governed workflows cannot run under SIM (WorkflowRunner busy-wait).

**Already at the bar (defend, don't regress):** byte-identical replay with
seed reproducers, loud-outcome invariants, design-bound latency assertions,
justified-exclusion docstrings + seam lints, power-loss restart with
torn-write debris, the disk-fault→invariant pipeline.
