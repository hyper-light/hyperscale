# Hyperscale — Project Assessment

*Written 2026-08-21, the way an incoming tech lead would report after a first deep week. **Revised 2026-08-23** — see §0. Graded evidence only: every verdict below is "the doc's own acceptance bar vs. the code", never generic best practice. Receipts are `file:line`. Full graded ledgers (450 rows covering ~540 extracted promises, plus three delta re-verifications) live in `.scratch/assess-project/` — `promises/` (what the docs claim, quoted verbatim), `reality/` (what six code surveys found), `grades/` (the row-by-row verdicts with searches recorded).*

**Headline verdicts: Built 263 · Partial 149 · Absent 37** (one N/A), as first graded. The revision moves a handful of rows in both directions — itemized in §0 rather than silently re-totalled, since only the changed areas were re-derived. Nothing graded was pure vaporware at the module level — the recurring failure mode is not "missing code" but **"built and never wired"**, followed by **"doc froze while code moved on."**

---

## 0. Revision 2026-08-23 — what moved, and what I got wrong

Four commits landed against this report's findings on the day it was revised — `9422b810`, `023b1b3b`, `681736ed`, `a991de63`. I re-derived the affected areas plus an adversarial re-check of every Absent. Receipts below are stable at `a991de63`.

### Scoreboard — where §4's improvements stand

| # | Improvement | Status |
|---|---|---|
| — | *Durability reporting honesty* (was #2) | **Closed** `681736ed` — including the checkpoint-watermark and default-parameter halves |
| — | *Checkpoint cadence* (was #3) | **Closed** `681736ed` — `maybe_checkpoint()` wired on both tiers |
| — | *Phantom-attribute ratchet* (added this revision) | **Closed** — defect fixed *and* `test_no_phantom_attributes.py` added with an empty snapshot |
| 1 | Put unit + simulation tiers in CI | **Addressed, unproven** — `.github/workflows/tests.yml` added: ratchet lints and unit gate every PR, simulation/VOPR/chaos run nightly. Structurally validated only; **no run has gone green yet**, so this stays open until one does |
| 2 | Wire-or-delete the dormant layer | Open (~6,611 LOC) |
| 3 | FIX.md's two security items + `TLS_VERIFY_HOSTNAME` | **Open** — re-verified: no `strict=` at any of the three sites, default still `"false"` |
| 4 | Persist Raft or document volatility | Open — only production hit is a docstring example |
| 5 | Burn down `except: pass` | Open — 462 in `hyperscale/` (core 265, distributed 126, other 71; 10 bare) |
| 6 | Finish or remove `serve` CLI | Open — committed to the tree but still unregistered in `root.py` |
| 7 | Ship the missing wire fields | Open |
| 8 | One doc-honesty pass | Open |

**The pattern worth naming:** every closed item was closed *with a ratchet*, not just a fix — duplicate methods, phantom attributes, and durability honesty each ship with a lint or a test that fails in both directions. That is why the crash class is genuinely gone rather than temporarily absent, and it is the strongest engineering habit visible in this repo. The lint suite is now 7 files.

**Closed since the first draft**

- **All six cold-path crashes fixed** (`023b1b3b`), and — the durable part — a **duplicate-method ratchet lint** now walks every class body under `hyperscale/` and fails on any repeated method name. It found *five* sites where this report named one, including `HTTPResponse.reason`, whose surviving copy looked up a `str` key in a `Dict[bytes, bytes]` and therefore **always returned `None`** in the flagship engine. An independent AST scan confirms the claim: 1,480 files, 0 parse failures, 0 duplicates.
- **The global-result push was entirely dead**, not merely crash-prone as this report said. `GateServer` defined `_push_global_job_result` twice; the surviving copy sent wire action `global_job_result`, for which no handler exists (the client's receiver is `receive_global_job_result`), and the receive dispatch drops unknown actions silently. **Every job completion through a gate burned a full 5s timeout and delivered nothing.** Fixed in `9422b810`, along with the gate durable tier (a restarted gate now replays its WAL and recovers status, target DCs, fence token, client contact, and AD-34 tracking with the *remaining* budget).
- **The durability-reporting fix is done** (`681736ed`): a missing replicator now reports the level actually reached and names the cause, instead of returning `True`. Pinned by `test_commit_durability_honesty.py` in both directions. It is narrow by construction — all 9 production sites pass `LOCAL`, so the new branch is unreachable today and nothing regressed.
- The Phase-8 aspirational skip is now a live test — the simulation suite runs **242 passed, zero skips** per the commit's recorded verification.

**Corrections to this report — two over-grades and one false accusation**

1. **RETRACTED: the AES-GCM "4 SECURITY-flagged `except: pass` blocks."** They are not swallows. They are the key-rotation fallback ladder, and it terminates in `raise EncryptionError`; no SECURITY markers exist in the file. This claim entered via a doc's own wording and should not have survived grading. The crypto module does not swallow its errors.
2. **Manager peer state sync was graded Built; it was dead.** `ManagerServer.state_sync_request` referenced `self._logger`, which has never existed in that class's MRO, so it raised `AttributeError` on every request that cleared mTLS (fixed in `9422b810`). The grade rested on "sender exists, handler exists" — evidence that cannot see a handler raising on its first statement.
3. **AD-35 role-aware confirmation was graded Built; three of its callbacks raised.** `HealthAwareServer` — base class of all three node types — had live `self._logger` references at `health_aware_server.py:1049,1066,1085`, wired at construction (`:223-226`) and invoked without `try`/`except` at `confirmation_manager.py:172,308,330`, so each cleanup pass that confirmed or removed a peer aborted its batch inside `ErrorContext`. **Since closed, and closed properly:** the three references are gone *and* `tests/simulation/lints/test_no_phantom_attributes.py` now walks every class under `hyperscale/`, resolving `__slots__`, class attributes, annotations and methods across the MRO, with an empty snapshot. The bug class that produced corrections 2 and 3 is now extinct by construction rather than by sweep.

**New findings from the delta pass**

- **The durability lie ran one layer deeper than the pipeline — now closed.** `checkpoint()` stamped `regional_lsn`/`global_lsn` from the *local* fsync watermark. It now reads `last_regional_lsn`/`last_global_lsn`, which advance only inside `mark_regional`/`mark_global` after real replication and start at 0, so an unreplicated deployment reports 0 rather than borrowing the local number. `restore_durability_watermarks` re-seeds them monotonically at recovery, closing a wrinkle this report had missed: in-memory watermarks would otherwise reset on restart and the next checkpoint would report *less* durability than the previous one recorded.
- **The honesty fix briefly armed a booby trap, and the fix for that was better than the one suggested.** `JobLedger`'s durability defaults were `GLOBAL`/`REGIONAL` against a product with no replicator wired, so an omitted `durability=` would have returned `success=False` silently. Both remedies shipped: the defaults are now `LOCAL`, *and* `_require_satisfiable_durability` refuses an impossible request before the append, off a new `CommitPipeline.max_achievable_durability` that correctly tops out at `LOCAL` when a global replicator has no regional one behind it.
- **The residual divergence this report raised was resolved by a third option neither side proposed.** Rather than compensating for a failed commit or filtering replay on state, the in-memory apply is now **unconditional** — the append is fsync'd and replay reproduces it regardless, so gating the apply on replication was itself what split live from recovered state. The class docstring rejects the compensation route on the correct grounds: an abandonment record can itself fail to land, reintroducing the same divergence through a smaller window. `test_refused_replication_leaves_live_and_recovered_in_agreement` pins it across a real restart, and asserts the regional watermark stays 0 — proving convergence wasn't bought by re-lying about durability.
- **WAL compaction landed.** `maybe_checkpoint()` is wired on both tiers (`manager/server.py:2879`, `gate/server.py:6723`) with count/age triggers and 3-deep retention, the retention depth reasoned out in a comment: `_load_latest` walks newest-first and skips undecodable files, so older copies are the fallback for a checkpoint torn mid-write.
- **`from ..parent` imports violated the house rule in 41 places** — 28 under `distributed/`, 13 in vendored trees, plus a docstring teaching the form. All now absolute (`a991de63`). Worth recording *how* they were nearly missed: the obvious regex requires a space after the dots that the real form never has, and the system `python3` (3.9.6) cannot parse this repo's 3.12+ syntax, silently skipping 63 files. Both traps produce a confident "none found."
- Dormant inventory is **worse than estimated: ~6,611 LOC**, not ~5,000 (`sync`/`dispatch`/`workflow_lifecycle` constructed with real collaborators and zero `self._attr.` call sites; `version_skew`/`rate_limiting` never constructed at all). Offsetting correction: `RobustMessageQueue` **is** live (`wal_writer.py:157`) — strike it from the dormant list, −492.

---

## 1. The one-paragraph read

Hyperscale is two projects sharing a repo. The first is a **load-testing framework** (14 hand-rolled protocol engines, 32 reporter backends, a `@step`-decorated workflow DSL, a reactive TUI) — the README's boldest claims are its most solidly true. The second is a **distributed control plane** (SWIM+Lifeguard failure detection, per-job Raft, fenced leases, an event-sourced job ledger over a CRC-framed WAL, Vivaldi-coordinate adaptive routing) with a genuinely exceptional deterministic-simulation test rig: seeded VOPR fault schedules that must replay **byte-identically**, chaos suites judged on safety-always/liveness-post-quiesce, and AST "ratchet" lints that ban nondeterminism from production code. The gap between promise and delivery is concentrated in three places: **cross-region durability is a facade** (the deep legs — VSR, Merkle anti-entropy, REGIONAL/GLOBAL commit — are absent or unreachable; the pipeline no longer reports success for replication it didn't do, but the persisted checkpoint still does); **~6,611 lines of coordinator/discovery/consensus-persistence machinery exist but are never called**; and **no CI runs a single test**, which is why cold paths reach `main` broken. That last one is not theoretical: since this report was first written, the team found and fixed six guaranteed crashes, a completion-push that delivered nothing on every job, and a peer-state-sync handler that raised on its first statement — none of which had a test, and all of which a single CI run of the suites that *already exist* would have caught.

---

## 2. Grade table

Rows are deduplicated by mechanism (many promises appear in both `docs/architecture.md` and their AD); where two graders scored the same mechanism independently, they agreed. Per-ledger counts:

| Ledger (doc family) | Rows | Built | Partial | Absent |
|---|---|---|---|---|
| Root docs (README, TODO, FIX, SCAN family, WAL.md, SCENARIOS, AGENTS) | 71 | 34 | 29 | 8 |
| architecture.md §1 (core distributed mechanisms) | 78 | 53 | 25 | 0 |
| architecture.md §2 (stats, timeouts, Vivaldi, AD-38 ledger, bootstrap) | 72 | 37 | 25 | 10 |
| architecture.md §3 (WAL internals, idempotency, SLO, capacity, AD-45) | 57 | 22 | 28 | 7 |
| AD_1–18 | 18 | 12 | 6 | 0 |
| AD_19–36 | 24 | 18 | 6 | 0 |
| AD_37–53 + audit + compliance | 31 | 21 | 5 | 5 |
| Dev docs (simulation framework, rigor checklist, session mapping, REFACTOR) | 99 | 66 | 25 | 7 |

### 2.1 Built — the bar was met (selected, strongest receipts)

| Promise | Doc bar | Receipt |
|---|---|---|
| SWIM+Lifeguard failure detection with LHM | AD-30/45 formulas, caps 10K/1K/50K, poll tiers 1000/250/50ms | `swim/health_aware_server.py` (timers verified digit-for-digit); `test_timing_wheel.py`, `test_hierarchical_failure_detector.py` |
| Phase C timer composition + LHM-pump gating | multipliers [1.0,3.0]×[1.0,2.5]×[1.0,1.5], worst-case 11.25× | `health_aware_server.py:2653-2658` |
| AD-53 cross-layer death escalation | 62s formula, burst threshold 2/30s, sim acceptance tests | manager `server.py:2984→3044`; `health_aware_server.py:2882`; `test_membership_churn_gaps.py:362,394` |
| Pre-vote elections, configured quorum, fencing-from-terms (AD-3/5/10) | verbatim code contracts | `local_leader_election.py:437`, manager `server.py:804`, token = `(term<<32)\|counter` |
| Raft phases 1–4 (algorithm) | 150–300ms elections, 50ms heartbeat, §5.4.1 vote check, 22/21 command enums | `raft/` live at manager `server.py:522`, gate `server.py:897`; byte-equal replay test `test_apply_replay.py:252` |
| Idempotent submissions (AD-40) | at-most-once, O(1), crash-surviving, WAL-before-ack | client keygen → `GateIdempotencyCache` (waiter coalescing) → `manager_ledger.py:113-115` (persist-before-index, torn-tail replay), wired manager `server.py:905` |
| Logger WAL extension (AD-39) | durability modes, CRC binary frames, 128-bit LSN, batch fsync 100/10ms, executor purity | `logging/` + `lsn/`; 9 dedicated unit-test files |
| Adaptive routing pipeline (AD-35/36/42/45) | RTT-UCB, hysteresis 30s/20%/120s, SLO factor clamp [0.5,3.0], EWMA blend | `gate/server.py:4111-4121` (router IS wired — AD-51's own checklist is stale), `slo_compliance_score.py:62-63`, `routing_state.py:97`, blend at `server.py:3484-3489` |
| Four-phase cancellation (AD-20), version negotiation (AD-25), InFlightTracker (AD-32), StatsBuffer backpressure (AD-23/37), overload detection (AD-18), extension grants (AD-26 base) | quoted threshold tables | all constants verified exact; e.g. `load_shedding.py:46-53`, `backpressure.py:85-101`, `extension_tracker.py:31-101` |
| Deterministic simulation program (dev docs phases 1–8) | byte-identical replay "timestamps included", seed reproducers, bounds `20≤lat≤70` "never widened" | 4 VOPR suites + chaos plan + `run_swarm.py`; ratchet lints; bounds verified verbatim; rigor-checklist "open" items G3/E1-E4/H1 have since **landed** (`chaos_runner.py:495`) |
| README product claims: 14 engines, 32 reporters, local=distributed API, TUI, scaffolding CLI | feature matrices | `core/engines/` (own HTTP/2 codec, vendored aioquic/asyncssh/PyDTLS), `reporting/` (31 guarded real integrations), `ui/` (~17k lines) |
| Wire protocol security layers | msgspec→zstd→AES-256-GCM→4-byte framing, bomb checks | `distributed/encryption/`, `server/` (real `cryptography` HKDF per-message keys, weak-secret denylist; the key-rotation ladder terminates in `raise EncryptionError` — see §0 retraction) |
| Gate durable tier *(new, `9422b810`)* | restarted gate serves its own jobs; schema additions trailing-defaulted | `gate/server.py:1039` opens the manager's JobLedger composite; `start()` replays WAL → status, target DCs, fence token, client contact, AD-34 resumed on remaining budget; Phase-8 skip-pin now a live test |
| Durability reporting honesty *(new, uncommitted)* | a `CommitResult` may never claim a level the deployment can't provide | `commit_pipeline.py` returns the level reached + a naming error when a replicator is absent; `test_commit_durability_honesty.py` pins both directions. **Narrow:** all 9 production sites pass LOCAL, so the branch is unreachable today |
| Duplicate-method ratchet *(new, `023b1b3b`)* | "the snapshot is EMPTY and the test fails in both directions" | `tests/simulation/lints/test_no_duplicate_method_definitions.py`; independently re-scanned: 1,480 files, 0 duplicates |

### 2.2 Partial — code exists, bar unmet (the ones that matter)

| Promise | What's missing, precisely | Receipt |
|---|---|---|
| AD-38 three-tier durability | Tier-3 WAL **live** (appends at manager `server.py:9060/10451/7612`; gate `server.py:1039` since `9422b810`), but still **4 of 8** job event types emitted — `JobProgressReported`, `JobCancellationAcked`, `JobFailed`, `JobTimedOut` are declared, never written, and absent from `_apply_entry`. Pointedly, the gate's new timeout terminal writes `JOB_COMPLETED` with `final_status=TIMEOUT`, so `JOB_TIMED_OUT` stays dead on the one path that would use it | `job_event.py:73,108,144,161` |
| WAL compaction *(Built as of 2026-08-23, uncommitted)* | was: zero call sites, WAL never compacts, replay from LSN 0. Now: `maybe_checkpoint()` wired on both tiers with count/age triggers, 3-deep retention, and a cadence test suite. AD-38's "WAL ≤ 2× active state" criterion becomes meetable | `manager/server.py:2879`, `gate/server.py:6723`, `tests/unit/distributed/ledger/test_checkpoint_cadence.py` |
| CommitPipeline REGIONAL/GLOBAL | still unreachable — all 9 production calls pass LOCAL and no replicators are injected. **Improved:** a `None` replicator no longer returns `True` (§0). **Remaining lie one layer down:** `checkpoint()` persists `regional_lsn` *and* `global_lsn` = `last_synced_lsn`, stamping locally-fsync'd entries as globally durable | `job_ledger.py:615-616` |
| Manager peer state sync *(corrected — was graded Built)* | the receive handler raised `AttributeError` on its first statement (`self._logger` absent from the MRO), so peer sync answered an error for every request past mTLS. Fixed in `9422b810`; regraded Built for the fix, recorded here because the original grade was wrong | `manager/server.py:8046` |
| AD-35 role-aware confirmation *(corrected — was graded Built)* | three phantom-logger callbacks still raise on every peer confirm/remove; batch aborts inside `ErrorContext` and retries next tick | `health_aware_server.py:1049,1066,1085`; wired `:223-226`; called `confirmation_manager.py:172,308,330` |
| Raft durability (WAL.md Phase 6) | `RaftWAL`, `SnapshotManager`, `ReplicatedMembershipLog`, `ReplicatedStatsStore` exist, unit-tested, **never instantiated** — the live Raft log is in-memory; restart loses it | grep: zero production constructors |
| AD-41 resource guards | Kalman monitoring + process-tree tracking real and in heartbeats; the entire enforcement tier (WARN→THROTTLE→KILL, 2σ kill gating, all 9 wire messages, gate aggregation) absent; `ManagerResourceGossip` fully built, zero consumers | searches recorded in `grades/architecture-3.md` |
| AD-28 discovery | facade live (`DiscoveryService`, `RoleValidator`); the documented selection layer (~1,300 lines: rendezvous, EWMA, sticky pool) never called — and buggy (inverted `PeerHealth` ordering evicts on *success*) | `discovery/` |
| AD-44 retry budgets / best-effort | budgets enforced in dispatcher (`workflow_dispatcher.py`, checked at :601) but `JobSubmission` lacks the wire fields — `getattr(submission, "retry_budget", 0)` always defaults, so **clients can't request them**; `BestEffortManager` has zero references, not even tests | `grades/ad-37-53.md` |
| AD-43 capacity spillover | ladder + env exact, but the 5 promised `ManagerHeartbeat` capacity fields: 3 declared, **none ever set** at the sole construction site — heartbeats ship zeros, hollowing the wait-estimation math | `models/distributed.py:848-850` vs `manager/server.py:4391` |
| TODO.md "64/64 complete" | majority verify by direct read; but task 37 is a live TypeError (duplicate `_push_global_job_result` defs), task 19 is dead code, task 53's registries are never populated | `grades/root-docs.md` G-42 |
| AGENTS.md/CLAUDE.md "we never swallow errors" | **430** in `hyperscale/` (600 repo-wide) by AST count; split **core/ 258, distributed/ 101**, and all 10 bare `except:` live in `core/`. The `aes_gcm.py` blocks are **struck** — they are the key-rotation ladder ending in `raise EncryptionError` (§0) | AST census, 2026-08-23 |
| HLC (AD-38/39) | exists as `HybridLamportClock` with receive/witness/recover, but pure Lamport — no bounded-physical-drift invariant, no wall-first ordering | `logging/lsn/` |
| AD-1 composition-over-inheritance | callbacks all real, but ~18 base-method overrides across manager/gate/worker falsify "never overriding" | AST scan in `grades/ad-1-18.md` |
| AD-19 three-signal health | signals + routing matrix live (`worker_health.py:151`); the ">50% systemic" eviction hold replaced by a count-based guard in a tracker with zero live consumers | `grades/ad-19-36.md` |
| Gate/manager module reorganization (AD-27, SCAN.md) | coordinators extracted **and then not used**: manager `server.py` 10,810 lines with ~1,800 lines of constructed-but-never-called coordinators (sync 783, version_skew 393, dispatch 375, rate_limiting 306, workflow_lifecycle 268) duplicated inline; gate `server.py` 6,901 lines | `reality/distributed-nodes-jobs-ledger.md` |

### 2.3 Absent — promised, no code (all survived the search-under-other-names rule; searches recorded in the grade files)

| Promise | Doc | Evidence of absence |
|---|---|---|
| Per-job VSR replication (view changes, ring succession, durable-before-ack quorum) | architecture.md AD-38 §VSR | zero hits: `vsr`, `ViewChange`, `StaleViewError`; failover is SWIM-leader orphan takeover + fence bump |
| Merkle anti-entropy | architecture.md | zero hits for `merkle`/`root_hash`/`hash_tree`. Sharper: `reliability/load_shedding.py:86-87` assigns request priorities to `AntiEntropyRequest`/`AntiEntropyResponse` — **classes that were never written**. The nearest real code, `gate/replication_coordinator.py:421`, is a wholesale peer-replica pull: no tree, no hash compare |
| Acknowledgment windows | architecture.md | no `AckWindowManager`/`AWAITING_ACK` under any name |
| Bootstrap module (parallel probe, 4-byte PING/PONG, backoff-forever) | architecture.md §bootstrap | no cluster-join `bootstrap/` (`routing/bootstrap.py` is AD-36 coordinate-immature routing — a name collision, not the module). Split verdict: `DiscoveryService` **is** live (`gate/server.py:670,692`) with 6 of 17 `DiscoveryConfig` fields consumed; dead with zero consumers is exactly the join/backoff/pool half — `probe_timeout`, `initial_backoff`, `max_backoff`, `backoff_multiplier`, `jitter_factor`, `refresh_interval`, `promotion_jitter_min/max`, `connection_max_age`, `primary_connections`, `backup_connections` |
| WAL buffer layer (BufferPool, DoubleBuffer, SingleWriter/SingleReaderBuffer, ReaderPool, IndexedReader, `wait_durable`) | architecture.md Parts 14–16 | zero code; superseded by the ledger WAL stack, doc never updated |
| AD-52 cluster creation (joint consensus, learners, seed locators, phi accrual, watch streams, `--initial-members`) | AD_52 + 39-item plan | only Phase 0 (HLC through Raft apply) exists; `commands/serve.py` is unregistered (`root.py:68`) and constructs servers **without ever starting them** |
| FIX.md's own two open fixes: mTLS `strict=` at 3 call sites; timeout-tracker fence validation | FIX.md §1.1/§1.2 | `manager/server.py:6101`, `tcp_worker_registration.py:116`, gate `tcp_manager.py:305` still non-strict; `gate_job_timeout_tracker.py:184-210` stores any fence unconditionally |
| `F_FULLFSYNC` on darwin | architecture.md | zero matches — macOS durability relies on fsync the doc itself distrusts |
| REFACTOR.md program (one-class-per-file, complexity lint, LOC reduction) | REFACTOR.md | no mccabe/C901 anywhere in ruff config; god files unshrunk |

### 2.4 Undocumented — load-bearing code no doc owns

- **The 1µs time-remainder epsilon** and the transient-rejection retry vocabulary in `distributed/protocol/` — livelock fix every tier depends on; no AD owns it.
- **The hook-type inference engine** (`core/.../hook.py:114-190`): a step *is* a load-test because its return annotation is `HTTPResponse`. This is the framework's central UX trick; README shows it, nothing specifies it.
- **`tests/framework/` scenario library** — specs/actions/runner driving all 88 e2e sections; it is the de facto integration contract of the cluster and has no doc.
- **The determinism seam system as implemented** — `_DEFAULT_CLOCK: Clock = RealClock()` module-global + `swap_defaults()` sys.modules walk in **166 files**, enforced by ratchet lints. The sim docs describe the *idea*; the global-swap mechanism (and the fact that DI is half-applied — many classes accept a clock, then read the global) is documented nowhere.
- **Deliberate policy reversals that beat the docs**: fencing on `WorkflowResultPush` consciously retired ("Receivers must NOT apply this…" — it dropped legitimate work); orphans requeued instead of failed; `worker_count==0` → BUSY not UNHEALTHY; gate cancel retry moved client-side. In each case the code is *more* correct than the doc; the doc still says otherwise.

---

## 3. Patterns and structure

**The map as it is:** four node roles (gate → manager → worker, thin client) all built on `MercurySyncBaseServer` (2,507 lines) → `HealthAwareServer` (6,547 lines) — so SWIM is the substrate, not a sidecar, exactly as the docs draw it. The docs' *module* boundaries, however, are aspirational: the extracted-coordinator layer the SCAN docs mandate exists mostly as dead weight beside inline reimplementations.

**Patterns (three-plus sightings each):**

*Good*
1. **TaskRunner-everywhere** — ~293 sites; the raw-`create_task` audit finding (47) driven to zero and held by a lint allowlist that is down to 3 taskex internals.
2. **Determinism seams + ratchet lints** — 166 files, 5 AST lint tests, byte-identical replay as an enforced invariant.
3. **Regression-pinned tests with bug archaeology** — 145 AD-citing why-comments; pins like `test_client_vanish_mid_execution_never_spins_the_manager` ("the ceiling IS the test").
4. **Copy-on-write snapshots** — lockless `ProbeScheduler`, `MappingProxyType` ledger snapshots, state stores.
5. **Constants-match-docs discipline** — where code is live, quoted threshold tables verify digit-for-digit (AD-18/22/23/26/30/32/33/36/37). The numbers in these docs are unusually trustworthy; it's the *wiring* claims that lie.

*Bad*
1. **Built-but-unwired** — the signature pathology, **~6,611 lines** re-measured: manager coordinators (2,125 — `sync`/`dispatch`/`workflow_lifecycle` constructed with real collaborators and *zero* `self._attr.` call sites; `version_skew`/`rate_limiting` never constructed at all), discovery selection and pool (1,819 across 5 classes with zero external references), Raft persistence (929), BOCPD progress-witness stack (1,193 — dormant purely because `manager/server.py:598` never passes `throughput_witness`), `ManagerResourceGossip`, `BestEffortManager` (3 hits total: definition, re-export, doc), AD-33 state machine, `serve.py`. Ten-plus sightings. *Correction: `RobustMessageQueue` is live at `wal_writer.py:157` — struck, −492.*
2. **Swallowed exceptions** — **430** in `hyperscale/` against a house rule of zero, split `core/` 258 and `distributed/` 101; all 10 bare `except:` are in `core/`. (The crypto blocks previously cited here are struck — see §0.)
3. **God files** — manager 10.8k, gate 6.9k, `HealthAwareServer` 6.5k, `models/distributed.py` 132 classes, vs. the one-class-per-file rule.
4. **Copy-paste triplication** — snowflake ×3 (core/logging/taskex), TimeParser ×4, restricted unpickler ×2 (diverged), `ExtensionTracker` ×2 (diverged), per-verb engine bodies.
5. **Crashes on cold paths** — **largely closed as of `023b1b3b`/`9422b810`**: all six named instances are fixed, and a duplicate-method ratchet lint now makes that sub-class unable to return silently. What remains is the *phantom-attribute* sub-class the ratchet does not cover — three live `self._logger` references in `HealthAwareServer` (§0, correction 3). The cause was always structural, and only half of it is addressed: these paths still have no test, and **no CI would run one anyway**.
6. **Doc drift both directions** — docs claim dead things live (FIX.md "0 issues", compliance "no action items") and live things dead (rigor checklist's "open" P0s that have since landed; AD-51's unchecked wiring boxes vs. the wired router at `gate/server.py:4121`). The drift is honest in aggregate — the *dev* docs under-claim while the *status* docs over-claim.

---

## 4. Improvements (ranked; each traces to a graded finding)

*Re-ranked 2026-08-23 against the §0 scoreboard. Three items closed during this pass — durability honesty, checkpoint cadence, and the phantom-attribute ratchet — and are struck from the list rather than restated. #1 is unchanged and now has four more receipts arguing for it.*

1. **Put unit + simulation tiers in CI** (traces: tests-ci survey — `release.yml` publishes but *no workflow runs any test*, and its `paths: [pyproject.toml]` trigger means a source-only PR runs literally nothing). Consequence: the crash class becomes unmergeable instead of undiscovered. The last two days are the argument — six crashes, a dead completion-push, and a dead state-sync handler, all found by hand, none by a gate. The suites already exist and the team already runs them locally; CI only removes the requirement to remember.
   **Status:** `.github/workflows/tests.yml` now exists — three jobs: `lints` (the 7 ratchets, seconds), `unit` (`tests/unit` minus the ~1h multiprocess SIM tier), both gating every PR; and `simulation` (SIM + VOPR + chaos, serial, artifacts uploaded) nightly and on demand. Written against the tier runtimes recorded in `9422b810`'s own verification block. **It has never been executed** — the workflow's YAML, job graph, test paths and `uv sync --group dev` resolution are verified, the tests themselves are not, per this repo's rule that a human runs them. Treat the first run as the real acceptance test; the likely adjustments are the pinned interpreter (3.12 vs. the 3.14 dev venv) and the `unit` job's 30-minute timeout.
2. **Wire-or-delete the dormant layer** (traces: G-59, AD-27, AD-11, AD-28, AD-33). Each dormant coordinator sitting beside an inline twin is a drift bomb — the `_job_dc_managers` split-store bug is the demonstrated cost. Deleting alone removes **~6,611** lines of false map. Cheapest live win in the set: the BOCPD stack (1,193 lines) is dormant only because `manager/server.py:598` never passes `throughput_witness`.
3. **Apply FIX.md's own two open security items and flip `TLS_VERIFY_HOSTNAME` default** (traces: G-52/G-53/G-76, re-verified 2026-08-23 — all three call sites still pass no `strict=`; `gate_job_timeout_tracker.py:202` still writes the fence unconditionally, and `:197` refreshes `dc_last_progress` *before* the token is read, so a stale report still bumps the clock). Consequence: cert claims actually enforced at the three trust boundaries; hostname verification stops being opt-in.
4. **Persist Raft or document volatility** (traces: G-25; re-verified — still 0 production constructors, 929 dormant LOC, live log is in-memory `RaftLog(job_id)` at `raft_node.py:107`). Instantiating it turns "consensus that forgets on restart" into consensus.
5. **Burn down `except: pass`** (traces: G-65; **462** in `hyperscale/` by AST census today vs. 557 in January). Start in `core/`, which holds 265 of them and all 10 bare `except:`; `distributed/` holds 126.
6. **Finish or remove `serve` CLI** (traces: peripherals survey; re-verified — `root.py:14-16` still imports only `new, ping, run`, and all three subcommands end on the constructor assignment with no `.start()`, the file ending mid-line without a newline). Registering it is the difference between "distributed mode exists" and "a user can launch it".
7. **Ship the missing wire fields** (traces: AD-44 G-51, AD-43 G-44): `JobSubmission` retry-budget/best-effort fields, and the 3 of 5 `ManagerHeartbeat` capacity fields that `manager/server.py:4391` still leaves as structural zeros — small diffs that activate two already-built subsystems.
8. **One doc-honesty pass** (traces: §3 bad-6): fix `distributed_rewrite/` paths, retire Parts 14–16 buffer designs, update FIX/TODO/compliance claims, check the rigor checklist's landed P0s, and delete the `AntiEntropyRequest`/`AntiEntropyResponse` priority entries for classes that were never written. The dev docs earned trust; the status docs spend it. (Also: sweep the ~50 `longrunningtestworkflow_*.json` artifacts and node logs out of the repo root.)

---

## 5. Strengths — graded Built *and* distinctive

**1. The deterministic simulation program.**
*Engineer's line:* seeded VOPR fault schedules replay byte-identically (timestamps included) because production code is forbidden — by AST ratchet lints — from touching time/random/uuid/create_task/disk directly; chaos runs are judged safety-always/liveness-post-quiesce with a cross-node trace oracle (leader exclusivity, executions ≤ retries+1); detection-latency assertions are two-sided bounds (`20.0 <= lat <= 70.0`) that have never been widened.
*Pitch line:* every distributed-systems bug found here comes with a seed that reproduces it exactly, forever — TigerBeetle-style simulation testing applied to a load-testing control plane.

**2. The exactly-once submission path.**
*Engineer's line:* client-generated `{client_id}:{seq}:{nonce}` keys flow through a gate cache that collapses concurrent duplicates onto one waiter, into a manager ledger that fsyncs the WAL *before* indexing and survives torn tails on replay; the at-most-once window is TTL-bounded and the delta is documented.
*Pitch line:* submit the same job twice — through different gates, across a crash — and it runs once.

**3. The failure-detection stack.**
*Engineer's line:* SWIM with Lifeguard's LHM, a two-layer hierarchical detector whose confirmations update state rather than timers (starvation-immune by construction), Vivaldi-adaptive timeouts, and cross-layer escalation with a 62-second worst-case formula — every constant in the AD tables verified in code, key behaviors regression-pinned.
*Pitch line:* dead nodes are detected in bounded, tested time — under load, across datacenters, without false-positive storms.

**4. Breadth with a two-line API.**
*Engineer's line:* 14 protocol engines (hand-rolled HTTP/1.1 wire bytes and HTTP/2 frames+HPACK, vendored QUIC/SSH/DTLS) and 32 real reporter integrations behind one duck-typed interface; a workflow is a class with `vus`/`duration` and `@step` methods whose return annotations classify them; the same file runs single-machine or clustered.
*Pitch line:* one Python file load-tests over any of 14 protocols and streams results to any of 32 backends — locally today, on a cluster tomorrow, unchanged.

## 6. Weaknesses — walked consequences

- **No CI runs tests.** A regression in the crown-jewel VOPR suite merges silently, and `release.yml`'s `paths: [pyproject.toml]` trigger means a source-only PR runs nothing at all. The six cold-path crashes, the dead completion-push, and the dead state-sync handler are the observed consequence, not a hypothetical — every one was found by hand after reaching `main`.
- **Durability is locally real, globally theater.** A region loss loses every job event not on a surviving node's local WAL. The commit pipeline no longer *claims* otherwise (fixed 2026-08-23), but the persisted checkpoint still stamps `regional_lsn`/`global_lsn` from local state, so the lie survives a layer down. WAL growth is unbounded on **both** the manager and the gate tier, and restart cost grows with history until someone calls the checkpoint that nothing calls.
- **Consensus forgets.** Raft state is in-memory; a manager restart re-derives job leadership from peers/workers rather than its own log — the gap between "we run Raft" and what an operator will assume those words mean.
- **The security defaults betray the security work.** Real AES-256-GCM and mTLS claims machinery — with `TLS_VERIFY_HOSTNAME` defaulted to `"false"` (`env.py:31`), strict cert-claim parsing never enabled at the three call sites its own FIX doc names (so a *malformed* cert falls back to defaults and passes), and a dev-default auth secret whose hard failure is gated on `HYPERSCALE_ENV` and otherwise degrades to a `UserWarning`. Any process that can reach a socket and knows the dev secret is a cluster member. *(The crypto module itself is exonerated — see §0.)*
- **Distributed mode has no front door.** Helm ships `helm create` nginx; Docker installs last-published PyPI; `serve` is unregistered and never starts its servers. Everything the ADs describe can only be started by test harnesses.
- **Enforcement gaps behind rich telemetry:** resource guards measure and never kill (a runaway workload OOMs the worker while beautifully Kalman-filtered); SLO classification computes and doesn't gate health; client-requested retry budgets can't cross the wire.

## 7. The honesty section — demo vs. docs

**A demo would show, and docs can back:** local multi-process runs with live TUI, all engines/reporters, cluster failure-detection latencies inside documented bounds, exactly-once submission under crash, byte-identical replay of a chaos schedule.

**The docs claim, and a demo would embarrass:** three-tier cross-region durability (LOCAL only), VSR failover and Merkle repair (absent — with priority-table entries for classes never written), a sub-second bootstrap module (absent; the join/backoff/pool constants are fossils), AD-52 cluster creation (proposed; only HLC-through-Raft exists), "0 high-priority issues" (FIX.md — the last two days produced eight fixes it did not predict), "millions of requests per minute" (no benchmark in the repo backs the number), and every performance figure in architecture.md's WAL sections (mechanisms partially real, numbers unverified).

**And the standing lesson for this report's own method:** two of its Built grades were wrong in the same way — `AttributeError` on a handler's first statement is invisible to "the sender exists and the handler exists," which is the strongest evidence a read-only grader can gather without executing. Static grading systematically over-credits wiring. That is an argument for CI, and equally an argument for reading the *first* statement of every handler a grade depends on.

**Where reality outruns the docs** (the pleasant surprises): the rigor checklist's "open" P0 gaps — chaos spine, cross-node trace oracle, disk-fault knobs, gate-durable restart — have all since landed; the manager ledger this project's own memory recorded as dormant is live; AD-51's router is wired despite its own unchecked checklist.

---

## Coverage and method

**Docs read (all in full, via 8 parallel readers):** README, AGENTS, EXECUTION_WORKFLOW, WAL, SCENARIOS, TODO, FIX, SCAN, GATE_SCAN, MODULAR_SCAN; `docs/architecture.md` (38,790 lines, three slices); AD_1–AD_53 + AD_52_PLAN + 2026-01-11 audit + compliance reports; `docs/dev/` (slo, simulation_framework, simulation_rigor_checklist, client_session_mapping, improvements, REFACTOR, TODO) + `docs/SCENARIOS.md`. **Code read (6 parallel surveys):** all of `hyperscale/` by area; `tests/` (505 files inventoried; unit 209, simulation 32+55-file harness, integration 23, e2e 89), `.github/workflows`, pyproject, docker, helm, examples. `.claude/worktrees/` excluded everywhere. **Tests were NOT run** — per the project's own standing rule; every test-dependent grade says "test-existence" in its evidence tier, and a static import check (505 test files: 0 unresolved; 65 example files: 2 broken) stands in for collection. Where this report cites passing suites (3,862 units+lints; 242 sim with zero skips; 8 chaos/VOPR), those are the **committer's** recorded runs quoted from commit messages, not mine. **Nothing in the repo was changed** beyond this report and the `.scratch/assess-project/` evidence files.

**Revision pass, 2026-08-23:** three delta agents re-derived (a) everything the two new commits and the uncommitted change touched, (b) all ten Absent verdicts under adversarial re-grep, (c) the ten Partials that drive the top recommendations — plus an independent AST re-scan of the new duplicate-method lint (1,480 files) and a fresh `except: pass` census. Grades not in those three sets are carried forward from 2026-08-21 and are labelled by their original evidence tier. The working tree was **actively being edited during the pass** (`job_ledger.py` grew 606 → 745 lines mid-scan), so uncommitted receipts are a moving snapshot; committed receipts are stable at `023b1b3b`.

**Onward:** the gaps worth building (CI gating, checkpoint wiring, AD-52) go through `/grill-with-docs` and `/to-spec`; the pitch material in §5 feeds `/distill-docs`; the wire-or-delete candidates and god-file decomposition are `/improve-codebase-architecture` material.
