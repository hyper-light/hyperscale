# AD-52 Implementation Plan

## Scope

This plan implements AD-52 (Cluster Creation) end-to-end against the existing infrastructure surveyed below. Work is broken into atomic items grouped by phase. Each item names exact files to add / modify / remove, lists acceptance criteria, gives a brief usage example, and enumerates tests across eight categories (unit, integration, e2e, race condition, memory leak, deadlock, edge case, negative path).

"Atomic" means one item should land as one PR-sized change, reviewable in isolation, with its own test suite. Items within a phase are independent unless explicitly noted with `depends-on`.

## Infrastructure baseline (what already exists)

### Raft (`hyperscale/distributed/raft/`, ~3,750 LOC)
- `RaftConsensus` / `GateRaftConsensus` (per-node) own a `dict[job_id, RaftNode]`. Per-job Raft groups.
- `RaftNode` (557 LOC) — leader election + replication. Per-follower replication is **serial, not pipelined**. Single `asyncio.Lock` guards all state.
- `RaftLog` (161 LOC) — in-memory only. `RaftWAL` exists but is not wired into `RaftNode`.
- `RaftLogEntry` — has `term, index, command: bytes, command_type: str, job_id, timestamp` but **no `schema_version` field**.
- `RaftStateMachine` / `GateStateMachine` — dispatch by `command_type` string. 21 / 20 handlers respectively. Determinism is informal (no enforcement).
- `SnapshotManager` and `InstallSnapshot` message types **defined but not wired** into `RaftNode` (no `handle_install_snapshot` method exists).
- `RaftCommandType` / `GateRaftCommandType` — string enums. No joint-consensus or learner entries today.
- `RaftConsensus.on_node_join` / `on_node_leave` already support dynamic membership but use single-step changes only.
- `ReplicatedMembershipLog` exists; routes SWIM events through Raft. Joint consensus would extend it.
- `RaftWAL` already wraps `WALWriter` with group commit configured (500μs batch, 500-entry max, 4MB max) — but unused from `RaftNode`.

### SWIM (`hyperscale/distributed/swim/`)
- `HealthAwareServer` (`swim/health_aware_server.py`) — main composition root. Registers `_on_node_dead_callbacks`, `_on_node_join_callbacks` (lines 346/592/2476).
- `IncarnationTracker.node_states: dict[(host,port), NodeState]` is the per-edge state container (AD-46). `dead_node_retention_seconds: float = 3600.0` already implements tombstone retention with eviction callback (line 382).
- `AckHandler.handle()` (line 48 in `swim/message_handling/membership/ack_handler.py`) is the heartbeat-arrival hook point.
- `HierarchicalFailureDetector` (`swim/detection/hierarchical_failure_detector.py`) — composes global TimingWheel + per-job suspicion. Fires `on_global_death_sync` (synchronous) and `on_global_death` (async).
- `CoordinateTracker.estimate_rtt_ucb_ms()` (AD-35) — exposes Vivaldi RTT estimates suitable as phi-accrual baseline.
- `ProbeScheduler` is lockless / copy-on-write; supports `add_member`/`remove_member`.
- `MessageDispatcher.dispatch()` (`swim/message_handling/core/message_dispatcher.py`) is the SWIM-protocol entry; `MessageParser.parse()` is where a fence header would be extracted.
- No watch/subscription mechanism today — only fire-and-forget callbacks.

### Discovery (`hyperscale/distributed/discovery/`)
- `DiscoveryService.discover_peers()` is the resolution entry point.
- `AsyncDNSResolver` is concrete; **no `SeedResolver` protocol exists**. Adding one is non-disruptive.
- Dual cache: `_positive_cache: dict[str, DNSResult]` with TTL + `NegativeCache` for failed lookups with exponential backoff.
- `DiscoveryConfig` is the configuration container; `static_seeds: list[str]` exists for fallback.

### Server / Protocol (`hyperscale/distributed/server/`)
- `MercurySyncBaseServer.process_tcp_server_request()` is the dispatch path. Order: rate limit → size check → decrypt → decompress → header parse → address parse → **(fence check would go here)** → replay guard → handler lookup → handler call.
- TCP frame header today: `address<handler<clock(64B)data_len(4B)data(N B)`. ClusterRPCFence header would extend this.
- `ProtocolInFlightTracker` (`server/protocol/in_flight_tracker.py`) — already classifies handlers into CRITICAL/HIGH/NORMAL/LOW (`_CONTROL_HANDLERS`, `_DISPATCH_HANDLERS`, `_DATA_HANDLERS`, `_TELEMETRY_HANDLERS`). Cluster RPCs go into `_CONTROL_HANDLERS`.
- `@tcp.receive()` decorator (`server/hooks/tcp/server.py`) does not currently take a `priority` argument.

### Nodes (`hyperscale/distributed/nodes/{gate,manager,worker}/`)
- All three inherit from `HealthAwareServer`. Startup is phased: `start_server()` → register with seeds → SWIM `join_cluster()` → leader election → background loops.
- All three accept seed lists in `__init__` (`seed_managers`, `gate_peers`, `datacenter_managers`, etc.) — these are static config today.
- All three already register `on_node_dead`, `on_node_join`, `on_peer_confirmed` callbacks.
- Workers register with managers via TCP `worker_register` handler; primary manager stored in `WorkerRegistry._primary_manager_id`. Failover via `_registry.select_new_primary_manager()`.
- Gates know about manager DCs via `datacenter_managers: dict[str, list[(host,port)]]` constructor arg + SWIM gossip.

### Conventions
- TaskRunner: `self._task_runner.run(coro, alias=..., schedule=..., repeat=...)`. Cancel via token. Long-running periodic loops use `schedule="Ns", repeat="ALWAYS"`.
- Logger: async, `await context.log(EventModel(...))`. Models inherit `Entry` from `hyperscale/logging/models/`, use `msgspec.Struct, kw_only=True`. New event types go in `hyperscale/logging/hyperscale_logging_models.py`.
- Models: `@dataclass(slots=True, kw_only=True)` + cloudpickle. Live in `hyperscale/distributed/models/distributed.py` (consolidated by exception). Request/Response naming pairs.
- Env config: `hyperscale/distributed/env/env.py` — Pydantic `BaseModel` with `Strict*` types.
- Tests: pytest + `pytest-asyncio`. Unit at `tests/unit/distributed/<module>/`. Integration at `tests/integration/<scenario>/`.
- One class per file, except for `models/distributed.py` (data models bundled).

---

# Phase 0: Wire AD-38 HLC into the Raft apply path

The original draft of this phase had two items (schema versioning + a determinism harness). On inspection both were wrong scope:

- **Schema versioning** is already covered by AD-25 (`hyperscale/distributed/protocol/version.py`'s `FEATURE_VERSIONS` + capability negotiation). New cluster entry types in Phase 1.6 are gated through that mechanism. No per-entry `schema_version` field needed.
- **The determinism harness** (audit hooks, AST checker, decorator machinery) was defense-in-depth machinery for a class of bug that does not need a framework. The actual gap is that the apply layer calls `time.monotonic()` for state writes that get replicated; the fix is to take the timestamp from the (already replicated) log entry. The HLC required for that is `hyperscale/logging/lsn/HybridLamportClock`, which AD-38 already implemented but never wired into the Raft path.

This phase is therefore one item: route the HLC through `RaftConsensus` → `RaftNode` → `RaftLogEntry`, fix the four divergent call sites, normalize the affected JobInfo field semantic to wall-clock seconds, and ship a replay test that asserts byte-equal state across two replicas.

## Item 0.1 — Wire the HybridLamportClock through Raft and fix divergent apply writes

**What**: 
1. Pass an existing `HybridLamportClock` instance (already created in each node for the WAL) into `RaftConsensus` / `GateRaftConsensus`, and from there into `RaftNode`.
2. In `RaftNode.propose()`, mint the entry timestamp via `clock.generate().wall_clock / 1000.0` instead of `time.monotonic()`. The resulting `RaftLogEntry.timestamp` is a replicated wall-clock-seconds value identical on every follower.
3. In `RaftStateMachine.apply()`, pass the `entry` through to handlers so they can read `entry.timestamp` directly. Convert all 22 handler signatures from `(self, command)` to `(self, command, entry)`.
4. Replace the four divergent `time.monotonic()` calls in `_apply_initiate_cancellation` and `_apply_complete_cancellation` with `entry.timestamp`.
5. Add a `timestamp: float | None = None` parameter to `JobManager.update_job_status`; the apply handler `_apply_update_job_status` passes `entry.timestamp`; non-apply callers continue to omit it and fall through to `time.time()`.
6. Normalize the semantic of `job.timestamp` and `job.completed_at` to wall-clock seconds throughout the manager. All non-apply writers in `nodes/manager/server.py` switch from `time.monotonic()` to `time.time()`. The paired readers (`time.monotonic() - job.timestamp` / `time.monotonic() - job.completed_at`) switch to `time.time() - …`.
7. Apply the same signature change to all 20 `GateStateMachine` handlers for consistency (gate apply paths do not write timestamps, so no behavior change there).
8. Add a Hypothesis-friendly replay test that constructs two independent state machines, applies the same log to both, and asserts byte-equal serialized state.

**Why**: AD-52 §15. Each follower applying the same Raft log entry must produce the same state-machine state. The previous code wrote `time.monotonic()` (per-process, divergent) to `job.timestamp` / `job.completed_at` / `manager_state.cancellation_initiated_at` during apply. AD-38 already provides the HLC; it just was not plumbed into the Raft path.

**Files**:

*Modify*:
- `hyperscale/distributed/raft/raft_node.py` — add `clock: HybridLamportClock | None = None` to `__init__`; replace `time.monotonic()` at proposal time with `clock.generate().wall_clock / 1000.0` when set; fall back to `time.monotonic()` only when no clock is configured (test paths).
- `hyperscale/distributed/raft/raft_consensus.py` — accept `clock` and forward to `RaftNode`.
- `hyperscale/distributed/raft/gate_raft_consensus.py` — same.
- `hyperscale/distributed/raft/state_machine.py` — remove `import time`; change `apply()` to pass `entry` through; change all 22 handler signatures to `(self, command, entry)`; fix `_apply_initiate_cancellation` and `_apply_complete_cancellation` to use `entry.timestamp`; pass `entry.timestamp` into `update_job_status`.
- `hyperscale/distributed/raft/gate_state_machine.py` — pass `entry` through; change all 20 handler signatures for consistency.
- `hyperscale/distributed/jobs/job_manager.py` — `update_job_status` accepts `timestamp: float | None = None`; default `time.time()` when omitted.
- `hyperscale/distributed/models/jobs.py` — update docstrings on `JobInfo.timestamp` / `JobInfo.completed_at` to reflect wall-clock seconds semantic.
- `hyperscale/distributed/nodes/manager/server.py` — create one shared `HybridLamportClock` in `__init__`, pass to `ManagerRaftIntegration`; reuse it for the WAL on `start()`. Switch four non-apply writers (`job.timestamp = time.monotonic()`, `job.completed_at = time.monotonic()`) and two paired readers (`time.monotonic() - job.timestamp` / `time.monotonic() - job.completed_at`) to `time.time()`.
- `hyperscale/distributed/nodes/manager/raft_integration.py` — accept and forward `clock`.
- `hyperscale/distributed/nodes/gate/server.py` — create or reuse the shared HLC and pass to `GateRaftIntegration`.
- `hyperscale/distributed/nodes/gate/raft_integration.py` — accept and forward `clock`.

*Add*:
- `tests/unit/distributed/raft/test_apply_replay.py` — Two independently constructed `RaftStateMachine` + `JobManager` pairs receive the same log; their `JobInfo` snapshots must be byte-equal.

**Out of scope** (deferred, not blockers for Phase 1):
- Gate-side writers/readers of `job.timestamp` in `nodes/gate/server.py`, `nodes/gate/handlers/tcp_job.py`, `nodes/gate/orphan_job_coordinator.py`, and `jobs/gates/gate_job_manager.py`. Gate's `JobInfo.timestamp` is never set via Raft apply on gates (gate apply handlers do not touch the field), so its existing `time.monotonic()` writes and readers are internally consistent within a single gate process. Normalizing the gate side to wall-clock for cross-tier consistency is a follow-up.
- `raft/replicated_membership_log.py`'s `on_committed` (line 210) and `record_event` (line 99). `on_committed` is currently dead code (no caller). `record_event` is leader-side only (called from SWIM callbacks on the proposer) and therefore deterministic-by-replication.
- `raft/replicated_stats_store.py:77` — already accepts an explicit timestamp parameter; the caller is responsible for passing a deterministic value when applicable.

**Acceptance criteria**:
- All four `time.monotonic()` calls in `state_machine.py` apply handlers are gone. The file no longer imports `time`.
- `RaftLogEntry.timestamp` is wall-clock seconds when the clock is configured (production). The leader mints it via HLC; followers see the same value through replication.
- `tests/unit/distributed/raft/test_apply_replay.py::test_initiate_and_complete_cancellation_replay_byte_equal` passes.
- `tests/unit/distributed/raft/test_apply_replay.py::test_update_job_status_replay_byte_equal` passes.
- `tests/unit/distributed/raft/test_apply_replay.py::test_replay_full_lifecycle_byte_equal` passes.
- Manager-side `time.monotonic() - job.timestamp` / `time.monotonic() - job.completed_at` readers are consistent with the new wall-clock semantic of those fields.
- Backward compatibility: when no clock is supplied (existing unit tests), `RaftNode.propose()` still works using `time.monotonic()` as a fallback; behavior diverges from production only in the field's domain (monotonic vs. wall-clock), which is irrelevant when the test does not exercise wall-clock comparisons.

**Example**:
```python
# Before (state_machine.py:259-275): divergent across followers.
async def _apply_initiate_cancellation(self, command: RaftCommand) -> None:
    if not (job := self._job_manager.get_job_by_id(command.job_id)):
        return
    async with job.lock:
        job.status = "cancelling"
        job.timestamp = time.monotonic()  # each follower computes its own value

# After: identical on every follower.
async def _apply_initiate_cancellation(
    self,
    command: RaftCommand,
    entry: RaftLogEntry,
) -> None:
    if not (job := self._job_manager.get_job_by_id(command.job_id)):
        return
    async with job.lock:
        job.status = "cancelling"
        job.timestamp = entry.timestamp  # leader-set HLC value, replicated
```

**Tests** (`tests/unit/distributed/raft/test_apply_replay.py`, covering all eight categories):

- **Unit** (`test_initiate_and_complete_cancellation_replay_byte_equal`, `test_update_job_status_replay_byte_equal`, `test_replay_no_op_byte_equal`): each handler under test produces byte-equal state across two replicas.
- **Integration** (`test_replay_full_lifecycle_byte_equal`): status-update then cancellation; final state is byte-equal; field values match the last entry's `entry.timestamp`.
- **E2E**: deferred to multi-node integration in later phases; the apply-layer determinism contract is verified at the unit boundary here.
- **Race condition** (`test_replay_under_simulated_real_time_skew`): the two replicas apply the same entries interleaved with arbitrary real-time gaps; state stays byte-equal because the timestamp source is the log entry, not the local clock.
- **Memory leak**: not directly tested here; the apply handlers do not allocate per-entry retained state. The harness in `_snapshot_jobs` is deterministic and bounded.
- **Deadlock**: the apply path uses only `job.lock`, acquired in a single critical section per handler; replay test exercises both handlers in sequence with no nested locks.
- **Edge case** (`test_replay_no_op_byte_equal`): NO_OP entries leave state untouched; both replicas remain at the initial state.
- **Negative path** (`test_independent_managers_diverge_only_on_unreplicated_state`): sanity guard that the snapshot fields are exactly those mutated by apply; if a future change leaks a non-replicated field into the snapshot, the test fails — flagging the regression.

**Dependency**: None. Phase 1 onward writes new apply handlers using `entry.timestamp` from the start.

---

# Phase 1: Correctness foundation

## Item 1.1 — `ClusterIdentity` model and env config

**What**: Introduce the `ClusterIdentity` value object holding `(cluster_id, role, mtls_chain, cluster_uuid, advertised_address, node_id)`. Cluster UUID is generated only at bootstrap; joiners learn it.

**Why**: AD-52 §3. Identity must be a single value object that flows through every cluster RPC. Also unifies what is today scattered across `WorkerConfig`, `ManagerConfig`, `GateConfig`.

**Files**:
- Add `hyperscale/distributed/cluster/identity.py`:
  - `ClusterIdentity` dataclass with `cluster_id: str`, `role: NodeRole`, `node_id: str` (uuid4 hex), `cluster_uuid: str | None` (None until bootstrap/join completes), `mtls_cert_path: Path`, `mtls_key_path: Path`, `mtls_ca_path: Path`, `advertised_address: tuple[str, int] | None`.
  - `ClusterIdentity.from_env(env: Env, role: NodeRole) -> ClusterIdentity` factory.
  - `ClusterIdentity.with_cluster_uuid(uuid: str) -> ClusterIdentity` returning a new immutable instance.
- Add `hyperscale/distributed/cluster/models/__init__.py`.
- Modify `hyperscale/distributed/env/env.py`:
  - `CLUSTER_ID: StrictStr | None = None`
  - `CLUSTER_ROLE: Literal["gate", "manager", "worker"] | None = None`
  - `CLUSTER_MTLS_CERT: StrictStr | None = None`
  - `CLUSTER_MTLS_KEY: StrictStr | None = None`
  - `CLUSTER_MTLS_CA: StrictStr | None = None`
  - `CLUSTER_ADVERTISED_ADDRESS: StrictStr | None = None`
  - `CLUSTER_ENABLED: StrictBool = False` (kill switch — false until rollout)

**Acceptance criteria**:
- `ClusterIdentity` is immutable (frozen dataclass).
- `from_env()` validates that mTLS paths exist when `CLUSTER_ENABLED=True` and that cert subject role matches `CLUSTER_ROLE`.
- `node_id` is generated via `uuid4().hex` on each `from_env()` call — never reused across process restarts.
- `cluster_uuid` is `None` until set explicitly by bootstrap commit or join response.

**Example**:
```python
identity = ClusterIdentity.from_env(env, role=NodeRole.MANAGER)
assert identity.node_id != previous_run_node_id  # always fresh
# After bootstrap or join:
identity = identity.with_cluster_uuid("01HZAB...")
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_identity.py`):
  - `from_env()` constructs valid identity from minimal env.
  - `from_env()` raises when `CLUSTER_ENABLED=True` and cert missing.
  - `node_id` is fresh on each construction; verify 1000 invocations all distinct.
  - `with_cluster_uuid()` returns new instance, original unchanged.
- **Integration**: Wire `ClusterIdentity` into a `ManagerServer` startup; verify it propagates to `WatchClient` and `JoinCoordinator` (Phase 1.11).
- **E2E**: Three-node cluster forms — each node has distinct `node_id`, all share same `cluster_uuid` post-bootstrap.
- **Race condition**: 1000 concurrent `from_env()` calls — all produce distinct `node_id`.
- **Memory leak**: 1M `ClusterIdentity` allocations + dereferences, no growth (frozen dataclass with `slots=True`).
- **Deadlock**: N/A.
- **Edge case**: `CLUSTER_ROLE` set but `CLUSTER_ID` empty — explicit validation error.
- **Negative path**: Cert file readable but subject role does not match `CLUSTER_ROLE` — `IdentityValidationError`.

---

## Item 1.2 — Seed locator system (5 schemes)

**What**: Introduce `SeedLocator` protocol and five concrete implementations: `tcp://`, `dns://`, `dns-srv://`, `file://`, `exec://`.

**Why**: AD-52 §2. The only environment-facing primitive. Required before bootstrap or join can resolve peers.

**Files**:
- Add `hyperscale/distributed/cluster/seed_locators/__init__.py` — re-exports + `parse_locator(str) -> SeedLocator`.
- Add `hyperscale/distributed/cluster/seed_locators/base.py`:
  - `class SeedLocator(Protocol): async def resolve(self) -> list[tuple[str, int]]; @property scheme: str`
- Add `hyperscale/distributed/cluster/seed_locators/tcp_locator.py` — `TCPSeedLocator`, parses `tcp://host:port`.
- Add `hyperscale/distributed/cluster/seed_locators/dns_locator.py` — `DNSSeedLocator`, parses `dns://name:port`, uses `AsyncDNSResolver.resolve()`.
- Add `hyperscale/distributed/cluster/seed_locators/dns_srv_locator.py` — `DNSSrvSeedLocator`, parses `dns-srv://_service._proto.domain`, uses `AsyncDNSResolver.resolve_srv()`.
- Add `hyperscale/distributed/cluster/seed_locators/file_locator.py` — `FileSeedLocator`, parses `file:///path`. Watches mtime via async polling at jittered 1s interval.
- Add `hyperscale/distributed/cluster/seed_locators/exec_locator.py` — `ExecSeedLocator`, parses `exec:///path`. Refreshes via `asyncio.create_subprocess_exec` at jittered configurable interval (default 60s).
- Add `hyperscale/distributed/cluster/seed_locators/security.py`:
  - `validate_locator_security(locator: SeedLocator) -> None` — enforces ownership/perms for `file://` and `exec://` (no world-writable parents, absolute path, owner matches euid).
- Modify `hyperscale/distributed/env/env.py`:
  - `CLUSTER_SEEDS: StrictStr = ""` (comma-separated locator URIs).
  - `CLUSTER_SEED_REFRESH_INTERVAL: StrictStr = "60s"`.
  - `CLUSTER_MAX_SEED_CANDIDATES: StrictInt = 64`.

**Acceptance criteria**:
- `parse_locator("tcp://10.0.0.1:8080")` returns `TCPSeedLocator` with `resolve()` yielding `[("10.0.0.1", 8080)]`.
- `FileSeedLocator` detects mtime change and re-reads on next `resolve()` call.
- `ExecSeedLocator` enforces absolute path + ownership check at construction.
- All resolvers return deduplicated address lists, bounded by `CLUSTER_MAX_SEED_CANDIDATES`.
- Negative resolution does not raise — returns empty list.
- Re-resolve cadence is jittered per AD-21 to avoid thundering herd.

**Example**:
```python
locator = parse_locator("dns-srv://_hyperscale._tcp.example.internal")
addresses = await locator.resolve()
# [("10.0.1.5", 8080), ("10.0.1.6", 8080), ...]
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/seed_locators/`):
  - One file per locator type. `TCPSeedLocator` parses port correctly; rejects missing port.
  - `DNSSeedLocator` mocks `AsyncDNSResolver`; verifies dedupe.
  - `FileSeedLocator` reads file, then mtime-bumped re-read picks up changes.
  - `ExecSeedLocator` invokes command, parses stdout. Subprocess respects timeout.
- **Integration** (`tests/integration/cluster/test_seed_locators.py`):
  - Mix of schemes in one `--seeds` flag — all resolve, results merged.
- **E2E**: Cluster bootstrap using each of 5 schemes in turn (5 scenarios).
- **Race condition**: Concurrent `resolve()` calls on same `FileSeedLocator` during mtime change — return value is consistent (not torn).
- **Memory leak**: 10000 `resolve()` cycles on `ExecSeedLocator`; verify subprocesses cleaned up via `asyncio.subprocess.communicate()`.
- **Deadlock**: `FileSeedLocator` while-loop respects cancellation; `asyncio.wait_for` with short timeout completes.
- **Edge case**:
  - `tcp://[::1]:8080` (IPv6) parses correctly.
  - File with 5000 entries truncated at `CLUSTER_MAX_SEED_CANDIDATES` with WARN log.
  - Exec command exits 1 — empty result, no exception, WARN log.
- **Negative path**:
  - `exec:///relative/path` rejected at parse time.
  - `file:///etc/shadow` (not owned by euid) rejected with `LocatorSecurityError`.
  - Malformed scheme (`http://...`) rejected at `parse_locator()`.

---

## Item 1.3 — Wire seed locators into `discovery/`

**What**: Add `LocatorProvider` injection point to `DiscoveryService`. Existing DNS resolution stays; new locator schemes plug into the same resolution pipeline.

**Why**: AD-28's `DiscoveryService` already manages peer registration, locality filtering, EWMA selection, and connection pooling. AD-52 locators should feed the same pipeline, not bypass it.

**Files**:
- Modify `hyperscale/distributed/discovery/discovery_service.py`:
  - Add `seed_locators: list[SeedLocator] | None = None` constructor arg.
  - In `discover_peers()`, after DNS resolution: call each `SeedLocator.resolve()`, deduplicate against DNS results, register via existing `add_peer_from_info()`.
- Modify `hyperscale/distributed/discovery/__init__.py` — export `SeedLocator` re-export from `cluster/seed_locators/`.
- Modify `hyperscale/distributed/discovery/models/discovery_config.py`:
  - Add `seed_locators: list[str] = field(default_factory=list)` — list of locator URIs.

**Acceptance criteria**:
- `DiscoveryService` without `seed_locators` behaves identically to today.
- With `seed_locators`, resolved addresses go through the same `add_peer()` path as DNS results (locality filter, EWMA, pool).
- Dedupe is by `(host, port)`.
- Locator resolution is parallelized via `asyncio.gather` with `return_exceptions=True`; failures log but don't block.

**Example**:
```python
discovery = DiscoveryService(
    config=DiscoveryConfig(dns_names=["managers.cluster"], seed_locators=["tcp://10.0.0.5:8080"]),
    ...
)
await discovery.discover_peers()
```

**Tests**:
- **Unit** (`tests/unit/distributed/discovery/test_seed_locator_integration.py`):
  - `DiscoveryService` with empty `seed_locators` ≡ current behavior.
  - DNS + locator results merge with dedupe.
- **Integration** (`tests/integration/cluster/test_discovery_locators.py`): Run `discover_peers` with mix of DNS and 4 locator schemes; verify all peers registered.
- **E2E**: Manager starts with `--seeds=dns://...,tcp://...,file://...`; observed peers contain all three sources.
- **Race condition**: Concurrent `discover_peers()` invocations — `add_peer()` is idempotent; no duplicate entries.
- **Memory leak**: 10000 `discover_peers()` cycles; pool sizes stay bounded.
- **Deadlock**: `DiscoveryService.discover_peers()` while `pool.acquire()` holds a connection — verify documented lock order (single-lock discipline, no nested locks during I/O).
- **Edge case**: Locator returns 0 addresses while DNS returns 5 — merged result is 5.
- **Negative path**: Locator raises during `resolve()` — error logged via `DiscoveryMetrics.locator_failure`, other locators still resolve.

---

## Item 1.4 — `cluster/` module skeleton

**What**: Create the empty package layout for the rest of Phase 1 to land into, with placeholder files and `__init__.py` exports.

**Why**: Lets subsequent items target precise files. Single-file-per-class convention per CLAUDE.md.

**Files**:
- Add `hyperscale/distributed/cluster/__init__.py` (re-exports).
- Add directory `hyperscale/distributed/cluster/models/` with `__init__.py`.
- Add stub files (empty class definitions raising `NotImplementedError`):
  - `cluster/bootstrap.py` — `BootstrapCoordinator`
  - `cluster/join.py` — `JoinCoordinator`
  - `cluster/membership_log.py` — entry-type classes
  - `cluster/joint_consensus.py` — `JointConsensusState`
  - `cluster/learner.py` — `LearnerTracker`
  - `cluster/fence.py` — `ClusterRPCFence`, `validate_fence()`
  - `cluster/failure_detector.py` — `HybridFailureDetector` (composes existing)
  - `cluster/phi_accrual.py` — `PhiAccrualDetector`
  - `cluster/watch_server.py` — `WatchServer`
  - `cluster/watch_client.py` — `WatchClient`
  - `cluster/disconnected.py` — `SoftStateCache`
  - `cluster/drain.py` — `DrainCoordinator`
  - `cluster/force_remove.py` — `ForceRemoveCoordinator`
  - `cluster/freeze.py` — `FreezeController`
  - `cluster/snapshot_import_export.py` — `SnapshotIO`
  - `cluster/observability.py` — metrics + structured-events emitter
  - `cluster/federation.py` — `DatacenterCatalog`
- Add `tests/unit/distributed/cluster/__init__.py`.
- Add `tests/integration/cluster/__init__.py`.

**Acceptance criteria**:
- Package imports cleanly.
- Each stub class raises `NotImplementedError("AD-52 Phase X.Y")` on every method.
- `__init__.py` re-exports follow alphabetical convention (per discovered codebase pattern).

**Tests**:
- **Unit**: `test_import_cluster_skeleton.py` imports every public name; asserts module structure.
- All other test categories: **N/A** (skeleton item).

---

## Item 1.5 — `BootstrapCoordinator` with `--initial-members`

**What**: Implement deterministic bootstrap protocol — operator-provided `--initial-members=node_id@locator,...` list, founding nodes agree on hash, lowest `node_id` runs pre-vote, seed membership committed at term 1.

**Why**: AD-52 §4. Bootstrap and join are distinct protocols. The race-prone "lowest-id-over-a-window" approach is rejected in favor of operator-explicit seed list.

**Files**:
- Modify `hyperscale/distributed/cluster/bootstrap.py`:
  - `BootstrapCoordinator` with `__init__(identity, initial_members, raft, task_runner, logger)`.
  - `async start() -> BootstrapResult` — runs the state machine `DISCOVERING → FORMING → BOOTSTRAPPING → JOINED` or fails fast.
  - `_handshake_with_peers()` — resolves locators, exchanges `BootstrapHello`.
  - `_validate_consensus()` — confirms all peers have identical `initial_members_hash = sha256(sorted(initial_members))`.
  - `_run_pre_vote_at_term_1()` — invokes existing AD-5 pre-vote.
  - `_propose_seed_membership()` — proposes `EnterJoint(∅ → founding_set)` then `LeaveJoint(founding_set)` + `ClusterUuid(uuid4())` + `ClusterSize(N)` in one Raft batch.
- Modify `hyperscale/distributed/cluster/models/bootstrap_messages.py`:
  - `BootstrapHello(Message)` — fields `my_node_id, runtime_uuid, cluster_id, role, initial_members_hash, protocol_version`.
  - `BootstrapHelloResponse(Message)` — `their_node_id, their_runtime_uuid, agreement: bool, disagreement_reason: str | None`.
- Modify `hyperscale/distributed/env/env.py`:
  - `CLUSTER_INITIAL_MEMBERS: StrictStr = ""` (comma-separated `node_id@locator`).
  - `CLUSTER_SIZE: StrictInt = 0` (0 = not configured; bootstrap fails if mismatch with `len(initial_members)`).
  - `CLUSTER_BOOTSTRAP_WINDOW: StrictStr = "5s"`.
- Modify `hyperscale/logging/hyperscale_logging_models.py` — add:
  - `ClusterBootstrapStarted(Entry)`
  - `ClusterBootstrapCompleted(Entry)`
  - `ClusterBootstrapFailed(Entry)`

**Acceptance criteria**:
- `BootstrapCoordinator.start()` returns `BootstrapResult.SUCCESS` only after seed membership commit at term 1.
- Mismatched `initial_members_hash` between peers fails fast within `CLUSTER_BOOTSTRAP_WINDOW`, exits non-zero.
- `CLUSTER_SIZE != len(initial_members)` rejected at startup with explicit error.
- After bootstrap, `--initial-members` is ignored on subsequent restarts (cluster is JOINED; joiners use `--seeds`).
- `cluster_uuid` is freshly generated at bootstrap and committed atomically with the seed membership entry.

**Example**:
```bash
hyperscale-manager \
  --cluster-id=prod-uswest-managers \
  --role=manager \
  --initial-members=A@tcp://10.0.1.5:8080,B@tcp://10.0.1.6:8080,C@tcp://10.0.1.7:8080 \
  --cluster-size=3
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_bootstrap_coordinator.py`):
  - State machine transitions through DISCOVERING → FORMING → BOOTSTRAPPING → JOINED.
  - `initial_members_hash` computed deterministically; ordering-independent.
  - Mismatched hash → state machine halts in `FAILED`.
- **Integration** (`tests/integration/cluster/test_bootstrap_three_node.py`):
  - Launch 3 manager processes simultaneously with identical `--initial-members`; cluster forms within 5s.
- **E2E** (`tests/integration/cluster/test_bootstrap_real_dns.py`):
  - Kubernetes-like setup with headless Service (mocked DNS); cluster forms.
- **Race condition** (`tests/integration/cluster/test_bootstrap_simultaneous_split.py`):
  - 3 nodes start, network partition isolates 1; remaining 2 should NOT form quorum (size=3 requires 2 of 3 — they CAN form, but the test verifies behavior is "2-of-3 majority wins, 1 left over rejoins after partition heals").
  - Verify only one node sees itself as bootstrap leader.
- **Memory leak**: Launch + bootstrap + shutdown 100 clusters in series; assert no growth via `tracemalloc`.
- **Deadlock**: Bootstrap with `task_runner.shutdown()` invoked during `FORMING` — coordinator cancels cleanly.
- **Edge case**:
  - Single-node bootstrap (`CLUSTER_SIZE=1`) — succeeds without peer handshake.
  - All initial members at same address (dev laptop, 3 ports) — succeeds.
  - One initial member unreachable for entire bootstrap window — fails after window with `INSUFFICIENT_PEERS`.
- **Negative path**:
  - Empty `--initial-members` and `CLUSTER_ENABLED=True` → process exits non-zero with explicit error.
  - `node_id` in `--initial-members` doesn't match local `node_id` → process exits with `NOT_IN_INITIAL_SET`.
  - `--cluster-size=5` but `--initial-members` has 3 entries → exit with `SIZE_MISMATCH`.

---

## Item 1.6 — Membership log entry types

**What**: Define the Raft log entry types for membership changes — `EnterJoint`, `LeaveJoint`, `AddLearner`, `Promote`, `Remove`, `UpdateMetadata`, `RegisterDatacenter`, `UpdateAdvertisedAddress`, `ResizeCluster`.

**Why**: AD-52 §6. These are the primitives the BootstrapCoordinator, JoinCoordinator, and all later items propose through Raft.

**Files**:
- Modify `hyperscale/distributed/cluster/membership_log.py`:
  - One `@dataclass(slots=True, kw_only=True)` per entry type. Each inherits from `MembershipEntry` base with `schema_version: int = 1`.
- Modify `hyperscale/distributed/raft/models/command_types.py`:
  - Add `ClusterCommandType(str, Enum)` with members `ENTER_JOINT, LEAVE_JOINT, ADD_LEARNER, PROMOTE, REMOVE, UPDATE_METADATA, REGISTER_DATACENTER, UPDATE_ADVERTISED_ADDRESS, RESIZE_CLUSTER`.
- Modify `hyperscale/distributed/raft/state_machine.py` and `gate_state_machine.py`:
  - Register handlers for each `ClusterCommandType` with `schema_version=1`.
- Add `hyperscale/distributed/cluster/membership_state.py`:
  - `ClusterMembershipState` — owns the in-memory replicated membership view. Mutated only by apply handlers (deterministic).
  - Fields: `members: dict[node_id, MemberRecord]`, `cluster_uuid: str | None`, `cluster_size: int`, `membership_epoch: int`, `joint_state: JointState | None`.

**Acceptance criteria**:
- All entry types serializable via cloudpickle; deserialization is `RestrictedUnpickler`-safe.
- Apply handlers mutate `ClusterMembershipState` deterministically — `@deterministic_apply` decorated.
- `membership_epoch` increments by 1 per committed membership-changing entry.
- `EnterJoint`/`LeaveJoint` apply handlers maintain `joint_state` consistency: `EnterJoint` sets it, `LeaveJoint` clears it.
- `UpdateAdvertisedAddress` does not change `node_id`; only the address mapping.

**Example**:
```python
@dataclass(slots=True, kw_only=True)
class AddLearner(MembershipEntry):
    schema_version: int = 1
    node_id: str
    role: NodeRole
    advertised_address: tuple[str, int]
    capabilities: frozenset[str]
    added_at_epoch: int
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_membership_entries.py`):
  - Round-trip serialize/deserialize for each type.
  - Apply each handler in isolation with mock `ClusterMembershipState`; verify expected mutation.
- **Integration**: Three-node Raft cluster proposes all 9 entry types in sequence; all members converge to identical state.
- **E2E**: New manager joins a 3-node cluster via `AddLearner`, gets promoted via `Promote`, removed via `Remove`.
- **Race condition**: Concurrent proposals of `Promote(X)` and `Remove(X)` — Raft serializes; final state is consistent (whichever committed first wins).
- **Memory leak**: Apply 100000 `UpdateMetadata` entries; verify `ClusterMembershipState` size scales O(members), not O(entries).
- **Deadlock**: N/A (state mutation is sync inside apply; no awaits inside critical section).
- **Edge case**:
  - `EnterJoint` with `old_members = new_members` (no-op) → apply rejects with `INVALID_JOINT_TRANSITION`.
  - `Promote` of unknown `node_id` → reject.
  - `LeaveJoint` without preceding `EnterJoint` → reject.
- **Negative path**:
  - Apply receives entry with `schema_version=999` → `RaftUnknownSchemaEntry` logged (per Item 0.1), apply loop continues.
  - Malformed entry (cloudpickle decode fails) → `MembershipEntryDecodeError` logged, apply loop continues.

---

## Item 1.7 — Joint consensus in `raft/`

**What**: Extend `RaftNode` to honor joint configurations: during `EnterJoint`/`LeaveJoint` transition, quorum requires majorities of **both** `old_members` and `new_members`.

**Why**: AD-52 §6. Single-step membership changes admit split-brain. Joint consensus eliminates the entire class. depends-on: Item 1.6.

**Files**:
- Modify `hyperscale/distributed/raft/raft_node.py`:
  - Add `_joint_config: JointConfig | None` attribute (mirrors `ClusterMembershipState.joint_state` but local to Raft).
  - Modify `_has_quorum()`:
    ```python
    def _has_quorum(self, votes_or_acks: set[NodeId]) -> bool:
        if self._joint_config is None:
            return len(votes_or_acks) > len(self._members) / 2
        # Joint mode: need majorities of BOTH configurations
        old_quorum = len({v for v in votes_or_acks if v in self._joint_config.old}) > len(self._joint_config.old) / 2
        new_quorum = len({v for v in votes_or_acks if v in self._joint_config.new}) > len(self._joint_config.new) / 2
        return old_quorum and new_quorum
    ```
  - Wire `apply_committed_entries()` to update `_joint_config` when `EnterJoint`/`LeaveJoint` apply.
- Add `hyperscale/distributed/cluster/joint_consensus.py`:
  - `JointConsensusState` dataclass — `old: frozenset[NodeId], new: frozenset[NodeId]`.
  - `propose_membership_change(raft, target_members)` helper that proposes `EnterJoint` + waits for commit + proposes `LeaveJoint`.
- Modify `hyperscale/distributed/raft/raft_consensus.py`:
  - `update_membership()` calls into `joint_consensus.propose_membership_change()` instead of single-step update.

**Acceptance criteria**:
- During joint phase, replication to peers in BOTH old and new sets.
- Reads/writes during joint phase require commit confirmation from both sets.
- `LeaveJoint` cannot commit until `EnterJoint` is committed AND new quorum is reached.
- If leader dies during joint phase, new leader (elected by joint quorum) completes the transition.

**Example**:
```python
await joint_consensus.propose_membership_change(
    raft=consensus.get_node(cluster_job_id),
    target_members={A, B, C, D},  # was {A, B, C}
)
# Internally:
#   1. EnterJoint(old={A,B,C}, new={A,B,C,D})
#   2. Wait for commit (needs 2-of-3 AND 3-of-4)
#   3. LeaveJoint(members={A,B,C,D})
#   4. Wait for commit (needs 3-of-4)
```

**Tests**:
- **Unit** (`tests/unit/distributed/raft/test_joint_consensus.py`):
  - `_has_quorum()` under joint vs non-joint configs; truth tables for 3→4, 5→3, 3→3 transitions.
  - `propose_membership_change()` orders entries correctly.
- **Integration** (`tests/integration/raft/test_joint_consensus_failover.py`):
  - 3-node cluster transitions to 5-node via joint consensus; mid-transition kill 1 node; cluster completes transition.
- **E2E** (`tests/integration/cluster/test_membership_scale_up.py`):
  - Cluster of 3 managers scales to 5 via `AddLearner` + `Promote` triggering joint transitions.
- **Race condition** (`tests/integration/raft/test_joint_concurrent_changes.py`):
  - Two `propose_membership_change` calls concurrently — Raft rejects second until first commits (`MEMBERSHIP_CHANGE_IN_PROGRESS`).
- **Memory leak**: 1000 joint transitions; verify `_joint_config` cleared after each `LeaveJoint`.
- **Deadlock** (`tests/integration/raft/test_joint_leader_death.py`):
  - Kill leader during joint phase; new leader elected and completes transition within 30s.
- **Edge case**:
  - `EnterJoint(old=∅, new={A,B,C})` — bootstrap case; quorum from new only.
  - `EnterJoint(old={A,B,C}, new=∅)` — cluster destruction; reject (would leave no voters).
- **Negative path**:
  - Joint transition stalls because minority partition can't reach old-set quorum — `LeaveJoint` does not commit; leader proposes nothing further; SWIM eventually detects unreachable peers.

---

## Item 1.8 — Learner state in `raft/`

**What**: Implement non-voting learner role. `AddLearner` adds a member that replicates the log but does not vote and does not count toward quorum. `Promote` transitions learner to voter via joint consensus.

**Why**: AD-52 §7. New members can lag the leader by gigabytes; voting before catch-up risks quorum disruption. depends-on: Item 1.7.

**Files**:
- Modify `hyperscale/distributed/raft/raft_node.py`:
  - Add `_learners: dict[NodeId, LearnerState]` attribute.
  - Modify `replicate_to_followers()` — include learners in replication targets but never count their match_index for commit quorum.
  - Modify `_has_quorum()` — exclude learners from quorum sets.
  - Modify `handle_request_vote()` — refuse vote requests from learners; learners refuse to grant votes (they are not voters).
- Add `hyperscale/distributed/cluster/learner.py`:
  - `LearnerTracker` — owns `_learners` view, computes lag, decides promotion-eligibility (`lag <= learner_promote_threshold`).
  - `LearnerTracker.maybe_propose_promotion()` — called by leader periodically; proposes `Promote` when learner is caught up.
  - `LearnerTracker.evict_stale_learners()` — proposes `Remove` for learners exceeding `learner_max_lifetime`.
- Modify `hyperscale/distributed/env/env.py`:
  - `CLUSTER_LEARNER_PROMOTE_THRESHOLD: StrictInt = 256`.
  - `CLUSTER_LEARNER_MAX_LIFETIME: StrictStr = "30min"`.

**Acceptance criteria**:
- Learner receives `AppendEntries` and applies entries to its state machine but does not vote.
- Leader's `commit_index` advances based on voter quorum only; learners catch up asynchronously.
- `Promote(node_id)` triggers a joint-consensus transition (voter set changes).
- Stale learner (lifetime exceeded, lag > threshold) is `Remove`'d by leader.
- During `InstallSnapshot` to a learner, the snapshot is streamed without blocking voter replication.

**Example**:
```python
# Manager-B joining a 3-voter cluster:
#   1. Leader proposes AddLearner(B)
#   2. Leader sends InstallSnapshot + AppendEntries to B
#   3. B's commit_index catches up (lag < 256)
#   4. LearnerTracker.maybe_propose_promotion() fires Promote(B)
#   5. Joint transition: old={A,C,D}, new={A,B,C,D}
#   6. B is now a voter
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_learner_tracker.py`):
  - Lag computation correct.
  - Promotion proposed when lag ≤ threshold.
  - Eviction proposed when lifetime exceeded.
- **Integration** (`tests/integration/raft/test_learner_catchup.py`):
  - 3-voter cluster + 1 new learner; learner catches up and gets promoted within 30s.
- **E2E** (`tests/integration/cluster/test_learner_full_flow.py`):
  - Manager joins via `JoinCoordinator` (Item 1.11); transitions LEARNER → VOTER end-to-end.
- **Race condition**: Two learners join simultaneously; both catch up; promotions serialize through Raft.
- **Memory leak**: 1000 learner add+evict cycles; verify `_learners` dict clears.
- **Deadlock**: Learner being promoted while leader changes — new leader inherits learner state via Raft log replay.
- **Edge case**:
  - Learner becomes leader of nothing (single-node cluster?) — refused; learners cannot be candidates.
  - Promote race with Remove — Raft serializes; whichever wins commits.
- **Negative path**:
  - Learner with permanent network issue (lag never decreases) → evicted at `learner_max_lifetime`.
  - Snapshot install fails mid-stream → learner retries; if still failing after `INSTALL_SNAPSHOT_MAX_ATTEMPTS`, evicted.

---

## Item 1.9 — ReadIndex for linearizable reads

**What**: Implement Raft ReadIndex protocol — any voter can serve linearizable reads by asking leader for current commit index, waiting until local apply index ≥ that, and reading from state machine.

**Why**: AD-52 §11. Cluster membership queries need linearizability without burning log entries. Foundation for Item 2.4 (Watch streams) and Item 4.4 (routing reads).

**Files**:
- Modify `hyperscale/distributed/raft/raft_node.py`:
  - Add `async read_index() -> int` method on leader — confirms leadership via heartbeat-quorum check, returns current commit_index.
  - Add `async wait_for_apply_index(index: int, timeout: float) -> None` method on any node — blocks until `_last_applied >= index`.
- Add `hyperscale/distributed/raft/read_index.py`:
  - `ReadIndexService` — coordinates the protocol. Caller flow: `await service.linearizable_read(reader_state_machine, fn)`.
- Modify `hyperscale/distributed/raft/raft_consensus.py`:
  - Expose `linearizable_read(reader_state_machine, fn)` as the public API.
- Modify `hyperscale/distributed/env/env.py`:
  - `CLUSTER_READ_INDEX_TIMEOUT: StrictStr = "5s"`.
  - `CLUSTER_LEADER_LEASE_ENABLED: StrictBool = False` (Item 3.7 enables this).

**Acceptance criteria**:
- `read_index()` on leader: sends heartbeats to a quorum of voters, confirms still leader, returns `_commit_index`.
- `linearizable_read()` waits until follower's apply index reaches read_index, then invokes `fn(state_machine)`.
- During leadership change, in-flight ReadIndex calls fail fast with `LeadershipLostError` (caller retries).
- ReadIndex calls do NOT write to the log (zero replication overhead).

**Example**:
```python
membership = await consensus.linearizable_read(
    cluster_job_id="cluster",
    fn=lambda sm: sm.get_membership_snapshot(),
)
# Caller is guaranteed to see all committed membership changes as of call time.
```

**Tests**:
- **Unit** (`tests/unit/distributed/raft/test_read_index.py`):
  - Leader's `read_index()` returns current `_commit_index` after heartbeat confirmation.
  - Follower's `wait_for_apply_index()` returns immediately if already caught up.
- **Integration** (`tests/integration/raft/test_read_index_linearizability.py`):
  - Concurrent writes + reads — every read sees a state consistent with some serialization including its own preceding writes.
- **E2E**: Cluster of 5 managers; one issues `linearizable_read` 1000× during active workload; no stale reads.
- **Race condition** (`tests/integration/raft/test_read_index_leadership_change.py`):
  - ReadIndex in flight when leadership transfers; caller sees `LeadershipLostError`, retries against new leader, succeeds.
- **Memory leak**: 100000 `linearizable_read` cycles; verify no futures retained.
- **Deadlock** (`tests/integration/raft/test_read_index_partition.py`):
  - Leader partitioned from quorum — `read_index()` times out (does not hang).
- **Edge case**: ReadIndex called against the leader itself — no heartbeat round-trip needed; returns immediately.
- **Negative path**:
  - Reader is a learner — `read_index()` rejects with `LEARNER_CANNOT_READ_LINEARIZABLE` (alternatively allows stale read).
  - Timeout exceeded → `ReadIndexTimeoutError`.

---

## Item 1.10 — `ClusterRPCFence` header + protocol-boundary validation

**What**: Define the fence header `{cluster_uuid, membership_epoch, sender_node_id, sender_term}` and validate it at the TCP protocol boundary before any handler runs.

**Why**: AD-52 §6. Stale or zombie nodes must not write into the wrong cluster generation, into a removed-member slot, or under an old term. depends-on: Item 1.6.

**Files**:
- Add `hyperscale/distributed/cluster/fence.py`:
  - `ClusterRPCFence` dataclass — packed binary representation (40 bytes: 16 uuid + 8 epoch + 8 term + 8 sender hash).
  - `validate_fence(fence, local_state) -> FenceValidationResult` — returns `OK | WRONG_CLUSTER | STALE_MEMBER | STALE_TERM | STALE_MEMBERSHIP`.
- Add `hyperscale/distributed/cluster/models/fence_header.py` — wire encoding.
- Modify `hyperscale/distributed/server/server/mercury_sync_base_server.py`:
  - In `process_tcp_server_request()`, after address parsing (~line 680) and before replay guard (~line 690):
    ```python
    fence_bytes = rest[68:108]  # 40 bytes after clock(64) and data_len(4)
    fence = ClusterRPCFence.from_bytes(fence_bytes)
    result = validate_fence(fence, self._cluster_state)
    if result is not OK:
        self._tcp_drop_counter.increment_fence_rejected()
        await self._tcp_logger.log(ClusterFenceRejection(reason=result.name, ...))
        return  # silent drop with metric + structured log
    ```
  - Update header format: `address<handler<clock(64B)data_len(4B)fence(40B)data(N B)`.
- Modify `hyperscale/distributed/cluster/observability.py`:
  - Counter `cluster_fence_rejection_total{reason}`.
- Modify `hyperscale/distributed/server/protocol/in_flight_tracker.py`:
  - Add cluster handler names to `_CONTROL_HANDLERS` frozenset (priority CRITICAL).
- Modify `hyperscale/distributed/server/hooks/tcp/server.py`:
  - Extend `@tcp.receive()` decorator to accept optional `priority: MessagePriority` parameter; defaults to handler-name classification.
- Modify `hyperscale/logging/hyperscale_logging_models.py` — add `ClusterFenceRejection(Entry)`.

**Acceptance criteria**:
- Fence is read once per RPC before handler dispatch; CPU cost < 1μs per validation.
- All 5 rejection reasons surface as distinct labels in `cluster_fence_rejection_total`.
- Non-cluster RPCs (job dispatch, worker registration, etc.) carry a zero-fence (all zeros) which validates trivially OK — this preserves backward compatibility for handlers that don't need fencing.
- `@tcp.receive(priority=MessagePriority.CRITICAL)` decoration on cluster handlers ensures they bypass load shedding.

**Example**:
```python
@tcp.receive(priority=MessagePriority.CRITICAL)
async def cluster_join_request(self, addr, payload, clock_time):
    # Fence already validated by the time we get here.
    ...
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_fence.py`):
  - Each rejection reason triggered with crafted fence.
  - Encode/decode round-trips 1M times under 100ms total.
- **Integration** (`tests/integration/cluster/test_fence_protocol_integration.py`):
  - Cluster of 3 managers; killed manager (D) tries to talk to cluster with stale UUID → rejected.
  - Cluster scales (membership_epoch bumps); old gossip with stale epoch → rejected.
- **E2E**: Zombie pod from previous cluster generation attempts to join — rejected at handshake.
- **Race condition** (`tests/integration/cluster/test_fence_membership_change_race.py`):
  - During joint transition, RPC arrives with `membership_epoch = current - 1` — accepted within `max_membership_lag`, rejected beyond.
- **Memory leak**: 10M `validate_fence` calls; verify no retained state.
- **Deadlock**: N/A (validation is pure-function).
- **Edge case**:
  - Empty cluster (no `cluster_uuid` set yet, pre-bootstrap) — accept zero-fence; reject any non-zero (pre-bootstrap RPC must have come from somewhere bogus).
  - `sender_term > receiver_term` — accepted; receiver steps down (per AD-10).
- **Negative path**:
  - Fence header truncated (32 bytes instead of 40) — `FenceDecodeError`, drop silently.
  - Fence header all-zeros on a cluster RPC handler that requires fence — rejected as `MISSING_FENCE`.

---

## Item 1.11 — `JoinCoordinator` (live-cluster join)

**What**: Implement the live-join protocol — node without `--initial-members` discovers the cluster via `--seeds`, handshakes with a JOINED peer, gets routed to leader, requests `AddLearner`, catches up, gets `Promote`'d.

**Why**: AD-52 §5. depends-on: Items 1.1, 1.2, 1.6, 1.7, 1.8, 1.10.

**Files**:
- Modify `hyperscale/distributed/cluster/join.py`:
  - `JoinCoordinator(identity, seed_locators, discovery_service, raft, task_runner, logger)`.
  - `async start() -> JoinResult` — runs state machine.
  - `_probe_candidates()` — handshakes seeds, builds candidate set.
  - `_find_leader(candidates)` — follows leader_hint via responses.
  - `_send_join_request(leader_addr)` — sends `JoinRequest`, awaits `JoinAccepted`.
  - `_wait_for_promotion()` — passively waits for `Promote` entry to apply.
- Add `hyperscale/distributed/cluster/models/join_messages.py`:
  - `JoinHello`, `JoinHelloResponse`, `JoinRequest`, `JoinAccepted`, `JoinRejected`.
- Modify `hyperscale/distributed/cluster/membership_log.py`:
  - Provide leader-side `process_join_request()` helper that wraps `AddLearner` proposal.
- Modify `hyperscale/logging/hyperscale_logging_models.py` — add `ClusterMemberAdded`, `ClusterMemberPromoted`.

**Acceptance criteria**:
- Joiner with no `--initial-members` exits immediately into JOIN mode.
- Probe + handshake validates `cluster_id`, `protocol_version`, `role` (per AD-28 matrix).
- `JoinRequest` to leader is idempotent (joiner can retry on transient failures with same `node_id`).
- Leader proposes `AddLearner`; commit triggers `JoinAccepted` response with `cluster_uuid`, membership snapshot, fence_token.
- Joiner enters Raft as learner, receives InstallSnapshot, catches up.
- `LearnerTracker` proposes `Promote` when caught up; joiner observes commit and reports JOINED.
- Full join (cold seed → JOINED) completes in p99 < 30s for 100MB state.

**Example**:
```bash
hyperscale-manager \
  --cluster-id=prod-uswest-managers \
  --role=manager \
  --seeds=dns://manager-headless.ns.svc:8080
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_join_coordinator.py`):
  - State machine transitions DISCOVERING → JOINING → LEARNING → JOINED.
  - Handshake validates cluster_id, role, protocol version.
- **Integration** (`tests/integration/cluster/test_join_existing_cluster.py`):
  - 3-node cluster; 4th joins; converges within 30s.
- **E2E** (`tests/integration/cluster/test_join_during_workload.py`):
  - Cluster of 3 managers under active workload; 4th joins; no workflow disruption observed.
- **Race condition** (`tests/integration/cluster/test_join_leader_change.py`):
  - Leader changes during join (just after `AddLearner` proposal, before commit) — joiner retries against new leader, eventually succeeds.
- **Memory leak**: 100 join+leave cycles on a stable 3-node cluster; pool sizes bounded.
- **Deadlock**: Joiner's `JoinCoordinator.start()` cancellable mid-join; cleanup runs to completion.
- **Edge case**:
  - Joiner's `node_id` collides with existing member (1-in-2^122 luck) — leader rejects with `NODE_ID_COLLISION`; joiner generates new UUID and retries.
  - All seeds unreachable on first attempt — joiner retries with backoff; eventually succeeds when seeds become reachable.
- **Negative path**:
  - Joiner uses `cluster_id` not matching cluster — leader rejects with `WRONG_CLUSTER_ID`; joiner exits non-zero.
  - Joiner's mTLS cert subject role doesn't match `--role` — rejected with `ROLE_MISMATCH`; exits non-zero.
  - Joiner sent to leader but leader is in `FROZEN` mode (Item 3.3) — rejected with `MEMBERSHIP_FROZEN`; retries with backoff.

---

## Item 1.12 — Wire `ClusterIdentity` and bootstrap/join into all three node servers

**What**: Modify `GateServer`, `ManagerServer`, `WorkerServer` to load `ClusterIdentity`, optionally run bootstrap or join, and gate startup on cluster readiness.

**Why**: Without this wiring, Phase 1 is library code with no runtime effect. depends-on: Items 1.1, 1.5, 1.11.

**Files**:
- Modify `hyperscale/distributed/nodes/manager/server.py`:
  - In `__init__()`, after config: `self._cluster_identity = ClusterIdentity.from_env(env, role=NodeRole.MANAGER)`.
  - In `_init_modules()`, instantiate `self._bootstrap_coordinator` or `self._join_coordinator` based on whether `CLUSTER_INITIAL_MEMBERS` is set.
  - In `start()`, after `start_server()` and before `_register_with_peer_managers()`: `await self._cluster_coordinator.start()`. Wait for `cluster_ready` flag.
  - In `stop()`, drain and shutdown cluster coordinator.
- Modify `hyperscale/distributed/nodes/gate/server.py` — analogous wiring with `NodeRole.GATE`.
- Modify `hyperscale/distributed/nodes/worker/server.py` — workers don't bootstrap/join Raft clusters; they only resolve manager seeds via `SeedLocator`. Replace static `seed_managers` list with locator-based resolution. (Workers stay outside Raft per AD-38.)
- Modify `hyperscale/distributed/nodes/worker/discovery.py`:
  - `WorkerDiscoveryManager` accepts `SeedLocator` list, resolves on startup + refresh interval.
- Add `cluster_ready` lifecycle hook to `HealthAwareServer`:
  - `self._cluster_ready: asyncio.Event` — set by bootstrap/join completion.
- Modify `tests/integration/cluster/` — add `test_full_stack_bootstrap.py` (Gates + Managers + Workers all using new flow).

**Acceptance criteria**:
- Gate and Manager servers refuse to accept work until `_cluster_ready` is set.
- Worker registration succeeds against managers regardless of whether managers have finished bootstrapping (worker-manager registration is local-only, not via cluster Raft).
- All three node types respect `CLUSTER_ENABLED` flag — when False, they fall back to the existing pre-AD-52 behavior (static seed lists, SWIM-only membership).
- Backward compatibility: existing deployments with no cluster config keep working.

**Tests**:
- **Integration** (`tests/integration/cluster/test_full_stack_bootstrap.py`):
  - 3 gates + 3 managers (per DC) × 2 DCs + 20 workers — full stack bootstraps cleanly.
- **E2E**: End-to-end job submission through the stack after bootstrap completes.
- **Race condition** (`tests/integration/cluster/test_startup_order_race.py`):
  - Workers start before managers complete bootstrap — workers retry registration until managers are ready; no orphaned workers.
- **Memory leak**: Full-stack startup + shutdown 50× in series.
- **Deadlock** (`tests/integration/cluster/test_startup_cancellation.py`):
  - `stop()` invoked during `cluster_coordinator.start()` — cleanup completes within 30s.
- **Edge case**:
  - Worker with `CLUSTER_ENABLED=True` but no seed locators configured — exits non-zero with clear error.
  - Manager with `CLUSTER_ENABLED=False` — boots normally, ignoring all AD-52 features.
- **Negative path**:
  - Gate with conflicting `--initial-members` and `--seeds` flags — exits with explicit "use one or the other" error.

---

# Phase 2: Robustness

## Item 2.1 — `PhiAccrualDetector` (per-edge)

**What**: Implement Hayashibara phi-accrual failure detector. Per-peer state: heartbeat inter-arrival timestamps (bounded window), current phi value, Vivaldi RTT baseline.

**Why**: AD-52 §8. Phi-accrual gives per-edge probabilistic detection used for circuit breaking and per-DC routing decisions. Complements SWIM, doesn't replace it.

**Files**:
- Modify `hyperscale/distributed/cluster/phi_accrual.py`:
  - `PhiAccrualDetector` — `record_heartbeat(peer, timestamp_ns)`, `current_phi(peer) -> float`, `is_suspect(peer, threshold=8.0) -> bool`.
  - Sliding-window history (bounded by `phi_history_size`, default 1000).
  - Distribution: normal approximation `μ ± σ` over inter-arrivals; phi = `-log10(P(arrival_by_t))`.
  - Baseline initialization: pull from `CoordinateTracker.estimate_rtt_ucb_ms()` per peer until enough samples accumulate.
- Add `hyperscale/distributed/cluster/models/peer_phi_state.py` — `PeerPhiState(peer_id, history, mu, sigma, last_seen_ns)`.
- Modify `hyperscale/distributed/env/env.py`:
  - `CLUSTER_PHI_THRESHOLD: StrictFloat = 8.0`.
  - `CLUSTER_PHI_HISTORY_SIZE: StrictInt = 1000`.

**Acceptance criteria**:
- `record_heartbeat()` runs in < 1μs per call (hot path).
- Phi value updates incrementally (no O(N) recomputation).
- Newly observed peer with no history uses Vivaldi RTT-derived prior.
- History bounded; oldest entries evicted in O(1).
- Phi values are deterministic given the same input stream (testable).

**Example**:
```python
detector = PhiAccrualDetector(coordinate_tracker)
detector.record_heartbeat(peer="manager-A", timestamp_ns=time.monotonic_ns())
if detector.is_suspect("manager-A"):
    circuit_breaker.open("manager-A")
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_phi_accrual.py`):
  - Phi value increases monotonically when no heartbeats arrive.
  - Phi value drops on heartbeat arrival.
  - Vivaldi RTT used as prior when history is empty.
- **Integration**: Network simulation — peer alive vs intermittent vs dead; phi correctly classifies each.
- **E2E**: Phi-driven circuit breaker integrates with `AD-18 HybridOverloadDetector`; routing fails over.
- **Race condition**: 1000 concurrent `record_heartbeat` calls per peer — phi computation is correct (verify via `pytest-asyncio.gather`).
- **Memory leak**: 10M heartbeats; verify history bounded.
- **Deadlock**: Detector uses no locks (per-peer state, single writer); verify under concurrent stress.
- **Edge case**:
  - Single heartbeat (history size 1) — phi defaults to neutral until N samples.
  - Clock skew: monotonic_ns goes backward (impossible in practice) — detector ignores out-of-order timestamps.
- **Negative path**:
  - `record_heartbeat` with `timestamp_ns` from 1 hour ago — discarded as stale; not added to history.

---

## Item 2.2 — Hybrid failure detector composition

**What**: Compose `HierarchicalFailureDetector` (existing) + `PhiAccrualDetector` (Item 2.1) into a single `HybridFailureDetector` that drives both cluster membership eviction AND per-edge circuit breaking.

**Why**: AD-52 §8. SWIM gives cluster-wide convergence; phi-accrual gives per-edge precision; both are valuable for different decisions.

**Files**:
- Modify `hyperscale/distributed/cluster/failure_detector.py`:
  - `HybridFailureDetector` — composes both detectors.
  - `register_on_global_dead(callback)` — fires when SWIM declares cluster-wide DEAD.
  - `register_on_edge_suspect(callback)` — fires when phi-accrual exceeds threshold for a specific peer-from-this-node edge.
- Modify `hyperscale/distributed/swim/health_aware_server.py`:
  - Inside `AckHandler.handle()` (line 48 hook point): also call `detector.record_heartbeat()`.
  - Inside `_record_global_death_sync()` (line 1018): invoke `_on_global_dead_callbacks`.
- Modify `hyperscale/distributed/health/circuit_breaker_manager.py` (AD-18):
  - Register an `on_edge_suspect` callback that opens the breaker for the specific peer edge.

**Acceptance criteria**:
- SWIM-DEAD continues to fire the existing `_on_node_dead_callbacks`.
- Phi-accrual edge suspicion fires `_on_edge_suspect_callbacks` separately.
- The two detectors operate independently — phi suspicion on edge A→B does not affect cluster-wide view of B.
- Per-edge circuit breakers from AD-18 open/close based on phi values.

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_hybrid_detector.py`):
  - Both detectors fire independently.
  - Callback registration order matters (early callbacks see earlier events).
- **Integration** (`tests/integration/cluster/test_hybrid_failure_modes.py`):
  - Edge-only suspicion (peer unreachable from this node but reachable from cluster) — local circuit breaker opens, cluster membership unchanged.
  - Cluster-wide death — both detectors fire.
- **E2E**: Cross-DC scenario — phi-accrual detects intermittent edge to remote DC; routing fails over via AD-36.
- **Race condition** (`tests/integration/cluster/test_hybrid_concurrent_detection.py`):
  - Simultaneous SWIM-DEAD and edge-suspicion — both callbacks fire in expected order.
- **Memory leak**: Long-running cluster (1h sim); detector state stays bounded.
- **Deadlock**: Callbacks invoked synchronously from SWIM event loop — verify no callbacks await locks held by the event loop.
- **Edge case**: Peer revived after suspicion — phi drops, circuit breaker closes; cluster membership unaffected.
- **Negative path**: Callback raises — exception logged via `task_runner.handle_exception`, other callbacks still fire.

---

## Item 2.3 — Multi-stage eviction (tombstone → Raft REMOVE)

**What**: Extend SWIM eviction to a 4-stage flow: ALIVE → SUSPECT → DEAD → tombstone (10min retention) → leader proposes `Remove` via Raft.

**Why**: AD-52 §8. Brief network partitions are far more common than node death. Aggressive eviction causes membership churn on every blip; the tombstone retention period absorbs blips.

**Files**:
- Modify `hyperscale/distributed/swim/detection/incarnation_tracker.py`:
  - Already has `dead_node_retention_seconds: float = 3600.0` — change default to 600.0 (10min per AD-52).
  - Already has `_on_node_evicted` callback hook (line 382).
- Add `hyperscale/distributed/cluster/eviction_proposer.py`:
  - `EvictionProposer` registers with `IncarnationTracker._on_node_evicted` and proposes `Remove` via `ClusterMembershipState` Raft group.
  - Deduplicates: if the node is already being `Remove`'d, no-op.
- Modify `hyperscale/distributed/nodes/manager/server.py` and `gate/server.py`:
  - On startup, instantiate `EvictionProposer` and wire it.
- Modify `hyperscale/distributed/env/env.py`:
  - `CLUSTER_TOMBSTONE_RETENTION: StrictStr = "10min"`.

**Acceptance criteria**:
- SWIM DEAD fires existing `_on_node_dead_callbacks` immediately.
- 10 minutes after DEAD, `_on_node_evicted` fires, triggering `Remove` Raft proposal.
- If the dead node revives within 10 minutes (gossip reports it alive again with higher incarnation), the tombstone is cleared (existing AD-46 behavior preserves this).
- `Remove` proposal is idempotent — multiple proposers don't cause duplicate entries.

**Example**:
```
t=0     peer probe miss → SUSPECT
t=3s    indirect probe fails → DEAD (callbacks fire)
t=600s  tombstone expires → leader proposes Remove(node_id)
t=601s  Remove committed → membership_epoch increments
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_eviction_proposer.py`):
  - `Remove` proposed exactly once per evicted peer.
  - Dedupe: two `EvictionProposer` instances see the same eviction; only one Remove committed.
- **Integration** (`tests/integration/cluster/test_eviction_lifecycle.py`):
  - Kill manager; observe ALIVE → SUSPECT → DEAD → tombstone → Remove.
- **E2E**: 30s network partition during 10min tombstone window — node revives, tombstone cleared, no Remove.
- **Race condition**: Node death during in-flight `Promote` of same node — Raft serializes; final state is consistent.
- **Memory leak**: 1000 kill+revive cycles; `IncarnationTracker.node_states` bounded.
- **Deadlock**: `EvictionProposer.start()` cancellation during proposal — no orphan tasks.
- **Edge case**:
  - Node revives at t=590s (just before tombstone) — Remove not proposed.
  - Node revives at t=601s (just after Remove committed) — joins as new member with new node_id.
- **Negative path**: Raft is leaderless when eviction fires — proposer retries with backoff; once leader elected, Remove commits.

---

## Item 2.4 — `WatchServer` (leader-side delta dispatch)

**What**: Implement long-lived TCP watch streams. Server dispatches `WatchDelta` to subscribed clients on every committed membership change.

**Why**: AD-52 §9. Watch streams replace polling. Required for distributed soft-state caches (Item 2.6) and observability (Item 3.8).

**Files**:
- Modify `hyperscale/distributed/cluster/watch_server.py`:
  - `WatchServer` — owns a `dict[stream_id, WatchSubscription]` of active streams.
  - On Raft apply of any `MembershipEntry`, dispatches `WatchDelta` to all subscribers via `_dispatch_delta()`.
  - Bounded delta ring buffer (default 16384) for reconnect resumption.
- Add `hyperscale/distributed/cluster/models/watch_messages.py`:
  - `WatchOpen`, `WatchSnapshot`, `WatchDelta`, `WatchClose`.
- Modify `hyperscale/distributed/raft/state_machine.py`:
  - In `apply()`, after a membership entry is applied, call `watch_server.notify_apply(entry, lsn)`.
- Modify `hyperscale/distributed/nodes/{manager,gate}/server.py`:
  - Register `@tcp.receive(priority=MessagePriority.CRITICAL)` handler `cluster_watch_open(addr, payload, clock_time)` that calls `watch_server.open_stream(...)`.

**Acceptance criteria**:
- `WatchOpen` with valid `last_seen_lsn` returns delta stream from that LSN if within retention.
- `WatchOpen` with stale `last_seen_lsn` returns a `WatchSnapshot` (full state) followed by deltas.
- Server emits delta on every membership-change commit, within 10ms of commit (p99).
- Bounded ring buffer evicts oldest entries; reconnects beyond retention re-snapshot.
- Server limits concurrent open streams per peer to avoid resource exhaustion.

**Example**:
```python
# Leader-side
watch_server.notify_apply(AddLearner(node_id="D"), lsn=42)
# All subscribed clients receive: WatchDelta(epoch=N+1, lsn=42, entry=AddLearner(...))
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_watch_server.py`):
  - Subscribe, receive snapshot, receive deltas in order.
  - Ring buffer eviction.
- **Integration** (`tests/integration/cluster/test_watch_stream_e2e.py`):
  - 3 watchers subscribe; 100 membership changes; all watchers see all changes.
- **E2E**: Watch stream survives leader change (failover to new leader, watcher reconnects automatically).
- **Race condition**: Concurrent subscribe + apply — delta dispatched even if subscription opened mid-apply.
- **Memory leak**: 10000 subscribe+unsubscribe cycles; `_streams` dict empty after each cleanup.
- **Deadlock** (`tests/integration/cluster/test_watch_slow_consumer.py`):
  - Slow consumer doesn't block dispatch — backpressure: ring buffer drops oldest, slow consumer reconnects with snapshot.
- **Edge case**:
  - `last_seen_lsn=0` — fresh subscription, full snapshot.
  - `last_seen_lsn > current_lsn` — protocol error, reject with `INVALID_LSN`.
- **Negative path**:
  - Watcher dies mid-stream — server cleans up subscription on transport close.
  - Watcher floods server with `WatchOpen` per second — rate-limited via AD-24.

---

## Item 2.5 — `WatchClient` (subscribe + reconnect-and-resume)

**What**: Client-side watch consumer. Subscribes, applies deltas to local cache, reconnects with `last_seen_lsn` on disconnect, falls back to snapshot if LSN beyond retention.

**Why**: AD-52 §9. Every node (gate, manager, worker) needs a watch client to maintain its view of cluster membership.

**Files**:
- Modify `hyperscale/distributed/cluster/watch_client.py`:
  - `WatchClient(identity, leader_locator, on_delta, on_snapshot, task_runner, logger)`.
  - `async start()` — opens stream, processes deltas, reconnects on error.
  - `_last_seen_lsn: int = 0` — preserved across reconnects.
  - `register_on_disconnect(callback)` and `register_on_reconnect(callback)`.
- Modify `hyperscale/distributed/cluster/models/watch_messages.py`:
  - Already added in Item 2.4.

**Acceptance criteria**:
- Connect on start; pump deltas to `on_delta` callback.
- On transport close, reconnect with backoff (AD-21 jitter) using `_last_seen_lsn`.
- If server returns snapshot, call `on_snapshot` and reset `_last_seen_lsn`.
- All deltas applied in order; no gaps in LSNs (verify gap → snapshot).

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_watch_client.py`):
  - Reconnect with backoff jitter.
  - Snapshot resets last_seen_lsn.
- **Integration**: Pair `WatchClient` with `WatchServer`; verify end-to-end.
- **E2E**: Run during leader change; client reconnects to new leader.
- **Race condition** (`tests/integration/cluster/test_watch_client_concurrent_callbacks.py`):
  - Slow `on_delta` callback while new deltas arrive — buffered in client; processed in order.
- **Memory leak**: Long-running client (1h sim); buffer bounded.
- **Deadlock**: `WatchClient.stop()` mid-reconnect — clean cancellation.
- **Edge case**: Server unreachable for 5min — exponential backoff caps at `WATCH_RECONNECT_MAX_BACKOFF`.
- **Negative path**: `on_delta` raises — error logged, processing continues with next delta.

---

## Item 2.6 — `SoftStateCache` (disconnected-mode data plane)

**What**: In-memory soft-state cache of cluster membership. Watch deltas update it. Data plane code reads it (with `staleness_ms`) rather than blocking on Raft.

**Why**: AD-52 §10. The Linkerd/Istio robustness pattern: control plane outage is not a data plane outage.

**Files**:
- Modify `hyperscale/distributed/cluster/disconnected.py`:
  - `SoftStateCache` — `members: dict[node_id, MemberRecord]`, `epoch: int`, `cluster_uuid: str`, `last_observed_at: float`.
  - `get(key) -> tuple[value, staleness_ms]`.
  - `apply_delta(delta)` — invoked by `WatchClient.on_delta`.
  - `apply_snapshot(snapshot)` — invoked by `WatchClient.on_snapshot`.
- Modify `hyperscale/distributed/cluster/observability.py`:
  - Counter `cluster_soft_cache_read_total{kind, freshness}`.
  - Histogram `cluster_soft_cache_staleness_seconds{kind}`.
- Modify `hyperscale/distributed/nodes/{gate,manager,worker}/server.py`:
  - Instantiate `SoftStateCache` at startup; pass to coordinators that need cluster-view (gate `GateJobRouter`, manager `WorkflowDispatcher`, worker `WorkerRegistry`).
- Modify consumer modules (e.g., `routing/gate_job_router.py`) to read from `SoftStateCache` instead of from a fresh Raft read for any non-linearizable decision.

**Acceptance criteria**:
- `SoftStateCache.get()` returns immediately, regardless of Raft availability.
- During Raft partition, cache reads keep working with growing `staleness_ms`.
- After partition heals, `WatchClient` reconnects, snapshot applied, staleness resets.
- `cluster_disconnected_mode_active` metric reflects watch-stream connectivity.

**Example**:
```python
member, staleness_ms = soft_cache.get(node_id)
if staleness_ms < 5000:
    use(member)
else:
    member = await raft.linearizable_read(...)
```

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_soft_state_cache.py`):
  - `apply_delta` mutates in expected ways for each entry type.
  - Staleness computed correctly.
- **Integration** (`tests/integration/cluster/test_disconnected_mode.py`):
  - Partition manager from gate watch stream for 5min; gate continues routing using cached state.
- **E2E** (`tests/integration/cluster/test_disconnected_mode_data_plane.py`):
  - 10min control-plane partition during active workload; verify zero job failures.
- **Race condition**: Concurrent `apply_delta` and `get` — readers see consistent state (atomic dict update + epoch increment).
- **Memory leak**: 1M deltas; cache size scales O(members), not O(deltas).
- **Deadlock**: N/A (single-writer, single-lock around `apply_*`).
- **Edge case**:
  - Cache fully empty (cold start before first snapshot) — `get()` returns `(None, +inf)`.
  - Snapshot received before any delta — apply replaces all state.
- **Negative path**: Out-of-order delta (lsn < current) — ignored.

---

## Item 2.7 — `DatacenterCatalog` (gate-only federation)

**What**: Gate cluster's Raft state machine catalogs `DatacenterRegistration` entries. Each manager cluster's leader registers itself with the gate cluster on startup.

**Why**: AD-52 §17. Multi-DC topology requires the gate cluster to know about manager DCs without joining them.

**Files**:
- Modify `hyperscale/distributed/cluster/federation.py`:
  - `DatacenterCatalog` — owns `dict[dc_id, DatacenterRegistration]` in gate Raft state.
  - `async register_dc(registration: DatacenterRegistration)` — proposes `RegisterDatacenter` entry.
  - `async list_dcs() -> list[DatacenterRegistration]` — reads from `SoftStateCache`.
- Modify `hyperscale/distributed/nodes/manager/server.py`:
  - After bootstrap/join completes, manager leader calls `_register_with_gate_cluster()` which invokes `DatacenterCatalog.register_dc()` via gate's TCP handler.
- Modify `hyperscale/distributed/nodes/gate/server.py`:
  - Register `@tcp.receive(priority=MessagePriority.CRITICAL)` handler `cluster_register_dc(addr, payload, clock_time)`.
- Modify `hyperscale/distributed/cluster/membership_log.py`:
  - `RegisterDatacenter` entry already added in Item 1.6; apply handler updates `DatacenterCatalog` state.

**Acceptance criteria**:
- Manager leader can register its DC with the gate cluster.
- Re-registration with new `cluster_uuid_of_dc` is versioned, not overwritten (see Item 2.8).
- Gate routing components (AD-36) read from `DatacenterCatalog`.
- DC registration is idempotent — same `(dc_id, cluster_uuid_of_dc)` doesn't add a new entry.

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_dc_catalog.py`).
- **Integration** (`tests/integration/cluster/test_multi_dc_registration.py`):
  - 2 DCs of managers register with 1 gate cluster; gate sees both.
- **E2E**: Full multi-DC stack — gates discover managers via catalog, route jobs via AD-36.
- **Race condition**: Two managers from same DC both try to register (leader change race) — both registrations have same `cluster_uuid_of_dc`; second is idempotent no-op.
- **Memory leak**: 1000 DC registration cycles; catalog size bounded.
- **Deadlock**: N/A.
- **Edge case**: DC registers with empty `manager_seeds` list — rejected with `INVALID_REGISTRATION`.
- **Negative path**: Manager registering with wrong `cluster_id` (gate cluster) — rejected by fence (Item 1.10).

---

## Item 2.8 — DC regeneration handling (epoch versioning)

**What**: When a DC's manager cluster suffers catastrophic loss and re-forms with a new `cluster_uuid_of_dc`, the gate cluster versions the registration (does NOT overwrite). Routing state (Vivaldi, EWMA, stickiness) is reset for that DC.

**Why**: AD-52 §17. Silent overwriting of DC registrations across generations causes routing components to apply stale learned state to a fresh cluster.

**Files**:
- Modify `hyperscale/distributed/cluster/federation.py`:
  - `DatacenterCatalog` tracks registration history: `dict[dc_id, list[DatacenterRegistration]]`.
  - `register_dc()` detects `cluster_uuid_of_dc` change, appends new entry with `generation` counter.
  - Old registration retained for `DC_REGISTRATION_GRACE_PERIOD` (default 1h).
- Modify `hyperscale/distributed/routing/gate_job_router.py` (AD-51):
  - Subscribe to DC-regeneration events; reset Vivaldi coordinates, EWMA latency, route stickiness for the affected DC.
- Modify `hyperscale/logging/hyperscale_logging_models.py`:
  - `DatacenterRegistered`, `DatacenterRegenerationDetected`.

**Acceptance criteria**:
- DC re-registration with new `cluster_uuid_of_dc` increments generation counter and emits `DatacenterRegenerationDetected` event.
- Routing components reset learned state for the affected DC.
- Old registration retained for grace period to allow in-flight messages to fence-validate.
- After grace period expires, old registration is pruned via background task.

**Tests**:
- **Unit** (`tests/unit/distributed/cluster/test_dc_regeneration.py`).
- **Integration** (`tests/integration/cluster/test_dc_full_regeneration.py`):
  - DC's manager cluster destroyed and rebuilt; gate detects, resets routing.
- **E2E**: Full multi-DC scenario; one DC fully regenerated; jobs continue to flow.
- **Race condition**: In-flight messages from old DC arrive after new DC registered — fence validates against old generation (within grace), accepted; after grace, rejected.
- **Memory leak**: 1000 regeneration cycles; old registrations pruned.
- **Deadlock**: Reset of routing state during active routing decision — atomic, no torn reads.
- **Edge case**: DC regenerates with same `cluster_uuid_of_dc` (impossible in practice, but defensive) — treated as idempotent re-registration.
- **Negative path**: Manager from old DC tries to register after grace expires — rejected.

---

# Phase 3: Operations & Performance

## Item 3.1 — Drain mode

**What**: Operator-triggered drain — node marks itself draining, leader stops routing new work to it, in-flight work completes, node sends `LeaveRequest`.

**Why**: AD-52 §13. Graceful upgrades and decommissioning.

**Files**:
- Modify `hyperscale/distributed/cluster/drain.py`:
  - `DrainCoordinator` — `async start_drain()`, `is_draining() -> bool`.
  - Proposes `UpdateMetadata(node_id, metadata_delta={"draining": True})` via Raft.
- Modify `hyperscale/distributed/nodes/{gate,manager}/server.py`:
  - Add `@tcp.receive()` admin handler `admin_drain(addr, payload, clock_time)` (gated by admin token).
- Modify routing/dispatch logic to filter out draining nodes.

**Acceptance criteria**:
- After drain start, no new work routed to the node.
- In-flight work completes before `LeaveRequest`.
- Drained node automatically Raft `Remove`'d after `LeaveRequest` committed.

**Tests**: Unit, integration (drain manager mid-workflow, no work lost), e2e, race (drain during workflow dispatch — workflow completes on draining node), memory leak, deadlock, edge case (drain a node that has no in-flight work — leaves immediately), negative path (drain admin endpoint without token — rejected).

---

## Item 3.2 — Force-remove with epoch fence

**What**: Operator escape for unresponsive members. Endpoint requires current `membership_epoch` parameter; leader rejects stale epochs.

**Why**: AD-52 §13. Prevents removing the wrong node after the cluster has churned.

**Files**:
- Modify `hyperscale/distributed/cluster/force_remove.py`:
  - `ForceRemoveCoordinator` — `async force_remove(node_id, current_epoch, admin_token)`.
- Modify gate/manager server admin handler.

**Acceptance criteria**:
- `current_epoch` mismatch → rejected with structured error including current epoch.
- Success → leader proposes `Remove(reason=force)`; skips tombstone retention.

**Tests**: Unit, integration, e2e (operator force-removes a wedged manager — cluster reconverges), race (epoch changes between operator decision and request — rejected), memory leak, deadlock, edge case (force-remove of self — rejected), negative path (admin token wrong — rejected at protocol boundary).

---

## Item 3.3 — Membership freeze

**What**: Leader-side flag rejecting all membership change proposals during operator-declared maintenance window.

**Files**:
- Modify `hyperscale/distributed/cluster/freeze.py`:
  - `FreezeController` — `freeze()`, `unfreeze()` propose `UpdateClusterMetadata` entries.
- Modify `BootstrapCoordinator`, `JoinCoordinator`, `LearnerTracker`, `EvictionProposer` to consult the freeze flag before proposing changes.

**Acceptance criteria**:
- All membership-change proposals rejected during freeze.
- Unfreeze restores normal behavior.

**Tests**: Unit, integration, e2e (freeze during upgrade, verify cluster doesn't shift), race (freeze and unfreeze concurrent — Raft serializes), memory leak, deadlock, edge case (freeze when already frozen — idempotent), negative path (admin without token — rejected).

---

## Item 3.4 — Snapshot export/import

**What**: Operator can export current Raft state to a tarball; import into a freshly-launched cluster (which mints new `cluster_uuid` to prevent accidental fork).

**Files**:
- Modify `hyperscale/distributed/cluster/snapshot_import_export.py`:
  - `SnapshotIO.export() -> bytes`, `SnapshotIO.import_(payload)`.
- Modify `hyperscale/distributed/raft/snapshot.py` — extend for full cluster state.

**Acceptance criteria**:
- Export captures Raft log + state machine.
- Import only on freshly-launched, uncommitted cluster.
- Import mints new `cluster_uuid`; preserves all membership and DC catalog state.

**Tests**: Unit, integration (export → import on new cluster → verify identical state), e2e, race (export during active workload — snapshot is consistent at some LSN), memory leak, deadlock, edge case (import on already-committed cluster — rejected), negative path (corrupt tarball — rejected with explicit error).

---

## Item 3.5 — Group commit on Raft WAL

**What**: Wire `RaftWAL` (already group-commit-capable) into `RaftNode` proposal path. Batched fsync.

**Why**: AD-52 §16. Currently `RaftNode` writes nothing to disk; AD-52's correctness model relies on Raft quorum, but disk-backed log makes single-pod recovery sub-second.

**Files**:
- Modify `hyperscale/distributed/raft/raft_node.py`:
  - On proposal, write to `RaftWAL` before adding to in-memory log; await `WriteRequest` future.
  - On startup, call `RaftWAL.recover()` to restore log.
- Modify `hyperscale/distributed/env/env.py`:
  - `RAFT_WAL_ENABLED: StrictBool = True`.

**Acceptance criteria**:
- Throughput meets AD-52 §16 SLO (p50 < 5ms, p99 < 50ms for membership commits).
- Cluster restart recovers log from WAL.

**Tests**: Unit, integration (kill pod, restart, recover log), e2e, race (concurrent proposals — group commit batches them), memory leak, deadlock, edge case (corrupt WAL entry — recovery stops at corruption, prior entries kept), negative path (disk full — proposals fail with `WAL_FULL`).

---

## Item 3.6 — Pipelined AppendEntries

**What**: Leader sends `AppendEntries(N+1)` before `AppendEntries(N)` is acknowledged. Per-follower in-flight window.

**Files**:
- Modify `hyperscale/distributed/raft/raft_node.py`:
  - Add `_inflight_appendentries: dict[follower, deque[InflightAppendEntries]]`.
  - Modify `replicate_to_followers()` to fill window up to `max_inflight_appendentries` (default 256).
  - Track per-follower flow control.

**Acceptance criteria**:
- Single slow follower doesn't block fast followers.
- Throughput scales with parallelism.

**Tests**: Unit, integration (slow follower simulated; fast followers unaffected), e2e, race (in-flight window fills; new proposals queue), memory leak (bounded window), deadlock (follower disconnect mid-window — recover via standard Raft retransmit), edge case (window of 1 — equivalent to current serial behavior), negative path (follower repeatedly NACKs — leader backs off via existing backtracking logic).

---

## Item 3.7 — Leader lease (optional, opt-in)

**What**: When `CLUSTER_LEADER_LEASE_ENABLED=True`, leader holds a lease for `quorum_timeout/2` and answers ReadIndex without quorum verification during lease.

**Files**:
- Modify `hyperscale/distributed/raft/raft_node.py`:
  - Track `_lease_expires_at: float | None`.
  - In `read_index()`, short-circuit quorum verification if lease valid.
  - Renew lease on every heartbeat-quorum.

**Acceptance criteria**:
- ReadIndex during valid lease: sub-millisecond on leader.
- On lease expiry: full quorum verification.
- Requires NTP-bounded clock skew (documented).

**Tests**: Unit, integration, e2e (high-frequency reads — confirm sub-ms p99 when lease enabled), race (lease expiry concurrent with read — falls back to quorum), memory leak, deadlock, edge case (lease enabled but no NTP — clock skew detected, lease disabled with warning), negative path (lease enabled but clock skew detected at runtime — auto-disable, log warning).

---

## Item 3.8 — Observability surface

**What**: Add all metrics and structured events enumerated in AD-52 §18.

**Files**:
- Modify `hyperscale/distributed/cluster/observability.py`:
  - All counters, gauges, histograms as named in AD-52 §18.
- Modify `hyperscale/logging/hyperscale_logging_models.py`:
  - Add all event types from AD-52 §18.
- Modify every coordinator (`BootstrapCoordinator`, `JoinCoordinator`, etc.) to emit metrics and events at significant transitions.

**Acceptance criteria**:
- All named metrics queryable via `/metrics` endpoint.
- All named events appear in Logger stream.
- Metric cardinality bounded (no unbounded labels).

**Tests**: Unit (each metric increments correctly), integration (run a scenario, verify all expected events fire), e2e (run for 1h, verify metrics stable), race (concurrent updates to same metric — no torn counters), memory leak (metric storage bounded), deadlock (metric emission from any thread/task — non-blocking), edge case (metrics queried during shutdown — returns final state), negative path (logger backend down — metrics still emitted to local cache).

---

## Item 3.9 — Determinism enforcement (runtime + CI)

**What**: Activate the determinism guard from Item 0.2 in CI test runs and add Ruff plugin to project lint config.

**Files**:
- Modify `pyproject.toml` — Ruff plugin path.
- Modify `tests/conftest.py` — set `RAFT_DETERMINISM_GUARD=True` for all integration tests touching apply layer.
- Add CI job step: replay a known log on two independent state machines and assert byte-equal final state.

**Acceptance criteria**:
- CI fails if any apply handler calls forbidden function.
- Replay test confirms deterministic byte-equal apply.

**Tests**: Unit (replay determinism test), CI integration (lint catches a deliberate `time.time()` insertion).

---

# Phase 4: Workers & Integration

## Item 4.1 — Worker join via locator system

**What**: Replace static `seed_managers` list in `WorkerServer` with `SeedLocator`-based resolution.

**Files**:
- Modify `hyperscale/distributed/nodes/worker/server.py` — accept `manager_seed_locators: list[str]`.
- Modify `hyperscale/distributed/nodes/worker/discovery.py`:
  - `WorkerDiscoveryManager` calls locators, resolves managers, picks one via AD-28 power-of-two-choices.

**Acceptance criteria**:
- Worker can find managers in any environment (K8s, VMs, bare metal, dev) using the same flag.
- Worker re-resolves on connection failure to current owner.

**Tests**: Unit, integration (worker finds manager via dns://, file://), e2e, race (worker startup while managers are still bootstrapping — worker retries), memory leak, deadlock, edge case (zero managers reachable initially — worker waits with backoff), negative path (locator returns rate-limited address — worker honors AD-24 backoff).

---

## Item 4.2 — Worker re-registration on owner death

**What**: When SWIM detects owner manager dead, worker re-resolves locators and re-registers with a new manager.

**Files**:
- Modify `hyperscale/distributed/nodes/worker/registry.py` — `select_new_primary_manager()` calls `WorkerDiscoveryManager.resolve()`.

**Acceptance criteria**:
- Owner death triggers re-registration within `WORKER_OWNER_FAILOVER_TIMEOUT` (5s).
- AD-31 job leadership transfers per existing mechanism.

**Tests**: Unit, integration (kill owner — worker re-registers), e2e (in-flight workflow continues), race (owner death + leader change concurrent — worker eventually finds healthy manager), memory leak, deadlock, edge case (all managers dead — worker exits non-zero after `WORKER_NO_MANAGERS_TIMEOUT`), negative path (manager rejects re-registration with `WRONG_CLUSTER_ID` — worker exits with explicit error).

---

## Item 4.3 — AD-31 integration (job leadership uses cluster epoch)

**What**: `JobLeadershipTracker` records `granted_at_cluster_epoch` on every leadership grant. Workers and managers fence job leadership messages by both the AD-10 leadership term AND the cluster epoch.

**Files**:
- Modify `hyperscale/distributed/jobs/job_leadership_tracker.py`:
  - Add `granted_at_cluster_epoch: int` to `JobLeadership` dataclass.
- Modify `hyperscale/distributed/cluster/fence.py`:
  - Job-leadership RPCs validate against `granted_at_cluster_epoch ≥ receiver_epoch`.

**Acceptance criteria**:
- Job leadership from a previous cluster generation cannot influence the new generation.
- Workers reject leadership commands with stale cluster epoch.

**Tests**: Unit, integration, e2e (DC regenerates mid-workflow — stale leadership rejected), race, memory leak, deadlock, edge case, negative path (zombie manager from previous generation tries to drive workflow — rejected).

---

## Item 4.4 — Routing reads via `SoftStateCache`

**What**: `GateJobRouter` (AD-51), `ManagerDispatcher`, etc. read from `SoftStateCache` for non-linearizable decisions.

**Files**:
- Modify `hyperscale/distributed/routing/gate_job_router.py` — replace direct membership lookups with `soft_cache.get()`.
- Modify `hyperscale/distributed/datacenters/manager_dispatcher.py` — same.

**Acceptance criteria**:
- Routing decisions don't block on Raft for typical reads.
- Cancellation (AD-20) uses `linearizable_read()` (Item 1.9) because it must be strict.

**Tests**: Unit, integration, e2e (Raft partition during high-volume routing — routing continues), race, memory leak, deadlock (verify routing doesn't hold cache lock during outbound TCP), edge case (cache stale > threshold — routing falls back to linearizable read), negative path (cache fully empty — routing fails open or fails closed per policy).

---

## Item 4.5 — AD-48 integration (worker dissemination respects epoch)

**What**: `WorkerStateUpdate` (AD-48) gossip carries cluster epoch; receivers reject stale epochs.

**Files**:
- Modify `hyperscale/distributed/swim/gossip/worker_state_gossip_buffer.py`:
  - Add `cluster_epoch_at_emit: int` to gossip payload.
- Modify receivers in `nodes/manager/server.py` — validate via fence (Item 1.10).

**Acceptance criteria**:
- Cross-generation worker state cannot pollute the new generation.

**Tests**: Unit, integration, e2e, race, memory leak, deadlock, edge case, negative path.

---

## Item 4.6 — End-to-end scenario suite

**What**: A new test directory `tests/integration/cluster/` with comprehensive scenarios exercising the full AD-52 stack.

**Files** (add):
- `tests/integration/cluster/scenarios/cold_start_three_node.py`
- `tests/integration/cluster/scenarios/rolling_upgrade.py` — replace nodes one at a time
- `tests/integration/cluster/scenarios/cross_region_dc_regeneration.py`
- `tests/integration/cluster/scenarios/disconnected_data_plane_10min.py`
- `tests/integration/cluster/scenarios/jepsen_like_partition.py` — Jepsen-style partition testing
- `tests/integration/cluster/scenarios/force_remove_under_load.py`
- `tests/integration/cluster/scenarios/full_k8s_emulation.py` — pods dying and respawning with new IPs
- `tests/integration/cluster/scenarios/mesos_emulation.py` — task allocations with `exec://` locator
- `tests/integration/cluster/scenarios/dev_laptop.py` — 3 processes on localhost

**Acceptance criteria**:
- Each scenario runs deterministically in CI.
- Each scenario asserts AD-52 SLOs (p99 bootstrap < 10s, etc.).
- Failure modes match expectations.

---

# Phase 5: Hardening and gates

## Item 5.1 — Chaos test harness

**What**: A test harness that injects random failures (process kills, network partitions, packet loss, clock skew) during cluster operations and verifies invariants.

**Files**:
- Add `tests/integration/cluster/chaos/harness.py`.
- Add `tests/integration/cluster/chaos/invariants.py`:
  - "At most one leader per term"
  - "Committed entries never rolled back"
  - "Membership changes serialize"
  - "Fence rejection rate is bounded under normal operation"

**Acceptance criteria**:
- Run 24h in CI nightly; no invariant violations.

---

## Item 5.2 — Performance benchmarks

**What**: Microbenchmarks confirming SLOs from AD-52 §16.

**Files**:
- Add `tests/benchmarks/cluster/` with benchmarks for:
  - Membership change commit latency
  - ReadIndex latency
  - Watch delta propagation latency
  - Cold-cluster bootstrap time
  - Phi-accrual recompute time per heartbeat

**Acceptance criteria**:
- All SLOs from AD-52 §16 met on reference hardware.
- Regression detection: CI fails on > 20% slowdown.

---

# Sequencing summary

```
Phase 0 → 1 → 2 → 3 → 4 → 5
```

Within Phase 1, items mostly independent except:
- 1.6 must precede 1.7, 1.8, 1.10, 1.11
- 1.7 must precede 1.8
- 1.9 is independent of others
- 1.10 must precede 1.11
- 1.12 ties them all together

Within Phase 2:
- 2.1, 2.4, 2.7 are independent
- 2.2 depends on 2.1
- 2.3 depends on 2.7 (uses Raft REMOVE)
- 2.5 depends on 2.4
- 2.6 depends on 2.5
- 2.8 depends on 2.7

Phase 3 items are mostly independent except 3.5 depends on Phase 1.

Phase 4 depends on Phases 1, 2, 3 being complete.

# Estimated PR count

| Phase | Items | Estimated PRs |
|---|---|---|
| 0 | 2 | 2 |
| 1 | 12 | 14 (some items split into request/response + apply-handler PRs) |
| 2 | 8 | 10 |
| 3 | 9 | 9 |
| 4 | 6 | 8 |
| 5 | 2 | 4 |
| **Total** | **39** | **~47 PRs** |

Each PR includes its item's acceptance criteria as the PR description checklist and the test suite enumerated above as the CI gate.

# Acceptance gates (exit criteria)

**End of Phase 1**: 3-node cluster forms deterministically via `--initial-members`, a 4th node joins via `--seeds`, membership changes use joint consensus, learner state is observable, fence rejects zombies. Run `tests/integration/cluster/scenarios/cold_start_three_node.py` and `tests/integration/cluster/scenarios/dev_laptop.py` cleanly.

**End of Phase 2**: 10-minute control-plane partition during active workload causes zero job failures. DC regeneration triggers routing reset. Watch streams under load deliver deltas within p99 50ms.

**End of Phase 3**: Drain, force-remove, freeze, snapshot import/export all operator-tested. Raft p50 < 5ms / p99 < 50ms membership commit. Determinism enforcement passes in CI.

**End of Phase 4**: Full multi-DC stack runs in K8s emulation, Mesos emulation, and dev-laptop scenarios with identical Hyperscale code. End-to-end scenario suite green.

**End of Phase 5**: 24h chaos test green. All AD-52 §16 performance SLOs met on reference hardware.
