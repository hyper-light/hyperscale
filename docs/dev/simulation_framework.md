# Distributed Simulation Framework — Design

**Status:** Draft for review
**Author:** Claude (under direction)
**Date:** 2026-05-01

## 1. Goal

Build a Python harness that drives the actual `GateServer`, `ManagerServer`,
and `WorkerServer` implementations through scripted scenarios to vigorously
exercise job submission, routing, leadership election, and failover at three
escalating fidelities:

- **L1 — single-node:** smoke and protocol-handler tests against one manager
  and one worker.
- **L2 — single-DC:** client → manager quorum → worker pool. Exercises
  manager peer discovery, worker registration, dispatch, intra-DC failover,
  manager leader election.
- **L3 — multi-DC:** client → gate cluster → multiple DC-local clusters.
  Exercises gate election, cross-DC routing, gate-tier failover, partition
  correlation.

The same harness, fault primitives, scenarios, expectations, and invariants
serve all three levels — only `ClusterSpec` changes between levels.

## 2. One harness, two execution modes

The harness is **one** implementation with **two** dependency-injection
configurations. This mirrors TigerBeetle's VOPR: there is no separate
"real-server VOPR" and "simulated VOPR" — there is one simulator that runs
the production code with pluggable dependencies, and the mode is which set
of dependencies gets wired in.

```python
ClusterHarness(spec, mode=ExecutionMode.REAL)
    # real OS clock, real asyncio sockets, real scheduler, real OS random
    # for: kernel/network-edge bugs, real timing, real crash semantics
    # cost:  per-scenario wall-clock seconds; not replayable

ClusterHarness(spec, mode=ExecutionMode.SIM)
    # virtual clock, in-process transport, deterministic scheduler, seeded random
    # for:   logic bugs, exhaustive seed-driven exploration, replay
    # cost:  requires Clock/Random/Transport injection completed (Phases 2–3)
```

Same `ClusterSpec`, `WorkloadSpec`, `FaultMatrix`, `Expectation`,
`SafetyInvariant`, `LivenessInvariant`. Same scenarios — written once, run
in both modes via `pytest.mark.parametrize`. Same production code under
test; the servers don't know which mode they're in.

Practically: Phase 1 ships REAL mode only (it's all that exists today). The
harness API is shaped now so SIM mode is a configuration, not a
reimplementation. Phases 2–3 enable SIM mode without scenarios changing.

## 3. Non-goals

- **Cross-host orchestration.** Everything on `127.0.0.1`. Multi-host later.
- **Replacing existing integration tests immediately.** The harness lives at
  `tests/simulation/`; we migrate tests opportunistically.
- **Modeling the user's workflow code.** Workflows execute the real
  workload (the worker subprocesses run real Python). The harness controls
  *the cluster*, not the workflow internals.

## 4. The three levels in one harness

```python
# L1
ClusterSpec(
    gates=0,
    datacenters={"local": DCSpec(managers=1, workers=1, cores_per_worker=2)},
)

# L2
ClusterSpec(
    gates=0,
    datacenters={"main": DCSpec(managers=3, workers=4, cores_per_worker=2)},
)

# L3
ClusterSpec(
    gates=3,
    datacenters={
        "east": DCSpec(managers=3, workers=4, cores_per_worker=2),
        "west": DCSpec(managers=3, workers=4, cores_per_worker=2),
    },
)
```

The harness routes the client to the right tier (managers for L1/L2, gates
for L3) based on the spec.

## 5. Architecture

```
ClusterHarness                       # async context manager; entry point
├── ExecutionMode                    # REAL or SIM (Phase 3+)
├── Supervisor                       # owns lifetime + cleanup of every artifact
│   ├── _server_handles              # list[ServerHandle]
│   ├── _tracked_pids                # dict[node_id, set[int]] (worker subprocesses)
│   ├── _baseline_pids               # set[int] from __aenter__ snapshot
│   ├── _harness_run_id              # uuid; injected into subprocess env
│   ├── _harness_pgid                # for nuclear-button cleanup
│   ├── _ports                       # PortAllocator
│   ├── _temp_dirs                   # cleanup on exit
│   └── _leak_policy                 # what to do on detected leaks
├── PortAllocator                    # try-bind probe + contiguous range reservation
├── FaultMatrix                      # kill / restart / pause / partition / delay / drop
├── ConditionWaiter                  # wait_until(predicate, timeout, on_fail=dump)
├── DiagnosticDumper                 # snapshot per-node state on failure
├── InvariantChecker                 # continuous safety + liveness checking
├── WorkloadDriver                   # client + submission + expectation tracking
└── ScenarioRunner                   # executes a scenario, applies retry policy
```

A test reads:

```python
async with ClusterHarness(SINGLE_DC_3M_4W, mode=ExecutionMode.REAL) as cluster:
    async with cluster.workload(STANDARD_HTTPBIN) as workload:
        await workload.submit_and_wait_running()
        await cluster.kill("main.manager.0")
        await cluster.wait_until(cluster.has_quorum, timeout=15)
        await workload.expect_completion(timeout=120)
        # invariants verified continuously throughout the scenario
```

Failure of any line — including a continuous-invariant violation — triggers
full cleanup and a diagnostic dump.

## 6. Process tracking (REAL mode)

Workers spawn a `ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn"))`
with one subprocess per core (`hyperscale/core/jobs/runner/local_server_pool.py`).
At L3 with 2 DCs × 4 workers × 2 cores that is 16 grandchild OS processes
per test. If a test crashes leaving them orphaned, they keep running, hold
ports, and break the next run. Three layers, ordered escalation.

### 6.1 Baseline + diff via psutil

```python
# At __aenter__:
self._baseline_pids: set[int] = {
    proc.pid for proc in psutil.Process().children(recursive=True)
}
# At cleanup:
leftover = {
    proc.pid for proc in psutil.Process().children(recursive=True)
} - self._baseline_pids
```

`psutil` walks the full descendant tree regardless of which process spawned
what — the only reliable way to catch grandchildren whose immediate parent
(the `WorkerServer`) is gone. Already a transitive dep via
`process_resource_monitor.py`, no new top-level requirement.

### 6.2 Per-server tracked PIDs (attribution layer)

Right after `await worker_server.start()` returns, the harness snapshots
`server._lifecycle_manager._server_pool._executor._processes` (the same
private dict the existing `WorkerLifecycleManager.kill_child_processes()`
uses). Re-snapshots on a 1 s tick because `ProcessPoolExecutor` silently
replaces dead workers.

This gives **named attribution** ("worker east-2 / core 1 / pid 47291") for
diagnostic dumps, not just "some leftover python".

### 6.3 Reap escalation — graceful → forced → nuclear

Per worker, on cleanup:

```python
async def reap_worker(self, worker_id: str, server: WorkerServer) -> None:
    # 1. Graceful — let WorkerServer drain and stop its own pool.
    try:
        await asyncio.wait_for(server.stop(drain_timeout=2.0), timeout=10.0)
    except asyncio.TimeoutError:
        pass

    # 2. Forced via the lifecycle manager's existing kill path.
    try:
        await asyncio.wait_for(
            server._lifecycle_manager.kill_child_processes(), timeout=3.0,
        )
    except asyncio.TimeoutError:
        pass

    # 3. Direct on tracked PIDs.
    procs = [
        psutil.Process(pid)
        for pid in self._tracked_pids.get(worker_id, set())
        if psutil.pid_exists(pid)
    ]
    for proc in procs:
        try:
            proc.terminate()  # SIGTERM
        except psutil.NoSuchProcess:
            pass
    _gone, alive = psutil.wait_procs(procs, timeout=3.0)
    for proc in alive:
        try:
            proc.kill()  # SIGKILL
        except psutil.NoSuchProcess:
            pass
```

After every node has been reaped, the harness does **one final descendant
sweep** — `descendants_now - baseline_pids` — and kills any remaining
leftover, attributed or not. Safety net for orphaned grandchildren whose
worker crashed before we registered its PIDs.

### 6.4 Process group as the panic button

At `__aenter__`, the harness calls `os.setpgrp()` so every descendant
inherits a fresh PGID. On *total* cleanup failure (the structured cleanup
itself raised), fall back to `os.killpg(harness_pgid, SIGKILL)` — one
syscall, everything we spawned dies. Used only when structured cleanup
fails. Logged loudly when it happens.

### 6.5 Pre-flight zombie reap from prior runs

`__aenter__` step 0:

1. Generate `self._harness_run_id = uuid.uuid4().hex`.
2. Set `os.environ["HYPERSCALE_HARNESS_RUN_ID"] = self._harness_run_id` so
   `multiprocessing.get_context("spawn")` subprocesses inherit it.
3. Walk `psutil.process_iter(["environ", "pid", "cmdline"])` looking for
   processes carrying **any** `HYPERSCALE_HARNESS_RUN_ID` env var (regardless
   of value — i.e., from a prior run). Terminate via the standard escalation.
4. Try-bind every port we plan to use; abort loudly with the holding PID if
   anything is still held.

### 6.6 Port-range tracking, not single ports

Workers derive `_local_udp_port = udp_port + total_cores ** 2`
(`worker/lifecycle.py:63`). The `PortAllocator` reserves
`[base_udp, base_udp + cores**2]` as a contiguous range and verifies the
entire range is bindable both before allocation and after teardown.

### 6.7 Diagnostic dump on leak

When the final sweep finds leftover descendants, the harness logs per-PID:

- `cmdline`, `ppid` at time of leak, age, open sockets (`proc.connections()`),
  state, attribution (which `tracked_pids[node_id]` set, if any).

Without this, "why didn't worker N's subprocess die?" is unanswerable from
a CI log.

### 6.8 Tradeoff

§6.2 reaches into `_executor._processes` (private). It is the same private
API `WorkerLifecycleManager.kill_child_processes()` already depends on, so
this is not new exposure. If `LocalServerPool` ever changes that internal,
*attribution* breaks; the `psutil` baseline-diff in §6.1 remains the source
of truth for *correctness*.

In SIM mode (Phase 3+), §6 is irrelevant — there are no subprocesses; all
"workers" run in-process. The Supervisor degrades cleanly: empty
`tracked_pids`, baseline equals descendants, no work to do.

## 7. Cleanup model — supervisor pattern

Every artifact (servers, tasks, ports, temp dirs) is registered at creation
and torn down in reverse order on `__aexit__`, with a timeout per step:

```
__aexit__:
    1. Stop scenario tasks (5 s)                    via TaskRunner.cancel_all
    2. Stop workers (15 s each, parallel)           §6.3
    3. Stop managers (10 s each, parallel)
    4. Stop gates (10 s each, parallel)
    5. Stop the client (5 s)
    6. Final descendant sweep (5 s)                 §6.1 final pass
    7. Verify all reserved ports released           try-bind
    8. Snapshot asyncio.all_tasks(); fail on leaks  except harness-owned
    9. Drop temp dirs, log files
   10. If 1–9 raised catastrophically:
         os.killpg(harness_pgid, SIGKILL)           §6.4 panic button
```

Rules:

- Cleanup **never** raises out of `__aexit__`. Errors collect into a
  `CleanupReport` attached to the test failure.
- All harness background tasks go through the project's `TaskRunner`
  (CLAUDE.md: "we never create asyncio orphaned tasks or futures").
- **Step 8 is the critical one.** Leaked asyncio tasks across tests cause
  cross-contamination. We fail loudly on a leak rather than silently inherit
  it. **Expect a triage period** where new harness scenarios fail until
  underlying production-code leaks are fixed. Worth doing.

## 8. Retries — two distinct kinds

### 8.1 Condition waits — replaces every hard-coded `asyncio.sleep`

```python
await harness.wait_until(
    predicate=lambda: harness.gate(0).has_quorum() and harness.dc("east").quorum_size() >= 2,
    timeout=30.0,
    poll=0.5,
    description="cluster stabilizes",
    on_fail=harness.dump_diagnostics,
)
```

On timeout, dumps a snapshot — per-node state, last 100 log lines, in-flight
RPCs, tracked PIDs — not just `TimeoutError: 30 s`. Fast-fails on knowable
bad states (e.g. node crashed during wait) instead of waiting the full
timeout.

Built-in predicates: `has_quorum`, `has_primary`, `has_n_workers`,
`gate_cluster_formed`, `job_running`, `job_completed`, `workflow_status`,
`no_in_flight_rpcs`.

### 8.2 Scenario-level retries — for legitimately stochastic tests

```python
@scenario(retries=3, retry_on=(ElectionTimeoutError, QuorumNotFormedError))
async def test_election_under_partition(harness): ...
```

The retry policy declares **what** is acceptable to retry, so true bugs are
not masked under blanket `@flaky`.

## 9. Configuration variety

```python
@dataclass(slots=True, frozen=True)
class EnvOverrides:
    request_timeout: str = "5s"
    log_level: str = "error"
    connect_timeout: str = "2s"
    # ... full Env field set, all optional

@dataclass(slots=True, frozen=True)
class HarnessTimeouts:
    stabilization_default: float = 30.0
    stop_default: float = 10.0
    condition_default: float = 15.0
    workload_default: float = 120.0
    reap_per_node: float = 15.0

@dataclass(slots=True, frozen=True)
class DCSpec:
    managers: int
    workers: int
    cores_per_worker: int = 2
    env: EnvOverrides | None = None       # DC-level override

@dataclass(slots=True, frozen=True)
class ClusterSpec:
    gates: int
    datacenters: dict[str, DCSpec]
    env: EnvOverrides = EnvOverrides()
    per_node_env: dict[str, EnvOverrides] = field(default_factory=dict)
        # e.g. {"east.manager.1": EnvOverrides(request_timeout="1s")}
    timeouts: HarnessTimeouts = HarnessTimeouts()
    base_port: int = 9000
```

`pytest.mark.parametrize` over `ClusterSpec` instances gives matrix testing.
SIM mode adds `seed: int | None = None` to `ClusterSpec` once Phase 3 lands.

## 10. Workload variety — separate from cluster spec

```python
@dataclass(slots=True, frozen=True)
class Submission:
    workflows: list[type[Workflow]]
    dc_count: int = 1
    timeout_seconds: float = 120.0

class SubmissionPattern(StrEnum):
    SINGLE = "single"
    PARALLEL = "parallel"
    STAGGERED = "staggered"
    SUSTAINED = "sustained"
    BURST = "burst"

@dataclass(slots=True, frozen=True)
class WorkloadSpec:
    submissions: list[Submission]
    pattern: SubmissionPattern = SubmissionPattern.SINGLE
    expectations: list[Expectation] = field(default_factory=list)
```

Workflow catalog under `tests/simulation/workflows/`: `SimpleHTTPWorkflow`,
`DependentWorkflow`, `LongRunningWorkflow`, `CancellingWorkflow`,
`PanickingWorkflow`, `LeakyWorkflow`. Any topology × workload × fault
schedule is composable.

## 11. Fault matrix

```python
class FaultMatrix:
    async def kill(self, node_id: str) -> None: ...
    async def restart(self, node_id: str, after: float = 0.0) -> None: ...
    async def pause(self, node_id: str) -> None: ...
    async def resume(self, node_id: str) -> None: ...
    async def partition(self, group_a: set[str], group_b: set[str]) -> None: ...
    async def heal_partition(self) -> None: ...
    async def delay(self, src: str, dst: str, ms: int, jitter_ms: int = 0) -> None: ...
    async def drop_rate(self, src: str, dst: str, probability: float) -> None: ...
    async def slow_disk(self, node_id: str, latency_ms: int) -> None: ...   # SIM only initially
    async def disk_full(self, node_id: str) -> None: ...                    # SIM only initially
```

In **REAL mode**, partition / delay / drop are implemented by wrapping each
server's `send_tcp` / `send_udp` methods (single indirection point per
node — they're already methods on `MercurySyncBaseServer`, not raw socket
calls). The wrapper consults the active `FaultMatrix` rules before
forwarding.

In **SIM mode** (Phase 3+), the same `FaultMatrix` rules drive the in-process
`Transport` directly. Same API, same scenarios, deterministic injection.

## 12. Continuous safety invariants

This is the piece my first draft missed and the audit-critique demanded.
A `SafetyInvariant` is an object that must hold **at all times**:

```python
class SafetyInvariant:
    name: str
    severity: Severity = Severity.CRITICAL

    def evaluate(self, snapshot: HarnessSnapshot) -> InvariantResult:
        """Return PASS or FAIL with detail. Pure function over snapshot."""
```

The `InvariantChecker` runs all registered invariants on a 100 ms tick (REAL
mode) or after every scheduled event (SIM mode) and **fails the scenario
immediately** on violation — no waiting until end-of-test.

Initial L2/L3 catalog:

- `AtMostOneJobLeaderPerJob` — across all managers in a DC, no two claim
  leadership of the same job.
- `MonotonicFenceTokens` — `_job_fence_tokens` per job is non-decreasing
  globally.
- `WorkerSubprocessAttribution` — every PID in any worker's
  `_executor._processes` is in `tracked_pids`. (Catches "spawn that the
  harness didn't see.")
- `NoOrphanWorkflows` — every active workflow has a known job leader.
- `LeakedLocksBounded` — `len(state._peer_state_locks)` ≤ active peer
  count + 1 (the audit-fix stays enforced).

The catalog is extensible per scenario: a scenario can register
scenario-specific invariants in its `setup`.

## 13. Liveness invariants

Distinct mechanism. A `LivenessInvariant` carries a `progress_predicate`
and a `staleness_budget`. If progress has not advanced within the budget,
the scenario fails:

```python
class LivenessInvariant:
    name: str
    progress_predicate: Callable[[HarnessSnapshot], int]   # returns monotonic counter
    staleness_budget: float                                 # seconds (REAL) / virtual (SIM)
```

Catches "deadlock that pretends to be slowness." Initial catalog:

- `JobMakesProgress` — workflow completion count strictly increases at
  least every N seconds while the job is active.
- `LeaderHeartbeatsContinue` — Raft leader heartbeats observed by every
  follower at least every `heartbeat_interval × 2`.
- `BackpressureEventuallyClears` — once load drops, backpressure level
  returns to NONE within bounded time.

## 14. Diagnostics

On condition-wait timeout, expectation failure, or invariant violation, the
harness automatically dumps to `tests/simulation/_artifacts/<scenario>/<ts>/`:

- **Per-node state:** `gate._datacenter_managers`, `manager._known_workers`,
  job-leadership table, raft log tail, in-flight RPCs.
- **Per-node logs:** tail of last 100 lines from each node's logger stream.
- **Process tree:** PIDs, parents, RSS, CPU, open sockets at moment of failure.
- **FaultMatrix state:** active partitions / delays / drops.
- **Tracked vs leaked PIDs** (§6.7).
- **Asyncio task census** under TaskRunner.
- **In SIM mode:** the seed and the operation log replay-able to that exact
  point.

## 15. Scenario authoring — Python, not data

Scenarios are plain `async def` functions taking a harness:

- Existing integration tests are Python — same idiom, no DSL to learn.
- IDE support, type hints, `pdb` debugging.
- Composition by function call.
- `pytest.mark.parametrize` for matrix testing (including over execution mode).
- `hypothesis` for property-based later.

Example:

```python
@scenario(retries=3, retry_on=(ElectionTimeoutError,))
@pytest.mark.parametrize("mode", [ExecutionMode.REAL, ExecutionMode.SIM])
async def test_manager_primary_dies_mid_dispatch(harness, mode):
    async with ClusterHarness(SINGLE_DC_3M_4W, mode=mode) as cluster:
        async with cluster.workload(STANDARD_HTTPBIN) as wl:
            await wl.submit()
            await wl.wait_until_running(timeout=15)

            primary = cluster.dc("main").current_primary()
            await cluster.kill(primary.node_id)

            await cluster.wait_until(
                lambda: cluster.dc("main").current_primary() != primary,
                timeout=15, description="new primary elected",
            )

            await wl.expect_completion(timeout=120)
            # safety + liveness invariants verified continuously throughout
```

## 16. SIM mode internals (Phase 3+)

To enable deterministic, replayable in-process execution, the production
code accepts injectable dependencies:

- **`Clock` interface:** `monotonic() -> float`, `now() -> float`,
  `async sleep(seconds: float) -> None`. The `sleep` method is asyncio-native
  — production code reads identically to today. REAL impl wraps `time` and
  `asyncio.sleep`. SIM impl is a virtual clock advanced by the scheduler.
- **`Random` interface:** seeded `random.Random` instance threaded through
  via config the same way `Env` is.
- **`Transport` interface:** the existing `send_tcp` / `send_udp` are
  already methods; SIM substitutes an in-process implementation that
  delivers messages via direct method dispatch on the receiving server.
  Drops/delays/reorders are applied by the `FaultMatrix` at delivery time.
- **`DeterministicTaskRunner`:** the existing `TaskRunner` already mediates
  background-task creation; SIM mode swaps its scheduler for a deterministic
  one that processes tasks in priority + insertion order, advancing the
  virtual clock between events.

Production-code refactor cost (counted directly):

```
$ grep -rn "time\.monotonic\|time\.time(\|loop\.time(\|asyncio\.sleep" \
    hyperscale/distributed/ --include="*.py" | wc -l
619
```

619 sites for the `Clock` swap, plus scattered `random.X` calls. Largely
mechanical, type-checker-assisted. Estimated 1–2 weeks for one engineer.

## 17. Layout

```
tests/simulation/
├── conftest.py                  # ClusterHarness fixture, parametrize helpers
├── harness/
│   ├── __init__.py
│   ├── env_overrides.py         # EnvOverrides
│   ├── timeouts.py              # HarnessTimeouts
│   ├── dc_spec.py               # DCSpec
│   ├── cluster_spec.py          # ClusterSpec
│   ├── execution_mode.py        # ExecutionMode enum
│   ├── port_allocator.py        # PortAllocator
│   ├── worker_ports.py          # WorkerPorts
│   ├── server_handle.py         # ServerHandle
│   ├── supervisor.py            # Supervisor
│   ├── cluster_harness.py       # ClusterHarness
│   └── errors.py                # exceptions
├── workflows/                   # the catalog (Phase 2+)
├── scenarios/
│   ├── l1_single_node/
│   ├── l2_single_dc/
│   └── l3_multi_dc/
└── _artifacts/                  # diagnostic dumps (gitignored)
```

## 18. Implementation phases

Strict order, each phase independently shippable.

### Phase 1 — Foundation (REAL mode only) — *partially landed*

**Shipped:**

- `Supervisor` with full process tracking (§6.1–6.7).
- `PortAllocator` with try-bind + range reservation.
- `ClusterHarness` + `ClusterSpec`/`DCSpec` building real servers.
- Framework-structure smoke scenarios at L1/L2/L3 (passing).

**Surfaced production bugs while constructing a single-manager cluster
(nine fixed inline as of 2026-05-01):**

1. `manager/config.py:242–244` — `env.get(...)` on a Pydantic `BaseModel`
   with no `.get`. Replaced with attribute access.
2. `manager/server.py:329` — `ManagerCancellationCoordinator` referenced
   `self._job_manager` 60 lines before its assignment. JobManager moved
   earlier in init.
3. `manager/discovery.py:64` — single-manager DC fails `DiscoveryConfig`
   validation (no seeds, no DNS, no dynamic registration). Added
   dynamic-registration fallback when seed list is empty.
4. `worker/server.py:1372` — read-only `@property env` overrode the
   parent's `self.env = env` assignment in `MercurySyncBaseServer.__init__`.
   Removed the redundant property.
5. `manager/server.py:707` — `WorkflowLifecycleStateMachine()` called with
   no args; class requires `(logger, node_host, node_port, node_id)`.
   Now passes the four arguments.
6. `manager/server.py:756` — `RaftIntegration` constructed during
   `__init__` with `task_runner=None` (parent populates `_task_runner` in
   `start_server`, called later). Re-binds the live task runner before
   `_raft.start()`.
7. `taskex/run.py:96, 110` — `setattr(bound_instance, method_name, method)`
   raises `AttributeError` on `__slots__` classes that don't list the
   method name. The setattr is an optimization; wrapped both call sites
   in try/except.
8. `worker/server.py:446` — `await super().start()` but parent only
   exposes `start_server`. Replaced with `super().start_server()`.
9. `tests/simulation/conftest.py` — pytest captures stdout, but
   `LoggerStream._setup_stdout_writer` calls `loop.connect_write_pipe(...)`
   which requires a TTY/pipe/socket. Disabled the global Logger via
   `LoggingConfig().disable()` for the simulation suite (Phase 2 will
   wire file-backed logging for diagnostic dumps).

**Worker startup time (initially mistaken for a hang):** `WorkerServer.start()`
returns in ~25 s on cold start because `ProcessPoolExecutor` spawn-mode
must fork two new Python interpreters and wait for them to register over
loopback. Earlier debugging mistook this for a hang because pytest's
default `fd` capture mode redirected stdout into a temp file, which
caused the project Logger's `connect_write_pipe` to raise — masking the
actual progress. Resolution: run pytest with `--capture=no` (or `-s`),
keep `LoggingConfig.log_directory` set so logs go to files (see
`tests/simulation/conftest.py`).

**Bugs the harness logs surfaced after the Logger started writing files:**

1. `manager/server.py:1955` — `_stats_push_loop` calls
   `_windowed_stats.get_active_job_ids()` which does not exist on
   `WindowedStatsCollector`. Fires every ~250 ms while a manager is
   running.
2. `manager/server.py:1932` — `_job_responsiveness_loop` iterates
   `_health_monitor.check_job_suspicion_expiry()` whose return is a
   coroutine (the method is async). Iteration raises
   `'coroutine' object is not iterable`.
3. `manager/server.py:2083` — `_gate_heartbeat_loop` constructs
   `ManagerHeartbeat(active_job_count=...)` but that kwarg does not
   exist on the model.

These run in background loops with their own `try/except` so the test
still passes; they're noisy but non-fatal.

**Pytest "missing await" warnings:** `_rate_limiter.check`,
`start_probe_cycle`, `check_job_suspicion_expiry` are called without
`await` in three spots. The warnings are real; do **not** add `await`
without verifying the call site. The rate-limiter case is load-bearing:
the truthy-coroutine return effectively disables rate limiting, and
properly awaiting it deadlocks worker registration over loopback.

**Phase 1 status:** all four scenarios (L1 framework + L1 lifecycle,
L2 framework, L3 framework) pass. L2/L3 lifecycle scenarios are still
deferred — the smoke deliverable is "harness builds and tears down
real clusters cleanly," which L1 lifecycle proves.

### Phase 2 — Conditions, diagnostics, invariant skeleton

- `wait_until` + built-in predicates (replaces every `asyncio.sleep` in
  scenarios).
- `DiagnosticDumper` with per-node snapshotters.
- `InvariantChecker` (§12, §13) with one safety + one liveness invariant
  active. Polling at 100 ms in REAL mode.
- `WorkloadDriver` + simplest two `Expectation` types.

**Exit criteria:** L2 scenario runtime drops to whatever real stabilization
actually requires; snapshots emitted on any timeout; one safety invariant
verifies continuously.

### Phase 3 — Lifecycle faults + subprocess crash fidelity — *landed*

**Shipped:**

- `FaultMatrix.kill / restart / pause / resume` — abrupt teardown,
  rebuild-from-builder restart, soft pause that cancels outbound loops
  while leaving transports open, and resume that rearms them. REAL-mode
  fidelity: in-process abort approximates SIGKILL; faithful OS-level
  semantics arrive in Phase 6 SIM mode.

- `WorkloadDriver` split into reusable steps for mid-workload faults:
  `submit()` → `wait_until_running(timeout)` → `wait_for_completion()`.
  `submit_and_wait()` retained as a convenience. `cancel(job_id, reason,
  timeout)` wraps the client's cancellation + await round-trip with
  diagnostic-dump-on-timeout.

- 11 scenarios under `tests/simulation/scenarios/phase3_faults/`:

  | File                              | Scenarios                                                                        |
  |-----------------------------------|----------------------------------------------------------------------------------|
  | `test_leader_faults.py`           | leader_kill_then_restart, follower_kill_then_restart                             |
  | `test_quorum_and_pause.py`        | quorum_loss_and_recovery, manager_pause_and_resume, cascade_two_managers         |
  | `test_worker_faults.py`           | worker_kill_then_restart, rapid_worker_churn                                     |
  | `test_faults_with_workload.py`    | worker_kill_mid_workload, leader_kill_mid_workload                               |
  | `test_cancellation_under_fault.py`| cancel_running_workflow, cancel_during_leader_failover                           |

**Exit criteria — met.** The cancellation-failover and election
scenarios from `tests/integration/raft/test_cancellation_failover.py`
and `test_raft_leadership_failover.py` have equivalent simulation
coverage: leader takeover (single + cascade), cancellation through
the push chain (stable + during failover), worker reaping, and
quorum loss / recovery. Subtle variants in the integration suite
(SwimLeaderPlusJobLeaderFails, GateOrphanJobHandling, etc.) are
mechanical compositions of the harness primitives now available
and can be added opportunistically.

### Phase 4 — Transport injection (REAL mode partition / delay / drop) — *landed*

**Shipped:**

- `FaultInjectingTransport` (`tests/simulation/harness/fault_transport.py`)
  wraps `send_tcp` / `send_udp` at the bound-method level on every
  harness-managed server. The wrapper closes over a reference to the
  harness's `FaultMatrix` and the `address_to_node_id` resolver, and:

  1. Returns a synthetic `(asyncio.TimeoutError, clock)` tuple — the
     same shape the original methods produce on error — when the
     (src, dst) pair is partitioned or hits a `drop_rate` roll.
  2. Sleeps for the configured delay (with jitter) before forwarding.
  3. Forwards to the original bound method when no rule matches.

  External addresses (e.g. the workload's `HyperscaleClient` port)
  return `None` from `address_to_node_id` and bypass rules — the
  client is treated as outside the harness.

- `FaultMatrix` extensions: `partition`, `heal_partition`,
  `delay(ms, *, src, dst, jitter_ms)`, `drop_rate(probability, *,
  src, dst)`, `clear_network_faults`. Wildcards on `src`/`dst`
  supported (`None` = any). Most-specific-rule wins; ties broken by
  insertion order so scenarios can install a wildcard baseline and
  override for named pairs.

- `ClusterHarness.address_to_node_id(address, kind)` — TCP/UDP
  port-table lookup. `_install_one` runs after `_start_servers`;
  `FaultMatrix.restart` re-installs on rebuild so partition rules
  survive kill / restart cycles.

- 4 scenarios under `tests/simulation/scenarios/phase4_network/`:

  | Scenario                                     | Topology | What it validates                              |
  |----------------------------------------------|----------|-----------------------------------------------|
  | `dc_to_dc_partition_then_heal`               | L3 (2DC) | Symmetric cross-DC partition; intra-DC peers  |
  |                                              |          | stay stable; heal converges                   |
  | `one_way_drop_rate`                          | L3 (2DC) | Asymmetric east→west 100% drop; reverse OK    |
  | `flapping_partition`                         | L3 (2DC) | 3 partition/heal cycles; no leaked state      |
  | `intra_dc_delay_does_not_break_quorum`       | L2 (3M)  | 200ms±50ms jitter; leader election succeeds   |

**Exit criteria — met.** SWIM partition-correlation paths exercised
at L3 with symmetric, asymmetric, and flapping faults; the FaultMatrix
primitives compose with the Phase 3 lifecycle faults so any future
scenario can mix kill/restart with partition/delay/drop without new
harness code.

### Phase 5 — Clock / Random / Transport interface refactor

- Production code: introduce `Clock`, `Random`, `Transport` interfaces.
- Mechanical replacement of 619 `time.X` / `asyncio.sleep` sites + scattered
  `random.X` sites with injected dependencies.
- All existing tests continue passing — pure dependency-injection move,
  no behavior change.

**Exit criteria:** integration + simulation test suites green; no
production-code direct calls to `time.monotonic` / `time.time` /
`asyncio.sleep` / `random.X` outside the interface implementations.

### Phase 6 — SIM mode

- `VirtualClock`, `SeededRandom`, `InProcessTransport`, `DeterministicTaskRunner`.
- `ExecutionMode.SIM` wired into `ClusterHarness`.
- All Phase 1–4 scenarios run under SIM mode via parametrize.
- Replay command: `pytest --sim-replay=<seed>`.

**Exit criteria:** every scenario passes deterministically in SIM mode;
seed-driven random fault schedules generate ≥ 1000 scenarios per CI minute.

### Phase 7 — Storage faults + linearizability oracle

- `FaultMatrix.slow_disk / disk_full / fsync_reorder` (SIM-mode
  implementations).
- Linearizability checker for the client-facing job-submission API.

## 19. Open questions

1. **Asyncio leak policy:** log-only with grace period, or fail-immediately
   from day one? **Lean: fail-immediately.** Triage the leaks now; that's
   the point.
2. **Per-test temp log dir or per-run?** Per-test for isolation. Per-run
   opt-in via fixture parameter.
3. **Workflow catalog entries — owned by `tests/simulation/workflows/` or
   inline in scenarios?** Catalog for reusable shapes; one-off scenarios may
   define their own inline.

## 20. References

- Current integration tests: `tests/integration/gates/test_gate_cross_dc_dispatch.py`
  — the prototype this harness generalizes.
- Worker subprocess model: `hyperscale/distributed/nodes/worker/lifecycle.py`,
  `hyperscale/core/jobs/runner/local_server_pool.py`.
- Audit follow-ups: `docs/architecture/AUDIT_DISTRIBUTED_2026_01_11.md` —
  the harness's leak detector enforces these fixes.
- TigerBeetle VOPR — the bar SIM mode aspires to.
- CLAUDE.md — coding rules the harness must follow.
