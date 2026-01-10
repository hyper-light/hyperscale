# Required Fixes (AD-10 through AD-37)

## AD-37 (Explicit Backpressure Policy) — NOT fully compliant

### 1) Gate must consume backpressure and throttle forwarded updates
**Problem**: Gate does not apply manager backpressure to forwarded updates; only load shedding is enforced.

**Exact changes**:
- Add backpressure state in Gate (e.g., `_manager_backpressure` dict keyed by manager_id).
- When receiving progress acks from managers, extract backpressure fields and update gate state.
- Throttle/batch any gate-originated progress/stat forwarding based on max backpressure.

**References**:
- `hyperscale/distributed_rewrite/nodes/gate.py:5755`
- `hyperscale/distributed_rewrite/nodes/gate.py:5173`

---

### 2) Unify message classification for load shedding + bounded execution
**Problem**: AD-37 specifies CONTROL/DISPATCH/DATA/TELEMETRY classes, but code uses local mappings in load shedding and in-flight tracking.

**Exact changes**:
- Centralize classification in a shared policy module (e.g., `reliability/message_class.py`).
- Use it in both load shedding and in-flight tracking to prevent drift.

**References**:
- `hyperscale/distributed_rewrite/reliability/load_shedding.py:58`
- `hyperscale/distributed_rewrite/server/protocol/in_flight_tracker.py:30`
- `hyperscale/distributed_rewrite/reliability/message_class.py:5`

---

## AD-34 (Adaptive Job Timeout, Multi‑DC) — HARDENING

### 3) Make timeout check interval configurable
**Problem**: Manager timeout loop uses a hardcoded `check_interval = 30.0` with a TODO to move to env.

**Exact changes**:
- Add `JOB_TIMEOUT_CHECK_INTERVAL` to `env.py` and use it in `_unified_timeout_loop()`.

**References**:
- `hyperscale/distributed_rewrite/nodes/manager.py:9369`
- `hyperscale/distributed_rewrite/env/env.py:146`

---

## AD-33 (Workflow State Machine) — HARDENING

### 4) Add optional progress callbacks for timeout strategy
**Problem**: Timeout strategy relies on manager-side manual progress reporting; state machine does not emit callbacks.

**Exact changes**:
- Add optional callbacks to `WorkflowStateMachine` for progress transitions.
- Wire `ManagerServer` to register a callback that forwards progress to timeout strategy.

**References**:
- `hyperscale/distributed_rewrite/workflow/state_machine.py:1`
- `hyperscale/distributed_rewrite/nodes/manager.py:9586`
