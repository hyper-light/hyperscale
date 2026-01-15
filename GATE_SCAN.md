# Gate Server Analysis Workflow

**Scope:** `hyperscale/distributed/nodes/gate/server.py` and related modules.

## Key Components

**Coordinators** (in `hyperscale/distributed/nodes/gate/`):
- `GateDispatchCoordinator` (dispatch_coordinator.py)
- `GateStatsCoordinator` (stats_coordinator.py)
- `GatePeerCoordinator` (peer_coordinator.py)
- `GateHealthCoordinator` (health_coordinator.py)
- `GateLeadershipCoordinator` (leadership_coordinator.py)

**Trackers/Managers** (in `hyperscale/distributed/jobs/`):
- `JobLeadershipTracker` (job_leadership_tracker.py)
- `GateJobManager` (gates/gate_job_manager.py)
- `GateJobTimeoutTracker` (gates/gate_job_timeout_tracker.py)

**Handlers** (in `hyperscale/distributed/nodes/gate/handlers/`):
- TCP and UDP message handlers

---

## Phase 1: Find All External Calls

Scan server.py for ALL calls to injected dependencies:

```bash
# Coordinators
grep -n "_dispatch_coordinator\." server.py
grep -n "_stats_coordinator\." server.py
grep -n "_peer_coordinator\." server.py
grep -n "_health_coordinator\." server.py
grep -n "_leadership_coordinator\." server.py

# Trackers/Managers
grep -n "_job_leadership_tracker\." server.py
grep -n "_job_manager\." server.py
grep -n "_job_timeout_tracker\." server.py

# Other injected dependencies
grep -n "_job_router\." server.py
grep -n "_circuit_breaker_manager\." server.py
grep -n "_dispatch_time_tracker\." server.py
```

---

## Phase 2: Verify Methods Exist

For EACH method call found, verify the method exists:

```bash
grep -n "def method_name" target_file.py
```

**If missing → flag for implementation**

---

## Phase 3: Trace Full Call Chains

For each server method, trace backwards and forwards:

```
WHO CALLS IT?          WHAT DOES IT DO?           WHAT DOES IT CALL?
─────────────          ────────────────           ──────────────────
Handler method    →    Server wrapper method  →   Coordinator/Tracker method
     ↓                       ↓                           ↓
tcp_job.py             server.py                  coordinator.py
```

### Finding Callers

```bash
# Find what calls a server method
grep -rn "method_name" hyperscale/distributed/nodes/gate/handlers/
grep -n "self\.method_name\|self\._method_name" server.py
```

### Identifying Orphaned Methods

Server methods that:
- Are never called (dead code)
- Call non-existent coordinator methods (broken)
- Have inline logic that should be delegated (needs refactor)

---

## Phase 4: Check for Issues

### Issue Type 1: Missing Method
```
server.py calls coordinator.foo()
BUT coordinator.py has no def foo()
→ IMPLEMENT foo() in coordinator
```

### Issue Type 2: Signature Mismatch
```
server.py calls coordinator.foo(a, b, c)
BUT coordinator.py has def foo(x)
→ FIX call site OR fix method signature
```

### Issue Type 3: Duplicate Logic
```
server.py wrapper does X then calls coordinator.foo()
AND coordinator.foo() also does X
→ REMOVE X from server wrapper
```

### Issue Type 4: Missing Delegation
```
server.py method has business logic inline
BUT should delegate to coordinator
→ MOVE logic to coordinator, simplify server to delegation
```

### Issue Type 5: Circular Dependency
```
server.py calls coordinator.foo()
AND coordinator.foo() calls back to server via callback
AND callback does same thing as foo()
→ REFACTOR to eliminate circular logic
```

---

## Phase 5: Reference Implementation

Check `examples/old/gate_impl.py` for canonical behavior:

```bash
grep -n "def method_name" examples/old/gate_impl.py
```

Read the full method to understand:
- What parameters it expects
- What it returns
- What side effects it has
- What other methods it calls

---

## Phase 6: Decision Matrix

| Finding | Action |
|---------|--------|
| Method missing in target | Implement using old gate_impl.py as reference |
| Signature mismatch | Fix caller or callee to match |
| Server wrapper has business logic | Move to coordinator, simplify wrapper |
| Handler has inline logic | Note for future cleanup (handler is legacy) |
| Dead/orphaned server method | Remove if truly unused |
| Circular callback pattern | Refactor to inject dependency directly |

---

## Phase 7: Verification Checklist

After each fix:
- [ ] Method exists at target location
- [ ] Method signature matches call site
- [ ] Server wrapper is pure delegation (no business logic)
- [ ] No duplicate logic between layers
- [ ] LSP diagnostics clean on affected files
- [ ] Reference old gate_impl.py for correctness

---

## Automated Scan Script

```bash
#!/bin/bash
# Run from hyperscale root

SERVER="hyperscale/distributed/nodes/gate/server.py"

echo "=== COORDINATOR CALLS ==="
for coord in dispatch_coordinator stats_coordinator peer_coordinator health_coordinator leadership_coordinator; do
    echo "--- _${coord} ---"
    grep -on "_${coord}\.[a-zA-Z_]*" $SERVER | sort -u
done

echo ""
echo "=== TRACKER/MANAGER CALLS ==="
for tracker in job_leadership_tracker job_manager job_timeout_tracker job_router circuit_breaker_manager dispatch_time_tracker; do
    echo "--- _${tracker} ---"
    grep -on "_${tracker}\.[a-zA-Z_]*" $SERVER | sort -u
done
```

Then for each method found, verify it exists in the target class.

---

## Notes

- This workflow is gate-specific
- Manager and worker nodes have different architectures
- Reference `examples/old/gate_impl.py` for canonical behavior
- When in doubt, the coordinator should own the business logic
