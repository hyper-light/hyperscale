# Gate Coordinator Analysis Workflow

**Scope:** This workflow applies specifically to `hyperscale/distributed/nodes/gate/server.py` and its coordinator classes in `hyperscale/distributed/nodes/gate/`.

## Gate Coordinators

- `GateDispatchCoordinator` (dispatch_coordinator.py)
- `GateStatsCoordinator` (stats_coordinator.py)
- `GatePeerCoordinator` (peer_coordinator.py)
- `GateHealthCoordinator` (health_coordinator.py)
- `GateLeadershipCoordinator` (leadership_coordinator.py)

## Workflow Steps

### 1. Identify Coordinator Call Sites

For each coordinator, find where server.py calls its methods:

```bash
grep -n "_dispatch_coordinator\." server.py
grep -n "_stats_coordinator\." server.py
grep -n "_peer_coordinator\." server.py
grep -n "_health_coordinator\." server.py
grep -n "_leadership_coordinator\." server.py
```

### 2. Check if Method Exists

Verify each called method exists in the coordinator class:

```bash
grep -n "def method_name" coordinator_file.py
```

**If missing → flag as unimplemented functionality**

### 3. Trace the Call Chain

Map the full flow:
- **Handler** → calls server method → calls coordinator method
- OR **Handler** → has logic inline (duplicate of coordinator)

### 4. Check for Duplicate Functionality

Compare:
- What the **coordinator method** does (or should do)
- What the **server wrapper method** does
- What the **handler** does inline

**Red flags:**
- Server wrapper doing business logic before/after delegation
- Handler has same logic that exists in coordinator
- Coordinator method calls back to server (circular)

### 5. Check Dependencies

For each coordinator method, verify all injected callbacks exist:

```bash
grep -n "self._callback_name" coordinator_file.py
```

Then verify each exists in server.py:

```bash
grep -n "def _callback_name" server.py
```

### 6. Reference Old Implementation

Check `examples/old/gate_impl.py` for the canonical implementation:

```bash
grep -n "def method_name" examples/old/gate_impl.py
```

Read the full method to understand intended behavior.

### 7. Decision Matrix

| Situation | Action |
|-----------|--------|
| Method missing in coordinator | Implement in coordinator using old gate_impl.py as reference |
| Server wrapper has business logic | Move logic to coordinator, simplify wrapper to pure delegation |
| Handler has inline logic that coordinator has | Handler is legacy - coordinator is correct (future cleanup) |
| Coordinator calls back to server | Circular dependency - refactor to inject dependency or move logic |
| Dependency callback missing in server | Add missing method to server.py |

### 8. Cleanup Checklist

After fixing:
- [ ] Coordinator method fully implemented
- [ ] Server wrapper is pure delegation (no business logic)
- [ ] All coordinator dependencies exist in server
- [ ] No duplicate timing/tracking/logging between server and coordinator
- [ ] LSP diagnostics clean on both files

## Notes

- This workflow is gate-specific
- Manager and worker nodes have different architectures without this coordinator pattern
- Reference `examples/old/gate_impl.py` for canonical behavior when in doubt
