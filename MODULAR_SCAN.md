# Modular Architecture Analysis Workflow

**Purpose:** Identify missing, misplaced, or duplicated functionality in modular server architectures.

---

## Class Classification

### Coordinator Classes
**Purpose:** Orchestrate complex workflows involving multiple components.

**Characteristics:**
- Injected into server during `__init__`
- Receives callbacks to server methods
- Methods named: `handle_*`, `process_*`, `dispatch_*`, `coordinate_*`
- Contains multi-step business logic
- May call multiple trackers/managers

**Examples:** `GateDispatchCoordinator`, `GateStatsCoordinator`, `GateHealthCoordinator`

### Tracker/Manager Classes
**Purpose:** Store, retrieve, and manage state.

**Characteristics:**
- Injected into server during `__init__`
- Few or no callbacks needed
- Methods named: `get_*`, `set_*`, `has_*`, `delete_*`, `add_*`, `remove_*`
- CRUD-like operations
- Self-contained data logic

**Examples:** `JobLeadershipTracker`, `GateJobManager`, `CircuitBreakerManager`

### Handler Classes
**Purpose:** Parse incoming messages and route to appropriate logic.

**Characteristics:**
- Receive raw bytes/messages
- Validate and deserialize
- Call server methods or coordinators
- Return serialized responses

**Examples:** `GateJobHandler`, `GateStateSyncHandler`

---

## Decision Matrix: Where Does Logic Belong?

| Question | Yes → | No → |
|----------|-------|------|
| Is it CRUD (get/set/has/delete)? | Tracker/Manager | Continue |
| Does it orchestrate multiple steps? | Coordinator | Continue |
| Does it need server callbacks? | Coordinator | Tracker/Manager |
| Is it message parsing/routing? | Handler | Continue |
| Is it pure data transformation? | Tracker/Manager | Coordinator |

---

## Phase 1: Inventory Dependencies

For a server file, extract all injected dependencies:

```bash
# Find all self._X = patterns in __init__
grep -n "self\._[a-z_]* =" server.py | grep -v "self\._[a-z_]* = None"
```

Classify each as:
- **Coordinator** (has callbacks, orchestrates)
- **Tracker/Manager** (stores state, CRUD)
- **Handler** (message parsing)
- **Utility** (logging, config, etc.)

---

## Phase 2: Extract Method Calls

For each dependency, find all method calls:

```bash
grep -on "_dependency_name\.[a-zA-Z_]*" server.py | sort -u
```

---

## Phase 3: Verify Methods Exist

For each method call, verify it exists in the target class:

```bash
grep -n "def method_name" target_class.py
```

**If missing:**
1. Check if method exists with different name
2. Check if functionality exists in different method (e.g., `to_snapshot()` vs individual getters)
3. If truly missing, implement it

---

## Phase 4: Check for Misplaced Logic

### Server Wrapper Pattern (CORRECT)
```python
# Server method is thin wrapper
async def _do_thing(self, ...):
    if self._coordinator:
        await self._coordinator.do_thing(...)
```

### Server Has Business Logic (INCORRECT)
```python
# Server method has logic that belongs in coordinator
async def _do_thing(self, ...):
    # This logic should be in coordinator
    result = complex_calculation()
    self._tracker.set(result)
    if self._coordinator:
        await self._coordinator.do_thing(...)
```

**Fix:** Move business logic to coordinator, keep server as thin wrapper.

---

## Phase 5: Check for Signature Mismatches

Compare call sites with method definitions:

```python
# Server calls:
self._coordinator.foo(a, b, c)

# Coordinator defines:
def foo(self, x):  # MISMATCH!
```

**Fix:** Align signatures.

---

## Phase 6: Check for Missing Delegation

Look for server methods with inline logic that should delegate:

```bash
# Find server methods that don't delegate to coordinators
grep -A 20 "async def _" server.py | grep -B 5 -A 15 "# TODO\|# FIXME\|pass$"
```

---

## Phase 7: Reference Implementation

If `examples/old/` contains the original monolithic implementation:

```bash
grep -n "def method_name" examples/old/original_impl.py
```

Use as reference for:
- Expected parameters
- Expected return type
- Business logic that should exist
- Side effects

---

## Anti-Patterns to Detect

### 1. Circular Callbacks
```
Server → Coordinator.foo() → callback → Server.bar() → same logic as foo()
```
**Fix:** Remove circular path, inject dependency directly.

### 2. Duplicate Logic
```
Server._do_thing() does X
Coordinator.do_thing() also does X
```
**Fix:** Remove from server, keep only in coordinator.

### 3. Missing Delegation
```
Server._do_thing() has 50 lines of business logic
No coordinator method exists
```
**Fix:** Create coordinator method, move logic there.

### 4. CRUD in Coordinator
```
Coordinator.get_job() just returns self._jobs[job_id]
```
**Fix:** Move to tracker/manager class.

### 5. Orchestration in Tracker
```
Tracker.process_update() calls multiple other services
```
**Fix:** Move to coordinator, tracker should only store/retrieve.

---

## Verification Checklist

After refactoring:

- [ ] All coordinator methods exist and have correct signatures
- [ ] All tracker methods exist and have correct signatures  
- [ ] Server wrappers are thin (delegation only)
- [ ] No duplicate logic between layers
- [ ] No circular callback patterns
- [ ] CRUD operations in trackers, orchestration in coordinators
- [ ] LSP diagnostics clean

---

## Automated Scan Template

```bash
#!/bin/bash
SERVER="$1"
echo "=== DEPENDENCY CALLS ==="

# Extract dependency names from __init__
DEPS=$(grep -oP "self\.(_[a-z_]+)\s*=" "$SERVER" | sed 's/self\.//;s/\s*=//' | sort -u)

for dep in $DEPS; do
    CALLS=$(grep -on "${dep}\.[a-zA-Z_]*" "$SERVER" | sort -u)
    if [ -n "$CALLS" ]; then
        echo "--- ${dep} ---"
        echo "$CALLS"
    fi
done
```

Then for each method, verify existence in target class.
