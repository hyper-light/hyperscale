# Modular Node Refactoring Workflow (SCAN)

Complete workflow for verifying and fixing modular architecture integrity in node server files.

## Phase 1: Extract All Component Calls

**Objective**: Build complete inventory of every method call on every component.

**Steps**:
1. Run: `grep -n "self\._[a-z_]*\." server.py` to get all component access
2. Filter to unique component names: `self._job_manager`, `self._dispatch_coordinator`, etc.
3. For EACH component, extract every method called:
   ```bash
   grep -on "self\._<component>\.[a-zA-Z_]*" server.py | sort -u
   ```
4. Build a table:
   | Component | Method Called | Line(s) |
   |-----------|---------------|---------|

**Output**: Complete call inventory with line numbers.

---

## Phase 2: Build Component Registry

**Objective**: Map each component to its class definition.

**Steps**:
1. Find where each component is assigned in `__init__`:
   ```bash
   grep "self\._<component>\s*=" server.py
   ```
2. Identify the class (e.g., `self._job_manager = GateJobManager()`)
3. Locate the class file:
   ```bash
   grep -r "class <ClassName>" --include="*.py"
   ```
4. Build registry:
   | Component | Class | File Path |
   |-----------|-------|-----------|

**Output**: Component-to-class mapping with file locations.

---

## Phase 3: Build Method Existence Matrix

**Objective**: For each component, verify every called method exists.

**Steps**:
For EACH component:
1. Read the class file
2. Extract all public methods:
   ```bash
   grep -n "def [a-z_]*" <class_file>.py | grep -v "def _"
   ```
   (Include `def _` prefixed if called from server)
3. Build existence matrix:
   | Component | Method Called | Exists? | Actual Method Name (if different) |
   |-----------|---------------|---------|-----------------------------------|
4. Flag all `Exists? = NO` entries

**Output**: Complete matrix showing which calls will fail at runtime.

---

## Phase 4: Check Direct State Access

**Objective**: Find abstraction violations where server bypasses components.

**Steps**:
1. Identify the state object(s): `grep "self\._.*state" server.py`
2. Search for internal field access:
   ```bash
   grep "self\._<state>\._[a-z]" server.py
   ```
3. For each violation, document:
   | Line | Direct Access | Should Use |
   |------|---------------|------------|

**Output**: List of abstraction violations to fix.

---

## Phase 5: Reconcile Each Missing Method

**Objective**: For EACH missing method, find or create the correct implementation.

**For each missing method from Phase 3:**

### Step 5a: Search for Similar Functionality
```bash
# Search all modular classes for similar method names
grep -rn "def.*<method_name_fragment>" <node_directory>/*.py

# Search for similar behavior patterns
grep -rn "<key_operation_or_variable>" <node_directory>/*.py
```

### Step 5b: Analyze What Was Found

**If method exists in DIFFERENT class:**
- Document where it exists
- Determine if call site is using wrong component
- OR if method should be moved/exposed differently

**If SIMILAR method exists (different name):**
- Compare signatures and behavior
- Determine if it's a naming inconsistency
- Fix call site OR add alias

**If MULTIPLE implementations exist:**
- Read and understand EACH implementation fully
- Document differences:
  | Implementation | Location | Behavior | Edge Cases Handled |
  |----------------|----------|----------|-------------------|
- Design unified implementation that handles ALL cases
- Identify canonical owner based on:
  - Single Responsibility (which class SHOULD own this?)
  - Existing patterns in codebase
  - Dependency direction (avoid circular deps)

**If NO similar functionality exists:**
- Check git history: was it deleted?
- Check if call site is dead code (unreachable)
- If genuinely needed: implement it
- If dead code: remove the call

### Step 5c: Implement the Fix

**CRITICAL: The Robustness Principle**

**Never optimize for ease of fix. Always optimize for correctness of architecture.**

**MANDATORY: Do the refactor. No exceptions for complexity.**

When a refactor is identified as the correct solution, execute it fully regardless of:
- Number of files affected
- Number of call sites to update
- Complexity of the change
- Time required

**There is no "too complex to refactor now" exemption.** If the correct fix requires touching 50 files, touch 50 files. If it requires updating 200 call sites, update 200 call sites. Deferring correct fixes creates technical debt that compounds.

The only valid reasons to pause a refactor:
1. **Ambiguity in requirements** - unclear what the correct behavior should be (ask for clarification)
2. **Missing domain knowledge** - need to understand existing behavior before changing (research first)
3. **Risk of data loss** - change could corrupt persistent state (design migration first)

"This refactor is large" is NOT a valid reason to defer. "This refactor is complex" is NOT a valid reason to simplify. Execute the correct fix.

When faced with a problem, there are typically multiple solutions:
- **Shortcut**: Add alias, wrapper, shim, adapter, or duplicate to make the call site work
- **Correct**: Fix the root cause - update call sites, consolidate implementations, remove duplication

**Always choose the solution that:**
1. **Reduces total code** - fewer lines = fewer bugs, less maintenance
2. **Has single source of truth** - one implementation per behavior
3. **Makes the codebase more consistent** - same pattern everywhere
4. **Removes ambiguity** - one name for one concept
5. **Fixes the root cause** - not the symptom

**Before implementing ANY fix, ask:**
1. Am I adding code or removing/consolidating code?
2. Will there be two ways to do the same thing after this fix?
3. Am I papering over an inconsistency or resolving it?
4. Would a future developer be confused by this?
5. Is this how the codebase SHOULD have been written from the start?

**If the fix adds complexity, duplication, or ambiguity - it's wrong.** Find the solution that leaves the codebase cleaner than you found it.

This applies to:
- Method names (don't add aliases)
- Implementations (don't add wrappers)
- Abstractions (don't add adapter layers)
- Data structures (don't add translation code)
- Error handling (don't add catch-and-rethrow)

**For naming mismatch:**
- Update call site to use the existing correct method name
- Do NOT add aliases

**For wrong component:**
- Update call site to use correct component
- Verify the correct component is available in server

**For missing functionality:**
- Add method to canonical owner
- Follow existing patterns (docstrings, error handling, logging)
- Ensure method signature matches call site expectations

**For duplicate functionality:**
1. Create unified implementation in canonical owner
2. Update ALL call sites to use canonical location
3. Delete duplicate implementations
4. Search for any other references to deleted methods

### Step 5d: Document the Change
For each fix, note:
- What was broken
- Root cause (incomplete refactor, naming drift, etc.)
- What was changed
- Files modified

---

## Phase 5.5: Server-Side Consolidation

**Objective**: Ensure server is a thin orchestration layer, not a dumping ground for business logic.

### Step 5.5a: Identify Incomplete Delegation

Search for patterns that suggest logic should be moved to a coordinator:

```bash
# Find complex logic blocks (multiple operations on same component)
grep -n "self._<component>.*\n.*self._<component>" server.py

# Find business logic patterns (conditionals around component calls)
grep -B2 -A2 "if.*self._<component>" server.py
```

**Red flags**:
- Multiple sequential calls to same component that could be one method
- Conditional logic wrapping component calls (the condition should be inside the component)
- Data transformation before/after component calls (component should handle its own data format)
- Try/except blocks around component calls (component should handle its own errors)

### Step 5.5b: Identify Duplicate Server Code

```bash
# Find similar method patterns
grep -n "async def _" server.py | look for similar names
```

**Red flags**:
- Methods with similar names doing similar things (`_handle_X_from_manager`, `_handle_X_from_gate`)
- Copy-pasted code blocks with minor variations
- Same error handling pattern repeated

### Step 5.5c: Identify Useless Wrappers

Server methods that ONLY do:
```python
async def _do_thing(self, ...):
    return await self._coordinator.do_thing(...)
```

These should either:
- Be removed (caller uses coordinator directly)
- OR have the component method renamed to match the server's public interface

### Step 5.5d: Apply the Robustness Principle

For each issue found:
1. **Move logic to component** - don't keep it in server
2. **Consolidate duplicates** - one implementation, not two similar ones
3. **Remove useless wrappers** - direct delegation or nothing

---

## Phase 6: Clean Up Dead Code

**Objective**: Remove orphaned implementations.

**Steps**:
1. For each modular class, extract all public methods
2. Search server for calls to each method
3. If method is never called AND not part of public API:
   - Verify it's not called from OTHER files
   - If truly orphaned, remove it
4. Document removed methods

---

## Phase 7: Verify Completeness

**Objective**: Ensure refactor is complete and correct.

**Checklist**:
- [ ] Re-run Phase 3 matrix: all methods now exist
- [ ] Re-run Phase 4: no direct state access
- [ ] LSP diagnostics clean on ALL modified files
- [ ] No duplicate method implementations across modular classes
- [ ] No orphaned/dead methods in modular classes
- [ ] All call sites reference correct component and method

---

## Phase 8: Commit with Context

**Commit message should include**:
- What was broken (missing methods, duplicates, etc.)
- Root cause (incomplete refactor from X)
- What was unified/moved/added/removed

---

## Phase 9: Duplicate State Detection

**Objective**: Find and eliminate duplicate state between server and modular classes (state/coordinators).

### The Problem

Server often has instance variables that duplicate state already managed by `_modular_state` or coordinators:

```python
# In server __init__:
self._active_gate_peers: set[tuple[str, int]] = set()  # DUPLICATE
self._gate_peer_info: dict[...] = {}                    # DUPLICATE

# In GateRuntimeState:
self._active_gate_peers: set[tuple[str, int]] = set()  # CANONICAL
self._gate_peer_info: dict[...] = {}                    # CANONICAL
```

This causes:
- **Drift**: Values can differ between server and state
- **Confusion**: Which is source of truth?
- **Bugs**: Updates to one don't update the other
- **Maintenance burden**: Same logic duplicated

### Step 9a: Extract Server Instance Variables

```bash
# Get all instance variable declarations from __init__
grep -n "self\._[a-z_]* = \|self\._[a-z_]*: " server.py | head -200
```

Build table:
| Variable | Type | Line | Purpose |
|----------|------|------|---------|

### Step 9b: Extract State Class Variables

```bash
# Get all instance variables from state class
grep -n "self\._[a-z_]* = \|self\._[a-z_]*: " state.py
```

Build table:
| Variable | Type | Line | Purpose |
|----------|------|------|---------|

### Step 9c: Build Comparison Matrix

Cross-reference the two tables:

| Variable Name | In Server? | In State? | Verdict |
|---------------|------------|-----------|---------|
| `_active_gate_peers` | Yes (L327) | Yes (L52) | **DUPLICATE** |
| `_gate_peer_info` | Yes (L334) | Yes (L55) | **DUPLICATE** |
| `_job_manager` | Yes (L380) | No | OK - component ref |
| `_forward_throughput_count` | No | Yes (L111) | OK - state owns it |

### Step 9d: Classify Duplicates

For each duplicate, determine the pattern:

| Pattern | Description | Action |
|---------|-------------|--------|
| **Shadow Copy** | Server has copy of state variable | Remove from server, use `_modular_state.X` |
| **Initialization Copy** | Server initializes, never syncs | Remove from server, initialize in state |
| **Stale Migration** | Variable moved to state but not removed from server | Remove from server |
| **Access Convenience** | Server caches for faster access | Remove; access through state (perf is rarely an issue) |

### Step 9e: Consolidate to State

For each duplicate:

1. **Find all usages in server**:
   ```bash
   grep -n "self\._<variable>" server.py
   ```

2. **Replace with state access**:
   ```python
   # Before:
   self._active_gate_peers.add(addr)
   
   # After:
   self._modular_state._active_gate_peers.add(addr)
   # OR better - use a state method:
   self._modular_state.add_active_peer(addr)
   ```

3. **Remove declaration from server `__init__`**

4. **Verify with LSP diagnostics**

### Step 9f: Create State Methods (if needed)

If the server was doing multi-step operations on the variable, create a method in state:

```python
# In state.py:
def add_active_peer(self, addr: tuple[str, int]) -> None:
    """Add peer to active set."""
    self._active_gate_peers.add(addr)
    
def remove_active_peer(self, addr: tuple[str, int]) -> None:
    """Remove peer from active set."""
    self._active_gate_peers.discard(addr)
```

Then server uses:
```python
self._modular_state.add_active_peer(addr)
```

### Output

- Zero duplicate variables between server and state
- All state access goes through `_modular_state` or coordinator methods
- Server `__init__` only contains configuration and component references

---

## Phase 10: Delegation Opportunity Analysis

**Objective**: Proactively identify server methods that should be delegated to coordinators.

### The Goal

Server should be a **thin orchestration layer**:
- Receives requests
- Routes to appropriate coordinator
- Handles lifecycle events
- Wires components together

Business logic belongs in coordinators/state.

### Step 10a: Categorize Server Methods

List all private methods:
```bash
grep -n "async def _\|def _" server.py
```

Categorize each method:

| Category | Description | Where It Belongs |
|----------|-------------|------------------|
| **Business Logic** | Conditionals on domain data, iterations over collections, calculations | Coordinator |
| **Orchestration** | Calling coordinators, handling responses, wiring | Server (keep) |
| **Lifecycle Hook** | `_on_peer_confirmed`, `_on_node_dead` | Server (keep) |
| **Protocol Handler** | Network/message handling | Server (keep) |
| **Pure Delegation** | Single call to coordinator | Server or eliminate |

### Step 10b: Identify Delegation Candidates

A method is a **delegation candidate** if it:

1. **Contains conditional logic** (if/else, match) on domain data
2. **Iterates over domain collections** (workers, datacenters, jobs)
3. **Performs calculations** (counts, averages, selections)
4. **Has no I/O or coordinator calls** - pure computation
5. **Could be unit tested in isolation** without server context
6. **Is > 10 lines** of actual logic (not just delegation)

Build candidate list:

| Method | Lines | Logic Type | Target Coordinator |
|--------|-------|------------|-------------------|
| `_get_healthy_gates` | 33 | Iteration + construction | `peer_coordinator` |
| `_has_quorum_available` | 5 | Business logic | `leadership_coordinator` |
| `_legacy_select_datacenters` | 40 | Selection algorithm | `health_coordinator` |

### Step 10c: Match to Existing Coordinators

For each candidate, identify target:

| Candidate | Best Fit Coordinator | Reasoning |
|-----------|---------------------|-----------|
| `_get_healthy_gates` | `peer_coordinator` | Manages peer/gate state |
| `_has_quorum_available` | `leadership_coordinator` | Manages quorum/leadership |
| `_build_datacenter_candidates` | `health_coordinator` | Manages DC health |

**If no coordinator fits:**
- Consider if a new coordinator is warranted
- Or if the method is actually orchestration (keep in server)

### Step 10d: Execute Delegations

**No deferral for complexity.** If a method should be delegated, delegate it now. Not "later when we have time." Not "in a follow-up PR." Now.

For each candidate, one at a time:

1. **Move logic to coordinator**:
   - Copy method body
   - Adapt to use coordinator's state references
   - Add docstring if public API

2. **Replace server method with delegation**:
   ```python
   # Before (in server):
   def _get_healthy_gates(self) -> list[GateInfo]:
       gates = [...]
       for peer_addr in self._active_gate_peers:
           ...
       return gates
   
   # After (in server):
   def _get_healthy_gates(self) -> list[GateInfo]:
       return self._peer_coordinator.get_healthy_gates()
   ```

3. **Keep fallback in server** (temporarily) if coordinator may be None:
   ```python
   def _get_healthy_gates(self) -> list[GateInfo]:
       if self._peer_coordinator:
           return self._peer_coordinator.get_healthy_gates()
       # Fallback logic here (to be removed once all paths initialize coordinator)
   ```

4. **Run LSP diagnostics**

5. **Commit**

### Step 10e: Verify Server is "Thin"

After delegation, server methods should average:
- **< 15 lines** of actual code (not counting docstrings)
- **1-3 coordinator calls** per method
- **Minimal conditionals** (those should be in coordinators)

### Red Flags (methods to investigate)

```bash
# Find long methods
awk '/def _/{p=1;n=0} p{n++} /^    def |^class /{if(p&&n>20)print prev,n;p=0} {prev=$0}' server.py
```

Any method > 20 lines should be scrutinized for delegation opportunities.

---

## Phase 11: Dead Import Detection

**Objective**: Remove imports that were orphaned by modular refactoring.

### The Problem

When logic moves from server to handlers/coordinators, the imports often stay behind:

```python
# In server.py (BEFORE refactor):
from hyperscale.distributed.models import JobCancelRequest, JobCancelResponse
# ... used in server methods

# In server.py (AFTER refactor):
from hyperscale.distributed.models import JobCancelRequest, JobCancelResponse  # DEAD
# ... logic moved to tcp_cancellation.py handler

# In tcp_cancellation.py:
from hyperscale.distributed.models import JobCancelRequest, JobCancelResponse  # ACTIVE
```

Dead imports cause:
- **Slower startup** - unnecessary module loading
- **Confusion** - suggests server uses these types when it doesn't
- **Merge conflicts** - imports change frequently, dead ones create noise
- **Circular import risk** - unused imports can create hidden dependency cycles

### Step 11a: Extract All Imports

```python
import re

with open('server.py', 'r') as f:
    content = f.read()

# Find import section (before class definition)
class_start = content.find('class ')
import_section = content[:class_start]

# Extract all imported names
imported_names = set()

# Multi-line: from X import (A, B, C)
for block in re.findall(r'from\s+[\w.]+\s+import\s+\(([\s\S]*?)\)', import_section):
    for name, alias in re.findall(r'(\w+)(?:\s+as\s+(\w+))?', block):
        imported_names.add(alias if alias else name)

# Single-line: from X import A, B
for line in re.findall(r'from\s+[\w.]+\s+import\s+([^(\n]+)', import_section):
    for name, alias in re.findall(r'(\w+)(?:\s+as\s+(\w+))?', line):
        imported_names.add(alias if alias else name)

# Direct: import X
for name in re.findall(r'^import\s+(\w+)', import_section, re.MULTILINE):
    imported_names.add(name)

print(f"Found {len(imported_names)} imported names")
```

### Step 11b: Check Usage in Code Body

```python
# Code after imports (class definition onward)
code_section = content[class_start:]

unused = []
for name in imported_names:
    if name == 'TYPE_CHECKING':
        continue
    
    # Word boundary match to avoid partial matches
    pattern = r'\b' + re.escape(name) + r'\b'
    if not re.search(pattern, code_section):
        unused.append(name)

print(f"Potentially unused: {len(unused)}")
for name in sorted(unused):
    print(f"  {name}")
```

### Step 11c: Verify Against Modular Files

For each unused import, check if it's used in handlers/coordinators:

```bash
# For each unused import
grep -l "ImportName" handlers/*.py coordinators/*.py state.py
```

**Classification**:

| Found In | Action |
|----------|--------|
| Handler/Coordinator (imported there) | Remove from server - it's properly imported where used |
| Handler/Coordinator (NOT imported) | Bug - handler needs the import, add it there |
| Nowhere in gate module | **INVESTIGATE** - potentially unimplemented behavior; check if feature is missing |
| Only in TYPE_CHECKING block | Keep if used in type hints, remove otherwise |

**CRITICAL**: An import that exists nowhere in the module is a red flag. Before removing:
1. Check git history - was this recently used and accidentally deleted?
2. Check related modules - is there a handler/coordinator that SHOULD use this?
3. Check the model's purpose - does the server need to handle this message type?

If the import represents a message type (e.g., `JobCancelRequest`), the server likely needs a handler for it. Missing handler = missing feature, not dead import.

### Step 11d: Remove Dead Imports

Group removals by source module to minimize diff churn:

```python
# Before:
from hyperscale.distributed.models import (
    JobCancelRequest,      # DEAD
    JobCancelResponse,     # DEAD
    JobSubmission,         # USED
    JobStatus,             # USED
)

# After:
from hyperscale.distributed.models import (
    JobSubmission,
    JobStatus,
)
```

### Step 11e: Verify No Breakage

1. **Run LSP diagnostics** - catch any "undefined name" errors
2. **Check TYPE_CHECKING imports** - some imports only used in type hints
3. **Search for string references** - `getattr(module, "ClassName")` patterns

```bash
# Find string references to class names
grep -n "\"ClassName\"\|'ClassName'" server.py
```

### Step 11f: Commit

Commit message should note:
- Number of dead imports removed
- Root cause (modular refactor moved usage to X)

---

## Example Application

**Input**: `fence_token=self._leases.get_job_fencing_token(job_id)` at line 4629

**Phase 1-2**: `self._leases` is `ManagerLeaseCoordinator` in `leases.py`

**Phase 3**: Method `get_job_fencing_token` not found. Found `get_fence_token` exists.

**Phase 4**: Found 5 direct `_manager_state._job_fencing_tokens` accesses.

**Phase 5**: 
- `get_fence_token` exists - naming mismatch
- Direct state accesses need coordinator methods
- Added `set_fence_token()`, `update_fence_token_if_higher()`
- Refactored all call sites

**Phase 6**: No dead code found.

**Phase 7**: 
- Zero `_job_fencing_tokens` direct access
- All calls now use coordinator
- LSP clean

**Phase 8**: Committed with explanation of fence token consolidation.
