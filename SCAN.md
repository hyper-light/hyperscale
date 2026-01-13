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

**For naming mismatch:**
- Update call site to use correct name
- OR add method alias if multiple names are valid

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
