# Modular Node Refactoring Workflow

## Phase 1: Identify the Call

Given a call like `self._coordinator.method_name(args)`:

1. **Identify the coordinator type**: Check what `self._coordinator` is assigned to (search for `self._coordinator =` in `__init__`)
2. **Identify the target class**: Find the class definition (e.g., `class ManagerLeaseCoordinator`)

## Phase 2: Check if Method Exists

3. **Search for method definition**: `grep "def method_name" <coordinator_file>.py`
   - If exists → proceed to Phase 4
   - If not exists → proceed to Phase 3

## Phase 3: Check for Duplicate/Similar Functionality

4. **Search for similar method names across the codebase**:
   ```bash
   grep -r "def.*method_name\|def.*similar_name" --include="*.py" <project_path>
   ```

5. **Check other modular classes in the same node**:
   - List all coordinator/manager files in the node directory
   - Search each for similar functionality
   - Identify if the behavior exists elsewhere with different naming

6. **Check for direct state access patterns**:
   ```bash
   grep "_state\._field_name\|_manager_state\._field_name" <server_file>.py
   ```
   This reveals if server bypasses coordinators to access state directly.

## Phase 4: Determine the Fix

**If method exists with different name**:
- Fix call site to use correct method name

**If method doesn't exist but functionality is needed**:
- Add method to coordinator
- Follow existing patterns in that coordinator (docstrings, logging, etc.)

**If direct state access found (pattern violation)**:
- Map ALL direct state accesses for that field
- Add necessary coordinator methods (get/set/update)
- Refactor ALL call sites to use coordinator

## Phase 5: Refactor All Related Access

7. **Map every access point**:
   ```bash
   grep -n "_state\._field_name" <server_file>.py
   ```

8. **For each access, determine required coordinator method**:

   | Access Pattern | Required Method |
   |----------------|-----------------|
   | `.get(key, default)` | `get_X(key)` |
   | `[key] = value` | `set_X(key, value)` |
   | `if new > current: [key] = new` | `update_X_if_higher(key, new)` |
   | `[key] += 1` | `increment_X(key)` |
   | `.pop(key, None)` | `clear_X(key)` |

9. **Add missing methods to coordinator**

10. **Refactor each call site in server**

## Phase 6: Verify

11. **Confirm no direct access remains**:
    ```bash
    grep "_state\._field_name" <server_file>.py
    # Should return: No matches found
    ```

12. **Run LSP diagnostics**:
    - On coordinator file (should be clean)
    - On server file (pre-existing errors OK, no NEW errors)

## Example Application

**Input**: `fence_token=self._leases.get_job_fencing_token(job_id)` at line 4629

**Phase 1-2**: `self._leases` is `ManagerLeaseCoordinator`. Method `get_job_fencing_token` not found.

**Phase 3**: Found `get_fence_token` exists. Also found 5 direct `_manager_state._job_fencing_tokens` accesses in server.py.

**Phase 4**: Fix call site AND refactor all direct accesses.

**Phase 5**: 
- Added `set_fence_token()`, `update_fence_token_if_higher()`, `initialize_job_context()`, `get_job_context()`
- Refactored 6 call sites from direct state access to coordinator methods

**Phase 6**: Confirmed zero `_job_fencing_tokens` in server.py. LSP clean on leases.py.
