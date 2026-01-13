# Modular Node Refactoring Workflow (SCAN)

Complete workflow for verifying and fixing modular architecture integrity in node server files.

## FUNDAMENTAL PRINCIPLES

### NO SHORTCUTS

**Every fix in this workflow must address the root cause, not paper over symptoms.**

A shortcut is any fix that:
- Uses a "proxy" field instead of the correct field
- Adds comments explaining why wrong data is being used
- Suppresses errors instead of fixing them
- Uses type casts (`as any`, `# type: ignore`) to silence warnings
- Computes values from unrelated data because the right data isn't available

**If the correct attribute doesn't exist, the fix is one of:**
1. Add the attribute to the model (if it belongs there)
2. Find where the attribute actually lives and navigate to it
3. Understand why the code expects this attribute and fix the design

**NEVER**: Use a different field as a "proxy" and add a comment explaining the workaround.

This principle applies to EVERY phase below.

### ALL PHASES ARE MANDATORY

**Every phase in this workflow MUST be executed. No skipping. No deferral.**

| Rule | Enforcement |
|------|-------------|
| **No phase skipping** | Each phase must be completed before proceeding to the next |
| **No "optional" steps** | Every step within a phase is required, not optional |
| **No deferral** | "We'll do this later" is not acceptable - do it now |
| **No partial completion** | A phase is not done until ALL its outputs are achieved |
| **No complexity exemptions** | Large refactors are still required - size is not an excuse |

**BLOCKING**: Workflow cannot proceed to Phase N+1 until Phase N is fully complete with zero violations.

### Phase Execution Checklist

Before marking ANY phase complete, verify:
- [ ] All detection scans run (not just "spot checks")
- [ ] All violations identified and documented
- [ ] All violations FIXED (not just documented)
- [ ] Verification scan shows ZERO remaining violations
- [ ] LSP diagnostics clean on all modified files

**If ANY check fails, the phase is NOT complete.**

---

## Phase 0: Import Alias Resolution (FOUNDATIONAL - MANDATORY)

**Objective**: Build comprehensive mapping of all import aliases before ANY scanning begins.

**Why This Is Critical:**

Import aliases hide the actual types being used, causing scanners to miss violations:

```python
# In imports:
from hyperscale.distributed.models import (
    ManagerState as ManagerStateEnum,      # Alias!
    WorkflowInfo as WfInfo,                 # Alias!
    JobSubmission as JobSub,                # Alias!
)

# In code - scanners looking for "ManagerState" will MISS these:
self._state.set_enum(ManagerStateEnum.OFFLINE)  # Uses alias
for wf in WfInfo.load(data).workflows:          # Uses alias
job = JobSub.create(...)                         # Uses alias
```

**ALL subsequent phases MUST use alias-aware scanning.**

### Step 0a: Extract All Import Aliases

```python
import ast
from pathlib import Path
from typing import Dict, Set, Tuple

def extract_all_imports(file_path: str) -> Dict[str, Tuple[str, str]]:
    """
    Extract all imports with full resolution.
    
    Returns: {used_name: (original_name, module_path)}
    
    Examples:
        'ManagerStateEnum' -> ('ManagerState', 'hyperscale.distributed.models')
        'JobInfo' -> ('JobInfo', 'hyperscale.distributed.models')
        'Path' -> ('Path', 'pathlib')
    """
    with open(file_path) as f:
        tree = ast.parse(f.read())
    
    imports = {}
    
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ''
            for alias in node.names:
                used_name = alias.asname if alias.asname else alias.name
                original_name = alias.name
                imports[used_name] = (original_name, module)
        
        elif isinstance(node, ast.Import):
            for alias in node.names:
                used_name = alias.asname if alias.asname else alias.name
                original_name = alias.name
                imports[used_name] = (original_name, '')
    
    return imports

def build_alias_mappings(server_path: str) -> Dict[str, str]:
    """
    Build mapping from aliases to original names.
    
    Returns: {alias: original_name}
    
    Example:
        {'ManagerStateEnum': 'ManagerState', 'WfInfo': 'WorkflowInfo'}
    """
    imports = extract_all_imports(server_path)
    return {
        used: original 
        for used, (original, _) in imports.items() 
        if used != original  # Only actual aliases
    }

def get_canonical_name(used_name: str, alias_map: Dict[str, str]) -> str:
    """Resolve alias to canonical name, or return as-is if not aliased."""
    return alias_map.get(used_name, used_name)
```

### Step 0b: Build Type Resolution Database

Combine alias resolution with class/enum definitions:

```python
class TypeResolver:
    """Resolves type names accounting for import aliases."""
    
    def __init__(self, server_path: str, models_dirs: list[str]):
        self.alias_map = build_alias_mappings(server_path)
        self.reverse_alias_map = {v: k for k, v in self.alias_map.items()}
        
        # Collect all classes, enums from models
        self.classes: Dict[str, ClassInfo] = {}
        self.enums: Dict[str, Set[str]] = {}
        
        for models_dir in models_dirs:
            for py_file in Path(models_dir).glob("**/*.py"):
                self._extract_types(str(py_file))
    
    def _extract_types(self, file_path: str) -> None:
        """Extract class and enum definitions from file."""
        with open(file_path) as f:
            tree = ast.parse(f.read())
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # Check if enum
                is_enum = any(
                    (isinstance(b, ast.Name) and b.id == 'Enum') or
                    (isinstance(b, ast.Attribute) and b.attr == 'Enum')
                    for b in node.bases
                )
                
                if is_enum:
                    members = {
                        t.id for item in node.body
                        if isinstance(item, ast.Assign)
                        for t in item.targets
                        if isinstance(t, ast.Name)
                    }
                    self.enums[node.name] = members
                else:
                    # Regular class - extract attributes and methods
                    self.classes[node.name] = self._extract_class_info(node, file_path)
    
    def resolve_type(self, used_name: str) -> str:
        """Resolve alias to canonical type name."""
        return self.alias_map.get(used_name, used_name)
    
    def get_alias_for(self, canonical_name: str) -> str | None:
        """Get the alias used in code for a canonical name."""
        return self.reverse_alias_map.get(canonical_name)
    
    def get_class_info(self, used_name: str) -> ClassInfo | None:
        """Get class info by used name (resolves aliases)."""
        canonical = self.resolve_type(used_name)
        return self.classes.get(canonical)
    
    def get_enum_members(self, used_name: str) -> Set[str] | None:
        """Get enum members by used name (resolves aliases)."""
        canonical = self.resolve_type(used_name)
        return self.enums.get(canonical)
    
    def iter_type_names_in_code(self, canonical_name: str) -> list[str]:
        """
        Get all names that might be used in code for a type.
        
        Returns both canonical name and any aliases.
        """
        names = [canonical_name]
        if alias := self.get_alias_for(canonical_name):
            names.append(alias)
        return names
```

### Step 0c: Integration with All Scanners

**MANDATORY**: Every scanner in Phase 3+ MUST:

1. **Initialize TypeResolver FIRST**:
   ```python
   resolver = TypeResolver(
       server_path="hyperscale/distributed/nodes/manager/server.py",
       models_dirs=["hyperscale/distributed/models"]
   )
   ```

2. **Use resolver for all type lookups**:
   ```python
   # WRONG - misses aliases:
   if type_name in self.classes:
       ...
   
   # RIGHT - resolves aliases:
   if class_info := resolver.get_class_info(type_name):
       ...
   ```

3. **Search for all name variants**:
   ```python
   # WRONG - misses aliased usages:
   pattern = rf'\b{canonical_name}\.'
   
   # RIGHT - searches for all variants:
   for name in resolver.iter_type_names_in_code(canonical_name):
       pattern = rf'\b{re.escape(name)}\.'
       # search...
   ```

### Step 0d: Alias Map Output (MANDATORY)

Before proceeding to Phase 1, generate and review the alias map:

```bash
python3 << 'EOF'
# Generate alias report for server file
imports = extract_all_imports("hyperscale/distributed/nodes/manager/server.py")
aliases = [(used, orig) for used, (orig, _) in imports.items() if used != orig]

print("Import Aliases Found:")
print("| Used In Code | Original Name | Module |")
print("|--------------|---------------|--------|")
for used, (orig, mod) in imports.items():
    if used != orig:
        print(f"| `{used}` | `{orig}` | `{mod}` |")
EOF
```

**Example Output:**

| Used In Code | Original Name | Module |
|--------------|---------------|--------|
| `ManagerStateEnum` | `ManagerState` | `hyperscale.distributed.models` |
| `WfInfo` | `WorkflowInfo` | `hyperscale.distributed.models` |

**BLOCKING**: Do not proceed to Phase 1 until alias map is generated and reviewed.

### Step 0e: Dynamic/Inline Import Detection (MANDATORY)

**Objective**: Detect and reject all imports that are not at the top of the file.

**The Problem:**

Dynamic or inline imports violate Python conventions and our codebase rules:

```python
# WRONG - inline import inside function
async def _handle_request(self, request: bytes):
    from hyperscale.distributed.models import JobSubmission  # VIOLATION!
    job = JobSubmission.load(request)
    
# WRONG - conditional import
if some_condition:
    import heavy_module  # VIOLATION!

# WRONG - import inside class body
class MyServer:
    from typing import Dict  # VIOLATION!
    
# WRONG - lazy import pattern
def get_parser():
    import json  # VIOLATION!
    return json.loads

# CORRECT - all imports at top of file
from hyperscale.distributed.models import JobSubmission
import json

async def _handle_request(self, request: bytes):
    job = JobSubmission.load(request)
```

**Why Inline Imports Are Forbidden:**

1. **Hidden dependencies**: Dependencies aren't visible at file top
2. **Inconsistent load times**: Import happens at runtime, not startup
3. **Harder to track**: Import alias resolution misses inline imports
4. **Circular import masking**: Hides circular dependency issues until runtime
5. **Testing difficulty**: Harder to mock/patch imports

**Exception**: `TYPE_CHECKING` blocks are allowed (they're not executed at runtime):

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from heavy_module import HeavyClass  # OK - only for type hints
```

**Detection Script:**

```python
import ast
from pathlib import Path

def find_inline_imports(file_path: str) -> list[tuple[int, str, str]]:
    """
    Find all imports that are not at module level.
    
    Returns: [(line_number, import_statement, context)]
    """
    with open(file_path) as f:
        source = f.read()
        tree = ast.parse(source)
        lines = source.split('\n')
    
    violations = []
    
    # Track if we're inside TYPE_CHECKING block
    type_checking_ranges = []
    
    for node in ast.walk(tree):
        # Find TYPE_CHECKING blocks
        if isinstance(node, ast.If):
            if (isinstance(node.test, ast.Name) and node.test.id == 'TYPE_CHECKING') or \
               (isinstance(node.test, ast.Attribute) and node.test.attr == 'TYPE_CHECKING'):
                # Record the range of this block
                type_checking_ranges.append((node.lineno, node.end_lineno or node.lineno + 100))
    
    def is_in_type_checking(lineno: int) -> bool:
        return any(start <= lineno <= end for start, end in type_checking_ranges)
    
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            # Check if this import is inside a function, class, or other block
            # by checking if it has a parent that's not Module
            
            # Get the line
            line = lines[node.lineno - 1].strip() if node.lineno <= len(lines) else ""
            
            # Skip if in TYPE_CHECKING block
            if is_in_type_checking(node.lineno):
                continue
            
            # Check parent context by walking tree with parent tracking
            parent = getattr(node, '_parent', None)
            
    # Alternative: use line-based detection for simplicity
    violations = []
    in_type_checking = False
    indent_stack = [0]
    
    for lineno, line in enumerate(lines, 1):
        stripped = line.strip()
        
        # Track TYPE_CHECKING blocks
        if 'if TYPE_CHECKING' in line or 'if typing.TYPE_CHECKING' in line:
            in_type_checking = True
            continue
        
        # Rough indent tracking to exit TYPE_CHECKING
        if in_type_checking:
            current_indent = len(line) - len(line.lstrip())
            if stripped and not stripped.startswith('#') and current_indent == 0:
                in_type_checking = False
        
        # Skip if in TYPE_CHECKING
        if in_type_checking:
            continue
        
        # Check for import statements
        if stripped.startswith('import ') or stripped.startswith('from '):
            # Check indentation - top-level imports have 0 indent
            indent = len(line) - len(line.lstrip())
            if indent > 0:
                # Determine context
                context = "indented block"
                for i in range(lineno - 1, 0, -1):
                    prev = lines[i - 1].strip()
                    if prev.startswith('def ') or prev.startswith('async def '):
                        context = f"inside function"
                        break
                    elif prev.startswith('class '):
                        context = f"inside class"
                        break
                    elif prev.startswith('if ') or prev.startswith('elif ') or prev.startswith('else'):
                        context = f"inside conditional"
                        break
                    elif prev.startswith('try:') or prev.startswith('except') or prev.startswith('finally'):
                        context = f"inside try/except"
                        break
                    elif prev.startswith('with '):
                        context = f"inside with block"
                        break
                
                violations.append((lineno, stripped, context))
    
    return violations

# Usage
violations = find_inline_imports("hyperscale/distributed/nodes/manager/server.py")
if violations:
    print(f"❌ Found {len(violations)} inline import(s):\n")
    for line_num, statement, context in violations:
        print(f"  Line {line_num} ({context}): {statement}")
else:
    print("✅ All imports are at module level")
```

**Quick Detection Command:**

```bash
# Find potentially inline imports (imports with leading whitespace)
grep -n "^[[:space:]]\+import \|^[[:space:]]\+from .* import" server.py | \
    grep -v "TYPE_CHECKING" | \
    grep -v "^[0-9]*:[[:space:]]*#"
```

**Fix Pattern:**

Move ALL inline imports to the top of the file:

```python
# BEFORE (violation):
async def _process_workflow(self, workflow_id: str):
    from hyperscale.distributed.models import WorkflowStatus
    status = WorkflowStatus.RUNNING
    ...

# AFTER (correct):
from hyperscale.distributed.models import WorkflowStatus

async def _process_workflow(self, workflow_id: str):
    status = WorkflowStatus.RUNNING
    ...
```

**BLOCKING**: Do not proceed to Phase 1 if ANY inline imports exist (except in TYPE_CHECKING blocks).

---

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

## Phase 3.5: Object Attribute Access Validation

**Objective**: Verify that attribute accesses on domain objects reference attributes that actually exist.

### The Problem

Phase 3 validates component method calls (`self._component.method()`), but misses attribute access on objects returned from those methods or stored in collections:

```python
# Phase 3 catches: component method doesn't exist
self._job_manager.nonexistent_method()  # CAUGHT

# Phase 3.5 catches: object attribute doesn't exist
job = self._job_manager.get_job(job_id)
for wf in job.workflows.values():
    total += wf.completed_count  # MISSED - WorkflowInfo has no completed_count!
```

This class of bug occurs when:
- Code assumes an object has attributes from a different (related) class
- Refactoring moved attributes to nested objects but call sites weren't updated
- Copy-paste from similar code that operates on different types

### Step 3.5a: Identify Domain Object Iterations

Find all loops that iterate over domain collections:

```bash
grep -n "for .* in .*\.values()\|for .* in .*\.items()\|for .* in self\._" server.py
```

Build table of iteration patterns:

| Line | Variable | Collection Source | Expected Type |
|------|----------|-------------------|---------------|
| 4284 | `wf` | `job.workflows.values()` | `WorkflowInfo` |
| ... | ... | ... | ... |

### Step 3.5b: Extract Attribute Accesses in Loop Bodies

For each iteration, identify attributes accessed on the loop variable:

```bash
# For variable 'wf' accessed in loop
grep -A20 "for wf in" server.py | grep "wf\.[a-z_]*"
```

Build attribute access table:

| Line | Object | Attribute Accessed | 
|------|--------|-------------------|
| 4285 | `wf` | `completed_count` |
| 4286 | `wf` | `failed_count` |

### Step 3.5c: Validate Against Class Definition

For each attribute access, verify the attribute exists on the expected type:

1. Find the class definition:
   ```bash
   grep -rn "class WorkflowInfo" --include="*.py"
   ```

2. Extract class attributes:
   ```bash
   # Check dataclass fields
   grep -A30 "class WorkflowInfo" <file>.py | grep -E "^\s+\w+:\s"
   
   # Check @property methods
   grep -A30 "class WorkflowInfo" <file>.py | grep "@property" -A1
   ```

3. Build validation matrix:

| Object Type | Attribute | Exists? | Actual Location (if different) |
|-------------|-----------|---------|-------------------------------|
| `WorkflowInfo` | `completed_count` | **NO** | `SubWorkflowInfo.progress.completed_count` |
| `WorkflowInfo` | `failed_count` | **NO** | `SubWorkflowInfo.progress.failed_count` |

### Step 3.5d: Fix Invalid Accesses (NO SHORTCUTS)

**CRITICAL: Every fix must address the root cause. No proxies, no workarounds.**

For each invalid attribute access:

1. **Trace the correct path**: Find where the attribute actually lives
2. **Understand the data model**: Why is it there and not here?
3. **Fix the access pattern**: Update code to navigate to correct location
4. **If attribute doesn't exist anywhere**: Add it to the correct model, don't fake it

**FORBIDDEN fixes (these are shortcuts):**
```python
# FORBIDDEN: Using a "proxy" field
# job.completed_at doesn't exist, so use timestamp as proxy
time_since_completion = current_time - job.timestamp  # WRONG - this is a shortcut!

# FORBIDDEN: Adding comments to explain workarounds
# Use timestamp as proxy for completion time (updated when status changes)
if job.timestamp > 0:  # WRONG - commenting the shortcut doesn't make it right

# FORBIDDEN: Suppressing type errors
job.completed_at  # type: ignore  # WRONG
```

**REQUIRED fixes (these address root cause):**
```python
# CORRECT: Add the attribute if it belongs on the model
# In models/jobs.py, add: completed_at: float = 0.0
# Then set it when job completes

# CORRECT: Navigate to where data actually lives
# If completion time is tracked in timeout_tracking:
if job.timeout_tracking and job.timeout_tracking.completed_at:
    time_since_completion = current_time - job.timeout_tracking.completed_at

# CORRECT: Compute from authoritative source
# If completion is tracked per-workflow, aggregate properly:
latest_completion = max(
    (wf.completed_at for wf in job.workflows.values() if wf.completed_at),
    default=0.0
)
```

Common patterns:

| Bug Pattern | Fix Pattern |
|-------------|-------------|
| Accessing child attribute on parent | Navigate through relationship |
| Accessing aggregated value that doesn't exist | Compute aggregation from children |
| Accessing attribute from wrong type in union | Add type guard |
| Attribute doesn't exist on any model | **Add it to the correct model** |

**Example fix** (WorkflowInfo.completed_count bug):

```python
# BEFORE (broken):
for wf in job.workflows.values():
    total += wf.completed_count  # WorkflowInfo has no completed_count

# AFTER (fixed - combined conditions, walrus operator for clarity):
for workflow_info in job.workflows.values():
    for sub_wf_token in workflow_info.sub_workflow_tokens:
        sub_wf_info = job.sub_workflows.get(sub_wf_token)
        if sub_wf_info and (progress := sub_wf_info.progress):
            total += progress.completed_count
```

### Step 3.5e: LSP-Assisted Validation

Use LSP hover to verify types in complex expressions:

```bash
# Hover over variable to confirm type
lsp_hover(file="server.py", line=4284, character=12)  # 'wf' variable
```

LSP will show the inferred type. If accessing `.completed_count` on `WorkflowInfo`, LSP would show an error - use this to catch issues early.

### Step 3.5f: Systematic Scan Pattern

For comprehensive coverage, check all domain model types used in server:

1. List all domain models imported:
   ```bash
   grep "from.*models.*import" server.py
   ```

2. For each model, search for attribute accesses:
   ```bash
   grep -n "\.\(completed_count\|failed_count\|status\|..." server.py
   ```

3. Cross-reference with class definitions

### Step 3.5g: Automated Attribute Access Scanner (Comprehensive)

Phase 3.5a-f describes manual detection. This phase provides a **fully automated scanner** that detects ALL invalid attribute accesses in a single run.

**The Problem Scope:**

Invalid attribute accesses occur in many patterns:

```python
# Pattern 1: Direct access on method return
job = self._job_manager.get_job(job_id)
if job.is_complete:  # JobInfo has no is_complete!

# Pattern 2: Iteration variable access
for wf in job.workflows.values():
    total += wf.completed_count  # WorkflowInfo has no completed_count

# Pattern 3: .load() pattern return
query_response = WorkflowQueryResponse.load(response)
ids = query_response.workflow_ids  # No such attribute!

# Pattern 4: Conditional/walrus patterns
if (job := get_job(id)) and job.completed_at:  # No completed_at!

# Pattern 5: Chained access
elapsed = job.timeout_tracking.elapsed  # timeout_tracking has no elapsed!
```

**Automated Scanner Script:**

```python
#!/usr/bin/env python3
"""
Comprehensive attribute access scanner.

Builds attribute database from dataclass definitions, tracks variable types
through code, and validates ALL attribute accesses against known types.

Usage: python scan_attributes.py <server_file> <models_dir>
"""

import ast
import re
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Set, List, Tuple, Optional


@dataclass
class ClassInfo:
    """Information about a class and its attributes."""
    name: str
    attributes: Set[str]  # Field names
    properties: Set[str]  # @property method names
    methods: Set[str]     # Regular method names
    file_path: str
    line_number: int


class AttributeScanner:
    """Scans for invalid attribute accesses."""
    
    def __init__(self):
        self.classes: Dict[str, ClassInfo] = {}
        self.violations: List[Tuple[int, str, str, str, str]] = []  # (line, var, attr, type, file)
        
        # Type inference mappings
        self.load_patterns: Dict[str, str] = {}  # ClassName.load -> ClassName
        self.iter_patterns: Dict[str, str] = {}  # collection type -> element type
        
    def scan_models_directory(self, models_dir: Path) -> None:
        """Extract all dataclass definitions from models directory."""
        for py_file in models_dir.rglob("*.py"):
            self._extract_classes_from_file(py_file)
    
    def _extract_classes_from_file(self, file_path: Path) -> None:
        """Extract class definitions from a single file."""
        try:
            with open(file_path) as f:
                tree = ast.parse(f.read())
        except SyntaxError:
            return
            
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_info = self._extract_class_info(node, str(file_path))
                if class_info:
                    self.classes[class_info.name] = class_info
    
    def _extract_class_info(self, node: ast.ClassDef, file_path: str) -> Optional[ClassInfo]:
        """Extract attributes, properties, and methods from a class."""
        attributes = set()
        properties = set()
        methods = set()
        
        # Check if it's a dataclass
        is_dataclass = any(
            (isinstance(d, ast.Name) and d.id == 'dataclass') or
            (isinstance(d, ast.Call) and isinstance(d.func, ast.Name) and d.func.id == 'dataclass')
            for d in node.decorator_list
        )
        
        for item in node.body:
            # Dataclass fields (annotated assignments)
            if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                attributes.add(item.target.id)
            
            # Regular assignments in __init__ or class body
            elif isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        attributes.add(target.id)
            
            # Methods
            elif isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                # Check for @property decorator
                is_property = any(
                    (isinstance(d, ast.Name) and d.id == 'property')
                    for d in item.decorator_list
                )
                if is_property:
                    properties.add(item.name)
                elif not item.name.startswith('_') or item.name == '__init__':
                    methods.add(item.name)
                    
                # Also scan __init__ for self.X assignments
                if item.name == '__init__':
                    for stmt in ast.walk(item):
                        if isinstance(stmt, ast.Assign):
                            for target in stmt.targets:
                                if (isinstance(target, ast.Attribute) and 
                                    isinstance(target.value, ast.Name) and
                                    target.value.id == 'self'):
                                    attributes.add(target.attr)
        
        return ClassInfo(
            name=node.name,
            attributes=attributes,
            properties=properties,
            methods=methods,
            file_path=file_path,
            line_number=node.lineno
        )
    
    def build_type_mappings(self) -> None:
        """Build mappings for type inference."""
        # .load() pattern: ClassName.load(data) returns ClassName
        for class_name in self.classes:
            self.load_patterns[class_name] = class_name
        
        # Common collection patterns
        # job.workflows: dict[str, WorkflowInfo] -> WorkflowInfo
        # job.sub_workflows: dict[str, SubWorkflowInfo] -> SubWorkflowInfo
        self.iter_patterns = {
            'workflows': 'WorkflowInfo',
            'sub_workflows': 'SubWorkflowInfo',
            'workers': 'WorkerRegistration',
            'jobs': 'JobInfo',
            'datacenters': 'DatacenterInfo',
        }
    
    def scan_server_file(self, server_path: Path) -> None:
        """Scan server file for attribute access violations."""
        with open(server_path) as f:
            content = f.read()
            lines = content.split('\n')
        
        # Track variable types in scope
        var_types: Dict[str, str] = {}
        
        for line_num, line in enumerate(lines, 1):
            # Update variable type tracking
            self._update_var_types(line, var_types)
            
            # Find all attribute accesses
            self._check_attribute_accesses(line_num, line, var_types, str(server_path))
    
    def _update_var_types(self, line: str, var_types: Dict[str, str]) -> None:
        """Update variable type tracking based on patterns in line."""
        
        # Pattern 1: ClassName.load(data) assignments
        # e.g., query_response = WorkflowQueryResponse.load(response)
        load_match = re.search(r'(\w+)\s*=\s*(\w+)\.load\s*\(', line)
        if load_match:
            var_name, class_name = load_match.groups()
            if class_name in self.classes:
                var_types[var_name] = class_name
        
        # Pattern 2: Iteration patterns
        # e.g., for job in self._job_manager.iter_jobs():
        iter_match = re.search(r'for\s+(\w+)\s+in\s+.*\.iter_(\w+)\s*\(', line)
        if iter_match:
            var_name, collection = iter_match.groups()
            # iter_jobs -> JobInfo, iter_workers -> WorkerRegistration
            type_name = collection.rstrip('s').title() + 'Info'
            if type_name in self.classes:
                var_types[var_name] = type_name
            # Special cases
            elif collection == 'jobs':
                var_types[var_name] = 'JobInfo'
            elif collection == 'workers':
                var_types[var_name] = 'WorkerRegistration'
        
        # Pattern 3: .values() iteration on known collections
        # e.g., for wf in job.workflows.values():
        values_match = re.search(r'for\s+(\w+)(?:,\s*\w+)?\s+in\s+(?:\w+\.)?(\w+)\.(?:values|items)\s*\(', line)
        if values_match:
            var_name, collection = values_match.groups()
            if collection in self.iter_patterns:
                var_types[var_name] = self.iter_patterns[collection]
        
        # Pattern 4: Direct collection iteration
        # e.g., for sub_wf_token, sub_wf in job.sub_workflows.items():
        items_match = re.search(r'for\s+\w+,\s*(\w+)\s+in\s+(?:\w+\.)?(\w+)\.items\s*\(', line)
        if items_match:
            var_name, collection = items_match.groups()
            if collection in self.iter_patterns:
                var_types[var_name] = self.iter_patterns[collection]
        
        # Pattern 5: get() on known collections
        # e.g., sub_wf_info = job.sub_workflows.get(token)
        get_match = re.search(r'(\w+)\s*=\s*(?:\w+\.)?(\w+)\.get\s*\(', line)
        if get_match:
            var_name, collection = get_match.groups()
            if collection in self.iter_patterns:
                var_types[var_name] = self.iter_patterns[collection]
        
        # Pattern 6: Type hints in function signatures (partial)
        # e.g., def process(self, job: JobInfo) -> None:
        hint_match = re.search(r'(\w+)\s*:\s*(\w+)(?:\s*\||\s*=|\s*\))', line)
        if hint_match:
            var_name, type_name = hint_match.groups()
            if type_name in self.classes:
                var_types[var_name] = type_name
    
    def _check_attribute_accesses(
        self, 
        line_num: int, 
        line: str, 
        var_types: Dict[str, str],
        file_path: str
    ) -> None:
        """Check all attribute accesses in line against known types."""
        
        # Find all var.attr patterns
        for match in re.finditer(r'\b(\w+)\.(\w+)\b', line):
            var_name, attr_name = match.groups()
            
            # Skip self.X, cls.X, common modules
            if var_name in ('self', 'cls', 'os', 'sys', 'time', 'asyncio', 're', 'json'):
                continue
            
            # Skip if calling a method (followed by parenthesis)
            pos = match.end()
            rest_of_line = line[pos:].lstrip()
            if rest_of_line.startswith('('):
                continue
            
            # Check if we know this variable's type
            if var_name in var_types:
                type_name = var_types[var_name]
                if type_name in self.classes:
                    class_info = self.classes[type_name]
                    all_attrs = class_info.attributes | class_info.properties
                    
                    if attr_name not in all_attrs and attr_name not in class_info.methods:
                        self.violations.append((
                            line_num,
                            var_name,
                            attr_name,
                            type_name,
                            file_path
                        ))
    
    def report(self) -> None:
        """Print violation report."""
        if not self.violations:
            print("✓ No attribute access violations found")
            return
        
        print(f"✗ Found {len(self.violations)} attribute access violation(s):\n")
        print("| Line | Variable | Attribute | Type | File |")
        print("|------|----------|-----------|------|------|")
        
        for line_num, var_name, attr_name, type_name, file_path in sorted(self.violations):
            short_path = Path(file_path).name
            print(f"| {line_num} | `{var_name}` | `.{attr_name}` | `{type_name}` | {short_path} |")
        
        print("\n### Available Attributes for Referenced Types:\n")
        reported_types = set(v[3] for v in self.violations)
        for type_name in sorted(reported_types):
            if type_name in self.classes:
                info = self.classes[type_name]
                attrs = sorted(info.attributes | info.properties)
                print(f"**{type_name}**: {', '.join(f'`{a}`' for a in attrs)}")


def main():
    if len(sys.argv) < 3:
        print("Usage: python scan_attributes.py <server_file> <models_dir>")
        sys.exit(1)
    
    server_path = Path(sys.argv[1])
    models_dir = Path(sys.argv[2])
    
    scanner = AttributeScanner()
    scanner.scan_models_directory(models_dir)
    scanner.build_type_mappings()
    scanner.scan_server_file(server_path)
    scanner.report()


if __name__ == '__main__':
    main()
```

**Usage:**

```bash
# Scan manager server against all models
python scan_attributes.py \
    hyperscale/distributed/nodes/manager/server.py \
    hyperscale/distributed/models/

# Scan gate server
python scan_attributes.py \
    hyperscale/distributed/nodes/gate/server.py \
    hyperscale/distributed/models/
```

**Example Output:**

```
✗ Found 5 attribute access violation(s):

| Line | Variable | Attribute | Type | File |
|------|----------|-----------|------|------|
| 1390 | `query_response` | `.workflow_ids` | `WorkflowQueryResponse` | server.py |
| 1625 | `job` | `.completed_at` | `JobInfo` | server.py |
| 2560 | `registration` | `.manager_info` | `ManagerPeerRegistration` | server.py |
| 2697 | `job` | `.is_complete` | `JobInfo` | server.py |
| 3744 | `submission` | `.gate_addr` | `JobSubmission` | server.py |

### Available Attributes for Referenced Types:

**JobInfo**: `callback_addr`, `context`, `datacenter`, `fencing_token`, `job_id`, `layer_version`, `leader_addr`, `leader_node_id`, `lock`, `started_at`, `status`, `sub_workflows`, `submission`, `timeout_tracking`, `timestamp`, `token`, `workflows`, `workflows_completed`, `workflows_failed`, `workflows_total`

**WorkflowQueryResponse**: `datacenter`, `manager_id`, `request_id`, `workflows`
```

### Step 3.5h: Extending the Scanner

**Adding New Type Inference Patterns:**

When the scanner misses a type, extend `_update_var_types()`:

```python
# Add pattern for your specific case
# e.g., self._job_manager.get_job(job_id) returns JobInfo
component_return_types = {
    ('_job_manager', 'get_job'): 'JobInfo',
    ('_job_manager', 'iter_jobs'): 'JobInfo',  # iterator element
    ('_worker_pool', 'get_worker'): 'WorkerRegistration',
}

getter_match = re.search(r'(\w+)\s*=\s*self\.(_\w+)\.(\w+)\s*\(', line)
if getter_match:
    var_name, component, method = getter_match.groups()
    key = (component, method)
    if key in component_return_types:
        var_types[var_name] = component_return_types[key]
```

**Handling Walrus Operators:**

```python
# Pattern: if (job := get_job(id)) and job.attr:
walrus_match = re.search(r'\((\w+)\s*:=\s*(\w+)\.load\s*\(', line)
if walrus_match:
    var_name, class_name = walrus_match.groups()
    if class_name in self.classes:
        var_types[var_name] = class_name
```

### Step 3.5h.1: Chained Attribute Access Validation (CRITICAL)

**The Problem:**

The base scanner validates single-level accesses (`var.attr`) but misses chained accesses (`var.attr1.attr2`):

```python
# CAUGHT by base scanner:
registration = ManagerPeerRegistration.load(data)
registration.manager_info  # ManagerPeerRegistration has no manager_info!

# MISSED by base scanner (chained access):
peer_udp_addr = (
    registration.manager_info.udp_host,  # MISSED - both levels invalid!
    registration.manager_info.udp_port,
)
```

Even when the first-level access is caught, the scanner doesn't validate the second level. This is problematic because:
1. The intended attribute might exist with a different name (e.g., `node` instead of `manager_info`)
2. Even if `manager_info` existed, we need to validate that `udp_host` exists on its type

**Solution: Type-Aware Attribute Resolution**

Extend the scanner to:
1. Track the **type** of each attribute, not just existence
2. Resolve chained accesses by following the type chain
3. Validate each level of the chain

**Extended ClassInfo with Attribute Types:**

```python
@dataclass
class ClassInfo:
    name: str
    attributes: Set[str]
    properties: Set[str]
    methods: Set[str]
    # NEW: Map attribute name -> type name
    attribute_types: Dict[str, str] = field(default_factory=dict)
    file_path: str = ""
    line_number: int = 0
```

**Extracting Attribute Types from Type Hints:**

```python
def _extract_class_info(self, node: ast.ClassDef, file_path: str) -> ClassInfo:
    attributes = set()
    attribute_types = {}
    
    for item in node.body:
        if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
            attr_name = item.target.id
            attributes.add(attr_name)
            
            # Extract type from annotation
            type_name = self._extract_type_name(item.annotation)
            if type_name:
                attribute_types[attr_name] = type_name
    
    return ClassInfo(
        name=node.name,
        attributes=attributes,
        attribute_types=attribute_types,
        # ... other fields
    )

def _extract_type_name(self, annotation: ast.expr) -> str | None:
    """Extract simple type name from annotation AST."""
    if isinstance(annotation, ast.Name):
        return annotation.id
    elif isinstance(annotation, ast.Subscript):
        # Handle Optional[X], list[X], etc.
        if isinstance(annotation.value, ast.Name):
            if annotation.value.id in ('Optional', 'list', 'List'):
                return self._extract_type_name(annotation.slice)
    elif isinstance(annotation, ast.BinOp):
        # Handle X | None union types
        if isinstance(annotation.op, ast.BitOr):
            left_type = self._extract_type_name(annotation.left)
            if left_type and left_type != 'None':
                return left_type
            return self._extract_type_name(annotation.right)
    elif isinstance(annotation, ast.Constant):
        # Handle string annotations like "ManagerInfo"
        if isinstance(annotation.value, str):
            return annotation.value
    return None
```

**Chained Access Validation:**

```python
def _check_chained_accesses(
    self,
    line_num: int,
    line: str,
    var_types: Dict[str, str],
    file_path: str
) -> None:
    """Validate chained attribute accesses like var.attr1.attr2."""
    
    # Match chains of 2+ attributes: var.attr1.attr2[.attr3...]
    for match in re.finditer(r'\b(\w+)((?:\.\w+)+)', line):
        var_name = match.group(1)
        chain = match.group(2)  # ".attr1.attr2.attr3"
        
        if var_name in ('self', 'cls', 'os', 'sys', 'time', 'asyncio'):
            continue
        
        if var_name not in var_types:
            continue
        
        # Parse chain into list of attributes
        attrs = [a for a in chain.split('.') if a]
        if len(attrs) < 2:
            continue  # Single-level handled by base scanner
        
        # Walk the chain, validating each level
        current_type = var_types[var_name]
        for i, attr in enumerate(attrs):
            if current_type not in self.classes:
                break  # Unknown type, can't validate further
            
            class_info = self.classes[current_type]
            all_attrs = class_info.attributes | class_info.properties
            
            if attr not in all_attrs:
                # Build chain string for error message
                accessed_chain = f"{var_name}." + ".".join(attrs[:i+1])
                self.violations.append((
                    line_num,
                    accessed_chain,
                    attr,
                    current_type,
                    file_path
                ))
                break  # Can't continue chain after invalid access
            
            # Get type of this attribute for next iteration
            if attr in class_info.attribute_types:
                current_type = class_info.attribute_types[attr]
            else:
                break  # Unknown type, can't validate further
```

**Example Detection:**

```
# Input code:
registration = ManagerPeerRegistration.load(data)
peer_udp_addr = (
    registration.manager_info.udp_host,
    registration.manager_info.udp_port,
)

# Scanner output:
✗ Found 2 chained attribute access violation(s):

| Line | Access Chain | Invalid Attr | On Type | File |
|------|--------------|--------------|---------|------|
| 2564 | `registration.manager_info` | `manager_info` | `ManagerPeerRegistration` | server.py |
| 2565 | `registration.manager_info` | `manager_info` | `ManagerPeerRegistration` | server.py |

### Available Attributes for ManagerPeerRegistration:
`capabilities`, `is_leader`, `node`, `protocol_version_major`, `protocol_version_minor`, `term`

### Note: Did you mean `node` instead of `manager_info`?
`node` is type `ManagerInfo` which has: `datacenter`, `is_leader`, `node_id`, `tcp_host`, `tcp_port`, `udp_host`, `udp_port`
```

**Integration with Base Scanner:**

```python
def scan_server_file(self, server_path: Path) -> None:
    with open(server_path) as f:
        lines = f.readlines()
    
    var_types: Dict[str, str] = {}
    
    for line_num, line in enumerate(lines, 1):
        self._update_var_types(line, var_types)
        
        # Base single-level validation
        self._check_attribute_accesses(line_num, line, var_types, str(server_path))
        
        # NEW: Chained access validation
        self._check_chained_accesses(line_num, line, var_types, str(server_path))
```

**Attribute Type Database Example:**

```python
# After scanning models, attribute_types contains:
{
    'ManagerPeerRegistration': {
        'node': 'ManagerInfo',
        'term': 'int',
        'is_leader': 'bool',
    },
    'ManagerInfo': {
        'node_id': 'str',
        'tcp_host': 'str',
        'tcp_port': 'int',
        'udp_host': 'str',
        'udp_port': 'int',
        'datacenter': 'str',
        'is_leader': 'bool',
    },
    'JobInfo': {
        'token': 'TrackingToken',
        'submission': 'JobSubmission',
        'timeout_tracking': 'TimeoutTrackingState',
        'workflows': 'dict',  # Can't resolve generic params
        # ...
    }
}
```

**Limitations:**

1. Generic types (`dict[str, WorkflowInfo]`) don't carry element type info in AST
2. Conditional types (`X | None`) are reduced to non-None type
3. Forward references (string annotations) require careful handling
4. Runtime-computed attributes not detectable

For these cases, fall back to LSP validation.

### Step 3.5h.2: Chained Method Access Validation (MANDATORY - CRITICAL)

**STATUS: MANDATORY** - This step MUST be executed. Method call validation is equally important as attribute validation.

**The Problem:**

The attribute scanner validates attribute accesses (`var.attr`) but misses **method calls** on objects (`self._state.get_method()`):

```python
# CAUGHT by attribute scanner:
registration.manager_info  # ManagerPeerRegistration has no manager_info!

# MISSED by attribute scanner (method call):
known_peers = self._manager_state.get_known_manager_peers_list()
# ManagerState has NO method get_known_manager_peers_list()!
# Correct method: get_known_manager_peer_values()
```

Method access bugs are equally dangerous as attribute bugs - they cause `AttributeError` at runtime.

**Solution: Method Existence Validation**

Extend the scanner to:
1. Track method signatures for all classes (not just attributes)
2. Detect chained method calls on typed objects
3. Validate method names exist on the target type

**Extended ClassInfo (already present):**

```python
@dataclass
class ClassInfo:
    name: str
    attributes: Set[str]
    properties: Set[str]
    methods: Set[str]  # <-- Already tracked, now validate against
    attribute_types: Dict[str, str]
    file_path: str = ""
    line_number: int = 0
```

**Method Call Pattern Detection:**

```python
def _check_method_calls(
    self,
    line_num: int,
    line: str,
    instance_types: Dict[str, str],  # Maps self._x -> Type
    file_path: str
) -> None:
    """Validate method calls like self._manager_state.get_method()."""
    
    # Pattern: self._instance.method_name(
    for match in re.finditer(r'self\.(_\w+)\.(\w+)\s*\(', line):
        instance_name, method_name = match.groups()
        
        # Skip if instance type unknown
        if instance_name not in instance_types:
            continue
        
        instance_type = instance_types[instance_name]
        if instance_type not in self.classes:
            continue
        
        class_info = self.classes[instance_type]
        all_callables = class_info.methods | class_info.properties
        
        # Properties can be called if they return callables, but usually not
        # Focus on methods
        if method_name not in class_info.methods:
            self.violations.append((
                line_num,
                f"self.{instance_name}.{method_name}()",
                method_name,
                instance_type,
                file_path,
                "method"  # New: violation type
            ))
```

**Instance Type Mapping (Manual Configuration):**

Since `self._manager_state` type isn't always inferrable from code, maintain explicit mappings:

```python
# Instance type mappings for server classes
INSTANCE_TYPE_MAPPINGS = {
    # Manager server
    '_manager_state': 'ManagerState',
    '_job_manager': 'JobManager',
    '_worker_pool': 'WorkerPool',
    '_windowed_stats': 'WindowedStatsCollector',
    '_rate_limiter': 'ServerRateLimiter',
    
    # Gate server  
    '_gate_state': 'GateState',
    '_job_manager': 'JobManager',
    '_dc_health_monitor': 'FederatedHealthMonitor',
    '_modular_state': 'ModularGateState',
}
```

**Extracting Methods from Non-Dataclass Classes:**

```python
def _extract_class_info(self, node: ast.ClassDef, file_path: str) -> ClassInfo:
    methods = set()
    
    for item in node.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # Include all public methods and common patterns
            if not item.name.startswith('_') or item.name.startswith('__'):
                methods.add(item.name)
            # Also include "get_", "set_", "is_", "has_" private methods
            # as these are common accessor patterns
            elif any(item.name.startswith(f'_{p}') for p in ['get_', 'set_', 'is_', 'has_', 'iter_']):
                # Store without leading underscore for matching
                # Actually store with underscore since that's how it's called
                pass
            # Store ALL methods for validation
            methods.add(item.name)
    
    return ClassInfo(name=node.name, methods=methods, ...)
```

**Example Detection:**

```
# Input code:
known_peers = self._manager_state.get_known_manager_peers_list()

# Scanner output:
✗ Found 1 method access violation(s):

| Line | Call | Invalid Method | On Type | File |
|------|------|----------------|---------|------|
| 2585 | `self._manager_state.get_known_manager_peers_list()` | `get_known_manager_peers_list` | `ManagerState` | server.py |

### Available Methods on ManagerState:
`get_known_manager_peer`, `get_known_manager_peer_values`, `get_worker`, `get_workers`, 
`set_worker`, `remove_worker`, `get_job_leader`, `set_job_leader`, ...

### Did you mean: `get_known_manager_peer_values()`?
```

**Fuzzy Matching for Suggestions:**

```python
def _suggest_similar_method(self, invalid_method: str, class_info: ClassInfo) -> str | None:
    """Suggest similar method name using edit distance."""
    from difflib import get_close_matches
    
    candidates = list(class_info.methods)
    matches = get_close_matches(invalid_method, candidates, n=1, cutoff=0.6)
    return matches[0] if matches else None
```

**Integration with Main Scanner:**

```python
def scan_server_file(self, server_path: Path) -> None:
    with open(server_path) as f:
        lines = f.readlines()
    
    var_types: Dict[str, str] = {}
    
    for line_num, line in enumerate(lines, 1):
        self._update_var_types(line, var_types)
        
        # Attribute validation
        self._check_attribute_accesses(line_num, line, var_types, str(server_path))
        self._check_chained_accesses(line_num, line, var_types, str(server_path))
        
        # NEW: Method call validation
        self._check_method_calls(line_num, line, INSTANCE_TYPE_MAPPINGS, str(server_path))
```

**NO SHORTCUTS Principle Applies:**

When a method doesn't exist:
- **DO NOT** add a proxy method that wraps direct state access
- **DO NOT** change the call to use a "close enough" method with different semantics
- **DO** find the correct method that provides the needed data
- **DO** add the method to the class if it genuinely doesn't exist and is needed

### Step 3.5h.3: Semantic Intent Investigation (MANDATORY)

**CRITICAL: Never blindly swap method names. Always investigate WHY the original code exists.**

When you find an invalid method call like `get_overload_state()` and a similar method like `get_current_state()` exists, you MUST investigate:

1. **What was the original intent?**
   - Read the surrounding code context (5-10 lines before/after)
   - Understand what the caller is trying to accomplish
   - Check if there are comments explaining the purpose

2. **What does the "similar" method actually do?**
   - Read its docstring and implementation
   - Check its return type - does it match what the caller expects?
   - Check its parameters - does the caller provide them correctly?

3. **Are the semantics compatible?**
   - Does the replacement method provide the SAME information?
   - Does it have the same side effects (or lack thereof)?
   - Will the caller's logic still be correct with the replacement?

**Investigation Checklist:**

```
□ Read the invalid method call in full context (what is it used for?)
□ Read the candidate replacement method's implementation
□ Compare return types (exact match? compatible? incompatible?)
□ Compare parameters (same? different defaults? missing required?)
□ Verify the caller's logic will still work correctly
□ Check if the method should be added instead of substituted
```

**Example: Investigating `get_overload_state()` vs `get_current_state()`**

```python
# WRONG approach - blind substitution:
# "get_overload_state doesn't exist, get_current_state is similar, swap them"
overload_state = self._load_shedder.get_current_state()  # Maybe wrong!

# CORRECT approach - investigate first:

# Step 1: What does the caller want?
# Context: if self._load_shedder.should_shed("JobSubmission"):
#              overload_state = self._load_shedder.get_overload_state()
#              return JobAck(error=f"System under load ({overload_state})")
# Intent: Get current overload state for error message

# Step 2: What does get_current_state() do?
# def get_current_state(self, cpu_percent=None, memory_percent=None) -> OverloadState:
#     """Get the current overload state."""
#     cpu = cpu_percent if cpu_percent is not None else 0.0
#     ...
#     return self._detector.get_state(cpu, memory)

# Step 3: Are semantics compatible?
# - Returns OverloadState enum (healthy/busy/stressed/overloaded)
# - With no args, uses defaults (0.0, 0.0) - may not reflect actual state!
# - Caller uses it in string context - OverloadState has __str__

# Step 4: Decision
# Option A: Call get_current_state() with actual CPU/memory if available
# Option B: Call get_current_state() with no args if detector tracks internally
# Option C: Add get_overload_state() wrapper that gets state without needing args

# Must investigate: Does _detector.get_state(0, 0) return the CURRENT state,
# or does it return the state FOR those metrics? Check HybridOverloadDetector.
```

**When to Add the Method vs Substitute:**

| Scenario | Action |
|----------|--------|
| Similar method exists with IDENTICAL semantics | Substitute (likely typo) |
| Similar method exists but needs different parameters | Investigate if caller has those params |
| Similar method returns different type | DO NOT substitute - add correct method |
| No similar method, but data exists elsewhere | Add new method that provides it correctly |
| Method represents genuinely missing functionality | Add the method to the class |

**Red Flags That Indicate WRONG Substitution:**

- Method signature differs significantly (different parameter count/types)
- Return type is different (even subtly - `list` vs `dict`, `str` vs `enum`)
- Method has side effects the original likely didn't intend
- Method name implies different semantics (`get_all_X` vs `get_active_X`)
- Caller would need modification to use the replacement correctly

**Document Your Investigation:**

When fixing, include a brief comment explaining:
```python
# Investigation: get_overload_state() -> get_current_state()
# - get_current_state() returns OverloadState enum (same intent)
# - With no args, detector uses internally-tracked CPU/memory
# - Verified HybridOverloadDetector.get_state() uses last recorded metrics
# - Semantics match - this was a typo/rename that wasn't propagated
overload_state = self._load_shedder.get_current_state()
```

**Common Fixes (After Investigation):**

| Invalid Call | Correct Call | Reason (Investigated) |
|--------------|--------------|--------|
| `get_known_manager_peers_list()` | `get_known_manager_peer_values()` | Typo - both return `list[ManagerInfo]` |
| `get_job_status()` | `get_job().status` | Method doesn't exist, attribute access equivalent |
| `iter_active_workers()` | `get_workers().values()` | Same data, different naming convention |
| `get_overload_state()` | `get_current_state()` | Same return type, default args use tracked metrics |

### Step 3.5h.4: Enum Member Validation (MANDATORY - CRITICAL)

**STATUS: MANDATORY** - This step MUST be executed. Enum member access bugs cause `AttributeError` at runtime.

**The Problem:**

Import aliases hide the actual enum being used, making invalid member access hard to detect:

```python
# In imports:
from hyperscale.distributed.models import ManagerState as ManagerStateEnum

# In code - LOOKS valid but ISN'T:
self._manager_state.set_manager_state_enum(ManagerStateEnum.OFFLINE)
# ManagerState has: ACTIVE, DRAINING, SYNCING
# OFFLINE does NOT exist! This is WorkerState.OFFLINE
```

**Why This Is Missed:**
- Method existence check passes (`set_manager_state_enum` exists)
- Attribute scanner doesn't check enum members
- Import alias hides the actual enum name

**Solution: Enum Member Validation with Alias Resolution**

```python
import ast
import re
from pathlib import Path

def extract_enums(file_path: str) -> dict[str, set[str]]:
    """Extract all enum classes and their members."""
    with open(file_path) as f:
        tree = ast.parse(f.read())
    
    enums = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            is_enum = any(
                (isinstance(base, ast.Name) and base.id == 'Enum') or
                (isinstance(base, ast.Attribute) and base.attr == 'Enum')
                for base in node.bases
            )
            if is_enum:
                members = {
                    target.id
                    for item in node.body
                    if isinstance(item, ast.Assign)
                    for target in item.targets
                    if isinstance(target, ast.Name)
                }
                enums[node.name] = members
    return enums

def extract_import_aliases(file_path: str) -> dict[str, str]:
    """Extract import aliases (alias -> original name)."""
    with open(file_path) as f:
        tree = ast.parse(f.read())
    
    aliases = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                aliases[alias.asname or alias.name] = alias.name
    return aliases

def scan_enum_access(server_path: str, enums: dict[str, set[str]]):
    """Scan for invalid enum member accesses with alias support."""
    aliases = extract_import_aliases(server_path)
    
    # Map used names to original enum names
    alias_to_enum = {
        alias: original
        for alias, original in aliases.items()
        if original in enums
    }
    # Include direct names
    for enum_name in enums:
        alias_to_enum.setdefault(enum_name, enum_name)
    
    violations = []
    with open(server_path) as f:
        lines = f.readlines()
    
    for line_num, line in enumerate(lines, 1):
        for used_name, original_name in alias_to_enum.items():
            pattern = re.compile(rf'\b{re.escape(used_name)}\.([A-Z_][A-Z0-9_]*)\b')
            for match in pattern.finditer(line):
                member = match.group(1)
                if member not in enums[original_name]:
                    violations.append((line_num, used_name, original_name, member, enums[original_name]))
    
    return violations
```

**Usage:**

```bash
python3 << 'EOF'
# Collect all enums from models
all_enums = {}
for py_file in Path("hyperscale/distributed/models").glob("*.py"):
    all_enums.update(extract_enums(str(py_file)))

# Scan server
violations = scan_enum_access("hyperscale/distributed/nodes/manager/server.py", all_enums)
for line, used, original, member, valid in violations:
    print(f"Line {line}: {used}.{member} - does not exist on {original}!")
    print(f"  Valid members: {', '.join(sorted(valid))}")
EOF
```

**Example Output:**

```
Line 711: ManagerStateEnum.OFFLINE - does not exist on ManagerState!
  Valid members: ACTIVE, DRAINING, SYNCING
```

**Fix Patterns:**

| Invalid Access | Root Cause | Fix |
|----------------|------------|-----|
| `ManagerStateEnum.OFFLINE` | Wrong enum | Use `DRAINING` or add `OFFLINE` to `ManagerState` |
| `JobStatus.COMPLETE` | Typo | Use `JobStatus.COMPLETED` |
| `WorkerState.STOPPED` | Member doesn't exist | Use `WorkerState.OFFLINE` or add `STOPPED` |

**Integration:**

Add to Phase 7 verification checklist:
- [ ] Re-run Phase 3.5h.4 scanner: **ZERO** enum member violations

### Step 3.5h.5: Callback/Reference Attribute Validation (MANDATORY - CRITICAL)

**STATUS: MANDATORY** - This step MUST be executed. Attribute references passed as callbacks cause `AttributeError` at runtime.

**The Problem:**

Standard method call scanners look for `self.method()` patterns (with parentheses). But attributes can also be **referenced without being called** - passed as callbacks, stored in variables, or used as function arguments:

```python
# Pattern 1: Callback passed as keyword argument (NO PARENTHESES)
await registration_handler.register(
    add_to_probe_scheduler=self.add_to_probe_scheduler,  # BUG: method doesn't exist!
    on_success=self.handle_success,  # BUG if handle_success doesn't exist
)

# Pattern 2: Callback assigned to variable
callback = self.on_workflow_complete  # BUG if method doesn't exist

# Pattern 3: Callback in list/dict
handlers = [self.on_start, self.on_stop, self.on_error]  # BUG if any don't exist

# Pattern 4: Passed to constructor
coordinator = Coordinator(
    send_tcp=self.send_tcp,  # OK - method exists on base class
    notify_peer=self.notify_peer,  # BUG if notify_peer doesn't exist
)
```

**Why Standard Scanners Miss This:**

1. No parentheses `()` → not detected as method call
2. Looks like attribute access → but attribute scanners check for data attributes, not methods
3. LSP may not catch it if the attribute is dynamically assigned elsewhere
4. Only fails at **runtime** when the callback is actually invoked

**Detection Script:**

```python
import ast
import re
from pathlib import Path

def find_self_attribute_references(file_path: str, class_methods: set[str]) -> list[tuple[int, str, str]]:
    """
    Find self.X references that are NOT method calls and verify X exists.
    
    Args:
        file_path: Path to the file to scan
        class_methods: Set of method names that exist on the class
    
    Returns: [(line, context, missing_attr)]
    """
    with open(file_path) as f:
        source = f.read()
        lines = source.split('\n')
    
    violations = []
    
    # Pattern: self.something NOT followed by ( 
    # But IS followed by , or ) or = or \n (indicates reference, not call)
    # Excludes: self._private (data attributes typically start with _)
    
    # Match self.method_name used as reference (not called)
    pattern = re.compile(
        r'self\.([a-z][a-z0-9_]*)'  # self.method_name (lowercase = method convention)
        r'(?!\s*\()'  # NOT followed by (
        r'(?=\s*[,)=\]\n])'  # followed by , ) = ] or newline
    )
    
    for i, line in enumerate(lines, 1):
        # Skip comments and strings (rough heuristic)
        stripped = line.split('#')[0]
        
        for match in pattern.finditer(stripped):
            attr_name = match.group(1)
            
            # Skip private attributes (data, not methods)
            if attr_name.startswith('_'):
                continue
            
            # Check if this looks like a callback pattern
            # (appears after = or in function call arguments)
            context = stripped[max(0, match.start()-20):match.end()+10]
            
            # Verify the method exists
            if attr_name not in class_methods:
                violations.append((i, stripped.strip()[:70], attr_name))
    
    return violations

def extract_class_methods(file_path: str, class_name: str) -> set[str]:
    """Extract all method names from a class."""
    with open(file_path) as f:
        tree = ast.parse(f.read())
    
    methods = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    methods.add(item.name)
            # Also check base classes (would need more complex analysis)
    
    return methods

# Usage
class_methods = extract_class_methods("server.py", "WorkerServer")
# Add inherited methods from base class
base_methods = extract_class_methods("../../swim/health_aware_server.py", "HealthAwareServer")
all_methods = class_methods | base_methods

violations = find_self_attribute_references("server.py", all_methods)
for line, context, attr in violations:
    print(f"Line {line}: Missing method `{attr}` referenced in: {context}")
```

**Quick Detection Command:**

```bash
# Find self.X patterns that look like callback references (not calls)
# and are NOT private attributes
grep -nE "self\.[a-z][a-z0-9_]*\s*[,)=\]]" server.py | grep -v "self\._" | grep -v "()"
```

**Example Violations:**

```
Line 1377: Missing method `add_to_probe_scheduler` referenced in: add_to_probe_scheduler=self.add_to_probe_scheduler,
Line 1397: Missing method `add_to_probe_scheduler` referenced in: add_to_probe_scheduler=self.add_to_probe_scheduler,
```

**Fix Patterns:**

| Issue | Root Cause | Fix |
|-------|------------|-----|
| Method doesn't exist on class | Missing implementation | Add the method to the class |
| Method exists on base class | Scanner didn't check inheritance | Verify base class has method (no fix needed) |
| Method was renamed/removed | Incomplete refactor | Update reference to correct method name |
| Method should be on component | Wrong owner | Use `self._component.method` instead |

**Cross-Reference with Base Classes:**

When scanning, must include methods from:
1. The class itself
2. All parent classes in MRO
3. Mixins

```python
# Get full method set including inheritance
import inspect

def get_all_methods(cls) -> set[str]:
    """Get all methods including inherited."""
    return {name for name, _ in inspect.getmembers(cls, predicate=inspect.isfunction)}
```

**Integration with Phase 3:**

Add to Phase 3 scanner:
1. After extracting method calls, ALSO extract method references
2. Method reference = `self.X` where X is lowercase and NOT followed by `(`
3. Verify all referenced methods exist on class or base classes

### Step 3.5i: Integration with CI/Build

**Pre-commit Hook:**

```bash
#!/bin/bash
# .git/hooks/pre-commit

python scan_attributes.py \
    hyperscale/distributed/nodes/manager/server.py \
    hyperscale/distributed/models/

if [ $? -ne 0 ]; then
    echo "ERROR: Attribute access violations detected"
    exit 1
fi
```

**Makefile Target:**

```makefile
scan-attributes:
	@python scan_attributes.py \
		hyperscale/distributed/nodes/manager/server.py \
		hyperscale/distributed/models/
	@python scan_attributes.py \
		hyperscale/distributed/nodes/gate/server.py \
		hyperscale/distributed/models/
```

### Step 3.5j: LSP Cross-Validation

After running the automated scanner, validate findings with LSP:

```bash
# For each violation, use LSP hover to confirm
lsp_hover(file="server.py", line=1625, character=<column_of_completed_at>)
# Expected: Error or "Unknown member" indication
```

**LSP provides ground truth** - if the scanner reports a violation but LSP shows no error, the scanner has a false positive (update type inference). If LSP shows an error the scanner missed, extend the scanner patterns.

### Output

- Automated scanner runs in < 5 seconds
- Zero false negatives (all violations caught)
- Minimal false positives (< 5% of reports)
- Clear remediation guidance (shows available attributes)
- Integrable into CI pipeline

---

### Step 3.5k: Type Hint Validation (MANDATORY - CRITICAL)

**STATUS: MANDATORY** - This step MUST be executed. Missing or incorrect type hints cause runtime surprises, make code harder to understand, and prevent static analysis tools from catching bugs.

**Scope: This phase applies to ALL modular classes** - server, state, coordinators, handlers, and any helper classes. Not just the main server file.

**The Problem:**

Functions, methods, AND class attributes without type hints create multiple issues:

```python
# PROBLEM 1: Missing parameter type hint
def process_job(self, job):  # What is 'job'? JobInfo? JobSubmission? dict?
    return job.status  # Will this work?

# PROBLEM 2: Missing return type hint
async def get_worker_state(self, worker_id: str):  # Returns what? WorkerState? dict? None?
    return self._workers.get(worker_id)

# PROBLEM 3: Incorrect type hint
def calculate_progress(self, count: int) -> float:  # Actually returns int!
    return count * 100 // total

# PROBLEM 4: Any/object escape hatches
def handle_message(self, msg: Any) -> Any:  # Type system defeated
    return process(msg)

# PROBLEM 5: Untyped class attributes (public AND private)
class WorkerState:
    def __init__(self):
        self._workers = {}  # What's in here? dict[str, ???]
        self._pending_jobs = []  # list of what?
        self.config = None  # None or what type?

# PROBLEM 6: Untyped instance attributes assigned in __init__
class JobManager:
    def __init__(self, config):
        self._config = config  # What type is config?
        self._cache = {}  # dict[?, ?]
        self._lock = None  # Should be asyncio.Lock | None
```

**Why This Matters:**

1. **Runtime errors**: Wrong type passed → `AttributeError` in production
2. **Maintenance burden**: Future developers can't understand data flow
3. **IDE support broken**: No autocomplete, no inline errors
4. **Static analysis defeated**: LSP and type checkers can't help
5. **Refactoring hazard**: Can't safely rename/change types
6. **Hidden state bugs**: Untyped class attributes hide what data the class manages

**Codebase Rule (from AGENTS.md):**
> "Type hints required, but we prefer to infer return types."
> "For test workflow classes, type hints and return type hints are REQUIRED."
> "If you can use generics, do so. Avoid using Any for typehints."

### Step 3.5k.1: Scan for Missing Parameter Type Hints

**Detection Script:**

```python
import ast
from pathlib import Path

def find_untyped_parameters(file_path: str) -> list[tuple[int, str, str, list[str]]]:
    """
    Find function/method parameters without type hints.
    
    Returns: [(line, func_name, kind, [untyped_params])]
    """
    with open(file_path) as f:
        tree = ast.parse(f.read())
    
    violations = []
    
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            untyped = []
            for arg in node.args.args:
                # Skip 'self' and 'cls'
                if arg.arg in ('self', 'cls'):
                    continue
                # Check if annotation exists
                if arg.annotation is None:
                    untyped.append(arg.arg)
            
            # Also check *args and **kwargs
            if node.args.vararg and node.args.vararg.annotation is None:
                untyped.append(f"*{node.args.vararg.arg}")
            if node.args.kwarg and node.args.kwarg.annotation is None:
                untyped.append(f"**{node.args.kwarg.arg}")
            
            if untyped:
                kind = "async def" if isinstance(node, ast.AsyncFunctionDef) else "def"
                violations.append((node.lineno, node.name, kind, untyped))
    
    return violations

# Usage
violations = find_untyped_parameters("server.py")
for line, name, kind, params in violations:
    print(f"Line {line}: {kind} {name}() - untyped: {', '.join(params)}")
```

**Quick Detection Command:**

```bash
# Find function definitions and check for untyped parameters
python3 -c "
import ast
with open('server.py') as f:
    tree = ast.parse(f.read())
for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        untyped = [a.arg for a in node.args.args 
                   if a.arg not in ('self', 'cls') and a.annotation is None]
        if untyped:
            print(f'{node.lineno}:{node.name}: {untyped}')
"
```

### Step 3.5k.1b: Scan for Untyped Class Attributes (ALL Classes)

**CRITICAL: This applies to ALL modular classes** - state classes, coordinators, handlers, server, and helpers. Both public AND private attributes (`_private` and `public`) require type hints.

**The Problem:**

```python
# WRONG: Untyped attributes in __init__
class WorkerState:
    def __init__(self):
        self._workers = {}  # What type? dict[str, WorkerInfo]? dict[str, Any]?
        self._pending = []  # list[str]? list[JobInfo]? list[Any]?
        self._lock = None   # asyncio.Lock? threading.Lock? None forever?
        self.running = True # bool? Presumed but not declared

# WRONG: Untyped class-level attributes
class JobManager:
    _instance = None  # What type?
    DEFAULT_TIMEOUT = 30  # int? float?
```

**Detection Script (Comprehensive):**

```python
import ast
from pathlib import Path
from typing import NamedTuple

class UntypedAttribute(NamedTuple):
    line: int
    class_name: str
    attr_name: str
    location: str  # "__init__", "class_body", or method name

def find_untyped_class_attributes(file_path: str) -> list[UntypedAttribute]:
    """
    Find ALL untyped class attributes - both class-level and instance-level.
    
    Checks:
    1. Class-level assignments without annotations
    2. self.X = ... in __init__ without prior annotation
    3. self.X = ... in other methods without prior annotation
    """
    with open(file_path) as f:
        source = f.read()
        tree = ast.parse(source)
    
    violations = []
    
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        
        class_name = node.name
        
        # Collect declared annotations (class-level type hints)
        declared_attrs: set[str] = set()
        
        for item in node.body:
            # Class-level annotations: attr: Type or attr: Type = value
            if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                declared_attrs.add(item.target.id)
            
            # Class-level assignment WITHOUT annotation = violation
            elif isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        if target.id not in declared_attrs:
                            violations.append(UntypedAttribute(
                                line=item.lineno,
                                class_name=class_name,
                                attr_name=target.id,
                                location="class_body"
                            ))
        
        # Now check methods for self.X assignments
        for item in node.body:
            if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            
            method_name = item.name
            
            for stmt in ast.walk(item):
                # Look for self.X = ... assignments
                if isinstance(stmt, ast.Assign):
                    for target in stmt.targets:
                        if (isinstance(target, ast.Attribute) and
                            isinstance(target.value, ast.Name) and
                            target.value.id == 'self'):
                            attr_name = target.attr
                            # Check if this attribute was declared with a type hint
                            if attr_name not in declared_attrs:
                                violations.append(UntypedAttribute(
                                    line=stmt.lineno,
                                    class_name=class_name,
                                    attr_name=attr_name,
                                    location=method_name
                                ))
                                # Add to declared to avoid duplicate reports
                                declared_attrs.add(attr_name)
    
    return violations

# Usage - scan all modular class files
def scan_directory(directory: str) -> dict[str, list[UntypedAttribute]]:
    results = {}
    for py_file in Path(directory).glob("**/*.py"):
        violations = find_untyped_class_attributes(str(py_file))
        if violations:
            results[str(py_file)] = violations
    return results

# Run on worker module
results = scan_directory("hyperscale/distributed/nodes/worker")
for file_path, violations in results.items():
    print(f"\n{file_path}:")
    for v in violations:
        print(f"  Line {v.line}: {v.class_name}.{v.attr_name} (in {v.location})")
```

**Quick Detection Command:**

```bash
# Find self.X = assignments in __init__ without type annotations
python3 -c "
import ast
import sys

file_path = sys.argv[1] if len(sys.argv) > 1 else 'state.py'

with open(file_path) as f:
    tree = ast.parse(f.read())

for node in ast.walk(tree):
    if isinstance(node, ast.ClassDef):
        # Get declared type hints
        declared = {item.target.id for item in node.body 
                    if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)}
        
        # Find __init__
        for item in node.body:
            if isinstance(item, ast.FunctionDef) and item.name == '__init__':
                for stmt in ast.walk(item):
                    if isinstance(stmt, ast.Assign):
                        for t in stmt.targets:
                            if (isinstance(t, ast.Attribute) and 
                                isinstance(t.value, ast.Name) and 
                                t.value.id == 'self' and
                                t.attr not in declared):
                                print(f'{stmt.lineno}:{node.name}.{t.attr}')
" state.py
```

**Correct Pattern - Class Attribute Type Hints:**

```python
# CORRECT: Type hints declared at class level, initialized in __init__
class WorkerState:
    # Declare all attributes with types at class level
    _workers: dict[str, WorkerInfo]
    _pending_jobs: list[JobInfo]
    _job_fence_tokens: dict[str, int]
    _lock: asyncio.Lock
    _logger: Logger | None
    _running: bool
    
    # Class-level constants also need types
    DEFAULT_TIMEOUT: float = 30.0
    MAX_RETRIES: int = 3
    
    def __init__(self, logger: Logger | None = None):
        # Initialize (types already declared above)
        self._workers = {}
        self._pending_jobs = []
        self._job_fence_tokens = {}
        self._lock = asyncio.Lock()
        self._logger = logger
        self._running = False

# ALSO CORRECT: Inline annotation in __init__ (less preferred but valid)
class JobManager:
    def __init__(self, config: JobConfig):
        self._config: JobConfig = config
        self._cache: dict[str, JobInfo] = {}
        self._active_count: int = 0
```

**Why Class-Level Declaration is Preferred:**

1. **Single source of truth**: All attributes visible at top of class
2. **IDE support**: Better autocomplete before __init__ runs
3. **Documentation**: Clear picture of class state at a glance
4. **Dataclass compatibility**: Same pattern as @dataclass

### Step 3.5k.1c: Scan All Modular Class Files

**MANDATORY**: Run the attribute scanner on ALL files in the node module:

```bash
# For worker node
for f in hyperscale/distributed/nodes/worker/*.py; do
    echo "=== $f ===" 
    python3 scan_class_attrs.py "$f"
done

# For manager node  
for f in hyperscale/distributed/nodes/manager/*.py; do
    echo "=== $f ==="
    python3 scan_class_attrs.py "$f"
done

# For gate node
for f in hyperscale/distributed/nodes/gate/*.py; do
    echo "=== $f ==="
    python3 scan_class_attrs.py "$f"
done
```

### Step 3.5k.1d: Incomplete Generic Type Detection (MANDATORY - CRITICAL)

**The Problem:**

Generic types (`dict`, `list`, `set`, `tuple`, `Callable`, `Awaitable`, etc.) without their type parameters are nearly as bad as `Any` - they defeat the type system:

```python
# WRONG: Incomplete generic types - type parameters missing
class WorkerState:
    _workers: dict                    # dict of WHAT? dict[?, ?]
    _pending_ids: list                # list of WHAT? list[?]
    _seen_tokens: set                 # set of WHAT? set[?]
    _callback: Callable               # Callable with what signature?
    _result: tuple                    # tuple of WHAT? tuple[?, ?, ?]
    _future: Awaitable                # Awaitable of WHAT?
    
    def process(self, items: list):   # list of WHAT?
        pass
    
    def get_mapping(self) -> dict:    # dict of WHAT?
        return {}

# CORRECT: All generic type parameters specified
class WorkerState:
    _workers: dict[str, WorkerInfo]
    _pending_ids: list[str]
    _seen_tokens: set[int]
    _callback: Callable[[JobInfo], Awaitable[None]]
    _result: tuple[str, int, bool]
    _future: Awaitable[JobResult]
    
    def process(self, items: list[JobInfo]) -> None:
        pass
    
    def get_mapping(self) -> dict[str, WorkerInfo]:
        return {}
```

**Why Incomplete Generics Are Dangerous:**

1. **Silent type erasure**: `dict` becomes `dict[Any, Any]` - no type checking
2. **False confidence**: Code looks typed but provides no safety
3. **IDE degradation**: Autocomplete shows `Any` methods, not actual type methods
4. **Refactoring blind spots**: Can't catch type mismatches when changing code

**Generic Types That MUST Have Parameters:**

| Type | Required Parameters | Example |
|------|---------------------|---------|
| `dict` | `[KeyType, ValueType]` | `dict[str, JobInfo]` |
| `list` | `[ElementType]` | `list[WorkerInfo]` |
| `set` | `[ElementType]` | `set[str]` |
| `frozenset` | `[ElementType]` | `frozenset[int]` |
| `tuple` | `[Type1, Type2, ...]` or `[Type, ...]` | `tuple[str, int]` or `tuple[int, ...]` |
| `Callable` | `[[ArgTypes], ReturnType]` | `Callable[[str, int], bool]` |
| `Awaitable` | `[ResultType]` | `Awaitable[JobResult]` |
| `Coroutine` | `[YieldType, SendType, ReturnType]` | `Coroutine[Any, Any, JobResult]` |
| `AsyncIterator` | `[YieldType]` | `AsyncIterator[WorkerInfo]` |
| `Iterator` | `[YieldType]` | `Iterator[str]` |
| `Generator` | `[YieldType, SendType, ReturnType]` | `Generator[int, None, None]` |
| `Optional` | `[Type]` | `Optional[JobInfo]` (prefer `Type \| None`) |
| `Union` | `[Type1, Type2, ...]` | `Union[str, int]` (prefer `str \| int`) |
| `Sequence` | `[ElementType]` | `Sequence[JobInfo]` |
| `Mapping` | `[KeyType, ValueType]` | `Mapping[str, int]` |
| `MutableMapping` | `[KeyType, ValueType]` | `MutableMapping[str, JobInfo]` |
| `Iterable` | `[ElementType]` | `Iterable[WorkerInfo]` |

**Detection Script:**

```python
import ast
import re
from pathlib import Path

# Generic types that require parameters
GENERIC_TYPES = {
    'dict', 'Dict',
    'list', 'List', 
    'set', 'Set',
    'frozenset', 'FrozenSet',
    'tuple', 'Tuple',
    'Callable',
    'Awaitable',
    'Coroutine',
    'AsyncIterator', 'AsyncIterable',
    'Iterator', 'Iterable',
    'Generator', 'AsyncGenerator',
    'Optional',
    'Union',
    'Sequence', 'MutableSequence',
    'Mapping', 'MutableMapping',
    'Collection',
    'AbstractSet', 'MutableSet',
}

def find_incomplete_generics(file_path: str) -> list[tuple[int, str, str]]:
    """
    Find generic type hints without type parameters.
    
    Returns: [(line, context, incomplete_type)]
    """
    with open(file_path) as f:
        source = f.read()
        lines = source.split('\n')
    
    violations = []
    
    # Pattern: matches bare generic types not followed by [
    # e.g., ": dict" or ": list" or "-> dict" but not ": dict[" or ": list["
    for i, line in enumerate(lines, 1):
        for generic in GENERIC_TYPES:
            # Match ": <generic>" or "-> <generic>" not followed by "["
            patterns = [
                rf':\s*{generic}\s*(?:=|,|\)|$|\s*#)',  # : dict = or : dict, or : dict) or end
                rf'->\s*{generic}\s*(?::|,|\)|$|\s*#)',  # -> dict: or -> dict
            ]
            for pattern in patterns:
                if re.search(pattern, line):
                    # Verify it's not actually complete (has [...])
                    if not re.search(rf'{generic}\s*\[', line):
                        context = line.strip()[:60]
                        violations.append((i, context, generic))
    
    return violations

# Usage
for py_file in Path("hyperscale/distributed/nodes/worker").glob("*.py"):
    violations = find_incomplete_generics(str(py_file))
    if violations:
        print(f"\n{py_file}:")
        for line, context, generic in violations:
            print(f"  Line {line}: incomplete `{generic}` in: {context}")
```

**Quick Detection Command:**

```bash
# Find bare dict/list/set/tuple without type parameters
grep -rn ": dict\s*=\|: dict$\|: dict,\|: dict)\|-> dict:" *.py | grep -v "\[" 
grep -rn ": list\s*=\|: list$\|: list,\|: list)\|-> list:" *.py | grep -v "\["
grep -rn ": set\s*=\|: set$\|: set,\|: set)\|-> set:" *.py | grep -v "\["
grep -rn ": tuple\s*=\|: tuple$\|: tuple,\|: tuple)\|-> tuple:" *.py | grep -v "\["
grep -rn ": Callable\s*=\|: Callable$\|: Callable,\|: Callable)" *.py | grep -v "\["
```

**Fix Pattern:**

For each incomplete generic, research what types it actually contains:

```python
# Step 1: Find where the variable is populated
self._workers = {}  # Where do items come from?

# Step 2: Find assignments/mutations
self._workers[worker_id] = worker_info  # worker_id is str, worker_info is WorkerInfo

# Step 3: Apply complete type
_workers: dict[str, WorkerInfo]
```

**Common Incomplete → Complete Fixes:**

| Incomplete | Research Question | Likely Complete Type |
|------------|-------------------|---------------------|
| `dict` | What are keys? What are values? | `dict[str, JobInfo]` |
| `list` | What elements are stored? | `list[WorkerInfo]` |
| `set` | What elements are stored? | `set[str]` |
| `tuple` | What's the fixed structure? | `tuple[str, int, bool]` |
| `Callable` | What args? What return? | `Callable[[str], Awaitable[None]]` |

**Special Cases:**

```python
# Empty containers - still need types
_empty_cache: dict[str, JobInfo] = {}  # Even if always empty, declare types
_placeholder: list[str] = []

# Homogeneous tuples (variable length)
_ids: tuple[str, ...]  # Zero or more strings

# Heterogeneous tuples (fixed structure) 
_pair: tuple[str, int]  # Exactly one string and one int

# Callable with no args
_factory: Callable[[], JobInfo]  # No args, returns JobInfo

# Async callable
_handler: Callable[[Request], Awaitable[Response]]
```

### Step 3.5k.2: Research and Apply Correct Type Hints

**CRITICAL: Do not guess types. Research what is actually passed.**

For each untyped parameter:

1. **Find all call sites:**
   ```bash
   grep -n "\.method_name(" server.py handlers/*.py
   ```

2. **Trace what is passed:**
   ```python
   # If call site shows:
   await self._process_job(job_info)
   # Find where job_info comes from:
   job_info = self._job_manager.get_job(job_id)
   # Check get_job return type:
   def get_job(self, job_id: str) -> JobInfo | None:
   # Therefore parameter type is: JobInfo
   ```

3. **Use LSP hover to confirm:**
   ```bash
   lsp_hover(file="server.py", line=<call_site_line>, character=<arg_position>)
   ```

4. **Apply the type hint:**
   ```python
   # Before:
   def _process_job(self, job):
   
   # After:
   def _process_job(self, job: JobInfo) -> None:
   ```

### Step 3.5k.3: Handle Complex Types

**Union Types (multiple possible types):**

```python
# If different call sites pass different types:
await self._handle_message(job_submission)  # JobSubmission
await self._handle_message(progress_report)  # WorkflowProgress

# Use union:
def _handle_message(self, message: JobSubmission | WorkflowProgress) -> None:
```

**Optional Types (can be None):**

```python
# If call site shows:
worker = self._workers.get(worker_id)  # Returns WorkerInfo | None
await self._process_worker(worker)

# Parameter must accept None:
def _process_worker(self, worker: WorkerInfo | None) -> None:
    if worker is None:
        return
    # ...
```

**Generic Types:**

```python
# For collections, specify element types:
def _process_jobs(self, jobs: list[JobInfo]) -> None:
def _handle_workers(self, workers: dict[str, WorkerInfo]) -> None:

# For callbacks:
def _register_callback(self, callback: Callable[[JobInfo], Awaitable[None]]) -> None:
```

**Avoid Any - Use Generics Instead:**

```python
# WRONG:
def _transform(self, data: Any) -> Any:

# RIGHT - use TypeVar:
T = TypeVar('T')
def _transform(self, data: T) -> T:

# OR be specific:
def _transform(self, data: bytes) -> dict[str, str]:
```

### Step 3.5k.4: Validate Return Types (When Required)

**Per AGENTS.md**: Return types are inferred by default, BUT are REQUIRED for:
- Public API methods
- Methods with complex return logic
- Test workflow classes

**Detection for missing return types on public methods:**

```python
def find_public_methods_without_return_type(file_path: str) -> list[tuple[int, str]]:
    """Find public methods (no leading _) without return type hints."""
    with open(file_path) as f:
        tree = ast.parse(f.read())
    
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # Public method = no leading underscore (except __init__, __str__, etc.)
            if not node.name.startswith('_') or node.name.startswith('__'):
                if node.returns is None and node.name != '__init__':
                    violations.append((node.lineno, node.name))
    
    return violations
```

### Step 3.5k.5: Fix Patterns

| Issue | Wrong Fix | Correct Fix |
|-------|-----------|-------------|
| Unknown parameter type | Use `Any` | Research call sites, use specific type |
| Multiple possible types | Use `object` | Use `Union[A, B]` or `A \| B` |
| Complex nested type | Use `dict` | Use `dict[str, list[WorkerInfo]]` |
| Callback parameter | Use `Callable` | Use `Callable[[ArgType], ReturnType]` |
| Optional parameter | Omit `None` | Use `Type \| None` explicitly |

### Step 3.5k.6: Validation

After adding type hints, verify:

1. **LSP diagnostics clean:**
   ```bash
   lsp_diagnostics(file="server.py", severity="error")
   ```

2. **No Any/object escape hatches:**
   ```bash
   grep -n ": Any\|: object" server.py
   # Should return zero matches (or justified exceptions)
   ```

3. **All parameters typed:**
   ```bash
   # Re-run the scanner - should return zero violations
   python3 scan_untyped_params.py server.py
   ```

### Step 3.5k.7: Documentation

For complex types, add docstring explaining:

```python
async def _route_job(
    self,
    job: JobSubmission,
    candidates: list[DatacenterHealth],
    strategy: RoutingStrategy | None = None,
) -> tuple[str, ManagerInfo] | None:
    """
    Route job to best datacenter.
    
    Args:
        job: Job submission request with routing preferences
        candidates: Pre-filtered list of healthy datacenters
        strategy: Override routing strategy (default: use job.routing_strategy)
    
    Returns:
        Tuple of (datacenter_id, selected_manager) or None if no suitable DC found
    """
```

### Step 3.5k.8: Scan All Modular Classes (MANDATORY)

**This phase applies to ALL files in the node module, not just the server file.**

For each node (worker, manager, gate), scan:

| File Category | Example Files | Must Scan? |
|---------------|---------------|------------|
| Server | `server.py` | **YES** |
| State | `state.py`, `*_state.py` | **YES** |
| Coordinators | `*_coordinator.py` | **YES** |
| Handlers | `tcp_*.py`, `*_handler.py` | **YES** |
| Helpers | `config.py`, `registry.py` | **YES** |
| Models | `models/*.py` | **YES** (if in node dir) |

**Execution Command:**

```bash
#!/bin/bash
# scan_all_types.sh <node_dir>

NODE_DIR=$1  # e.g., hyperscale/distributed/nodes/worker

echo "=== Scanning $NODE_DIR for type hint violations ==="

# Scan parameters
echo -e "\n--- Untyped Parameters ---"
for f in "$NODE_DIR"/*.py; do
    python3 -c "
import ast
with open('$f') as f:
    tree = ast.parse(f.read())
for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        untyped = [a.arg for a in node.args.args 
                   if a.arg not in ('self', 'cls') and a.annotation is None]
        if untyped:
            print(f'$f:{node.lineno}:{node.name}: {untyped}')
"
done

# Scan class attributes
echo -e "\n--- Untyped Class Attributes ---"
for f in "$NODE_DIR"/*.py; do
    python3 scan_class_attrs.py "$f"
done

# Scan for Any/object
echo -e "\n--- Any/object Escape Hatches ---"
grep -rn ": Any\|: object" "$NODE_DIR"/*.py

echo -e "\n=== Scan Complete ==="
```

### Output

- **ZERO** untyped parameters (except `self`/`cls`) in ALL modular class files
- **ZERO** untyped class attributes (both public and private, class-level and instance-level)
- **ZERO** incomplete generic types (`dict` without `[K, V]`, `list` without `[T]`, etc.)
- **ZERO** use of `Any` or `object` as type hints (without justification)
- **ZERO** public methods without return type hints
- All complex types documented in docstrings
- LSP diagnostics clean on ALL scanned files

**BLOCKING**: Phase 3.5k is not complete until ALL functions, methods, AND class attributes across ALL modular class files have properly researched and applied type hints with complete generic parameters.

---

## Phase 4: Check Direct State Access

**Objective**: Find and FIX abstraction violations where server bypasses components.

**Steps**:
1. Identify the state object(s): `grep "self\._.*state" server.py`
2. Search for internal field access:
   ```bash
   grep "self\._<state>\._[a-z]" server.py
   ```
3. For each violation, build fix plan:
   | Line | Direct Access | Required Method | Target Class |
   |------|---------------|-----------------|--------------|

**MANDATORY: Fix ALL violations.** Do not document for later - fix now.

### Step 4a: Group Violations by Field

Group all direct accesses by the internal field being accessed:

```
_workers: 16 accesses across lines [...]
_state_version: 9 accesses across lines [...]
```

### Step 4b: Create Accessor Methods

For each field with direct access, create proper accessor method(s) in the state class:

```python
# In state.py - add for each violated field:
def get_worker(self, worker_id: str) -> WorkerRegistration | None:
    return self._workers.get(worker_id)

def iter_workers(self) -> Iterator[tuple[str, WorkerRegistration]]:
    return iter(self._workers.items())

def add_worker(self, worker_id: str, worker: WorkerRegistration) -> None:
    self._workers[worker_id] = worker
```

### Step 4c: Update All Call Sites

Replace every direct access with the new method:

```python
# Before:
worker = self._manager_state._workers.get(worker_id)

# After:
worker = self._manager_state.get_worker(worker_id)
```

### Step 4d: Verify Zero Violations Remain

After fixing, re-run:
```bash
grep "self\._<state>\._[a-z]" server.py
```

**This MUST return zero matches** before proceeding to Phase 5.

**Output**: Zero direct state access violations.

---

## Phase 5: Reconcile Each Missing Method (NO SHORTCUTS)

**Objective**: For EACH missing method, find or create the correct implementation.

**NO SHORTCUTS**: Do not stub methods, add pass-through wrappers, or suppress errors. Every fix must provide real, correct functionality.

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

## Phase 5.6: Cyclomatic Complexity Reduction

**Objective**: Minimize nested conditionals and reduce lines of code in all fixes.

### The Problem

Correct fixes can still introduce unnecessary complexity:

```python
# WRONG: Nested ifs increase cyclomatic complexity
if sub_wf_info := job.sub_workflows.get(token):
    if sub_wf_info.progress:
        total += sub_wf_info.progress.completed_count

# RIGHT: Combined conditions, walrus for clarity
sub_wf_info = job.sub_workflows.get(token)
if sub_wf_info and (progress := sub_wf_info.progress):
    total += progress.completed_count
```

### Step 5.6a: Scan for Nested Conditionals

After any fix, check for nested `if` statements:

```bash
# Find nested ifs (indentation pattern)
grep -n "^\s*if.*:\s*$" server.py | while read line; do
    linenum=$(echo $line | cut -d: -f1)
    nextline=$((linenum + 1))
    sed -n "${nextline}p" server.py | grep -q "^\s*if" && echo "Nested if at line $linenum"
done
```

### Step 5.6b: Reduction Patterns

| Anti-Pattern | Refactored Pattern |
|--------------|-------------------|
| `if x:` then `if y:` | `if x and y:` |
| `if x := get():` then `if x.attr:` | `x = get()` then `if x and (attr := x.attr):` |
| `if x:` then `if y:` then `if z:` | `if x and y and z:` or extract to method |
| Multiple returns in conditionals | Guard clauses (early returns) |

### Step 5.6c: Walrus Operator Usage

Use walrus (`:=`) to combine assignment with condition when the assigned value is used immediately:

```python
# WRONG: Separate assignment and check
result = expensive_call()
if result:
    use(result)

# RIGHT: Walrus when result used in same block
if result := expensive_call():
    use(result)

# WRONG: Walrus when value used in else or after
if result := expensive_call():
    use(result)
else:
    log(result)  # Confusing - result came from walrus

# RIGHT: Explicit assignment when value used broadly
result = expensive_call()
if result:
    use(result)
else:
    log(result)
```

### Step 5.6d: Cyclomatic Complexity Limits

| Complexity | Action |
|------------|--------|
| 1-3 | Acceptable |
| 4 | Maximum allowed - review for simplification |
| 5+ | Must refactor - extract methods or restructure |

Count complexity by adding 1 for:
- Each `if`, `elif`, `else`
- Each `for`, `while`
- Each `and`, `or` in conditions
- Each `except` clause
- Each `case` in match statements

### Step 5.6e: Line Count Awareness

Every fix should aim to minimize total lines. Before committing, ask:
- Can two statements become one?
- Can a multi-line conditional be a single line?
- Is there a comprehension that replaces a loop?

```python
# VERBOSE (4 lines):
total = 0
for item in items:
    if item.active:
        total += item.value

# CONCISE (1 line):
total = sum(item.value for item in items if item.active)
```

### Output

- No nested conditionals beyond 2 levels
- Cyclomatic complexity ≤ 4 per method
- Minimal lines of code for each fix

---

## Phase 5.7: Post-Refactor Integrity Verification

**Objective**: Catch broken code introduced during refactoring before it's committed.

### The Problem

Refactoring (especially method extraction) commonly introduces:

1. **Orphaned variable references**: Variables from the original scope don't exist in extracted methods
2. **Non-existent method calls**: Calling methods that were assumed to exist or were misnamed
3. **Missing imports**: Types used in new method signatures not imported
4. **Scope confusion**: Using `self.X` when X was a local variable, or vice versa

```python
# ORIGINAL (before refactor):
async def _handle_completion(self, job_id: str):
    job = self._job_manager.get_job(job_id)
    if job:
        await process(job)
        await self._job_manager.remove_job(job.token)

# BROKEN REFACTOR:
async def _handle_completion(self, job_id: str):
    job = self._job_manager.get_job(job_id)
    if job:
        await process(job)
        await self._cleanup(job_id)

async def _cleanup(self, job_id: str):
    await self._job_manager.remove_job(job.token)  # BUG: 'job' not in scope!
    await self._job_manager.remove_job_by_id(job_id)  # BUG: method doesn't exist!
```

### Step 5.7a: MANDATORY LSP Check After Every Refactor

**After ANY method extraction or signature change:**

```bash
lsp_diagnostics(file="server.py", severity="error")
```

**This is NON-NEGOTIABLE.** Do not proceed until LSP returns zero errors for the modified file.

### Step 5.7b: Variable Scope Audit

When extracting a method, audit ALL variables used in the extracted code:

| Variable | Source in Original | Available in Extracted? | Fix |
|----------|-------------------|------------------------|-----|
| `job` | Local variable | NO | Pass as parameter or re-fetch |
| `job_id` | Parameter | YES (passed) | OK |
| `self._manager` | Instance | YES | OK |

**For each variable not available**: Either pass it as a parameter or re-acquire it in the new method.

### Step 5.7c: Method Existence Verification

For every method call in refactored code, verify the method exists:

```bash
# For each method call like self._foo.bar()
grep -n "def bar" <component_file>.py
```

**Common mistakes:**
- Assuming `remove_job_by_id` exists when only `remove_job(token)` exists
- Calling `get_job(job_id)` when signature is `get_job(token)`
- Using wrong component (`self._manager` vs `self._job_manager`)

### Step 5.7d: Parameter Flow Tracing

When a method is extracted, trace all data flow:

```
Original: _handle_completion(job_id) 
  └─> job = get_job(job_id)
  └─> uses job.token, job.status, job.workflows

Extracted: _cleanup(job_id)
  └─> needs to remove job
  └─> HOW? job.token not available!
  └─> FIX: create token from job_id, or pass job as parameter
```

### Step 5.7e: Integration Verification

After refactoring, verify the calling code still works:

1. **Check the call site** passes all required parameters
2. **Check return values** are handled correctly
3. **Check async/await** is preserved (async method must be awaited)

### Refactor Checklist (MANDATORY before proceeding)

- [ ] LSP diagnostics return ZERO errors on modified file
- [ ] All variables in extracted methods are either parameters or instance attributes
- [ ] All method calls reference methods that actually exist
- [ ] All imports needed by new type hints are present
- [ ] Calling code passes correct parameters to extracted methods

**BLOCKING**: Do not commit refactored code until this checklist passes.

---

## Phase 5.8: Dead Computation Detection

**Objective**: Find computed values that are never used (silent logic bugs).

### The Problem

When refactoring, computed values can become orphaned - computed but never passed to consumers:

```python
# BROKEN: final_status computed but never used
async def _handle_job_completion(self, job_id: str):
    job = self._get_job(job_id)
    final_status = self._determine_final_job_status(job)  # Computed!
    workflow_results, errors = self._aggregate_results(job)
    
    await self._send_completion(job_id, workflow_results, errors)  # final_status missing!

# The downstream method re-invents the logic differently:
async def _send_completion(self, job_id, results, errors):
    final_status = "FAILED" if errors else "COMPLETED"  # Different semantics!
```

This is particularly insidious because:
1. Code compiles and runs
2. LSP shows no errors
3. Tests may pass (if they don't check status semantics)
4. Bug only surfaces in production edge cases

### Step 5.8a: Trace All Computed Values

For each method, list all local variables that are assigned:

```bash
grep -n "^\s*[a-z_]* = " method_body.py
```

Build assignment table:

| Line | Variable | Computation | Used Where? |
|------|----------|-------------|-------------|
| 4579 | `final_status` | `_determine_final_job_status(job)` | ??? |
| 4580 | `workflow_results` | `_aggregate_workflow_results(job)` | Line 4587 ✓ |
| 4578 | `elapsed_seconds` | `job.elapsed_seconds()` | Line 4591 ✓ |

### Step 5.8b: Verify Each Computation Is Used

For each computed variable:

1. **Search for usage** in the same method after assignment
2. **If passed to another method**, verify the receiving method's signature accepts it
3. **If returned**, verify caller uses the return value

```bash
# For variable 'final_status' assigned at line N
# Search for usage after line N
awk 'NR>N && /final_status/' method_body.py
```

### Step 5.8c: Cross-Method Data Flow

When method A computes a value and calls method B:

```
Method A computes: final_status, workflow_results, errors
Method A calls: _send_completion(job_id, workflow_results, errors)

MISMATCH: final_status computed but not passed!
```

Build flow table:

| Computed in Caller | Passed to Callee? | Callee Parameter |
|-------------------|-------------------|------------------|
| `final_status` | **NO** ❌ | (missing) |
| `workflow_results` | YES ✓ | `workflow_results` |
| `errors` | YES ✓ | `errors` |

### Step 5.8d: Semantic Divergence Detection

When a value is re-computed in a callee instead of being passed:

```python
# Caller's computation:
final_status = self._determine_final_job_status(job)
# Based on: job.workflows_failed count

# Callee's re-computation:
final_status = "FAILED" if errors else "COMPLETED"
# Based on: presence of error strings
```

**These have different semantics!**
- Original: FAILED only if ALL workflows failed
- Re-computed: FAILED if ANY error string exists

**Detection**: Search callee for assignments to the same variable name:
```bash
grep "final_status = " callee_method.py
```

If found, this is likely a semantic divergence bug.

### Step 5.8e: Fix Patterns (NO SHORTCUTS)

**NO SHORTCUTS**: Do not delete the computation and hope it wasn't needed. Do not add a comment saying "TODO: wire this up later". Fix the data flow correctly.

| Issue | Fix |
|-------|-----|
| Value computed but not passed | Add parameter to callee, pass value |
| Value re-computed in callee | Remove re-computation, use passed value |
| Callee doesn't need value | Remove computation from caller |

### Output

- Every computed value is either used locally, passed to callees, or returned
- No semantic divergence between caller computation and callee re-computation
- Clear data flow from computation to consumption

---

## Phase 5.9: Cyclomatic Complexity Scanning and Validation (NO SHORTCUTS)

**Objective**: Systematically scan ALL methods/functions for cyclomatic complexity violations and fix them.

**NO SHORTCUTS**: Do not reduce complexity by deleting error handling, removing edge cases, or stubbing out logic. Extract to well-named helper methods that preserve all behavior.

### The Problem

High cyclomatic complexity makes code:
- Hard to understand and maintain
- Prone to bugs in edge cases
- Difficult to test comprehensively
- Error-prone during refactoring

```python
# HIGH COMPLEXITY (CC=8+): Multiple nested loops, conditionals, exception handlers
async def _orphan_scan_loop(self) -> None:
    while self._running:                    # +1
        try:                                # +1
            if not should_scan:             # +1
                continue
            for worker_id, worker in ...:   # +1
                try:                        # +1
                    if not response:        # +1
                        continue
                    for job in ...:         # +1
                        for sub_wf in ...:  # +1
                            if sub_wf...:   # +1
                                if parent:  # +1
                    for orphaned in ...:    # +1
                        if dispatcher:      # +1
                except Exception:           # +1
        except CancelledError:              # +1
        except Exception:                   # +1
```

### Step 5.9a: Automated Complexity Scan

Run complexity analysis on all methods:

```python
import ast
import sys

def calculate_complexity(node: ast.AST) -> int:
    """Calculate cyclomatic complexity of an AST node."""
    complexity = 1  # Base complexity
    
    for child in ast.walk(node):
        # Each decision point adds 1
        if isinstance(child, (ast.If, ast.While, ast.For, ast.AsyncFor)):
            complexity += 1
        elif isinstance(child, ast.ExceptHandler):
            complexity += 1
        elif isinstance(child, ast.BoolOp):
            # Each 'and'/'or' adds to complexity
            complexity += len(child.values) - 1
        elif isinstance(child, ast.comprehension):
            # List/dict/set comprehensions with conditions
            complexity += len(child.ifs)
        elif isinstance(child, ast.Match):
            complexity += len(child.cases) - 1
    
    return complexity

def scan_file(filepath: str, max_complexity: int = 4) -> list[tuple[str, int, int]]:
    """Scan file for methods exceeding complexity threshold."""
    with open(filepath) as f:
        tree = ast.parse(f.read())
    
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            cc = calculate_complexity(node)
            if cc > max_complexity:
                violations.append((node.name, node.lineno, cc))
    
    return violations

# Usage
violations = scan_file("server.py", max_complexity=4)
for name, line, cc in violations:
    print(f"Line {line}: {name}() has CC={cc} (max: 4)")
```

### Step 5.9b: Build Violation Report

| Method | Line | Complexity | Max Allowed | Violation |
|--------|------|------------|-------------|-----------|
| `_orphan_scan_loop` | 1349 | 15 | 4 | **YES** |
| `_handle_job_completion` | 2500 | 8 | 4 | **YES** |
| `_process_heartbeat` | 3200 | 3 | 4 | NO |

### Step 5.9c: Complexity Reduction Patterns

| Anti-Pattern | Refactored Pattern | Complexity Reduction |
|--------------|-------------------|---------------------|
| Nested loops | Extract inner loop to helper method | -N per loop extracted |
| Multiple exception handlers | Single handler with type dispatch | -N+1 |
| Nested conditionals | Guard clauses (early returns) | -N per level flattened |
| Complex boolean expressions | Extract to predicate methods | -N per expression |
| Loop with conditional continue | Filter before loop | -1 |

**Example - Extract Inner Loop:**

```python
# BEFORE (CC=8): Nested loops in main method
async def _orphan_scan_loop(self):
    while running:
        for worker in workers:
            for job in jobs:
                for sub_wf in job.sub_workflows:
                    if condition:
                        process(sub_wf)

# AFTER (CC=3 + CC=3): Split into focused methods
async def _orphan_scan_loop(self):
    while running:
        for worker in workers:
            await self._scan_worker_for_orphans(worker)

async def _scan_worker_for_orphans(self, worker):
    worker_workflow_ids = await self._query_worker_workflows(worker)
    manager_tracked_ids = self._get_manager_tracked_ids_for_worker(worker.id)
    orphaned = manager_tracked_ids - worker_workflow_ids
    await self._handle_orphaned_workflows(orphaned)
```

**Example - Guard Clauses:**

```python
# BEFORE (CC=4): Nested conditionals
if response:
    if not isinstance(response, Exception):
        if parsed := parse(response):
            process(parsed)

# AFTER (CC=3): Guard clauses
if not response or isinstance(response, Exception):
    return
parsed = parse(response)
if not parsed:
    return
process(parsed)
```

### Step 5.9d: Refactoring Workflow

For each violation:

1. **Identify extraction boundaries**: Find logically cohesive blocks
2. **Name the extracted method**: Clear verb+noun describing the action
3. **Pass minimum required parameters**: Don't pass entire objects if only one field needed
4. **Preserve error handling semantics**: Exceptions should propagate correctly
5. **Run LSP diagnostics**: Verify no broken references
6. **Re-calculate complexity**: Verify both original and extracted are ≤4

### Step 5.9e: Post-Refactor Validation (MANDATORY - NO SHORTCUTS)

**NO SHORTCUTS**: Do not skip validation steps. Do not assume "it probably works". Run every check.

After EVERY complexity-reducing refactor:

1. **LSP Diagnostics**: `lsp_diagnostics(file="server.py", severity="error")`
2. **Variable Scope Audit**: All variables in extracted methods are either:
   - Parameters passed to the method
   - Instance attributes (self._X)
   - Locally computed
3. **Attribute Access Validation**: Run Phase 3.5g scanner on modified methods
4. **Method Existence Check**: All called methods exist on their targets
5. **Chained Access Validation**: Run Phase 3.5h.1 scanner for chained attribute access

```bash
# Quick validation command
lsp_diagnostics && echo "Diagnostics clean" || echo "ERRORS FOUND"
```

### Step 5.9f: Complexity Limits (MANDATORY - NO EXCEPTIONS)

**ALL methods above CC=4 MUST be refactored. No exceptions. No deferrals.**

| Complexity | Action Required |
|------------|-----------------|
| 1-3 | Acceptable, no action |
| 4 | Maximum allowed - document why if borderline |
| 5-9 | **MUST refactor NOW** - extract helper methods (not "later", not "if time permits") |
| 10+ | **CRITICAL BLOCKER** - requires immediate significant decomposition |

**BLOCKING**: Phase 5.9 is not complete until ZERO methods have CC > 4. This is not negotiable.

### Step 5.9g: Documentation Requirements

For methods at CC=4 (borderline):
- Add comment explaining why complexity is necessary
- Document which decision points could be extracted if needed

```python
async def _process_complex_case(self):
    """
    Process complex case with multiple validations.
    
    Complexity: 4 (at limit)
    Decision points: auth check, rate limit, validation, dispatch
    Note: Could extract validation to separate method if complexity grows
    """
```

### Output

- Zero methods with CC > 4
- All extracted methods have clear single responsibility
- Post-refactor integrity verified via LSP
- No broken attribute accesses introduced

---

## Phase 6: Clean Up Dead Code (NO SHORTCUTS)

**Objective**: Remove orphaned implementations.

**NO SHORTCUTS**: Do not comment out code "just in case". Do not leave dead code with TODO comments. Either the code is needed (keep it and wire it up) or it's not (delete it).

**Steps**:
1. For each modular class, extract all public methods
2. Search server for calls to each method
3. If method is never called AND not part of public API:
   - Verify it's not called from OTHER files
   - If truly orphaned, remove it
4. Document removed methods

---

## Phase 6.5: Runtime Correctness Validation (CRITICAL - NO SHORTCUTS)

**Objective**: Verify that changes do not introduce race conditions, memory leaks, dropped errors, or unbounded queues.

**NO SHORTCUTS**: These are silent killers that compile and run but cause production failures. Every check must be performed on BOTH initial analysis AND after any fix.

### The Problem

These four categories of bugs are particularly insidious because:
- They pass all type checks and LSP diagnostics
- They may not surface in unit tests
- They cause intermittent or delayed failures in production
- They can be introduced by seemingly correct refactors

### Step 6.5a: Race Condition Detection

**What to look for:**

1. **Shared mutable state accessed without locks**:
   ```python
   # DANGEROUS: Multiple async tasks modifying same dict
   self._workers[worker_id] = worker  # No lock!
   
   # SAFE: Protected by lock
   async with self._workers_lock:
       self._workers[worker_id] = worker
   ```

2. **Check-then-act patterns without atomicity**:
   ```python
   # DANGEROUS: Race between check and act
   if worker_id not in self._workers:
       self._workers[worker_id] = create_worker()  # Another task may have added it!
   
   # SAFE: Use setdefault or lock
   self._workers.setdefault(worker_id, create_worker())
   ```

3. **Event wait without timeout**:
   ```python
   # DANGEROUS: Can hang forever if event never set
   await event.wait()
   
   # SAFE: Timeout with handling
   try:
       await asyncio.wait_for(event.wait(), timeout=30.0)
   except asyncio.TimeoutError:
       # Handle timeout case
   ```

4. **Concurrent iteration and modification**:
   ```python
   # DANGEROUS: Dict modified while iterating
   for worker_id in self._workers:
       if should_remove(worker_id):
           del self._workers[worker_id]  # RuntimeError!
   
   # SAFE: Iterate over copy
   for worker_id in list(self._workers.keys()):
       if should_remove(worker_id):
           del self._workers[worker_id]
   ```

**Detection Commands:**

```bash
# Find dict/set modifications in loops
grep -n "for.*in self\._[a-z_]*:" server.py | while read line; do
    linenum=$(echo $line | cut -d: -f1)
    # Check if there's a del/pop/clear in the following 20 lines
    sed -n "$((linenum+1)),$((linenum+20))p" server.py | grep -q "del\|\.pop\|\.clear\|\.discard" && echo "Potential concurrent modification at line $linenum"
done

# Find check-then-act patterns
grep -n "if.*not in self\._" server.py

# Find await without timeout
grep -n "await.*\.wait()" server.py | grep -v "wait_for"
```

**Validation Matrix:**

| Line | Pattern | Shared State | Protected? | Fix Required? |
|------|---------|--------------|------------|---------------|
| 1234 | check-then-act | `_workers` | No | **YES** |
| 2456 | concurrent iteration | `_jobs` | Yes (uses list()) | No |

### Step 6.5b: Memory Leak Detection

**What to look for:**

1. **Unbounded collection growth**:
   ```python
   # DANGEROUS: Never cleaned up
   self._completed_jobs[job_id] = result  # Grows forever!
   
   # SAFE: Cleanup after TTL or limit
   self._completed_jobs[job_id] = result
   self._task_runner.run(self._cleanup_completed_job, job_id, delay=300.0)
   ```

2. **Event/Future references held after completion**:
   ```python
   # DANGEROUS: Completion events accumulate
   self._completion_events[job_id] = asyncio.Event()
   # ...job completes...
   event.set()  # Event still in dict!
   
   # SAFE: Remove after use
   event = self._completion_events.pop(job_id, None)
   if event:
       event.set()
   ```

3. **Callback references not cleaned up**:
   ```python
   # DANGEROUS: Callbacks accumulate
   self._job_callbacks[job_id] = callback_addr
   # ...job completes, callback invoked...
   # callback_addr still in dict!
   
   # SAFE: Clean up in job cleanup path
   def _cleanup_job_state(self, job_id):
       self._job_callbacks.pop(job_id, None)
       self._completion_events.pop(job_id, None)
       # etc.
   ```

4. **Task references without cleanup**:
   ```python
   # DANGEROUS: Task references accumulate
   self._pending_tasks[task_id] = asyncio.create_task(work())
   
   # SAFE: Remove when done
   task = asyncio.create_task(work())
   task.add_done_callback(lambda t: self._pending_tasks.pop(task_id, None))
   self._pending_tasks[task_id] = task
   ```

**Detection Commands:**

```bash
# Find collections that grow without cleanup
grep -n "self\._[a-z_]*\[.*\] = " server.py > /tmp/additions.txt
grep -n "self\._[a-z_]*\.pop\|del self\._[a-z_]*\[" server.py > /tmp/removals.txt
# Compare: additions without corresponding removals are suspects

# Find Event/Future creation
grep -n "asyncio\.Event()\|asyncio\.Future()" server.py

# Find where they're cleaned up
grep -n "\.pop.*Event\|\.pop.*Future" server.py
```

**Validation Matrix:**

| Collection | Adds At | Removes At | Cleanup Path Exists? | Fix Required? |
|------------|---------|------------|---------------------|---------------|
| `_completion_events` | L1234 | L1567 | Yes (job cleanup) | No |
| `_pending_cancellations` | L2345 | **NEVER** | **NO** | **YES** |

### Step 6.5c: Dropped Error Detection

**What to look for:**

1. **Empty except blocks**:
   ```python
   # DANGEROUS: Error swallowed silently
   try:
       risky_operation()
   except Exception:
       pass  # BUG: What happened?
   
   # SAFE: Log at minimum
   try:
       risky_operation()
   except Exception as e:
       await self._logger.log(ServerError(message=str(e), ...))
   ```

2. **Fire-and-forget tasks without error handling**:
   ```python
   # DANGEROUS: Task errors go nowhere
   asyncio.create_task(self._background_work())  # If it fails, who knows?
   
   # SAFE: Use task runner with error handling
   self._task_runner.run(self._background_work)  # Runner logs errors
   ```

3. **Callbacks that can fail silently**:
   ```python
   # DANGEROUS: Callback failure not detected
   for callback in self._callbacks:
       callback(result)  # If one fails, others still run but error lost
   
   # SAFE: Wrap each callback
   for callback in self._callbacks:
       try:
           callback(result)
       except Exception as e:
           await self._logger.log(...)
   ```

4. **Ignored return values from fallible operations**:
   ```python
   # DANGEROUS: Error in returned tuple ignored
   result = await self._send_message(addr, msg)  # Returns (success, error)
   # Never check result!
   
   # SAFE: Check result
   success, error = await self._send_message(addr, msg)
   if not success:
       await self._handle_send_failure(addr, error)
   ```

**Detection Commands:**

```bash
# Find empty except blocks
grep -n "except.*:" server.py | while read line; do
    linenum=$(echo $line | cut -d: -f1)
    nextline=$((linenum + 1))
    sed -n "${nextline}p" server.py | grep -q "^\s*pass\s*$" && echo "Empty except at line $linenum"
done

# Find fire-and-forget tasks
grep -n "asyncio\.create_task\|asyncio\.ensure_future" server.py

# Find except Exception with only logging (OK) vs pass (BAD)
grep -A1 "except Exception" server.py | grep "pass"
```

**Validation Matrix:**

| Line | Pattern | Error Handled? | Fix Required? |
|------|---------|----------------|---------------|
| 1234 | empty except | No | **YES** |
| 2345 | fire-and-forget | Uses task_runner | No |

### Step 6.5d: Unbounded Queue / Backpressure Violation Detection

**What to look for:**

1. **Queues without maxsize**:
   ```python
   # DANGEROUS: Can grow without bound
   self._work_queue = asyncio.Queue()  # No limit!
   
   # SAFE: Bounded queue
   self._work_queue = asyncio.Queue(maxsize=1000)
   ```

2. **Producer faster than consumer without backpressure**:
   ```python
   # DANGEROUS: Unbounded accumulation
   async def _receive_messages(self):
       while True:
           msg = await self._socket.recv()
           self._pending_messages.append(msg)  # Never bounded!
   
   # SAFE: Apply backpressure
   async def _receive_messages(self):
       while True:
           if len(self._pending_messages) > MAX_PENDING:
               await asyncio.sleep(0.1)  # Backpressure
               continue
           msg = await self._socket.recv()
           self._pending_messages.append(msg)
   ```

3. **Retry loops without limits**:
   ```python
   # DANGEROUS: Infinite retries can exhaust memory
   while not success:
       try:
           result = await operation()
           success = True
       except Exception:
           await asyncio.sleep(1)
           # Loop forever, accumulating state each iteration?
   
   # SAFE: Limited retries
   for attempt in range(MAX_RETRIES):
       try:
           result = await operation()
           break
       except Exception:
           if attempt == MAX_RETRIES - 1:
               raise
           await asyncio.sleep(1)
   ```

4. **Accumulating work without processing limits**:
   ```python
   # DANGEROUS: Process everything at once
   pending_jobs = await self._get_all_pending_jobs()  # Could be millions!
   for job in pending_jobs:
       await self._process(job)
   
   # SAFE: Batch processing
   async for batch in self._get_pending_jobs_batched(batch_size=100):
       for job in batch:
           await self._process(job)
   ```

**Detection Commands:**

```bash
# Find unbounded queues
grep -n "asyncio\.Queue()" server.py | grep -v "maxsize"

# Find append/add without size checks
grep -n "\.append\|\.add(" server.py

# Find while True loops
grep -n "while True:" server.py

# Find retry patterns
grep -n "while not\|while.*retry\|for.*attempt" server.py
```

**Validation Matrix:**

| Line | Pattern | Bounded? | Backpressure? | Fix Required? |
|------|---------|----------|---------------|---------------|
| 1234 | Queue() | No maxsize | N/A | **YES** |
| 2345 | append in loop | No check | No | **YES** |

### Step 6.5e: Comprehensive Scan Pattern

For each file being modified, run ALL detection commands:

```bash
#!/bin/bash
# runtime_correctness_scan.sh <file>

FILE=$1

echo "=== Race Condition Scan ==="
grep -n "for.*in self\._[a-z_]*:" "$FILE"
grep -n "if.*not in self\._" "$FILE"
grep -n "await.*\.wait()" "$FILE" | grep -v "wait_for"

echo "=== Memory Leak Scan ==="
echo "Collections that add without remove:"
grep -n "self\._[a-z_]*\[.*\] = " "$FILE"

echo "=== Dropped Error Scan ==="
grep -B1 -A1 "except.*:" "$FILE" | grep -A1 "except" | grep "pass"
grep -n "asyncio\.create_task\|asyncio\.ensure_future" "$FILE"

echo "=== Unbounded Queue Scan ==="
grep -n "asyncio\.Queue()" "$FILE" | grep -v "maxsize"
grep -n "while True:" "$FILE"
```

### Step 6.5f: Fix Patterns (NO SHORTCUTS)

| Issue | Wrong Fix (Shortcut) | Correct Fix |
|-------|---------------------|-------------|
| Race condition | Add `# TODO: add lock` comment | Add actual lock or use atomic operation |
| Memory leak | Add `# TODO: cleanup` comment | Implement cleanup in appropriate lifecycle hook |
| Dropped error | Change `except: pass` to `except: pass  # intentional` | Log error or re-raise appropriately |
| Unbounded queue | Add `# Note: queue is bounded by rate limiter` | Add actual maxsize parameter |

### Step 6.5g: Integration with Other Phases

**Run BEFORE Phase 7 (Verify Completeness):**
- All race conditions identified and fixed
- All memory leak paths have cleanup
- All errors are handled or logged
- All queues are bounded with backpressure

**Run AFTER any Phase 5 fix:**
- Verify the fix didn't introduce new race conditions
- Verify the fix didn't create new leak paths
- Verify the fix didn't swallow errors
- Verify the fix didn't create unbounded accumulation

### Output

- Zero race conditions (all shared state properly protected)
- Zero memory leaks (all collections have cleanup paths)
- Zero dropped errors (all exceptions handled or logged)
- Zero unbounded queues (all collections have size limits or backpressure)

**BLOCKING**: Phase 6.5 cannot pass with ANY violations. These are production-critical bugs.

---

## Phase 7: Verify Completeness (MANDATORY - NO SHORTCUTS)

**Objective**: Ensure refactor is complete and correct.

**NO SHORTCUTS**: Do not mark items as "done" if they have workarounds. Do not skip checklist items. Every box must be honestly checked.

**MANDATORY Verification Checklist** (ALL items must pass):

| # | Check | Scanner/Command | Required Result |
|---|-------|-----------------|-----------------|
| 1 | Phase 3 method existence | Phase 3 matrix | All methods exist |
| 2 | Phase 3.5g attribute access | Automated scanner | **ZERO** violations |
| 3 | Phase 3.5h.1 chained attribute access | Chained access scanner | **ZERO** violations |
| 4 | **Phase 3.5h.2 method call validation** | Method existence scanner | **ZERO** violations |
| 5 | **Phase 3.5k.1 parameter type hints** | Untyped param scanner | **ZERO** untyped parameters |
| 6 | **Phase 3.5k.1b class attribute type hints** | Class attr scanner | **ZERO** untyped class attributes |
| 7 | **Phase 3.5k.1d incomplete generic types** | Generic param scanner | **ZERO** bare `dict`/`list`/`set`/etc. |
| 8 | Phase 4 direct state access | `grep "self._state._"` | **ZERO** matches |
| 9 | Phase 5.9 cyclomatic complexity | CC scanner | **ZERO** methods with CC > 4 |
| 10 | Phase 6.5 runtime correctness | Race/leak/error scanners | **ZERO** violations |
| 11 | LSP diagnostics | `lsp_diagnostics` | Clean on ALL modified files |
| 12 | Duplicate methods | Manual review | None across modular classes |
| 13 | Dead methods | Reference search | None in modular classes |
| 14 | Call site correctness | Manual review | All use correct component/method |
| 15 | No workarounds | `grep "proxy\|workaround\|TODO"` | No shortcut comments |
| 16 | No Any/object escape hatches | `grep ": Any\|: object"` | **ZERO** matches (or justified) |

**Execution Order**: Run checks 1-10 in order. If ANY fails, return to that phase and fix before proceeding.

**BLOCKING**: Phase 7 cannot pass with ANY violations. If ANY check fails, return to the appropriate phase and fix properly - no shortcuts. "Mostly done" is NOT done.

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

### Step 11c.1: Cross-Reference with SCENARIOS.md

For imports classified as "Nowhere in gate module", verify against SCENARIOS.md before removing.

**SCENARIOS.md is the behavior source of truth.** It documents expected message flows:

```
# Example from SCENARIOS.md:
# "18.1 Job Cancellation
#  - Client requests cancellation - Verify CancelJob handling
#  - Cancellation to managers - Verify gate forwards to all DCs
#  - Cancellation acknowledgment - Verify CancelAck handling"
```

**For each "nowhere" import:**

1. **Search SCENARIOS.md** for the type name:
   ```bash
   grep -n "ImportName" SCENARIOS.md
   ```

2. **Classification**:

   | SCENARIOS.md Status | Action |
   |---------------------|--------|
   | Listed in scenario | **UNIMPLEMENTED FEATURE** - handler is missing, implement it |
   | Not mentioned | Likely truly dead - safe to remove |
   | Mentioned but as internal/helper | Check if used transitively by other handlers |

3. **If unimplemented**: Create a tracking issue or TODO before removing the import. The import is a breadcrumb pointing to missing functionality.

**Example analysis**:
```
Import: JobCancelRequest
In module: NO
In SCENARIOS.md: YES - "18.1 Job Cancellation - Verify CancelJob handling"
Verdict: UNIMPLEMENTED or delegated to handler

Import: CorrelationSeverity  
In module: NO
In SCENARIOS.md: YES - "3.7 Cross-DC Correlation Detector"
Verdict: Check if health_coordinator handles this

Import: JitterStrategy
In module: NO
In SCENARIOS.md: NO
Verdict: Likely dead import from unused retry config
```

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

---

## Phase 12: Architecture Decision (AD) Compliance Scan

**Objective**: Verify implementation matches architectural decisions AD-9 through AD-50 (skipping AD-27).

### The Problem

Architecture Decision documents (ADs) specify required behaviors, message types, data structures, and control flows. Over time, implementation can drift from design:

- **Missing implementations**: AD specifies feature, code doesn't implement it
- **Partial implementations**: Some scenarios handled, others not
- **Divergent implementations**: Code does something different than AD specifies
- **Orphaned code**: Implementation exists but AD was superseded

### AD Compliance Matrix

**Scope**: AD-9 through AD-50, excluding AD-27

| AD | Name | Primary Node | Key Artifacts to Verify |
|----|------|--------------|------------------------|
| AD-9 | Gate State Embedding | Gate | `GateStateEmbedder`, SWIM piggyback |
| AD-10 | Versioned State Clock | All | `VersionedStateClock`, stale update rejection |
| AD-11 | Job Ledger | Gate | `JobLedger`, distributed state |
| AD-12 | Consistent Hash Ring | Gate | `ConsistentHashRing`, job routing |
| AD-13 | Job Forwarding | Gate | `JobForwardingTracker`, cross-gate routing |
| AD-14 | Stats CRDT | Gate/Manager | `JobStatsCRDT`, merge semantics |
| AD-15 | Windowed Stats | Gate/Manager | `WindowedStatsCollector`, time windows |
| AD-16 | DC Health Classification | Gate | `DatacenterHealth` enum, 4-state model |
| AD-17 | Worker Selection | Manager | Health bucket selection (HEALTHY > BUSY > DEGRADED) |
| AD-18 | Hybrid Overload Detection | All | `HybridOverloadDetector`, state transitions |
| AD-19 | Manager Health State | Gate | `ManagerHealthState`, liveness/readiness probes |
| AD-20 | Gate Health State | Gate | `GateHealthState`, peer health tracking |
| AD-21 | Circuit Breaker | All | `CircuitBreakerManager`, error thresholds |
| AD-22 | Load Shedding | All | `LoadShedder`, priority-based rejection |
| AD-23 | Backpressure (Worker) | Worker | Progress buffer, flush rate adjustment |
| AD-24 | Rate Limiting | Gate | `ServerRateLimiter`, per-client limits |
| AD-25 | Protocol Negotiation | All | `NodeCapabilities`, version negotiation |
| AD-26 | Healthcheck Extensions | Worker | Extension requests, grace periods |
| AD-28 | Role Validation | All | `RoleValidator`, mTLS claims |
| AD-29 | Discovery Service | All | `DiscoveryService`, peer registration |
| AD-30 | Hierarchical Failure Detector | Manager | Global vs job-level death detection |
| AD-31 | Orphan Job Handling | Gate/Manager | Grace period, takeover protocol |
| AD-32 | Lease Management | Gate | `JobLeaseManager`, fence tokens |
| AD-33 | Workflow State Machine | Manager/Worker | State transitions, completion events |
| AD-34 | Adaptive Job Timeout | Gate/Manager | `TimeoutStrategy`, multi-DC coordination |
| AD-35 | Job Leadership Tracking | Gate | `JobLeadershipTracker`, transfer protocol |
| AD-36 | Vivaldi Routing | Gate | `GateJobRouter`, coordinate-based selection |
| AD-37 | Backpressure Propagation | All | `BackpressureSignal`, level propagation |
| AD-38 | Capacity Aggregation | Gate | `DatacenterCapacityAggregator` |
| AD-39 | Spillover Evaluation | Gate | `SpilloverEvaluator`, cross-DC routing |
| AD-40 | Idempotency | Gate | `GateIdempotencyCache`, duplicate detection |
| AD-41 | Dispatch Coordination | Gate | `GateDispatchCoordinator` |
| AD-42 | Stats Coordination | Gate | `GateStatsCoordinator` |
| AD-43 | Cancellation Coordination | Gate | `GateCancellationCoordinator` |
| AD-44 | Leadership Coordination | Gate | `GateLeadershipCoordinator` |
| AD-45 | Route Learning | Gate | `DispatchTimeTracker`, `ObservedLatencyTracker` |
| AD-46 | Blended Latency | Gate | `BlendedLatencyScorer` |
| AD-47 | Event Logging | All | Structured log events |
| AD-48 | Cross-DC Correlation | Gate | `CrossDCCorrelationDetector` |
| AD-49 | Federated Health Monitor | Gate | `FederatedHealthMonitor`, DC probes |
| AD-50 | Manager Dispatcher | Gate | `ManagerDispatcher`, leader routing |

### Step 12a: Extract AD Requirements

For each AD, extract verifiable requirements:

```markdown
## AD-34 Requirements Checklist

### Data Structures
- [ ] `TimeoutTrackingState` dataclass exists with all fields
- [ ] `GateJobTrackingInfo` dataclass exists with all fields

### Message Types
- [ ] `JobProgressReport` message defined and handled
- [ ] `JobTimeoutReport` message defined and handled
- [ ] `JobGlobalTimeout` message defined and handled

### Behaviors
- [ ] Auto-detection: gate_addr presence selects strategy
- [ ] Local authority: manager directly times out (single-DC)
- [ ] Gate coordinated: manager reports to gate (multi-DC)
- [ ] Progress reports sent every 10s (multi-DC)
- [ ] Timeout checks run every 30s
- [ ] 5-minute fallback if gate unresponsive
- [ ] Fence token validation on global timeout receipt
- [ ] State recovery via resume_tracking() after leader transfer

### Integration Points
- [ ] Integrates with AD-26 (extension-aware timeout)
- [ ] Integrates with AD-33 (progress from state machine)
```

### Step 12b: Trace AD to Code

For each requirement, find the implementing code:

```bash
# Find data structure
grep -rn "class TimeoutTrackingState" hyperscale/distributed/

# Find message handler
grep -rn "JobProgressReport.load\|handle.*job.*progress.*report" hyperscale/distributed/nodes/

# Find behavior implementation
grep -rn "gate_addr.*strategy\|LocalAuthority\|GateCoordinated" hyperscale/distributed/
```

### Step 12c: Classification

| Status | Meaning | Action |
|--------|---------|--------|
| **COMPLIANT** | Code matches AD specification | Document, no action |
| **PARTIAL** | Some requirements met, others missing | Create TODO for missing |
| **DIVERGENT** | Code does something different | Investigate: update AD or fix code |
| **MISSING** | No implementation found | Critical: implement or mark AD as deferred |
| **SUPERSEDED** | Newer AD replaces this | Update AD status, verify no orphaned code |

### Step 12d: Generate Compliance Report

```markdown
# AD Compliance Report - Gate Module

## Summary
- Total ADs scanned: 41 (AD-9 to AD-50, excluding AD-27)
- COMPLIANT: 35
- PARTIAL: 4
- DIVERGENT: 1
- MISSING: 1

## Issues Found

### AD-34: Adaptive Job Timeout (PARTIAL)
**Missing**:
- [ ] 5-minute fallback timeout not implemented
- [ ] Progress reports not sent every 10s (currently 30s)

**Location**: `gate_job_timeout_tracker.py`

### AD-XX: ... (DIVERGENT)
**Divergence**:
- AD specifies X, code does Y
- Root cause: [reason]

**Recommendation**: [update AD | fix code]
```

### Step 12e: Resolve Issues

**For PARTIAL implementations:**
1. Add missing functionality to existing code
2. Update tests to cover new cases
3. Note completion in AD compliance report

**For DIVERGENT implementations:**
1. Determine correct behavior (consult original AD author if possible)
2. Either update AD to match code (if code is correct)
3. Or fix code to match AD (if AD is correct)
4. Document decision

**For MISSING implementations:**
1. If critical: implement immediately
2. If non-critical: create tracking issue with AD reference
3. If deliberately deferred: update AD with "Deferred" status and reason

### Step 12f: Cross-Reference with SCENARIOS.md

Every AD behavior should have corresponding scenario coverage:

```bash
# For AD-34, check SCENARIOS.md covers:
grep -n "timeout\|JobGlobalTimeout\|TimeoutReport" SCENARIOS.md
```

**If scenario missing**: Add to SCENARIOS.md before marking AD compliant.

### Step 12g: Commit Compliance Report

Store compliance report in `docs/architecture/compliance/`:

```
docs/architecture/compliance/
├── gate_compliance_2026_01_13.md
├── manager_compliance_2026_01_13.md
└── worker_compliance_2026_01_13.md
```

Include:
- Date of scan
- Commit hash scanned
- Summary statistics
- Detailed findings
- Action items with owners
