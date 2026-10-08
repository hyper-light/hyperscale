# Refactor Plan: Gate/Manager/Worker Servers

> **Status (checked 2026-10-06).** One class per file holds across `hyperscale/distributed` (lint `tests/simulation/lints/test_one_class_per_file.py`). Dataclass placement and `slots=True` are held by the ratchet `test_dataclass_conventions.py`; the node dataclasses now live in their node's `models/` package with `slots=True` (2026-10-07: `ClientConfig`, `ManagerConfig`, `WorkerConfig`, `ExtensionTriggerConfig`, `_PerWorkflowTriggerState`, `PendingResult`; the `config.py` modules keep only the env factories, and `extension_trigger.py`/`progress.py` stay the classes' pickle namespaces); the rest of the repo's outside-`models/` dataclasses are the ratchet snapshot's entries. Complexity: see the constraint below. **Lines of code grew, not shrank:** the complexity splits added methods in place, so `nodes/manager/server.py` is ~14,600 lines (10,810 in 2026-08), `nodes/gate/server.py` ~10,100 (6,901) and `swim/health_aware_server.py` ~7,600 (6,547); moving those domains into composed classes (REMAINING_WORK_PLAN Phase 8) has not started.

## Goals
- Enforce one-class-per-file across gate/manager/worker/client code.
- Group related logic into cohesive submodules with explicit boundaries.
- Ensure all dataclasses use `slots=True` and live in a `models/` submodule.
- Preserve behavior and interfaces; refactor in small, safe moves.
- Prefer list/dict comprehensions, walrus operators, and early returns.
- Reduce the number of lines of code significantly
- Optimize for readability *and* performance.

## Constraints
- One class per file (including nested helper classes).
- Dataclasses must be defined in `models/` submodules and declared with `slots=True`.
- Keep async patterns, TaskRunner usage, and logging patterns intact.
- Avoid new architectural behavior changes while splitting files.
- ~~Maximum cyclic complexity of 5 for classes and 4 for functions.~~ Superseded (REMAINING_WORK_PLAN D7): the ceiling is **3** per function, as CLAUDE.md states, enforced by the ratchet `tests/simulation/lints/test_complexity_ceiling.py` (`COMPLEXITY_CEILING = 3`) against a snapshot of current violators (1,566 functions, 244 under `hyperscale/distributed`, as of 2026-10-06). Owner decision 2026-10-06 (path-heat rule): per-message hot paths -- transport send/receive, SWIM per-datagram, Raft per-heartbeat, state embedders -- stay inlined; the control plane decomposes to ≤ 3.
- Examine AD-10 through AD-37 in architecture.md. DO NOT BREAK COMPLIANCE with any of these.
- Once you have generated a file or refactored any function/method/tangible unit of code, generate a commit.


## Style Refactor Guidance
- **Comprehensions**: replace loop-based list/dict builds where possible.
  - Example: `result = {dc: self._classify_datacenter_health(dc) for dc in dcs}`
- **Early returns**: reduce nested control flow.
  - Example: `if not payload: return None`
- **Walrus operator**: use to avoid repeated lookups.
  - Example: `if not (job := self._state.job_manager.get_job(job_id)):
      return`

## Verification Strategy
- Run LSP diagnostics on touched files.
- No integration tests (per repo guidance).
- Ensure all public protocol messages and network actions are unchanged.

