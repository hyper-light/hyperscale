# Refactor Plan: Gate/Manager/Worker Servers

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
- Maximum cyclic complexity of 5 for classes and 4 for functions.
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

