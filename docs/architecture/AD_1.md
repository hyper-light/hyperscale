---
ad_number: 1
name: Composition Over Inheritance
description: Extensibility is via callbacks and composition; base-server overrides are limited to deliberate template hooks
---

# AD-1: Composition Over Inheritance

**Decision**: Extensibility is via callbacks and composition. Node servers override base-server methods only where the base defines a deliberate template hook.

**Rationale**:
- Prevents fragile base class problems
- Makes dependencies explicit
- Easier to test individual components
- Allows runtime reconfiguration

**Implementation**:
- `StateEmbedder` protocol for heartbeat embedding
- Leadership callbacks: `register_on_become_leader()`, `register_on_lose_leadership()`
- Node status callbacks: `register_on_node_dead()`, `register_on_node_join()`
- All node types (Worker, Manager, Gate) subclass `HealthAwareServer` and register these callbacks
  (e.g. `nodes/manager/server.py:931-933`, `nodes/gate/server.py:694-697`, `nodes/worker/server.py:485-486`)
  for leadership and membership events.
- Overrides are template hooks the base calls by design: lifecycle (`stop`, `abort`), join/leave targets
  (`_join_node`, `_get_leave_targets`), address resolution (`_get_registered_node_id_for_addr`), election
  membership (`_get_election_member_count`, `_is_election_cohort_voter`), and the manager's SWIM piggyback
  get/process pairs (worker state, extension decisions and outcomes). Changing the doc rather than the code
  was decided on merit (REMAINING_WORK_PLAN D12, 2026-10-05): these hooks are the better design.
