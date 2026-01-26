---
ad_number: 1
name: Composition Over Inheritance
description: All extensibility is via callbacks and composition, never method overriding
---

# AD-1: Composition Over Inheritance

**Decision**: All extensibility is via callbacks and composition, never method overriding.

**Rationale**:
- Prevents fragile base class problems
- Makes dependencies explicit
- Easier to test individual components
- Allows runtime reconfiguration

**Implementation**:
- `StateEmbedder` protocol for heartbeat embedding
- Leadership callbacks: `register_on_become_leader()`, `register_on_lose_leadership()`
- Node status callbacks: `register_on_node_dead()`, `register_on_node_join()`
- All node types (Worker, Manager, Gate) use these instead of overriding UDPServer methods
