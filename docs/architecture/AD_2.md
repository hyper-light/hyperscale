---
ad_number: 2
name: TaskRunner for All Background Tasks
description: All background/async tasks must be managed through TaskRunner, not raw asyncio.create_task()
---

# AD-2: TaskRunner for All Background Tasks

**Decision**: All background/async tasks must be managed through TaskRunner, not raw `asyncio.create_task()`.

**Rationale**:
- Prevents orphaned tasks on shutdown
- Provides cancellation via tokens
- Enables task lifecycle monitoring
- Centralizes cleanup logic

**Implementation**:
- `self._task_runner.run(coro, *args)` returns a token
- `self._task_runner.cancel(token)` for cancellation
- Cleanup loops, state sync, progress reporting all use TaskRunner
