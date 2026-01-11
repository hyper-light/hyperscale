---
ad_number: 9
name: Retry Data Preserved at Dispatch
description: Original WorkflowDispatch bytes are stored when workflow is first dispatched, not reconstructed on retry
---

# AD-9: Retry Data Preserved at Dispatch

**Decision**: Original `WorkflowDispatch` bytes are stored when workflow is first dispatched, not reconstructed on retry.

**Rationale**:
- Ensures retry has exact same parameters (VUs, timeout, context)
- Avoids serialization round-trip errors
- Simplifies retry logic

**Implementation**:
- `_workflow_retries[workflow_id] = (count, original_dispatch_bytes, failed_workers)`
- On retry: deserialize original, create new dispatch with updated fence_token
- `failed_workers` set prevents re-dispatching to same worker
