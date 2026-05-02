"""Reusable workflow shapes for the distributed simulation harness.

Lives under ``hyperscale.distributed.testing.*`` rather than ``tests/``
because workflows are cloudpickled into JobSubmission / WorkflowDispatch
messages and have to deserialize on both the manager and the worker. The
``RestrictedUnpickler`` (deliberately) only allows ``hyperscale.*`` and a
small stdlib allowlist; placing test fixtures here keeps the security
boundary intact instead of poking a hole in it for test convenience.

Phase 2 ships ``SimpleWorkflow`` only — a minimal workflow that runs no
external IO so harness smoke tests do not depend on network reachability.
The catalog grows in later phases (DependentWorkflow, LongRunningWorkflow,
CancellingWorkflow, PanickingWorkflow, LeakyWorkflow).
"""

from hyperscale.distributed.testing.workflows.simple_workflow import (
    SimpleWorkflow,
)


__all__ = ["SimpleWorkflow"]
