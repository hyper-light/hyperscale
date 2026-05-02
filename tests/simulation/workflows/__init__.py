"""Reusable workflow shapes for simulation scenarios.

Phase 2 ships ``SimpleWorkflow`` only — a minimal workflow that runs no
external IO so harness smoke tests do not depend on network reachability.
The catalog grows in later phases (DependentWorkflow, LongRunningWorkflow,
CancellingWorkflow, PanickingWorkflow, LeakyWorkflow).
"""

from tests.simulation.workflows.simple_workflow import SimpleWorkflow


__all__ = ["SimpleWorkflow"]
