"""
Worker server composition root (Phase 15.2.7 placeholder).

This file will eventually contain the refactored WorkerServer as a
composition root that wires all modules together.

Currently, WorkerServer is imported from worker_impl.py via the
package __init__.py.
"""

# Re-export from parent package __init__.py for convenience
from hyperscale.distributed_rewrite.nodes.worker_impl import WorkerServer

__all__ = ["WorkerServer"]
