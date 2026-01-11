"""
Worker server module.

This module re-exports WorkerServer from the original worker.py
for backward compatibility during the refactoring process.

Once the refactoring is complete (Phase 15.2.7), this file will
contain the composition root that wires all modules together.
"""

# Re-export from original implementation for backward compatibility
# The original worker.py remains the source of truth during refactoring
from hyperscale.distributed_rewrite.nodes.worker_original import WorkerServer

__all__ = ["WorkerServer"]
