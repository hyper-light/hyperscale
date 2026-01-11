"""
Hyperscale Distributed Rewrite Module.

This module provides the distributed infrastructure for Hyperscale,
including:
- SWIM + Lifeguard UDP healthchecks
- TCP-based state sync and job management
- Gate, Manager, and Worker node types

Architecture:
    Client -> Gate -> Manager -> Worker

    - Gate (optional): Cross-datacenter coordination, global job state
    - Manager: Per-DC orchestration, quorum-based provisioning
    - Worker: Workflow execution, absolute source of truth for local state

    All nodes use UDP for SWIM healthchecks and TCP for data operations.

Usage:
    # Import nodes directly from their submodules to avoid circular imports
    from hyperscale.distributed_rewrite.nodes import WorkerServer, ManagerServer, GateServer
    from hyperscale.distributed_rewrite.swim import HealthAwareServer as SwimServer
"""

# Note: We intentionally do NOT re-export nodes here to avoid circular imports.
# The circular import chain is:
#   distributed_rewrite -> nodes -> worker -> remote_graph_manager -> protocols -> rate_limiter -> reliability
#
# Import nodes directly:
#   from hyperscale.distributed_rewrite.nodes import WorkerServer, ManagerServer, GateServer
