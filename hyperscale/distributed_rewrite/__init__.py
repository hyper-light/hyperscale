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
"""

# Re-export SWIM for healthchecks
from .swim import HealthAwareServer as SwimServer

# Node types
from .nodes import (
    WorkerServer as WorkerServer,
    ManagerServer as ManagerServer,
    GateServer as GateServer,
)

