"""
Gate job dispatch module.

Provides centralized dispatch to datacenter managers with retry and fallback.

Classes:
- ManagerDispatcher: Centralized dispatch with retry/fallback logic

This is re-exported from the datacenters package.
"""

from hyperscale.distributed_rewrite.datacenters import ManagerDispatcher

__all__ = [
    "ManagerDispatcher",
]
