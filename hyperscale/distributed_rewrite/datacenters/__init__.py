"""
Datacenter management components.

This module provides datacenter-level abstractions:
- DatacenterHealthManager: DC health classification based on manager health
- ManagerDispatcher: Manager selection and routing within a DC
"""

from hyperscale.distributed_rewrite.datacenters.datacenter_health_manager import (
    DatacenterHealthManager as DatacenterHealthManager,
    ManagerInfo as ManagerInfo,
)
from hyperscale.distributed_rewrite.datacenters.manager_dispatcher import (
    ManagerDispatcher as ManagerDispatcher,
    DispatchResult as DispatchResult,
    DispatchStats as DispatchStats,
)
