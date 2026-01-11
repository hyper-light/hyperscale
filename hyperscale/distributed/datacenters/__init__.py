"""
Datacenter management components.

This module provides datacenter-level abstractions:
- DatacenterHealthManager: DC health classification based on manager health
- ManagerDispatcher: Manager selection and routing within a DC
- LeaseManager: At-most-once delivery via leases and fence tokens
- CrossDCCorrelationDetector: Cross-DC correlation for eviction decisions (Phase 7)
"""

from hyperscale.distributed.datacenters.datacenter_health_manager import (
    DatacenterHealthManager as DatacenterHealthManager,
    ManagerInfo as ManagerInfo,
)
from hyperscale.distributed.datacenters.manager_dispatcher import (
    ManagerDispatcher as ManagerDispatcher,
    DispatchResult as DispatchResult,
    DispatchStats as DispatchStats,
)
from hyperscale.distributed.datacenters.lease_manager import (
    LeaseManager as LeaseManager,
    LeaseStats as LeaseStats,
)
from hyperscale.distributed.datacenters.cross_dc_correlation import (
    CrossDCCorrelationDetector as CrossDCCorrelationDetector,
    CrossDCCorrelationConfig as CrossDCCorrelationConfig,
    CorrelationDecision as CorrelationDecision,
    CorrelationSeverity as CorrelationSeverity,
    DCFailureRecord as DCFailureRecord,
    DCHealthState as DCHealthState,
    DCStateInfo as DCStateInfo,
    LatencySample as LatencySample,
    ExtensionRecord as ExtensionRecord,
)
