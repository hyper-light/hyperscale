"""
Datacenter management components.

This module provides datacenter-level abstractions:
- DatacenterHealthManager: DC health classification based on manager health
- ManagerDispatcher: Manager selection and routing within a DC
- LeaseManager: At-most-once delivery via leases and fence tokens
- CrossDCCorrelationDetector: Cross-DC correlation for eviction decisions (Phase 7)
- DatacenterOverloadClassifier: Threshold-based DC health classification
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
    DatacenterLeaseManager as DatacenterLeaseManager,
    LeaseStats as LeaseStats,
)

LeaseManager = DatacenterLeaseManager
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
from hyperscale.distributed.datacenters.datacenter_overload_config import (
    DatacenterOverloadConfig as DatacenterOverloadConfig,
    DatacenterOverloadState as DatacenterOverloadState,
    OVERLOAD_STATE_ORDER as OVERLOAD_STATE_ORDER,
)
from hyperscale.distributed.datacenters.datacenter_overload_classifier import (
    DatacenterOverloadClassifier as DatacenterOverloadClassifier,
    DatacenterOverloadSignals as DatacenterOverloadSignals,
    DatacenterOverloadResult as DatacenterOverloadResult,
)
