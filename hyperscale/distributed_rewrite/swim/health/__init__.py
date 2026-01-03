"""
Health monitoring and graceful degradation for SWIM protocol.
"""

from .local_health_multiplier import LocalHealthMultiplier

from .health_monitor import (
    EventLoopHealthMonitor,
    HealthSample,
    measure_event_loop_lag,
)

from .graceful_degradation import (
    GracefulDegradation,
    DegradationLevel,
    DegradationPolicy,
    DEGRADATION_POLICIES,
)

from .federated_health_monitor import (
    FederatedHealthMonitor,
    DCHealthState,
    DCReachability,
    CrossClusterProbe,
    CrossClusterAck,
    DCLeaderAnnouncement,
)


__all__ = [
    # Local Health Multiplier
    'LocalHealthMultiplier',
    # Event Loop Monitoring
    'EventLoopHealthMonitor',
    'HealthSample',
    'measure_event_loop_lag',
    # Graceful Degradation
    'GracefulDegradation',
    'DegradationLevel',
    'DegradationPolicy',
    'DEGRADATION_POLICIES',
    # Federated Health Monitor
    'FederatedHealthMonitor',
    'DCHealthState',
    'DCReachability',
    'CrossClusterProbe',
    'CrossClusterAck',
    'DCLeaderAnnouncement',
]

