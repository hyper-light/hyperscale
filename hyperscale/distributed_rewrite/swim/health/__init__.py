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

from .peer_health_awareness import (
    PeerHealthAwareness,
    PeerHealthAwarenessConfig,
    PeerHealthInfo,
    PeerLoadLevel,
)

from .out_of_band_health_channel import (
    OutOfBandHealthChannel,
    OOBHealthChannelConfig,
    OOBProbeResult,
    get_oob_port_for_swim_port,
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
    # Peer Health Awareness (Phase 6.2)
    'PeerHealthAwareness',
    'PeerHealthAwarenessConfig',
    'PeerHealthInfo',
    'PeerLoadLevel',
    # Out-of-Band Health Channel (Phase 6.3)
    'OutOfBandHealthChannel',
    'OOBHealthChannelConfig',
    'OOBProbeResult',
    'get_oob_port_for_swim_port',
]

