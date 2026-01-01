"""
SWIM + Lifeguard Protocol Implementation

A Python implementation of the SWIM (Scalable Weakly-consistent 
Infection-style Process Group Membership) protocol with Lifeguard
enhancements for more accurate failure detection.

Submodules:
- core: Types, node identity, errors, retry utilities, resource limits
- health: Local health multiplier, event loop monitoring, graceful degradation
- detection: Failure detection (incarnation, suspicion, probing)
- gossip: Message piggybacking and dissemination
- leadership: Leader election with flapping detection

Usage:
    from swim import TestServer
    
    server = TestServer(
        host='localhost',
        tcp_port=8670,
        udp_port=8671,
        dc_id='DC-EAST',
    )
"""

# Core types and utilities
from .core import (
    # Types
    Message,
    Status,
    UpdateType,
    LeaderRole,
    Nodes,
    Ctx,
    # Node Identity
    NodeId,
    NodeAddress,
    NodeState,
    # Error Handling
    SwimError,
    ErrorCategory,
    ErrorSeverity,
    NetworkError,
    ConnectionRefusedError,
    ProbeTimeoutError,
    IndirectProbeTimeoutError,
    ProtocolError,
    MalformedMessageError,
    UnexpectedMessageError,
    StaleMessageError,
    ResourceError,
    QueueFullError,
    TaskOverloadError,
    ElectionError,
    ElectionTimeoutError,
    SplitBrainError,
    NotEligibleError,
    InternalError,
    UnexpectedError,
    # Error Handler
    ErrorHandler,
    ErrorContext,
    ErrorStats,
    CircuitState,
    # Retry
    RetryPolicy,
    retry_with_backoff,
    retry_with_result,
    with_retry,
    PROBE_RETRY_POLICY,
    ELECTION_RETRY_POLICY,
    # Resource Limits
    BoundedDict,
    CleanupConfig,
    create_cleanup_config_from_context,
)

# Health monitoring
from .health import (
    LocalHealthMultiplier,
    EventLoopHealthMonitor,
    HealthSample,
    measure_event_loop_lag,
    GracefulDegradation,
    DegradationLevel,
    DegradationPolicy,
    DEGRADATION_POLICIES,
)

# Failure detection
from .detection import (
    IncarnationTracker,
    SuspicionState,
    SuspicionManager,
    PendingIndirectProbe,
    IndirectProbeManager,
    ProbeScheduler,
)

# Gossip
from .gossip import (
    PiggybackUpdate,
    GossipBuffer,
    MAX_PIGGYBACK_SIZE,
    MAX_UDP_PAYLOAD,
)

# Leadership
from .leadership import (
    LeaderState,
    LeaderEligibility,
    LocalLeaderElection,
    FlappingDetector,
    LeadershipChange,
)

# Main server
from .server import TestServer


__all__ = [
    # === Core ===
    # Types
    'Message',
    'Status',
    'UpdateType',
    'LeaderRole',
    'Nodes',
    'Ctx',
    # Node Identity
    'NodeId',
    'NodeAddress',
    'NodeState',
    # Error Handling
    'SwimError',
    'ErrorCategory',
    'ErrorSeverity',
    'NetworkError',
    'ConnectionRefusedError',
    'ProbeTimeoutError',
    'IndirectProbeTimeoutError',
    'ProtocolError',
    'MalformedMessageError',
    'UnexpectedMessageError',
    'StaleMessageError',
    'ResourceError',
    'QueueFullError',
    'TaskOverloadError',
    'ElectionError',
    'ElectionTimeoutError',
    'SplitBrainError',
    'NotEligibleError',
    'InternalError',
    'UnexpectedError',
    # Error Handler
    'ErrorHandler',
    'ErrorContext',
    'ErrorStats',
    'CircuitState',
    # Retry
    'RetryPolicy',
    'retry_with_backoff',
    'retry_with_result',
    'with_retry',
    'PROBE_RETRY_POLICY',
    'ELECTION_RETRY_POLICY',
    # Resource Limits
    'BoundedDict',
    'CleanupConfig',
    'create_cleanup_config_from_context',
    
    # === Health ===
    'LocalHealthMultiplier',
    'EventLoopHealthMonitor',
    'HealthSample',
    'measure_event_loop_lag',
    'GracefulDegradation',
    'DegradationLevel',
    'DegradationPolicy',
    'DEGRADATION_POLICIES',
    
    # === Detection ===
    'IncarnationTracker',
    'SuspicionState',
    'SuspicionManager',
    'PendingIndirectProbe',
    'IndirectProbeManager',
    'ProbeScheduler',
    
    # === Gossip ===
    'PiggybackUpdate',
    'GossipBuffer',
    'MAX_PIGGYBACK_SIZE',
    'MAX_UDP_PAYLOAD',
    
    # === Leadership ===
    'LeaderState',
    'LeaderEligibility',
    'LocalLeaderElection',
    'FlappingDetector',
    'LeadershipChange',
    
    # === Server ===
    'TestServer',
]
