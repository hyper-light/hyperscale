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
    from swim import UDPServer
    
    server = UDPServer(
        host='localhost',
        tcp_port=8670,
        udp_port=8671,
        dc_id='DC-EAST',
    )
"""

# Core types and utilities
from .core import (
    # Types
    Message as Message,
    Status as Status,
    UpdateType as UpdateType,
    LeaderRole as LeaderRole,
    Nodes as Nodes,
    Ctx as Ctx,
    # Node Identity
    NodeId as NodeId,
    NodeAddress as NodeAddress,
    NodeState as NodeState,
    # Error Handling
    SwimError as SwimError,
    ErrorCategory as ErrorCategory,
    ErrorSeverity as ErrorSeverity,
    NetworkError as NetworkError,
    ConnectionRefusedError as ConnectionRefusedError,
    ProbeTimeoutError as ProbeTimeoutError,
    IndirectProbeTimeoutError as IndirectProbeTimeoutError,
    ProtocolError as ProtocolError,
    MalformedMessageError as MalformedMessageError,
    UnexpectedMessageError as UnexpectedMessageError,
    StaleMessageError as StaleMessageError,
    ResourceError as ResourceError,
    QueueFullError as QueueFullError,
    TaskOverloadError as TaskOverloadError,
    ElectionError as ElectionError,
    ElectionTimeoutError as ElectionTimeoutError,
    SplitBrainError as SplitBrainError,
    NotEligibleError as NotEligibleError,
    InternalError as InternalError,
    UnexpectedError as UnexpectedError,
    # Error Handler
    ErrorHandler as ErrorHandler,
    ErrorContext as ErrorContext,
    ErrorStats as ErrorStats,
    CircuitState as CircuitState,
    # Retry
    RetryPolicy as RetryPolicy,
    retry_with_backoff as retry_with_backoff,
    retry_with_result as retry_with_result,
    with_retry as with_retry,
    PROBE_RETRY_POLICY as PROBE_RETRY_POLICY,
    ELECTION_RETRY_POLICY as ELECTION_RETRY_POLICY,
    # Resource Limits
    BoundedDict as BoundedDict,
    CleanupConfig as CleanupConfig,
    create_cleanup_config_from_context as create_cleanup_config_from_context,
    # State Embedders (Serf-style heartbeat embedding)
    StateEmbedder as StateEmbedder,
    NullStateEmbedder as NullStateEmbedder,
    WorkerStateEmbedder as WorkerStateEmbedder,
    ManagerStateEmbedder as ManagerStateEmbedder,
    GateStateEmbedder as GateStateEmbedder,
)

# Health monitoring
from .health import (
    LocalHealthMultiplier as LocalHealthMultiplier,
    EventLoopHealthMonitor as EventLoopHealthMonitor,
    HealthSample as HealthSample,
    measure_event_loop_lag as measure_event_loop_lag,
    GracefulDegradation as GracefulDegradation  ,
    DegradationLevel as DegradationLevel,
    DegradationPolicy as DegradationPolicy,
    DEGRADATION_POLICIES as DEGRADATION_POLICIES,
)

# Failure detection
from .detection import (
    IncarnationTracker as IncarnationTracker,
    SuspicionState as SuspicionState,
    SuspicionManager as SuspicionManager,
    PendingIndirectProbe as PendingIndirectProbe,
    IndirectProbeManager as IndirectProbeManager,
    ProbeScheduler as ProbeScheduler,
)

# Gossip
from .gossip import (
    PiggybackUpdate as PiggybackUpdate,
    GossipBuffer as GossipBuffer,
    MAX_PIGGYBACK_SIZE as MAX_PIGGYBACK_SIZE,
    MAX_UDP_PAYLOAD as MAX_UDP_PAYLOAD,
)

# Leadership
from .leadership import (
    LeaderState as LeaderState,
    LeaderEligibility as LeaderEligibility,
    LocalLeaderElection as LocalLeaderElection,
    FlappingDetector as FlappingDetector,
    LeadershipChange as LeadershipChange,
)

# Main server
from .udp_server import UDPServer as UDPServer

