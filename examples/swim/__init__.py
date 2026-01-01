"""
SWIM + Lifeguard Protocol Implementation

A Python implementation of the SWIM (Scalable Weakly-consistent 
Infection-style Process Group Membership) protocol with Lifeguard
enhancements for more accurate failure detection.

Components:
- LocalHealthMultiplier: Tracks node's own health score for adaptive timeouts
- IncarnationTracker: Manages incarnation numbers for message ordering
- SuspicionManager: Handles the suspicion subprotocol with dynamic timeouts
- IndirectProbeManager: Manages indirect probing via proxy nodes
- GossipBuffer: Handles piggybacking of membership updates
- ProbeScheduler: Implements randomized round-robin probing
- LocalLeaderElection: Hierarchical lease-based leadership election

Error Handling:
- SwimError: Base exception with category/severity classification
- ErrorHandler: Centralized handler with circuit breaker
- retry_with_backoff: Exponential backoff with jitter

Usage:
    from swim import TestServer
    
    server = TestServer(
        host='localhost',
        tcp_port=8670,
        udp_port=8671,
        dc_id='DC-EAST',
    )
"""

from .types import (
    Message,
    Status,
    UpdateType,
    LeaderRole,
    Nodes,
    Ctx,
)

from .node_id import NodeId, NodeAddress

# Error handling
from .errors import (
    SwimError,
    ErrorCategory,
    ErrorSeverity,
    NetworkError,
    ProbeTimeoutError,
    IndirectProbeTimeoutError,
    ConnectionRefusedError,
    ProtocolError,
    MalformedMessageError,
    UnexpectedMessageError,
    StaleMessageError,
    ResourceError,
    QueueFullError,
    TaskOverloadError,
    ElectionError,
    SplitBrainError,
    ElectionTimeoutError,
    NotEligibleError,
    InternalError,
    UnexpectedError,
)
from .error_handler import (
    ErrorHandler,
    ErrorStats,
    ErrorContext,
    CircuitState,
)
from .retry import (
    RetryPolicy,
    RetryResult,
    RetryDecision,
    retry_with_backoff,
    retry_with_result,
    with_retry,
    PROBE_RETRY_POLICY,
    ELECTION_RETRY_POLICY,
    GOSSIP_RETRY_POLICY,
)

from .resource_limits import (
    BoundedDict,
    CleanupConfig,
    create_cleanup_config_from_context,
)

from .local_health_multiplier import LocalHealthMultiplier
from .node_state import NodeState
from .incarnation_tracker import IncarnationTracker
from .suspicion_state import SuspicionState
from .suspicion_manager import SuspicionManager
from .pending_indirect_probe import PendingIndirectProbe
from .indirect_probe_manager import IndirectProbeManager
from .piggyback_update import PiggybackUpdate
from .gossip_buffer import GossipBuffer
from .probe_scheduler import ProbeScheduler
from .leader_eligibility import LeaderEligibility
from .leader_state import LeaderState
from .local_leader_election import LocalLeaderElection
from .server import TestServer

__all__ = [
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
    # Error Handling - Base
    'SwimError',
    'ErrorCategory',
    'ErrorSeverity',
    # Error Handling - Network Errors
    'NetworkError',
    'ProbeTimeoutError',
    'IndirectProbeTimeoutError',
    'ConnectionRefusedError',
    # Error Handling - Protocol Errors
    'ProtocolError',
    'MalformedMessageError',
    'UnexpectedMessageError',
    'StaleMessageError',
    # Error Handling - Resource Errors
    'ResourceError',
    'QueueFullError',
    'TaskOverloadError',
    # Error Handling - Election Errors
    'ElectionError',
    'SplitBrainError',
    'ElectionTimeoutError',
    'NotEligibleError',
    # Error Handling - Internal Errors
    'InternalError',
    'UnexpectedError',
    # Error Handler
    'ErrorHandler',
    'ErrorStats',
    'ErrorContext',
    'CircuitState',
    # Retry Utilities
    'RetryPolicy',
    'RetryResult',
    'RetryDecision',
    'retry_with_backoff',
    'retry_with_result',
    'with_retry',
    'PROBE_RETRY_POLICY',
    'ELECTION_RETRY_POLICY',
    'GOSSIP_RETRY_POLICY',
    # Resource Limits
    'BoundedDict',
    'CleanupConfig',
    'create_cleanup_config_from_context',
    # Components
    'LocalHealthMultiplier',
    'NodeState',
    'IncarnationTracker',
    'SuspicionState',
    'SuspicionManager',
    'PendingIndirectProbe',
    'IndirectProbeManager',
    'PiggybackUpdate',
    'GossipBuffer',
    'ProbeScheduler',
    'LeaderEligibility',
    'LeaderState',
    'LocalLeaderElection',
    # Server
    'TestServer',
]

