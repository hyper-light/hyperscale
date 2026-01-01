"""
Core types, utilities, and error handling for SWIM protocol.
"""

from .types import (
    Message,
    Status,
    UpdateType,
    LeaderRole,
    Nodes,
    Ctx,
)

from .node_id import (
    NodeId,
    NodeAddress,
)

from .node_state import NodeState

from .errors import (
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
)

from .error_handler import (
    ErrorHandler,
    ErrorContext,
    ErrorStats,
    CircuitState,
)

from .retry import (
    RetryPolicy,
    retry_with_backoff,
    retry_with_result,
    with_retry,
    PROBE_RETRY_POLICY,
    ELECTION_RETRY_POLICY,
)

from .resource_limits import (
    BoundedDict,
    CleanupConfig,
    create_cleanup_config_from_context,
)

from .metrics import Metrics

from .audit import (
    AuditEventType,
    AuditEvent,
    AuditLog,
)

from .protocols import (
    LoggerProtocol,
    TaskRunnerProtocol,
)


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
    'NodeState',
    # Errors
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
    # Error Handling
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
    # Metrics
    'Metrics',
    # Audit
    'AuditEventType',
    'AuditEvent',
    'AuditLog',
    # Protocols
    'LoggerProtocol',
    'TaskRunnerProtocol',
]

