"""
SWIM Protocol Error Hierarchy

Categorized exceptions for robust error handling in distributed
failure detection. Errors are classified by:
- Category: What kind of error (network, protocol, resource, internal)
- Severity: How serious (transient, degraded, fatal)

This enables appropriate recovery actions and LHM adjustments.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from enum import Enum, auto
from dataclasses import dataclass, field
import traceback

from .assertion_error import AssertionError
from .connection_refused_error import ConnectionRefusedError
from .election_error import ElectionError
from .election_timeout_error import ElectionTimeoutError
from .error_category import ErrorCategory
from .error_severity import ErrorSeverity
from .indirect_probe_timeout_error import IndirectProbeTimeoutError
from .internal_error import InternalError
from .malformed_message_error import MalformedMessageError
from .network_error import NetworkError
from .not_eligible_error import NotEligibleError
from .probe_timeout_error import ProbeTimeoutError
from .protocol_error import ProtocolError
from .queue_full_error import QueueFullError
from .quorum_circuit_open_error import QuorumCircuitOpenError
from .quorum_error import QuorumError
from .quorum_timeout_error import QuorumTimeoutError
from .quorum_unavailable_error import QuorumUnavailableError
from .resource_error import ResourceError
from .split_brain_error import SplitBrainError
from .stale_message_error import StaleMessageError
from .swim_error import SwimError
from .task_overload_error import TaskOverloadError
from .unexpected_error import UnexpectedError
from .unexpected_message_error import UnexpectedMessageError

_REHOMED = (
    ErrorSeverity,
    ErrorCategory,
    SwimError,
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
    QuorumError,
    QuorumUnavailableError,
    QuorumTimeoutError,
    QuorumCircuitOpenError,
    InternalError,
    AssertionError,
    UnexpectedError,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
