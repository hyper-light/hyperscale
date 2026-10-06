"""
Centralized Error Handler for SWIM Protocol

Provides:
- Circuit breaker pattern for cascading failure prevention
- Error rate tracking with sliding window
- LHM integration for health-aware error handling
- Recovery action registration
- Structured logging integration

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass, field
from typing import Callable, Awaitable
from collections import deque
from enum import Enum, auto
import asyncio
import traceback
from hyperscale.logging.hyperscale_logging_models import ServerError
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.logging.hyperscale_logging_models import ServerDebug
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning, ServerError, ServerFatal

from .errors import (
    SwimError,
    ErrorCategory,
    ErrorSeverity,
    NetworkError,
    ProtocolError,
    ResourceError,
    ElectionError,
    InternalError,
    UnexpectedError,
)
from .protocols import LoggerProtocol
from .error_stats import _DEFAULT_CLOCK
from .circuit_state import CircuitState
from .error_context import ErrorContext
from .error_handler_impl import ErrorHandler
from .error_stats import ErrorStats

_REHOMED = (
    CircuitState,
    ErrorStats,
    ErrorHandler,
    ErrorContext,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
