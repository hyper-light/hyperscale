"""
Cross-DC Correlation Detection for Eviction Decisions (Phase 7).

Detects when multiple datacenters are experiencing failures simultaneously,
which typically indicates a network partition or gateway issue rather than
actual datacenter failures. This prevents cascade evictions when the problem
is network connectivity rather than individual DC health.

Key scenarios:
1. Network partition between gate and DCs → multiple DCs appear unhealthy
2. Gateway failure → all DCs unreachable simultaneously
3. Cascading failures → genuine but correlated failures

When correlation is detected, the gate should:
- Delay eviction decisions
- Investigate connectivity (OOB probes, peer gates)
- Avoid marking DCs as permanently unhealthy

Anti-flapping mechanisms:
- Per-DC state machine with hysteresis for recovery
- Minimum failure duration before counting towards correlation
- Flap detection to identify unstable DCs
- Dampening of rapid state changes

Latency and extension-aware signals:
- Tracks probe latency per DC to detect network degradation vs DC failure
- Tracks extension requests to distinguish load from health issues
- Uses Local Health Multiplier (LHM) correlation across DCs
- High latency + high extensions across DCs = network issue, not DC failure

See tracker.py for within-DC correlation (workers within a manager).

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable
from hyperscale.distributed.runtime import Clock, RealClock

from .cross_dc_correlation_shared import _DEFAULT_CLOCK
from .correlation_decision import CorrelationDecision
from .correlation_severity import CorrelationSeverity
from .cross_dc_correlation_config import CrossDCCorrelationConfig
from .cross_dc_correlation_detector import CrossDCCorrelationDetector
from .dc_failure_record import DCFailureRecord
from .dc_health_state import DCHealthState
from .dc_state_info import DCStateInfo
from .extension_record import ExtensionRecord
from .latency_sample import LatencySample

_REHOMED = (
    CorrelationSeverity,
    DCHealthState,
    CorrelationDecision,
    CrossDCCorrelationConfig,
    DCFailureRecord,
    LatencySample,
    ExtensionRecord,
    DCStateInfo,
    CrossDCCorrelationDetector,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
