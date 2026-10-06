"""
DNS Security Validator for defense against DNS-based attacks.

Provides IP range validation and anomaly detection to protect against:
- DNS Cache Poisoning: Validates resolved IPs are in expected ranges
- DNS Hijacking: Detects unexpected IP changes
- DNS Spoofing: Alerts on suspicious resolution patterns

See: https://dnsmadeeasy.com/resources/16-dns-attacks-you-should-know-about

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import ipaddress
from dataclasses import dataclass, field
from enum import Enum
from hyperscale.distributed.runtime import Clock, RealClock

from .security_shared import _DEFAULT_CLOCK
from .dns_security_event import DNSSecurityEvent
from .dns_security_validator import DNSSecurityValidator
from .dns_security_violation import DNSSecurityViolation
from .host_history import HostHistory

_REHOMED = (
    DNSSecurityViolation,
    DNSSecurityEvent,
    HostHistory,
    DNSSecurityValidator,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
