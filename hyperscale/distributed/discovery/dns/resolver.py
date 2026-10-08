"""
Async DNS resolver with caching for peer discovery.

Provides DNS-based service discovery with positive and negative caching,
supporting both A and SRV records. Includes security validation against
DNS cache poisoning, hijacking, and spoofing attacks.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
import socket
from dataclasses import dataclass, field
from typing import Callable
import aiodns
from hyperscale.distributed.discovery.dns.negative_cache import NegativeCache
from hyperscale.distributed.discovery.dns.security import DNSSecurityValidator, DNSSecurityEvent, DNSSecurityViolation
from hyperscale.distributed.runtime import Clock, RealClock

from .resolver_shared import _DEFAULT_CLOCK
from .async_dns_resolver import AsyncDNSResolver
from .dns_error import DNSError
from .dns_result import DNSResult
from .srv_record import SRVRecord

_REHOMED = (
    DNSError,
    SRVRecord,
    DNSResult,
    AsyncDNSResolver,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
