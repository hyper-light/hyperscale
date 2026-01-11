"""DNS resolution components for the discovery system."""

from hyperscale.distributed_rewrite.discovery.dns.negative_cache import (
    NegativeCache as NegativeCache,
    NegativeEntry as NegativeEntry,
)
from hyperscale.distributed_rewrite.discovery.dns.resolver import (
    AsyncDNSResolver as AsyncDNSResolver,
    DNSResult as DNSResult,
    DNSError as DNSError,
)
