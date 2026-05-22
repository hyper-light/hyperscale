"""
Seed-locator package (AD-52 §2).

Five URI schemes resolved internally to (host, port) tuples:

    tcp://      Literal address.
    dns://      OS A/AAAA resolution (uses /etc/hosts, NSS, DNS).
    dns-srv://  SRV record lookup; service+port from the record.
    file://     Newline-delimited locator list; mtime watched.
    exec://     Command stdout; one locator per line; periodic refresh.

The locator system is the only environment-facing primitive on which
hyperscale cluster bootstrap depends. Every other detail (orchestrator
identity, K8s API, multicast, NTP) is explicitly forbidden by AD-52 §1.
"""

from .base import SeedLocator as SeedLocator
from .resolved_address import ResolvedAddress as ResolvedAddress
from .resolver import SeedResolver as SeedResolver
from .resolver import parse_locator_uri as parse_locator_uri
from .tcp_locator import TcpLocator as TcpLocator
from .dns_locator import DnsLocator as DnsLocator
from .dns_srv_locator import DnsSrvLocator as DnsSrvLocator
from .file_locator import FileLocator as FileLocator
from .exec_locator import ExecLocator as ExecLocator
