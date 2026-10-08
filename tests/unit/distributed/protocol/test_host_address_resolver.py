"""
HostAddressResolver: the IPv4 address a datagram for a peer is sent to.

IP literals pass through without a lookup; a DNS name is sent to the
first IPv4 address it resolves to (datagram sockets are IPv4); a name
with no IPv4 address, or one that fails to resolve, raises DNSError.
"""

import pytest

from hyperscale.distributed.discovery.dns.resolver import DNSError, DNSResult
from hyperscale.distributed.server.server.host_address_resolver import (
    HostAddressResolver,
)


class RecordingDNSResolver:
    """Answers lookups from a fixed table and records each one."""

    def __init__(self, answers: dict[str, list[str] | Exception]) -> None:
        self._answers = answers
        self.lookups: list[str] = []

    async def resolve(self, hostname: str) -> DNSResult:
        self.lookups.append(hostname)
        answer = self._answers[hostname]
        if isinstance(answer, Exception):
            raise answer

        return DNSResult(hostname=hostname, addresses=answer)


@pytest.mark.asyncio
async def test_an_ip_literal_passes_through_without_a_lookup():
    dns_resolver = RecordingDNSResolver({})
    resolver = HostAddressResolver(dns_resolver)

    assert await resolver.resolve(("10.0.4.7", 8241)) == ("10.0.4.7", 8241)
    assert dns_resolver.lookups == []


@pytest.mark.asyncio
async def test_a_name_is_sent_to_its_first_ipv4_address():
    dns_resolver = RecordingDNSResolver(
        {"manager-0.managers.ns.svc.cluster.local": ["fd00::7", "10.0.4.7", "10.0.4.8"]}
    )
    resolver = HostAddressResolver(dns_resolver)

    destination = await resolver.resolve(("manager-0.managers.ns.svc.cluster.local", 8241))

    assert destination == ("10.0.4.7", 8241)
    assert dns_resolver.lookups == ["manager-0.managers.ns.svc.cluster.local"]


@pytest.mark.asyncio
async def test_a_name_without_an_ipv4_address_raises():
    resolver = HostAddressResolver(RecordingDNSResolver({"v6-only.local": ["fd00::7"]}))

    with pytest.raises(DNSError, match="no IPv4 address"):
        await resolver.resolve(("v6-only.local", 8241))


@pytest.mark.asyncio
async def test_a_name_that_fails_to_resolve_raises():
    failure = DNSError("gone.local", "getaddrinfo failed")
    resolver = HostAddressResolver(RecordingDNSResolver({"gone.local": failure}))

    with pytest.raises(DNSError, match="getaddrinfo failed"):
        await resolver.resolve(("gone.local", 8241))
