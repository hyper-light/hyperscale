import socket

from hyperscale.distributed.discovery.dns.resolver import AsyncDNSResolver, DNSError


class HostAddressResolver:
    """
    The IPv4 address a datagram for a peer is sent to.

    A node is identified by the host it was started with -- an IP
    literal, or a stable DNS name such as a Kubernetes StatefulSet pod's
    -- and peers address it by that same host. Datagrams can only be
    sent to an IP, so a name is resolved off the event loop and cached
    by the wrapped ``AsyncDNSResolver`` for its TTL (a restarted pod's
    new IP is picked up on the next lookup); when a refresh fails, the
    last known address keeps serving. IP literals pass through untouched
    and are never cached.
    """

    __slots__ = ("_dns_resolver",)

    def __init__(self, dns_resolver: AsyncDNSResolver) -> None:
        self._dns_resolver = dns_resolver

    async def resolve(self, address: tuple[str, int]) -> tuple[str, int]:
        """
        ``address`` with its host replaced by an IPv4 address.

        Raises:
            DNSError: the name does not resolve (and has never resolved)
                to an IPv4 address.
        """
        host, port = address
        try:
            socket.inet_pton(socket.AF_INET, host)
            return address

        except OSError:
            pass

        result = await self._dns_resolver.resolve(host)
        for resolved_address in result.addresses:
            try:
                socket.inet_pton(socket.AF_INET, resolved_address)
                return (resolved_address, port)

            except OSError:
                continue

        raise DNSError(host, f"no IPv4 address among {result.addresses}")
