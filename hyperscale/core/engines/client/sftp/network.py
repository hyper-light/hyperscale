import ipaddress
import socket
from .types import IPNetwork


def _normalize_scoped_ip(addr: str) -> str:
    """Normalize scoped IP address

       The ipaddress module doesn't handle scoped addresses properly,
       so we normalize scoped IP addresses using socket.getaddrinfo
       before we pass them into ip_address/ip_network.

    """

    try:
        addrinfo = socket.getaddrinfo(addr, None, family=socket.AF_UNSPEC,
                                      type=socket.SOCK_STREAM,
                                      flags=socket.AI_NUMERICHOST)[0]
    except socket.gaierror:
        return addr

    if addrinfo[0] == socket.AF_INET6:
        sa = addrinfo[4]
        addr = sa[0]

        idx = addr.find('%')
        if idx >= 0: # pragma: no cover
            addr = addr[:idx]

        ip = ipaddress.ip_address(addr)

        if ip.is_link_local:
            scope_id: tuple[str, int, int, int] = sa[3]
            addr = str(ipaddress.ip_address(int(ip) | (scope_id << 96)))

    return addr


def ip_network(addr: str) -> IPNetwork:
    """Wrapper for ipaddress.ip_network which supports scoped addresses"""

    idx = addr.find('/')
    if idx >= 0:
        addr, mask = addr[:idx], addr[idx:]
    else:
        mask = ''

    return ipaddress.ip_network(_normalize_scoped_ip(addr) + mask)

