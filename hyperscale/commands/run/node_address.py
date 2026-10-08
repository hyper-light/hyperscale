import ipaddress
import re
from urllib.parse import SplitResult, urlsplit


NODE_ADDRESS_SCHEME = "tcp"

# One DNS label: letters, digits, hyphen, underscore (real resolvers such
# as Docker's embedded DNS serve underscore names), 1-63 characters, no
# leading/trailing hyphen. RFC 1035 caps a full name at 253 characters.
DNS_LABEL_PATTERN = re.compile(r"^(?!-)[A-Za-z0-9_-]{1,63}(?<!-)$")
DNS_NAME_MAX_LENGTH = 253


def parse_node_address(address: str) -> tuple[str, int]:
    """Parse a node address into the ``(host, port)`` tuple nodes expect.

    Accepts ``host:port``, ``[ipv6]:port``, and the AD-52 literal
    locator form ``tcp://host:port``. Parsing is delegated to
    ``urllib.parse.urlsplit`` so bracketed IPv6 and port range checks
    follow the standard grammar instead of a hand-rolled ``split(':')``.

    Raises:
        ValueError: the address contains whitespace/control characters,
            has another scheme or extra URL parts, has an invalid host,
            or a missing / non-numeric / out-of-range port.
    """
    _require_printable_address(address)

    parsed_locator = _split_locator(address)
    _require_node_address_scheme(parsed_locator, address)
    _require_bare_authority(parsed_locator, address)
    _require_valid_host(parsed_locator.hostname, address)

    return (parsed_locator.hostname, _parse_port(parsed_locator, address))


def parse_node_host(host: str, port: int) -> str:
    """The host a node is started with, validated and written the way
    ``parse_node_address`` writes hosts (DNS names lowercased, IPv6
    unbracketed). A node is identified by this exact string -- every
    frame it sends carries it -- so it must match how peers write it.

    Raises:
        ValueError: ``host`` is not an IP address or DNS name.
    """
    bracketed_host = f"[{host}]" if ":" in host else host
    return parse_node_address(f"{bracketed_host}:{port}")[0]


def parse_peer_addresses(
    tcp_addresses: list[str],
    udp_addresses: list[str],
    tcp_flag: str,
    udp_flag: str,
    own_tcp_address: tuple[str, int],
) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
    """Each peer's TCP and UDP address, paired by position, without this
    node's own entry -- every member of a cohort can be given the same
    full list.

    Raises:
        ValueError: an address is malformed, or the two lists differ in
            length.
    """
    if len(tcp_addresses) != len(udp_addresses):
        raise ValueError(
            f"{udp_flag} needs one address per {tcp_flag} address "
            f"(got {len(udp_addresses)} for {len(tcp_addresses)})"
        )

    peers = [
        (parse_node_address(tcp_address), parse_node_address(udp_address))
        for tcp_address, udp_address in zip(tcp_addresses, udp_addresses)
    ]
    other_peers = [
        (tcp_address, udp_address)
        for tcp_address, udp_address in peers
        if tcp_address != own_tcp_address
    ]
    return (
        [tcp_address for tcp_address, _ in other_peers],
        [udp_address for _, udp_address in other_peers],
    )


def _require_printable_address(address: str) -> None:
    # urlsplit silently DELETES tab/CR/LF (its CVE-2022-0391 hardening),
    # which would turn "127.0.0.1:8\t231" into port 8231 — reject every
    # whitespace or control character before the URL grammar sees it.
    if any(character.isspace() or not character.isprintable() for character in address):
        raise ValueError(
            f"node address {address!r} contains whitespace or control characters"
        )


def _split_locator(address: str) -> SplitResult:
    locator = address if "://" in address else f"{NODE_ADDRESS_SCHEME}://{address}"

    try:
        return urlsplit(locator)

    except ValueError as split_error:
        raise ValueError(f"invalid node address {address!r}: {split_error}") from split_error


def _require_node_address_scheme(parsed_locator: SplitResult, address: str) -> None:
    if parsed_locator.scheme != NODE_ADDRESS_SCHEME:
        raise ValueError(
            f"unsupported scheme {parsed_locator.scheme!r} in node address "
            f"{address!r} - expected host:port or tcp://host:port"
        )


def _require_bare_authority(parsed_locator: SplitResult, address: str) -> None:
    has_extra_parts = any(
        (
            parsed_locator.path,
            parsed_locator.query,
            parsed_locator.fragment,
            parsed_locator.username,
            parsed_locator.password,
        )
    )
    if has_extra_parts or not parsed_locator.hostname:
        raise ValueError(
            f"invalid node address {address!r} - expected host:port "
            "(IPv6 as [host]:port)"
        )


def _require_valid_host(host: str, address: str) -> None:
    if _is_ip_address(host) or _is_dns_name(host):
        return

    raise ValueError(
        f"invalid host {host!r} in node address {address!r} - expected an "
        "IP address or DNS name"
    )


def _is_ip_address(host: str) -> bool:
    try:
        ipaddress.ip_address(host)

    except ValueError:
        return False

    return True


def _is_dns_name(host: str) -> bool:
    fully_qualified_host = host.removesuffix(".")
    if not fully_qualified_host or len(fully_qualified_host) > DNS_NAME_MAX_LENGTH:
        return False

    return all(
        DNS_LABEL_PATTERN.match(label) for label in fully_qualified_host.split(".")
    )


def _parse_port(parsed_locator: SplitResult, address: str) -> int:
    try:
        port = parsed_locator.port

    except ValueError as port_error:
        raise ValueError(f"invalid port in node address {address!r}") from port_error

    if not port:
        raise ValueError(
            f"node address {address!r} needs a port between 1 and 65535"
        )

    return port
