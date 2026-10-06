"""
Seed locators (AD-52 section 2): a cohort flag's entries may be locators the
node resolves itself, so any environment can hand it its cohort with one
string -- ``tcp://host:port`` (or bare ``host:port``), ``dns://name:port``
(every A/AAAA record of the name), ``dns-srv://_service._proto.name`` (every
SRV target and port), ``file:///absolute/path`` and ``exec:///absolute/path``
(one locator per line, of the first three kinds).

A cohort is resolved once, at launch: it fixes the quorum floor, and only a
committed resize changes it after. Two nodes whose resolutions differ hold
different cohorts and refuse each other (the cohort digest), so a
disagreement can stall formation but never split the cluster. A
resolution's size is checked against the size every founder agrees on
(``--cohort-size``): a Kubernetes headless service publishes only ready
pods, and a pod that resolved itself alone would otherwise count a cohort of
one and found a cluster beside the real one.
"""

import asyncio
import os
import pathlib
import socket
import stat
import sys
import time
from urllib.parse import urlsplit

from hyperscale.distributed.discovery.dns.resolver import AsyncDNSResolver, DNSError

from .node_address import _is_dns_name, _is_ip_address, parse_node_address

DYNAMIC_LOCATOR_SCHEMES = frozenset({"dns", "dns-srv", "file", "exec"})
LISTING_LOCATOR_SCHEMES = frozenset({"file", "exec"})


def is_dynamic_locator(entry: str) -> bool:
    """Whether ``entry`` is resolved at launch rather than a literal address."""
    return "://" in entry and entry.split("://", 1)[0] in DYNAMIC_LOCATOR_SCHEMES


async def resolve_locators(
    entries: list[str], flag: str, resolution_timeout_seconds: float
) -> list[tuple[str, int]]:
    """Every address ``entries`` resolve to, without duplicates, in order.
    A ``file://`` or ``exec://`` locator lists further locators, which may
    not list any themselves (no nesting).

    Raises:
        ValueError: a locator is malformed, fails to resolve, or names a
            path that fails the ownership checks.
    """
    resolved: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for entry in entries:
        listed = (
            await _listed_locators(entry, flag, resolution_timeout_seconds)
            if "://" in entry and entry.split("://", 1)[0] in LISTING_LOCATOR_SCHEMES
            else [entry]
        )
        for locator in listed:
            if "://" in locator and locator.split("://", 1)[0] in LISTING_LOCATOR_SCHEMES:
                raise ValueError(f"{flag}: {entry} lists {locator!r}; a listing locator may not list another")
            for address in await _resolve_address_locator(locator, flag, resolution_timeout_seconds):
                if address not in seen:
                    seen.add(address)
                    resolved.append(address)
    return resolved


async def resolve_cohort_addresses(
    tcp_entries: list[str],
    udp_entries: list[str],
    tcp_flag: str,
    udp_flag: str,
    own_tcp_address: tuple[str, int],
    cohort_size: int | None,
    within_seconds: float,
    retry_interval_seconds: float,
    resolution_timeout_seconds: float,
) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
    """The cohort's peers' TCP and UDP addresses, resolved from locators and
    paired by host (members sharing a host by ascending port) -- this node
    itself left out.

    Resolution repeats every ``retry_interval_seconds`` until it yields
    exactly ``cohort_size`` members, this node among them, or
    ``within_seconds`` pass.

    Raises:
        ValueError: ``cohort_size`` is missing, a locator is malformed, a
            host has not one UDP address per TCP address, or the
            resolution never reached the cohort's size with this node in it.
    """
    if cohort_size is None or cohort_size < 1:
        raise ValueError(
            f"{tcp_flag} locators resolve at launch: give --cohort-size, the number of members "
            "every founder agrees the cohort has"
        )
    deadline = time.monotonic() + within_seconds
    while True:
        tcp_addresses = await resolve_locators(tcp_entries, tcp_flag, resolution_timeout_seconds)
        udp_addresses = await resolve_locators(udp_entries, udp_flag, resolution_timeout_seconds)
        # Pair by host -- answer order is not stable -- and, among one
        # host's members, by ascending port (the UDP port beside its TCP).
        tcp_ports_by_host: dict[str, list[int]] = {}
        for tcp_host, tcp_port in tcp_addresses:
            tcp_ports_by_host.setdefault(tcp_host, []).append(tcp_port)
        udp_ports_by_host: dict[str, list[int]] = {}
        for udp_host, udp_port in udp_addresses:
            udp_ports_by_host.setdefault(udp_host, []).append(udp_port)
        udp_by_tcp = {
            (host, tcp_port): (host, udp_port)
            for host, tcp_ports in tcp_ports_by_host.items()
            if len(tcp_ports) == len(udp_ports_by_host.get(host, ()))
            for tcp_port, udp_port in zip(sorted(tcp_ports), sorted(udp_ports_by_host[host]))
        }
        if len(udp_by_tcp) != len(tcp_addresses) or len(udp_addresses) != len(tcp_addresses):
            shortfall = (
                f"{tcp_flag} resolved {tcp_addresses}, {udp_flag} resolved {udp_addresses}: "
                "not one UDP address per TCP address on each host"
            )
        elif own_tcp_address not in udp_by_tcp:
            shortfall = (
                f"this node's {own_tcp_address[0]}:{own_tcp_address[1]} is not among {tcp_addresses}: launch it "
                "with --host the address its locator resolves it to"
            )
        elif len(tcp_addresses) != cohort_size:
            shortfall = f"{tcp_flag} resolved {len(tcp_addresses)} members, not the cohort's {cohort_size}"
        else:
            peers = [address for address in tcp_addresses if address != own_tcp_address]
            return peers, [udp_by_tcp[peer] for peer in peers]
        if time.monotonic() + retry_interval_seconds > deadline:
            raise ValueError(f"cohort did not resolve within {within_seconds}s: {shortfall}")
        await asyncio.sleep(retry_interval_seconds)


async def resolve_seed_addresses(
    entries: list[str],
    flag: str,
    within_seconds: float,
    retry_interval_seconds: float,
    resolution_timeout_seconds: float,
) -> list[tuple[str, int]]:
    """Seeds a node contacts to reach a cluster it is not a member of (a
    worker's managers): every address ``entries`` resolve to. Unlike a
    cohort, any non-empty resolution will do -- a seed only has to answer --
    so resolution repeats every ``retry_interval_seconds`` while it fails or
    finds nothing (a service not yet published at launch), until
    ``within_seconds`` pass.

    Raises:
        ValueError: no resolution found a seed within ``within_seconds``,
            with the last attempt's reason.
    """
    deadline = time.monotonic() + within_seconds
    while True:
        try:
            if addresses := await resolve_locators(entries, flag, resolution_timeout_seconds):
                return addresses
            shortfall = f"{flag} resolved no addresses"
        except ValueError as resolution_error:
            shortfall = str(resolution_error)
        if time.monotonic() + retry_interval_seconds > deadline:
            raise ValueError(f"{flag} did not resolve within {within_seconds}s: {shortfall}")
        await asyncio.sleep(retry_interval_seconds)


async def _resolve_address_locator(
    locator: str, flag: str, resolution_timeout_seconds: float
) -> list[tuple[str, int]]:
    if not ("://" in locator and locator.split("://", 1)[0] in DYNAMIC_LOCATOR_SCHEMES):
        return [parse_node_address(locator)]
    parsed = urlsplit(locator)
    if parsed.scheme == "dns":
        if not parsed.hostname or parsed.port is None or parsed.path or parsed.query:
            raise ValueError(f"{flag}: {locator!r} -- expected dns://name:port")
        try:
            answers = await asyncio.wait_for(
                asyncio.get_running_loop().getaddrinfo(parsed.hostname, parsed.port, type=socket.SOCK_STREAM),
                timeout=resolution_timeout_seconds,
            )
        except (OSError, asyncio.TimeoutError) as resolution_error:
            raise ValueError(f"{flag}: {locator} did not resolve: {resolution_error!r}") from resolution_error
        return sorted({(answer[4][0], parsed.port) for answer in answers})
    # dns-srv://_service._proto.name
    service_name = parsed.netloc
    if not service_name or parsed.path or parsed.query:
        raise ValueError(f"{flag}: {locator!r} -- expected dns-srv://_service._proto.name")
    try:
        records = await AsyncDNSResolver(resolution_timeout_seconds=resolution_timeout_seconds).resolve_srv(
            service_name
        )
    except DNSError as resolution_error:
        raise ValueError(f"{flag}: {locator} did not resolve: {resolution_error}") from resolution_error
    addresses = []
    for record in records:
        target = record.target.rstrip(".").lower()
        if not (_is_dns_name(target) or _is_ip_address(target)):
            raise ValueError(f"{flag}: {locator} names target {record.target!r}, not a host")
        addresses.append((target, record.port))
    return sorted(set(addresses))


async def _listed_locators(entry: str, flag: str, resolution_timeout_seconds: float) -> list[str]:
    """The locators a ``file://`` or ``exec://`` locator lists, one per
    line (blank lines and ``#`` comments skipped), after the path passes the
    section 2 checks: absolute, owned by this process's user, and no
    directory above it writable by everyone."""
    parsed = urlsplit(entry)
    if parsed.netloc or parsed.query or parsed.fragment or not parsed.path.startswith("/"):
        raise ValueError(f"{flag}: {entry!r} -- expected {parsed.scheme}:///absolute/path")
    if sys.platform == "win32":
        raise ValueError(f"{flag}: {parsed.scheme}:// locators need POSIX ownership checks (not on Windows)")
    path = pathlib.Path(parsed.path)
    try:
        path_status = path.stat()
    except OSError as stat_error:
        raise ValueError(f"{flag}: {parsed.scheme}:// locator path cannot be read: {stat_error!r}") from stat_error
    if path_status.st_uid != os.getuid():
        raise ValueError(f"{flag}: refused {parsed.scheme}:// locator: its path is not owned by this user")
    for directory in path.parents:
        if directory.stat().st_mode & stat.S_IWOTH:
            raise ValueError(
                f"{flag}: refused {parsed.scheme}:// locator: a directory above its path is writable by everyone"
            )
    if parsed.scheme == "file":
        listing = path.read_text()
    else:
        process = await asyncio.create_subprocess_exec(
            str(path), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=resolution_timeout_seconds)
        except asyncio.TimeoutError as timeout_error:
            process.kill()
            await process.wait()
            raise ValueError(
                f"{flag}: exec:// locator ran past {resolution_timeout_seconds}s"
            ) from timeout_error
        if process.returncode != 0:
            raise ValueError(
                f"{flag}: exec:// locator exited {process.returncode}: {stderr.decode(errors='replace').strip()}"
            )
        listing = stdout.decode()
    return [line.strip() for line in listing.splitlines() if line.strip() and not line.strip().startswith("#")]
