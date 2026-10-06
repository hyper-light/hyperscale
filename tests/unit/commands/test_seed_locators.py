"""
Seed locators (AD-52 section 2), resolved against the real filesystem, real
processes and the host's real name resolution.

* ``dns://`` returns every address of a name; ``file://`` and ``exec://``
  list further locators -- never another listing.
* Listing paths pass the section 2 checks: absolute, owned by this user, no
  directory above writable by everyone; a failing command is an error.
* A cohort resolves to exactly the size every founder agrees on, with this
  node in it, paired TCP to UDP by host -- or fails within its bound,
  saying why. Without the size it refuses at once: a pod that resolved
  itself alone would otherwise found a cluster of one beside the real one.
* A worker's manager seeds need no size: resolution retries while it fails
  (a listing not yet written at launch) and takes the first that finds any,
  or fails within its bound, saying why.
"""

import asyncio
import os
import pathlib
import stat
import sys
import tempfile

import pytest

from hyperscale.commands.run.seed_locators import (
    is_dynamic_locator,
    resolve_cohort_addresses,
    resolve_locators,
    resolve_seed_addresses,
)

RESOLUTION_TIMEOUT_SECONDS = 5.0
POSIX_ONLY = pytest.mark.skipif(sys.platform == "win32", reason="listing locators need POSIX ownership checks")


@pytest.fixture
def private_directory():
    """A directory owned by this user, with no world-writable directory
    above it (the platform's per-user temporary directory)."""
    with tempfile.TemporaryDirectory() as directory:
        path = pathlib.Path(directory)
        assert not any(parent.stat().st_mode & stat.S_IWOTH for parent in path.parents), (
            "the per-user temporary directory sits under a world-writable one"
        )
        yield path


def write_listing(directory: pathlib.Path, name: str, lines: list[str]) -> pathlib.Path:
    listing = directory / name
    listing.write_text("\n".join(["# managers", *lines, ""]))
    return listing


def test_only_resolving_schemes_are_dynamic() -> None:
    assert [
        is_dynamic_locator(entry)
        for entry in ["10.0.0.1:9000", "tcp://10.0.0.1:9000", "dns://a:1", "dns-srv://_a._tcp.b", "file:///a", "exec:///a"]
    ] == [False, False, True, True, True, True]


@pytest.mark.asyncio
async def test_dns_returns_every_address_of_a_name() -> None:
    resolved = await resolve_locators(["dns://localhost:9100"], "--managers", RESOLUTION_TIMEOUT_SECONDS)

    assert ("127.0.0.1", 9100) in resolved
    assert all(port == 9100 for _host, port in resolved)


@POSIX_ONLY
@pytest.mark.asyncio
async def test_a_file_lists_locators_and_may_not_list_another(private_directory: pathlib.Path) -> None:
    listing = write_listing(private_directory, "cohort", ["10.0.0.1:9000", "tcp://10.0.0.2:9000", "", "10.0.0.1:9000"])
    nested = write_listing(private_directory, "nested", [f"file://{listing}"])

    resolved = await resolve_locators([f"file://{listing}"], "--managers", RESOLUTION_TIMEOUT_SECONDS)
    with pytest.raises(ValueError, match="may not list another"):
        await resolve_locators([f"file://{nested}"], "--managers", RESOLUTION_TIMEOUT_SECONDS)

    assert resolved == [("10.0.0.1", 9000), ("10.0.0.2", 9000)]


@POSIX_ONLY
@pytest.mark.asyncio
async def test_a_command_lists_locators_and_its_failure_is_an_error(private_directory: pathlib.Path) -> None:
    listing_command = private_directory / "seeds.sh"
    listing_command.write_text("#!/bin/sh\necho 10.0.0.3:9000\necho 10.0.0.4:9000\n")
    listing_command.chmod(0o700)
    failing_command = private_directory / "fails.sh"
    failing_command.write_text("#!/bin/sh\necho no seeds >&2\nexit 3\n")
    failing_command.chmod(0o700)

    resolved = await resolve_locators([f"exec://{listing_command}"], "--managers", RESOLUTION_TIMEOUT_SECONDS)
    with pytest.raises(ValueError, match="exited 3: no seeds"):
        await resolve_locators([f"exec://{failing_command}"], "--managers", RESOLUTION_TIMEOUT_SECONDS)

    assert resolved == [("10.0.0.3", 9000), ("10.0.0.4", 9000)]


@POSIX_ONLY
@pytest.mark.asyncio
async def test_a_listing_under_a_world_writable_directory_is_refused(private_directory: pathlib.Path) -> None:
    shared = private_directory / "shared"
    shared.mkdir()
    listing = write_listing(shared, "cohort", ["10.0.0.1:9000"])
    shared.chmod(0o777)
    try:
        with pytest.raises(ValueError, match="writable by everyone"):
            await resolve_locators([f"file://{listing}"], "--managers", RESOLUTION_TIMEOUT_SECONDS)
    finally:
        shared.chmod(0o700)


@POSIX_ONLY
@pytest.mark.asyncio
async def test_a_cohort_resolves_to_its_agreed_size_with_this_node_in_it(private_directory: pathlib.Path) -> None:
    hosts = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
    tcp_listing = write_listing(private_directory, "tcp", [f"{host}:9000" for host in hosts])
    # UDP listed in another order: pairing is by host, never position.
    udp_listing = write_listing(private_directory, "udp", [f"{host}:9001" for host in reversed(hosts)])

    peers = await resolve_cohort_addresses(
        [f"file://{tcp_listing}"],
        [f"file://{udp_listing}"],
        "--managers",
        "--manager-udp",
        ("10.0.0.2", 9000),
        3,
        0.0,
        0.0,
        RESOLUTION_TIMEOUT_SECONDS,
    )

    assert peers == ([("10.0.0.1", 9000), ("10.0.0.3", 9000)], [("10.0.0.1", 9001), ("10.0.0.3", 9001)])


@POSIX_ONLY
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cohort_size", "own_address", "reason"),
    [
        (None, ("10.0.0.1", 9000), "give --cohort-size"),
        (3, ("10.0.0.1", 9000), "resolved 2 members, not the cohort's 3"),
        (2, ("10.0.0.9", 9000), "is not among"),
    ],
)
async def test_a_cohort_that_does_not_resolve_is_refused_within_its_bound(
    private_directory: pathlib.Path, cohort_size: int | None, own_address: tuple[str, int], reason: str
) -> None:
    tcp_listing = write_listing(private_directory, "tcp", ["10.0.0.1:9000", "10.0.0.2:9000"])
    udp_listing = write_listing(private_directory, "udp", ["10.0.0.1:9001", "10.0.0.2:9001"])
    started = asyncio.get_running_loop().time()

    with pytest.raises(ValueError, match=reason):
        await resolve_cohort_addresses(
            [f"file://{tcp_listing}"],
            [f"file://{udp_listing}"],
            "--managers",
            "--manager-udp",
            own_address,
            cohort_size,
            0.2,
            0.05,
            RESOLUTION_TIMEOUT_SECONDS,
        )

    assert asyncio.get_running_loop().time() - started < 1.0


@POSIX_ONLY
@pytest.mark.asyncio
async def test_members_sharing_a_host_pair_by_ascending_port(private_directory: pathlib.Path) -> None:
    """Several members on one host (a laptop, or one VM): each TCP port
    pairs with its host's UDP port of the same rank."""
    tcp_listing = write_listing(private_directory, "tcp", ["127.0.0.1:9004", "127.0.0.1:9000", "127.0.0.1:9002"])
    udp_listing = write_listing(private_directory, "udp", ["127.0.0.1:9003", "127.0.0.1:9005", "127.0.0.1:9001"])

    peers = await resolve_cohort_addresses(
        [f"file://{tcp_listing}"],
        [f"file://{udp_listing}"],
        "--managers",
        "--manager-udp",
        ("127.0.0.1", 9002),
        3,
        0.0,
        0.0,
        RESOLUTION_TIMEOUT_SECONDS,
    )

    assert peers == ([("127.0.0.1", 9004), ("127.0.0.1", 9000)], [("127.0.0.1", 9005), ("127.0.0.1", 9001)])


@POSIX_ONLY
@pytest.mark.asyncio
async def test_seeds_published_after_launch_are_found_by_a_retry(private_directory: pathlib.Path) -> None:
    listing = private_directory / "managers"

    async def publish_later() -> None:
        await asyncio.sleep(0.1)
        write_listing(private_directory, "managers", ["10.0.0.1:9000", "10.0.0.2:9000"])

    publisher = asyncio.ensure_future(publish_later())
    resolved = await resolve_seed_addresses([f"file://{listing}"], "--managers", 2.0, 0.05, RESOLUTION_TIMEOUT_SECONDS)
    await publisher

    assert resolved == [("10.0.0.1", 9000), ("10.0.0.2", 9000)]


@POSIX_ONLY
@pytest.mark.asyncio
async def test_seeds_that_never_resolve_are_refused_within_their_bound(private_directory: pathlib.Path) -> None:
    started = asyncio.get_running_loop().time()

    with pytest.raises(ValueError, match="did not resolve within"):
        await resolve_seed_addresses(
            [f"file://{private_directory / 'never-written'}"], "--managers", 0.2, 0.05, RESOLUTION_TIMEOUT_SECONDS
        )

    assert asyncio.get_running_loop().time() - started < 1.0
