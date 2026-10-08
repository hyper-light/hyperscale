"""
SRV resolution under the resolver's concurrency bound (AD-28).

Each DNS lookup takes one permit: the SRV query, then each target's address
lookup. SRV resolution used to hold its permit while each target lookup
took another, so once every permit was held by an SRV resolution, each
waited on the others' permits forever. As many concurrent SRV resolutions
as there are permits (and more) must all complete.
"""

import asyncio
import socket

import pytest

from hyperscale.distributed.discovery.dns.resolver import AsyncDNSResolver, SRVRecord

PERMITS = 2
TARGETS_PER_SERVICE = 2


class AnsweringResolver(AsyncDNSResolver):
    """A resolver whose SRV queries answer from memory; address lookups go
    through its real bounded path against a stubbed getaddrinfo."""

    async def resolve_srv(self, service_name: str) -> list[SRVRecord]:
        await asyncio.sleep(0)
        return [
            SRVRecord(priority=0, weight=10, port=9000 + target_index, target=f"{service_name}-target-{target_index}")
            for target_index in range(TARGETS_PER_SERVICE)
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrent_resolutions", [PERMITS, 2 * PERMITS])
async def test_concurrent_srv_resolutions_at_the_permit_bound_all_complete(
    concurrent_resolutions: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    loop = asyncio.get_running_loop()

    async def getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        await asyncio.sleep(0)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", port))]

    monkeypatch.setattr(loop, "getaddrinfo", getaddrinfo)
    resolver = AnsweringResolver(max_concurrent_resolutions=PERMITS)

    results = await asyncio.wait_for(
        asyncio.gather(
            *(resolver._do_resolve_srv(f"_svc{index}._tcp.local") for index in range(concurrent_resolutions))
        ),
        # Every lookup answers after one yield; a second is far beyond any
        # non-deadlocked completion.
        timeout=1.0,
    )

    assert all(len(result.srv_records) == TARGETS_PER_SERVICE for result in results)
    assert all(result.addresses == ["10.0.0.1"] for result in results)
