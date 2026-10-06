"""
DNS-discovered peers follow their names' answers (AD-28).

Every address a DNS name ever resolved to stayed a discovery peer, so a
long-lived node's peers grew with every pod replaced behind a headless
service, and it kept selecting addresses no pod held anymore.

* a peer whose address left its name's answer is retired;
* an address another configured name still answers keeps its peer;
* a failed lookup retires nothing (a DNS outage is not a departure);
* the DNS-discovered addresses are those the names answer now.
"""

import pytest

from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.discovery.dns.resolver import DNSError, DNSResult
from hyperscale.distributed.env import Env

PORT = 9000
FIRST_NAME = "managers-a.hyperscale.svc"
SECOND_NAME = "managers-b.hyperscale.svc"


class ScriptedResolver:
    """Answers each name with the addresses it is currently given."""

    def __init__(self) -> None:
        self.answers: dict[str, list[str] | None] = {}

    async def resolve(self, hostname: str, port: int | None = None, force_refresh: bool = False) -> DNSResult:
        addresses = self.answers[hostname]
        if addresses is None:
            raise DNSError(hostname, "lookup failed")
        return DNSResult(hostname=hostname, addresses=list(addresses), port=port)


def make_service(dns_names: list[str]) -> tuple[DiscoveryService, ScriptedResolver]:
    config = Env(DISCOVERY_DNS_NAMES=",".join(dns_names), DISCOVERY_DEFAULT_PORT=PORT).get_discovery_config(
        node_role="worker",
        allow_dynamic_registration=True,
    )
    service = DiscoveryService(config)
    resolver = ScriptedResolver()
    service._resolver = resolver
    return service, resolver


@pytest.mark.asyncio
async def test_a_peer_that_left_its_names_answer_is_retired() -> None:
    service, resolver = make_service([FIRST_NAME])
    resolver.answers[FIRST_NAME] = ["10.0.0.1", "10.0.0.2"]
    await service.discover_peers()

    resolver.answers[FIRST_NAME] = ["10.0.0.2", "10.0.0.3"]
    await service.discover_peers()

    assert sorted(service.get_dns_peer_addresses()) == [("10.0.0.2", PORT), ("10.0.0.3", PORT)]
    assert sorted((peer.host, peer.port) for peer in service.get_all_peers()) == [
        ("10.0.0.2", PORT),
        ("10.0.0.3", PORT),
    ]


@pytest.mark.asyncio
async def test_an_address_another_name_still_answers_keeps_its_peer() -> None:
    service, resolver = make_service([FIRST_NAME, SECOND_NAME])
    resolver.answers[FIRST_NAME] = ["10.0.0.1"]
    resolver.answers[SECOND_NAME] = ["10.0.0.1"]
    await service.discover_peers()

    resolver.answers[FIRST_NAME] = ["10.0.0.4"]
    await service.discover_peers()

    assert sorted(service.get_dns_peer_addresses()) == [("10.0.0.1", PORT), ("10.0.0.4", PORT)]


@pytest.mark.asyncio
async def test_a_failed_lookup_retires_nothing() -> None:
    service, resolver = make_service([FIRST_NAME])
    resolver.answers[FIRST_NAME] = ["10.0.0.1"]
    await service.discover_peers()

    resolver.answers[FIRST_NAME] = None
    await service.discover_peers()

    assert service.get_dns_peer_addresses() == [("10.0.0.1", PORT)]
