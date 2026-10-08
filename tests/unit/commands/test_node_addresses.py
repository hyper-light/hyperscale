"""
The node commands' host and peer-list parsing.

A node is identified by the exact host string it starts with, and peers
address it by that string, so ``--host`` is written exactly the way
``parse_node_address`` writes hosts. A cohort's members can all be
given the same full peer list: each skips its own entry, and the TCP
and UDP lists pair by position.
"""

import pytest

from hyperscale.commands.run.node_address import (
    parse_node_address,
    parse_node_host,
    parse_peer_addresses,
)


def test_a_dns_name_host_is_written_as_peers_parse_it():
    host = parse_node_host("Manager-0.Managers.NS.svc.cluster.local", 8231)

    assert host == "manager-0.managers.ns.svc.cluster.local"
    assert parse_node_address(f"{host}:8231") == (host, 8231)


def test_ip_hosts_pass_through():
    assert parse_node_host("10.0.4.7", 8231) == "10.0.4.7"
    assert parse_node_host("::1", 8231) == "::1"


def test_a_host_that_is_neither_an_ip_nor_a_name_is_refused():
    with pytest.raises(ValueError, match="invalid host"):
        parse_node_host("not!a!host", 8231)


def test_every_cohort_member_skips_itself_from_the_shared_list():
    tcp_addresses = ["m-0.s:8231", "m-1.s:8231", "m-2.s:8231"]
    udp_addresses = ["m-0.s:8241", "m-1.s:8241", "m-2.s:8241"]

    for ordinal in range(3):
        peer_tcp, peer_udp = parse_peer_addresses(
            tcp_addresses,
            udp_addresses,
            "--managers",
            "--manager-udp",
            (f"m-{ordinal}.s", 8231),
        )

        others = [index for index in range(3) if index != ordinal]
        assert peer_tcp == [(f"m-{index}.s", 8231) for index in others]
        assert peer_udp == [(f"m-{index}.s", 8241) for index in others]


def test_peers_are_paired_by_position():
    peer_tcp, peer_udp = parse_peer_addresses(
        ["a.s:1001", "b.s:2001"],
        ["a.s:1002", "b.s:2002"],
        "--gates",
        "--gate-udp",
        ("self.s", 3001),
    )

    assert list(zip(peer_tcp, peer_udp)) == [
        (("a.s", 1001), ("a.s", 1002)),
        (("b.s", 2001), ("b.s", 2002)),
    ]


def test_lists_of_different_lengths_are_refused():
    with pytest.raises(ValueError, match="--manager-udp needs one address per --managers address"):
        parse_peer_addresses(
            ["m-0.s:8231", "m-1.s:8231"],
            ["m-0.s:8241"],
            "--managers",
            "--manager-udp",
            ("m-0.s", 8231),
        )


def test_a_malformed_peer_is_refused():
    with pytest.raises(ValueError):
        parse_peer_addresses(
            ["m-0.s:notaport"],
            ["m-0.s:8241"],
            "--managers",
            "--manager-udp",
            ("self.s", 8231),
        )
