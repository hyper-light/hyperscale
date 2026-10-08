"""
VOPR round-trip of ``TrackingToken`` over every host form.

Node ids embed the node's host (``{dc}-{priority}-{host}-{port}-{ms}``),
and manager and worker ids are token components, so a token must parse
back to exactly the token that was formatted whether the host is an IPv4
literal, an IPv6 literal (which holds ``:``, the token separator; with or
without an RFC 6874 zone), or a DNS name. Seeded fuzzing also covers
adversarial components built from the separator and both brackets.

Compatibility (AD-25): a token whose components hold no ``:`` and begin
with no ``[`` -- every token the original unbracketed format could write
unambiguously -- formats byte-identically to that format and parses from
it unchanged.
"""

import random

import pytest

from hyperscale.distributed.models.tracking_token import TrackingToken
from hyperscale.distributed.swim.core.node_id_model import NodeId

SEEDS = range(64)
TOKENS_PER_SEED = 200
ADVERSARIAL_ALPHABET = ":[]a1-."
ADVERSARIAL_MAXIMUM_LENGTH = 12
PRIORITY_RANGE = (0, 99)
PORT_RANGE = (0, 65535)
CREATED_MILLISECONDS_RANGE = (0, 2**52)

IPV4_HOSTS = ("127.0.0.1", "10.0.0.7", "192.168.255.254")
IPV6_HOSTS = (
    "::1",
    "::",
    "fe80::1",
    "2001:db8::8a2e:370:7334",
    "2001:0db8:0000:0000:0000:ff00:0042:8329",
    "::ffff:192.0.2.128",
    "fe80::1%eth0",
)
DNS_HOSTS = ("localhost", "manager.example.com", "worker-3.dc-east.internal")
HOST_FORMS = {"ipv4": IPV4_HOSTS, "ipv6": IPV6_HOSTS, "dns": DNS_HOSTS}


def _node_id(randomness: random.Random, datacenter: str, host: str) -> str:
    return str(
        NodeId(
            datacenter=datacenter,
            priority=randomness.randint(*PRIORITY_RANGE),
            host=host,
            port=randomness.randint(*PORT_RANGE),
            created_ms=randomness.randint(*CREATED_MILLISECONDS_RANGE),
        )
    )


def _adversarial_component(randomness: random.Random) -> str:
    length = randomness.randint(0, ADVERSARIAL_MAXIMUM_LENGTH)
    return "".join(randomness.choice(ADVERSARIAL_ALPHABET) for _ in range(length))


def _node_component(randomness: random.Random, datacenter: str) -> str:
    host_form = randomness.choice(sorted(HOST_FORMS))
    return _node_id(randomness, datacenter, randomness.choice(HOST_FORMS[host_form]))


def _random_component(randomness: random.Random, datacenter: str) -> str:
    if randomness.random() < 0.5:
        return _node_component(randomness, datacenter)
    return _adversarial_component(randomness)


def _random_token(randomness: random.Random) -> TrackingToken:
    datacenter = randomness.choice(("dc-east", "local", _adversarial_component(randomness)))
    manager_id = _random_component(randomness, "dc-east")
    job_id = _random_component(randomness, "dc-east")
    level = randomness.randint(0, 2)
    # A workflow or worker id is never empty: an empty one is absent.
    workflow_id = (_random_component(randomness, "dc-east") or "workflow") if level >= 1 else None
    worker_id = (_random_component(randomness, "dc-east") or "worker") if level == 2 else None
    return TrackingToken(
        datacenter=datacenter,
        manager_id=manager_id,
        job_id=job_id,
        workflow_id=workflow_id,
        worker_id=worker_id,
    )


def _assert_round_trips(token: TrackingToken) -> None:
    assert TrackingToken.parse(str(token)) == token
    assert TrackingToken.parse(token.job_token) == TrackingToken.for_job(
        token.datacenter, token.manager_id, token.job_id
    )
    if token.workflow_token is not None:
        assert TrackingToken.parse(token.workflow_token) == TrackingToken.for_workflow(
            token.datacenter, token.manager_id, token.job_id, token.workflow_id
        )


@pytest.mark.parametrize("seed", SEEDS)
def test_every_token_round_trips(seed: int) -> None:
    randomness = random.Random(seed)
    for _token_index in range(TOKENS_PER_SEED):
        _assert_round_trips(_random_token(randomness))


@pytest.mark.parametrize("host_form", sorted(HOST_FORMS))
def test_every_host_form_round_trips_at_every_level(host_form: str) -> None:
    randomness = random.Random(host_form)
    for host in HOST_FORMS[host_form]:
        manager_id = _node_id(randomness, "dc-east", host)
        worker_id = _node_id(randomness, "dc-east", host)
        sub_workflow_token = TrackingToken.for_sub_workflow("dc-east", manager_id, "job-1", "workflow-1", worker_id)
        _assert_round_trips(sub_workflow_token)
        assert sub_workflow_token.to_parent_workflow_token() == TrackingToken.parse(sub_workflow_token.workflow_token)


def test_distinct_tokens_never_share_a_string() -> None:
    randomness = random.Random(0)
    tokens_by_string: dict[str, TrackingToken] = {}
    for _token_index in range(len(SEEDS) * TOKENS_PER_SEED):
        token = _random_token(randomness)
        assert tokens_by_string.setdefault(str(token), token) == token


def test_ipv6_hosts_are_bracketed_and_unambiguous() -> None:
    token = TrackingToken.for_sub_workflow("dc", "dc-01-::1-09000-0", "job", "workflow", "dc-02-fe80::1-09001-0")
    assert str(token) == "dc:[dc-01-::1-09000-0]:job:workflow:[dc-02-fe80::1-09001-0]"


@pytest.mark.parametrize("seed", SEEDS)
def test_original_format_tokens_are_byte_identical_and_still_parse(seed: int) -> None:
    randomness = random.Random(seed)
    for _token_index in range(TOKENS_PER_SEED):
        host = randomness.choice(IPV4_HOSTS + DNS_HOSTS)
        components = [
            "dc-east",
            _node_id(randomness, "dc-east", host),
            f"job-{randomness.getrandbits(32)}",
            f"workflow-{randomness.getrandbits(32)}",
            _node_id(randomness, "dc-east", host),
        ][: randomness.randint(3, 5)]
        original_format = ":".join(components)
        token = TrackingToken.parse(original_format)
        assert str(token) == original_format
        assert [token.datacenter, token.manager_id, token.job_id, token.workflow_id, token.worker_id][
            : len(components)
        ] == components


@pytest.mark.parametrize(
    "malformed",
    [
        "dc:manager",
        "dc:[manager:job",
        "dc:[manager]x:job",
        "a:b:c:d:e:f",
        "dc:[a::1]:job:workflow:worker:extra",
    ],
)
def test_malformed_tokens_raise(malformed: str) -> None:
    with pytest.raises(ValueError):
        TrackingToken.parse(malformed)
