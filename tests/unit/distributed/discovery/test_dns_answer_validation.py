"""
DNS change detection judges whole answers, not single addresses (AD-28).

The validator compared each address of an answer with the address
before it, so a headless Service's answer -- one address per pod -- was
a run of "IP changes": a single lookup of seven pods exceeded the
default five changes per window and was rejected as fast-flux rotation,
so DNS discovery of a Kubernetes service failed. Rolling updates and
scaling (answers that keep most of their addresses) counted as changes
too.

An answer is now one observation: it changes only when it shares no
address with the previous answer, the fast-flux signature.

* a multi-address answer, in any order and repeated, is not a change;
* answers that overlap (scaling, a rolling update) are not changes;
* disjoint answers past the per-window limit are rotation, and the
  rotating answer is rejected;
* the window resets the count;
* per-address policy (allowed ranges) still applies within an answer;
* a single-address lookup is a one-address answer, as before;
* the resolver validates an answer as one.
"""

import pytest

from hyperscale.distributed.discovery.dns import (
    dns_security_event as dns_security_event_module,
    dns_security_validator as dns_security_validator_module,
    host_history as host_history_module,
)
from hyperscale.distributed.discovery.dns.resolver import AsyncDNSResolver
from hyperscale.distributed.discovery.dns.security import (
    DNSSecurityValidator,
    DNSSecurityViolation,
)

SERVICE = "managers.hyperscale.svc.cluster.local"
POD_ADDRESSES = [f"10.244.1.{octet}" for octet in range(10, 17)]
MAX_CHANGES = 2
WINDOW_SECONDS = 300.0


class SteppedClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> SteppedClock:
    stepped_clock = SteppedClock()
    # The DNS security classes read the clock where each is defined.
    for security_class_module in (dns_security_validator_module, dns_security_event_module, host_history_module):
        monkeypatch.setattr(security_class_module, "_DEFAULT_CLOCK", stepped_clock)
    return stepped_clock


def make_validator(**settings) -> DNSSecurityValidator:
    return DNSSecurityValidator(
        detect_ip_changes=True,
        max_ip_changes_per_window=MAX_CHANGES,
        ip_change_window_seconds=WINDOW_SECONDS,
        **settings,
    )


def rotations(events: list) -> list:
    return [event for event in events if event.violation_type == DNSSecurityViolation.RAPID_IP_ROTATION]


def test_a_repeated_multi_address_answer_in_any_order_is_not_a_change(clock: SteppedClock) -> None:
    validator = make_validator()

    for rotation in range(len(POD_ADDRESSES) * 3):
        answer = POD_ADDRESSES[rotation % len(POD_ADDRESSES):] + POD_ADDRESSES[: rotation % len(POD_ADDRESSES)]
        accepted, events = validator.validate_answer(SERVICE, answer)

        assert accepted == answer
        assert events == []


def test_overlapping_answers_from_scaling_and_rolling_updates_are_not_changes(clock: SteppedClock) -> None:
    validator = make_validator()
    answers = [POD_ADDRESSES[start : start + 3] for start in range(len(POD_ADDRESSES) - 2)]

    for answer in answers:
        accepted, events = validator.validate_answer(SERVICE, answer)

        assert accepted == answer
        assert events == []


def test_disjoint_answers_past_the_limit_are_rotation_and_rejected(clock: SteppedClock) -> None:
    validator = make_validator()
    disjoint_answers = [[address] for address in POD_ADDRESSES[: MAX_CHANGES + 2]]

    outcomes = [validator.validate_answer(SERVICE, answer) for answer in disjoint_answers]

    for accepted, events in outcomes[: MAX_CHANGES + 1]:
        assert rotations(events) == []
        assert accepted
    rotating_accepted, rotating_events = outcomes[-1]
    assert rotating_accepted == []
    assert len(rotations(rotating_events)) == 1


def test_the_window_resets_the_change_count(clock: SteppedClock) -> None:
    validator = make_validator()
    for address in POD_ADDRESSES[: MAX_CHANGES + 1]:
        validator.validate_answer(SERVICE, [address])

    clock.now += WINDOW_SECONDS + 1.0
    accepted, events = validator.validate_answer(SERVICE, [POD_ADDRESSES[MAX_CHANGES + 1]])

    assert accepted == [POD_ADDRESSES[MAX_CHANGES + 1]]
    assert rotations(events) == []


def test_per_address_policy_still_applies_within_an_answer(clock: SteppedClock) -> None:
    validator = make_validator(allowed_cidrs=["10.244.0.0/16"])
    outside_address = "192.168.5.5"

    accepted, events = validator.validate_answer(SERVICE, [POD_ADDRESSES[0], outside_address])

    assert accepted == [POD_ADDRESSES[0]]
    assert [event.violation_type for event in events] == [DNSSecurityViolation.IP_OUT_OF_RANGE]


def test_a_single_address_lookup_is_a_one_address_answer(clock: SteppedClock) -> None:
    validator = make_validator()

    events = [validator.validate(SERVICE, address) for address in POD_ADDRESSES[: MAX_CHANGES + 2]]

    assert events[: MAX_CHANGES + 1] == [None] * (MAX_CHANGES + 1)
    assert events[-1].violation_type == DNSSecurityViolation.RAPID_IP_ROTATION


def test_the_resolver_validates_an_answer_as_one(clock: SteppedClock) -> None:
    resolver = AsyncDNSResolver(
        security_validator=DNSSecurityValidator(detect_ip_changes=True),
        reject_on_security_violation=True,
    )

    for _ in range(3):
        assert resolver._validate_addresses(SERVICE, POD_ADDRESSES) == POD_ADDRESSES
