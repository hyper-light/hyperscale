"""
AD-28 dispatch ordering within a datacenter (DatacenterManagerSelector).

The gate dispatched to a datacenter's managers in configured order; only
the DC leader accepts jobs, so a non-leader first in the list answered
"Not DC leader" — a transient rejection the gate retries WITH BACKOFF on
that same manager before moving on. Its per-DC discovery services (the
AD-28 selection layer) were populated but never consulted, held every
manager twice (keyed by address at construction, by node id on
heartbeat), never dropped stale managers, and did not exist for
datacenters that joined at runtime.

Pinned: known leader first (highest term wins); every manager exactly
once; one discovery peer per manager, removed on forget; on-demand
discovery for runtime datacenters; measured-slow managers rank last.
"""

from types import SimpleNamespace

import pytest

from hyperscale.logging import Logger
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.gate.datacenter_manager_selector import (
    DatacenterManagerSelector,
)

MANAGER_A = ("10.0.0.1", 9000)
MANAGER_B = ("10.0.0.2", 9000)
MANAGER_C = ("10.0.0.3", 9000)
MANAGERS = [MANAGER_A, MANAGER_B, MANAGER_C]


def _selector(heartbeats: dict) -> DatacenterManagerSelector:
    return DatacenterManagerSelector(
        create_discovery=lambda: DiscoveryService(
            Env().get_discovery_config(
                node_role="gate",
                static_seeds=[],
                allow_dynamic_registration=True,
            ),
            Logger(),
        ),
        get_manager_heartbeats=lambda datacenter_id: heartbeats.get(datacenter_id, {}),
    )


def _heartbeat(is_leader: bool, term: int) -> SimpleNamespace:
    return SimpleNamespace(is_leader=is_leader, term=term)


def _tracked(heartbeats: dict | None = None) -> DatacenterManagerSelector:
    selector = _selector(heartbeats or {})
    for manager in MANAGERS:
        selector.track_manager("dc-a", manager)
    return selector


def test_known_leader_is_tried_first() -> None:
    selector = _tracked({"dc-a": {MANAGER_C: _heartbeat(is_leader=True, term=4)}})

    assert selector.ordered_managers("dc-a", "job-1", MANAGERS)[0] == MANAGER_C


def test_highest_term_leader_claim_wins_over_a_stale_one() -> None:
    selector = _tracked(
        {
            "dc-a": {
                MANAGER_A: _heartbeat(is_leader=True, term=3),
                MANAGER_B: _heartbeat(is_leader=True, term=7),
            }
        }
    )

    assert selector.ordered_managers("dc-a", "job-1", MANAGERS)[0] == MANAGER_B


@pytest.mark.parametrize("key", [f"job-{index}" for index in range(20)])
def test_every_manager_appears_exactly_once(key: str) -> None:
    untracked = ("10.0.0.9", 9000)
    selector = _tracked({"dc-a": {MANAGER_A: _heartbeat(is_leader=True, term=1)}})

    ordered = selector.ordered_managers("dc-a", key, MANAGERS + [untracked])

    assert sorted(ordered) == sorted(MANAGERS + [untracked])


def test_one_discovery_peer_per_manager_and_forget_removes_it() -> None:
    selector = _tracked()
    for manager in MANAGERS:
        selector.track_manager("dc-a", manager)

    discovery = selector.discovery_by_datacenter["dc-a"]
    assert discovery.peer_count == len(MANAGERS)

    selector.forget_manager(MANAGER_B)

    assert discovery.peer_count == len(MANAGERS) - 1
    assert MANAGER_B in selector.ordered_managers("dc-a", "job-1", MANAGERS)


def test_runtime_datacenter_gets_discovery_on_demand() -> None:
    selector = _selector({})

    selector.track_manager("dc-joined-later", MANAGER_A)

    assert selector.discovery_by_datacenter["dc-joined-later"].peer_count == 1


def test_measured_slow_manager_ranks_last_among_non_leaders() -> None:
    selector = _tracked()
    for _ in range(50):
        selector.record_success("dc-a", MANAGER_A, 400.0)
        selector.record_success("dc-a", MANAGER_B, 2.0)
        selector.record_success("dc-a", MANAGER_C, 2.0)

    last_places = {
        selector.ordered_managers("dc-a", f"job-{index}", MANAGERS)[-1]
        for index in range(100)
    }

    assert last_places == {MANAGER_A}


def test_outcomes_for_unknown_datacenter_are_ignored() -> None:
    selector = _selector({})

    selector.record_success("dc-unknown", MANAGER_A, 1.0)
    selector.record_failure("dc-unknown", MANAGER_A)
    selector.forget_manager(MANAGER_A)

    assert selector.ordered_managers("dc-unknown", "job-1", [MANAGER_A]) == [MANAGER_A]
