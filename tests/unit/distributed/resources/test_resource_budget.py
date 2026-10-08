"""
AD-41 ResourceBudget validity: a budget that cannot be enforced -- a
non-positive limit, thresholds out of order, a negative grace -- is
refused at construction, and a budget that skipped ``__init__`` (as one
deserialized from a submission does) reports the same problems through
``validation_errors``.
"""

import copy
import dataclasses

import pytest

from hyperscale.distributed.resources.resource_budget import ResourceBudget

VALID = ResourceBudget(
    max_cpu_percent=100.0,
    max_memory_bytes=1024,
    warning_threshold=0.8,
    throttle_threshold=0.85,
    kill_threshold=1.0,
    warning_grace_seconds=1.0,
    kill_grace_seconds=0.0,
)
INVALID_OVERRIDES = [
    {"max_cpu_percent": 0.0},
    {"max_cpu_percent": float("nan")},
    {"max_memory_bytes": 0},
    {"warning_threshold": 0.0},
    {"warning_threshold": 1.5},
    {"throttle_threshold": 0.5},  # below the warning threshold
    {"throttle_threshold": 1.5},  # above the kill threshold
    {"throttle_threshold": float("nan")},
    {"kill_grace_seconds": -1.0},
    {"warning_grace_seconds": float("nan")},
]


def test_valid_budget_has_no_errors() -> None:
    assert VALID.validation_errors() == []


@pytest.mark.parametrize("override", INVALID_OVERRIDES)
def test_unenforceable_budget_is_refused_at_construction(override: dict) -> None:
    with pytest.raises(ValueError):
        dataclasses.replace(VALID, **override)


@pytest.mark.parametrize("override", INVALID_OVERRIDES)
def test_budget_that_skipped_init_reports_its_errors(override: dict) -> None:
    bypassed = copy.copy(VALID)
    for field_name, value in override.items():
        object.__setattr__(bypassed, field_name, value)

    assert bypassed.validation_errors() != []
