"""
The swarm runner's failing-seed ledger and reporting
(``tests/simulation/soak/run_swarm.py``): known failures replay first and
stay recorded until they pass, a budget stops before a seed that cannot
finish, and the exit code fails loudly on a failing or (when required)
unrun seed.
"""

import json
import pathlib

import pytest

from tests.simulation.soak import run_swarm

COMMIT = "abc123"
STAMP = "20261007T000000Z"


def test_known_failing_seeds_replay_before_the_window() -> None:
    suite_ledger = {"7": {}, "42": {}}

    assert run_swarm.ordered_seeds(suite_ledger, first_seed=5, count=4) == [7, 42, 5, 6, 8]


def test_failures_are_recorded_with_commit_and_cleared_when_they_pass() -> None:
    suite_ledger: run_swarm.SuiteLedger = {}
    cleared = run_swarm.update_ledger(suite_ledger, [(3, False, 1.0), (4, True, 1.0)], COMMIT, STAMP)
    assert cleared == []
    assert suite_ledger == {
        "3": {
            "first_failed_commit": COMMIT,
            "first_failed_at": STAMP,
            "last_failed_commit": COMMIT,
            "last_failed_at": STAMP,
        }
    }

    run_swarm.update_ledger(suite_ledger, [(3, False, 1.0)], "def456", "20261008T000000Z")
    assert suite_ledger["3"]["first_failed_commit"] == COMMIT
    assert suite_ledger["3"]["last_failed_commit"] == "def456"

    assert run_swarm.update_ledger(suite_ledger, [(3, True, 1.0)], "fed789", STAMP) == [3]
    assert suite_ledger == {}


def test_ledger_round_trips_as_sorted_json(tmp_path: pathlib.Path) -> None:
    ledger_path = str(tmp_path / "nested" / "ledger.json")
    ledger = {"tests/simulation/vopr": {"9": {"first_failed_commit": COMMIT}}, "tests/simulation/soak": {}}
    run_swarm.save_ledger(ledger_path, ledger)

    assert run_swarm.load_ledger(ledger_path) == {"tests/simulation/vopr": {"9": {"first_failed_commit": COMMIT}}}
    assert json.loads(pathlib.Path(ledger_path).read_text()) == run_swarm.load_ledger(ledger_path)
    assert run_swarm.load_ledger(str(tmp_path / "missing.json")) == {}


def test_budget_stops_before_a_seed_that_cannot_finish(monkeypatch: pytest.MonkeyPatch) -> None:
    now = [100.0]
    monkeypatch.setattr(run_swarm.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(run_swarm, "_refresh_lock", lambda: None)
    monkeypatch.setattr(run_swarm, "_print_outcome", lambda suite, outcome: None)

    def fake_run_seed(suite: str, seed: int, output_dir: str) -> run_swarm.SeedOutcome:
        now[0] += 30.0
        return (seed, True, 30.0)

    monkeypatch.setattr(run_swarm, "run_seed", fake_run_seed)

    outcomes = run_swarm.run_seeds("suite", [1, 2, 3, 4], deadline=170.0, output_dir="unused")
    assert [seed for seed, _passed, _elapsed in outcomes] == [1, 2]


def test_exit_code_fails_on_failures_and_on_required_unrun_seeds() -> None:
    clean = {"failing_seeds": [], "unrun_seeds": []}
    failing = {"failing_seeds": [3], "unrun_seeds": []}
    short = {"failing_seeds": [], "unrun_seeds": [9]}

    assert run_swarm.exit_code_for([clean], require_all=True) == 0
    assert run_swarm.exit_code_for([clean, failing], require_all=False) == 1
    assert run_swarm.exit_code_for([short], require_all=False) == 0
    assert run_swarm.exit_code_for([short], require_all=True) == 1


def test_summary_lists_every_failing_seed_replay_command() -> None:
    record = {
        "suite": "tests/simulation/vopr_chaos",
        "seeds_run": 3,
        "failing_seeds": [3],
        "cleared_seeds": [],
        "unrun_seeds": [],
        "elapsed_seconds": 60.0,
        "slowest_seed_seconds": 25.0,
        "repro": [run_swarm._repro_command(COMMIT, "tests/simulation/vopr_chaos", 3)],
    }
    summary = "".join(run_swarm._summary_lines([record]))

    assert "| tests/simulation/vopr_chaos | 3 | [3] |" in summary
    assert f"git checkout {COMMIT} && uv run pytest tests/simulation/vopr_chaos -k test_sim_replay --sim-replay=3" in summary
