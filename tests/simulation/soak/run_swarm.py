"""
F2 swarm runner: sweep a seed window through every VOPR-style suite
SERIALLY under the machine-wide simulation lock, one ``--sim-replay``
invocation per seed, and keep a ledger of failing seeds that the next run
replays first (I4: a seed reproduces against the tree it was drawn on --
schedules legitimately shift when production timing changes -- so every
failure is recorded with its commit).

Each seed runs as::

    python -m pytest <suite> -q -k test_sim_replay --sim-replay=<seed>

which is exactly the suite's sweep judge for that seed (expand, run twice,
check invariants, require a byte-identical replay). One invocation per
seed means a failing seed never hides the seeds after it, as one
``--sim-vopr-count`` sweep does (its loop asserts at the first failure).

Suites run one at a time (heavy multi-process simulations must never
overlap -- the per-child 300s wall deadman assumes an uncontended
machine), with the ``/tmp/hyperscale-sim-run.lock`` mkdir-mutex held for
the whole batch and its mtime refreshed before every seed so a long batch
is never mistaken for an abandoned lock (stale threshold: 25 minutes).

Seed order per suite: every seed the ledger records as failing for it,
then the window ``--first-seed`` .. ``--first-seed + --count - 1``.
With ``--budget-seconds`` the runner starts a seed only while the time
left covers the slowest seed seen so far, so the count a budget buys is
measured on the machine that runs it; seeds it could not start are
reported as unrun (and fail the run under ``--require-all``).

Outputs:

* the ledger (``--ledger``, JSON): per suite, per failing seed, the first
  and last commit and time it failed. A seed that passes again is cleared
  from it (and reported as cleared). It is plain sorted JSON, so it can be
  committed, cached between CI runs, or handed back with ``--ledger``.
* the seed log (``--log``, JSON lines): one record per suite run --
  commit, window, seeds run/failing/cleared/unrun, timings, and the exact
  reproduction command for every failing seed.
* per-seed pytest output for failing seeds under ``--artifacts-dir``.
* with ``--summary-markdown`` (CI passes ``$GITHUB_STEP_SUMMARY``), a
  markdown summary of the same.

Usage (from the repository root; defaults are cwd-relative)::

    uv run python tests/simulation/soak/run_swarm.py --count 25
    uv run python tests/simulation/soak/run_swarm.py \\
        --first-seed 1000 --count 100 --budget-seconds 3600 \\
        --suites tests/simulation/vopr tests/simulation/vopr_chaos

Measured cost per seed (2026-10-07, one judge + replay twin, Apple M-series,
single seeds via ``--sim-replay``): vopr ~6 s, vopr_gates ~16 s, vopr_mdc
~13 s, vopr_chaos ~21 s, soak ~34 s.
"""

import argparse
import datetime
import json
import os
import shutil
import subprocess
import sys
import time

_LOCK_PATH = "/tmp/hyperscale-sim-run.lock"
_LOCK_STALE_SECONDS = 25 * 60
_POLL_SECONDS = 30.0

_DEFAULT_SUITES = (
    "tests/simulation/vopr",
    "tests/simulation/vopr_gates",
    "tests/simulation/vopr_mdc",
)
_SOAK_SUITE = "tests/simulation/soak"
_DEFAULT_ARTIFACTS_DIR = os.path.join("tests", "simulation", "_artifacts")

SeedOutcome = tuple[int, bool, float]
SuiteLedger = dict[str, dict[str, str]]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--count", type=int, default=25, help="Seeds in each suite's window (default 25)")
    parser.add_argument("--first-seed", type=int, default=1, help="First seed of the window (default 1)")
    parser.add_argument(
        "--suites",
        nargs="+",
        default=list(_DEFAULT_SUITES),
        help="Suite paths to run, in order (default: the VOPR triad)",
    )
    parser.add_argument(
        "--log",
        default=os.path.join(_DEFAULT_ARTIFACTS_DIR, "swarm_seed_log.jsonl"),
        help="Seed log to append one JSON line per suite run to",
    )
    parser.add_argument(
        "--ledger",
        default=os.path.join(_DEFAULT_ARTIFACTS_DIR, "swarm_failing_seeds.json"),
        help="Failing-seed ledger: replayed first, updated in place",
    )
    parser.add_argument(
        "--artifacts-dir",
        default=_DEFAULT_ARTIFACTS_DIR,
        help="Directory for failing seeds' pytest output",
    )
    parser.add_argument(
        "--include-soak",
        action="store_true",
        help=f"Also run {_SOAK_SUITE} (opts in via HYPERSCALE_SIM_SOAK=1)",
    )
    parser.add_argument(
        "--budget-seconds",
        type=float,
        default=None,
        help="Start a seed only while the time left covers the slowest seed so far",
    )
    parser.add_argument(
        "--require-all",
        action="store_true",
        help="Fail the run when the budget left any seed of the window unrun",
    )
    parser.add_argument("--summary-markdown", default=None, help="Append a markdown summary to this file")
    parser.add_argument(
        "--max-wait-seconds",
        type=float,
        default=3600.0,
        help=(
            "Give up (loudly) if foreign pytest runs or the lock block "
            "for longer than this before the batch starts"
        ),
    )
    return parser.parse_args()


def _foreign_pytest_pids() -> list[str]:
    """Pids of already-running ``pytest tests/`` invocations (checked
    BEFORE this runner spawns its own, so any match is foreign)."""
    listing = subprocess.run(
        ["pgrep", "-fl", "pytest"], capture_output=True, text=True
    )
    return [
        line.split()[0]
        for line in listing.stdout.splitlines()
        if "tests/" in line
    ]


def wait_for_idle_machine(max_wait_seconds: float) -> None:
    """Block (with visible 30s progress polls) until no foreign
    ``pytest tests/`` process is running."""
    waited = 0.0
    while (foreign_pids := _foreign_pytest_pids()):
        if waited >= max_wait_seconds:
            raise SystemExit(
                f"gave up after {waited:.0f}s: pytest still running "
                f"(pids {foreign_pids})"
            )
        print(
            f"[swarm] waiting for running pytest {foreign_pids} "
            f"({waited:.0f}s/{max_wait_seconds:.0f}s)",
            flush=True,
        )
        time.sleep(_POLL_SECONDS)
        waited += _POLL_SECONDS


def _remove_lock_path() -> None:
    """Remove the lock with ``rm -rf`` semantics: the canonical mkdir
    directory, a regular-file variant, or a non-empty directory (all
    debris shapes seen on the shared machine — a zero-byte FILE left
    by an orphaned heartbeat once wedged every waiter, because
    ``mkdir`` fails EEXIST on it while ``rmdir`` cannot remove it).
    A stale lock of ANY shape must clear for the next waiter."""
    try:
        os.rmdir(_LOCK_PATH)
    except FileNotFoundError:
        pass
    except NotADirectoryError:
        try:
            os.unlink(_LOCK_PATH)
        except FileNotFoundError:
            pass
    except OSError:
        shutil.rmtree(_LOCK_PATH, ignore_errors=True)


def acquire_lock(max_wait_seconds: float) -> None:
    """Take the machine-wide sim lock (mkdir mutex): poll every 30s,
    steal only when the holder's mtime is older than 25 minutes."""
    waited = 0.0
    while True:
        try:
            os.mkdir(_LOCK_PATH)
            return
        except FileExistsError:
            lock_age = time.time() - os.stat(_LOCK_PATH).st_mtime
            if lock_age > _LOCK_STALE_SECONDS:
                print(
                    f"[swarm] stealing stale sim lock (age {lock_age:.0f}s)",
                    flush=True,
                )
                _remove_lock_path()
                continue
            if waited >= max_wait_seconds:
                raise SystemExit(
                    f"gave up after {waited:.0f}s: sim lock held "
                    f"(age {lock_age:.0f}s)"
                )
            print(
                f"[swarm] sim lock held (age {lock_age:.0f}s) — polling "
                f"({waited:.0f}s/{max_wait_seconds:.0f}s)",
                flush=True,
            )
            time.sleep(_POLL_SECONDS)
            waited += _POLL_SECONDS




def release_lock() -> None:
    _remove_lock_path()


def run_seed(suite: str, seed: int, output_dir: str) -> SeedOutcome:
    """Judge one seed with the suite's replay entry point; keep its output only if it failed."""
    output_path = os.path.join(output_dir, f"seed_{seed}.out")
    environment = dict(os.environ, HYPERSCALE_SIM_SOAK="1")
    started = time.monotonic()
    with open(output_path, "wb") as output_file:
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", suite, "-q", "-p", "no:cacheprovider",
             "-k", "test_sim_replay", f"--sim-replay={seed}"],
            stdout=output_file,
            stderr=subprocess.STDOUT,
            env=environment,
        )
    elapsed = time.monotonic() - started
    if completed.returncode == 0:
        os.unlink(output_path)
    return (seed, completed.returncode == 0, elapsed)


def run_seeds(suite: str, seeds: list[int], deadline: float | None, output_dir: str) -> list[SeedOutcome]:
    """Run ``seeds`` in order while each next one still fits before ``deadline``."""
    outcomes: list[SeedOutcome] = []
    for seed in seeds:
        if not _next_seed_fits(outcomes, deadline):
            break
        _refresh_lock()
        outcomes.append(run_seed(suite, seed, output_dir))
        _print_outcome(suite, outcomes[-1])
    return outcomes


def _next_seed_fits(outcomes: list[SeedOutcome], deadline: float | None) -> bool:
    return deadline is None or time.monotonic() + _slowest_seconds(outcomes) <= deadline


def _slowest_seconds(outcomes: list[SeedOutcome]) -> float:
    return max((elapsed for _seed, _passed, elapsed in outcomes), default=0.0)


def _refresh_lock() -> None:
    """Keep the lock's mtime fresh; re-take it (loudly) if a peer stole it."""
    try:
        os.utime(_LOCK_PATH)
    except FileNotFoundError:
        print("[swarm] lock vanished mid-batch; re-taking", flush=True)
        _retake_lock()


def _retake_lock() -> None:
    try:
        os.mkdir(_LOCK_PATH)
    except FileExistsError:
        print("[swarm] WARNING: a peer re-locked first -- possible overlapping heavy runs", flush=True)


def _print_outcome(suite: str, outcome: SeedOutcome) -> None:
    seed, passed, elapsed = outcome
    print(f"[swarm] {suite} seed {seed}: {'pass' if passed else 'FAIL'} ({elapsed:.1f}s)", flush=True)


def ordered_seeds(suite_ledger: SuiteLedger, first_seed: int, count: int) -> list[int]:
    """Known failing seeds first (replayed until they pass), then the window."""
    known_failing = sorted(int(seed) for seed in suite_ledger)
    return known_failing + _window_without(first_seed, count, set(known_failing))


def _window_without(first_seed: int, count: int, excluded_seeds: set[int]) -> list[int]:
    return [seed for seed in range(first_seed, first_seed + count) if seed not in excluded_seeds]


def update_ledger(suite_ledger: SuiteLedger, outcomes: list[SeedOutcome], commit_hash: str, stamp: str) -> list[int]:
    """Record failures and clear seeds that pass again; returns the cleared seeds."""
    cleared_seeds = _cleared_seeds(suite_ledger, outcomes)
    for seed, passed, _elapsed in outcomes:
        _apply_outcome(suite_ledger, str(seed), passed, commit_hash, stamp)
    return cleared_seeds


def _cleared_seeds(suite_ledger: SuiteLedger, outcomes: list[SeedOutcome]) -> list[int]:
    return [seed for seed, passed, _elapsed in outcomes if _was_failing_and_passed(suite_ledger, seed, passed)]


def _was_failing_and_passed(suite_ledger: SuiteLedger, seed: int, passed: bool) -> bool:
    return passed and str(seed) in suite_ledger


def _apply_outcome(suite_ledger: SuiteLedger, seed: str, passed: bool, commit_hash: str, stamp: str) -> None:
    if passed:
        suite_ledger.pop(seed, None)
        return
    entry = suite_ledger.setdefault(seed, {"first_failed_commit": commit_hash, "first_failed_at": stamp})
    entry.update(last_failed_commit=commit_hash, last_failed_at=stamp)


def run_suite(
    arguments: argparse.Namespace,
    suite: str,
    ledger: dict[str, SuiteLedger],
    run: dict[str, object],
) -> dict[str, object]:
    """Replay the suite's known failures, then its window; returns the suite's log record."""
    suite_ledger = ledger.setdefault(suite, {})
    seeds = ordered_seeds(suite_ledger, arguments.first_seed, arguments.count)
    output_dir = os.path.join(arguments.artifacts_dir, "swarm", suite.rstrip("/").replace("/", "_"))
    os.makedirs(output_dir, exist_ok=True)
    started = time.monotonic()
    outcomes = run_seeds(suite, seeds, run["deadline"], output_dir)
    cleared_seeds = update_ledger(suite_ledger, outcomes, run["commit"], run["stamp"])
    return _suite_record(suite, seeds, outcomes, cleared_seeds, run, time.monotonic() - started) | {
        "first_seed": arguments.first_seed,
        "count": arguments.count,
        "output_dir": output_dir,
    }


def _suite_record(
    suite: str,
    seeds: list[int],
    outcomes: list[SeedOutcome],
    cleared_seeds: list[int],
    run: dict[str, object],
    elapsed: float,
) -> dict[str, object]:
    failing_seeds = _failing_seeds(outcomes)
    return {
        "timestamp": run["stamp"],
        "commit": run["commit"],
        "tree_dirty": run["tree_dirty"],
        "suite": suite,
        "seeds_run": len(outcomes),
        "failing_seeds": failing_seeds,
        "cleared_seeds": cleared_seeds,
        "unrun_seeds": seeds[len(outcomes):],
        "elapsed_seconds": round(elapsed, 1),
        "slowest_seed_seconds": round(_slowest_seconds(outcomes), 1),
        "repro": [_repro_command(run["commit"], suite, seed) for seed in failing_seeds],
    }


def _failing_seeds(outcomes: list[SeedOutcome]) -> list[int]:
    return [seed for seed, passed, _elapsed in outcomes if not passed]


def _repro_command(commit_hash: object, suite: str, seed: int) -> str:
    return f"git checkout {commit_hash} && uv run pytest {suite} -k test_sim_replay --sim-replay={seed}"


def load_ledger(ledger_path: str) -> dict[str, SuiteLedger]:
    """The ledger at ``ledger_path``, or an empty one when none exists yet."""
    if not os.path.exists(ledger_path):
        return {}
    with open(ledger_path) as ledger_file:
        return json.load(ledger_file)


def save_ledger(ledger_path: str, ledger: dict[str, SuiteLedger]) -> None:
    """Write the ledger sorted and indented, dropping suites with no failing seed."""
    _ensure_parent_directory(ledger_path)
    with open(ledger_path, "w") as ledger_file:
        json.dump(_suites_with_failures(ledger), ledger_file, indent=2, sort_keys=True)
        ledger_file.write("\n")


def _suites_with_failures(ledger: dict[str, SuiteLedger]) -> dict[str, SuiteLedger]:
    return {suite: seeds for suite, seeds in ledger.items() if seeds}


def _ensure_parent_directory(path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)


def append_log(log_path: str, records: list[dict[str, object]]) -> None:
    """Append one JSON line per suite record."""
    _ensure_parent_directory(log_path)
    with open(log_path, "a") as log_file:
        log_file.writelines(json.dumps(record) + "\n" for record in records)


def write_summary(summary_path: str | None, records: list[dict[str, object]]) -> None:
    """Append a markdown summary of the run (CI passes ``$GITHUB_STEP_SUMMARY``)."""
    if summary_path is None:
        return
    with open(summary_path, "a") as summary_file:
        summary_file.writelines(_summary_lines(records))


def _summary_lines(records: list[dict[str, object]]) -> list[str]:
    header = [
        "### VOPR swarm\n\n",
        "| suite | seeds run | failing | cleared | unrun | elapsed s | slowest seed s |\n",
        "|---|---|---|---|---|---|---|\n",
    ]
    rows = [_summary_row(record) for record in records]
    return header + rows + _repro_lines(records)


def _repro_lines(records: list[dict[str, object]]) -> list[str]:
    commands = _repro_commands(records)
    if not commands:
        return []
    return ["\nFailing seeds (each line replays one):\n\n", *commands]


def _repro_commands(records: list[dict[str, object]]) -> list[str]:
    return [f"- `{command}`\n" for record in records for command in record["repro"]]


def _summary_row(record: dict[str, object]) -> str:
    return (
        f"| {record['suite']} | {record['seeds_run']} | {record['failing_seeds'] or '-'} "
        f"| {record['cleared_seeds'] or '-'} | {len(record['unrun_seeds'])} "
        f"| {record['elapsed_seconds']} | {record['slowest_seed_seconds']} |\n"
    )


def exit_code_for(records: list[dict[str, object]], require_all: bool) -> int:
    """1 when a seed failed, or when ``require_all`` and a seed went unrun."""
    return int(_any_record_has(records, "failing_seeds") or (require_all and _any_record_has(records, "unrun_seeds")))


def _any_record_has(records: list[dict[str, object]], field_name: str) -> bool:
    return any(record[field_name] for record in records)


def _git_output(*git_arguments: str) -> str:
    return subprocess.run(["git", *git_arguments], capture_output=True, text=True).stdout.strip()


def _run_context(arguments: argparse.Namespace) -> dict[str, object]:
    started = time.monotonic()
    return {
        "commit": _git_output("rev-parse", "HEAD"),
        "tree_dirty": bool(_git_output("status", "--porcelain")),
        "stamp": datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ"),
        "deadline": None if arguments.budget_seconds is None else started + arguments.budget_seconds,
    }


def run_batch(arguments: argparse.Namespace, suites: list[str]) -> int:
    """Run every suite under one deadline; persist the ledger, log and summary."""
    ledger = load_ledger(arguments.ledger)
    run = _run_context(arguments)
    records = [run_suite(arguments, suite, ledger, run) for suite in suites]
    save_ledger(arguments.ledger, ledger)
    append_log(arguments.log, records)
    write_summary(arguments.summary_markdown, records)
    return exit_code_for(records, arguments.require_all)


def main() -> int:
    arguments = parse_arguments()
    suites = list(dict.fromkeys([*arguments.suites, *([_SOAK_SUITE] if arguments.include_soak else [])]))
    wait_for_idle_machine(arguments.max_wait_seconds)
    acquire_lock(arguments.max_wait_seconds)
    try:
        exit_code = run_batch(arguments, suites)
    finally:
        release_lock()
    print(f"[swarm] done -- ledger: {arguments.ledger}, seed log: {arguments.log}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
