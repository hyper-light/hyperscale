"""
F2 swarm runner: widen every VOPR-style suite's seed sweep SERIALLY
under the machine-wide simulation lock, and record failing seeds as
per-commit reproducers (I4: a seed reproduces against the tree it was
drawn on — schedules legitimately shift when production timing
changes, so seed + commit hash are recorded TOGETHER).

Each suite invocation is::

    uv run pytest <suite> -q --sim-vopr-count=<N>

run one suite at a time (heavy multi-process simulations must never
overlap — the per-child 300s wall deadman assumes an uncontended
machine), with the ``/tmp/hyperscale-sim-run.lock`` mkdir-mutex held
for the whole batch and its mtime refreshed by the 30s progress
heartbeat so a long batch is never mistaken for an abandoned lock
(stale threshold: 25 minutes).

Every failing seed found in a suite's output (the invariant assertions
embed ``--sim-replay=<seed>``) is appended to the seed log as one JSON
line carrying the commit hash, timestamp, suite, seed list, and the
exact reproduction commands. Full suite output is preserved under the
artifacts directory.

Usage (from the repository root; defaults are cwd-relative)::

    uv run python tests/simulation/soak/run_swarm.py --count 25
    uv run python tests/simulation/soak/run_swarm.py \\
        --count 100 \\
        --suites tests/simulation/vopr tests/simulation/vopr_mdc \\
        --log my_seed_log.jsonl

Expected runtime scales linearly in ``--count`` (each seed is TWO full
cluster runs: judge + replay twin). MEASURED on the validation run
(N=2 per suite, serial, locked): vopr 55.0s, vopr_gates 85.1s,
vopr_mdc 140.1s — 4.7 minutes total, ~140 wall-seconds per seed
across the whole triad, so a nightly ``--count 100`` budgets roughly
4 hours. ``--include-soak`` appends the long-horizon soak suite
(``HYPERSCALE_SIM_SOAK=1``; budget ~10+ minutes PER SEED — keep its
count small via the shared ``--sim-vopr-count``).
"""

import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import time

_LOCK_PATH = "/tmp/hyperscale-sim-run.lock"
_LOCK_STALE_SECONDS = 25 * 60
_POLL_SECONDS = 30.0
_REPLAY_SEED_PATTERN = re.compile(r"--sim-replay=(\d+)")

_DEFAULT_SUITES = (
    "tests/simulation/vopr",
    "tests/simulation/vopr_gates",
    "tests/simulation/vopr_mdc",
)
_SOAK_SUITE = "tests/simulation/soak"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--count",
        type=int,
        default=25,
        help="Seeds per suite (passed as --sim-vopr-count; default 25)",
    )
    parser.add_argument(
        "--suites",
        nargs="+",
        default=list(_DEFAULT_SUITES),
        help="Suite paths to run, in order (default: the VOPR triad)",
    )
    parser.add_argument(
        "--log",
        default=os.path.join(
            "tests", "simulation", "_artifacts", "swarm_seed_log.jsonl"
        ),
        help=(
            "Seed log to append one JSON line per suite run to "
            "(cwd-relative default under the ignored artifacts dir)"
        ),
    )
    parser.add_argument(
        "--artifacts-dir",
        default=os.path.join("tests", "simulation", "_artifacts"),
        help="Directory for full per-suite output captures",
    )
    parser.add_argument(
        "--include-soak",
        action="store_true",
        help=(
            f"Also run {_SOAK_SUITE} (opts in via HYPERSCALE_SIM_SOAK=1; "
            "~10+ wall minutes PER SEED)"
        ),
    )
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


def run_suite(
    suite: str, count: int, output_path: str
) -> tuple[int, float, list[int]]:
    """Run one suite serially with a visible heartbeat; returns
    ``(exit_code, elapsed_seconds, failing_seeds)``.

    The heartbeat also refreshes the lock mtime so a long suite can
    never be mistaken for an abandoned lock by a polite peer.
    """
    command = [
        "uv",
        "run",
        "pytest",
        suite,
        "-q",
        f"--sim-vopr-count={count}",
    ]
    environment = dict(os.environ)
    if suite.rstrip("/").endswith("soak"):
        environment["HYPERSCALE_SIM_SOAK"] = "1"

    started = time.monotonic()
    last_heartbeat = started
    with open(output_path, "wb") as output_file:
        child = subprocess.Popen(
            command, stdout=output_file, stderr=subprocess.STDOUT,
            env=environment,
        )
        while child.poll() is None:
            time.sleep(5.0)
            if time.monotonic() - last_heartbeat >= _POLL_SECONDS:
                last_heartbeat = time.monotonic()
                try:
                    os.utime(_LOCK_PATH)
                except FileNotFoundError:
                    # A peer stole the lock despite the heartbeat.
                    # Re-take it best-effort and say so loudly — the
                    # suite is already mid-flight, so aborting would
                    # not undo any overlap.
                    print(
                        "[swarm] lock vanished mid-suite; re-taking",
                        flush=True,
                    )
                    try:
                        os.mkdir(_LOCK_PATH)
                    except FileExistsError:
                        print(
                            "[swarm] WARNING: a peer re-locked first — "
                            "possible overlapping heavy runs",
                            flush=True,
                        )
                print(
                    f"[swarm] {suite} running "
                    f"({time.monotonic() - started:.0f}s elapsed)",
                    flush=True,
                )
    elapsed = time.monotonic() - started

    with open(output_path, "r", errors="replace") as output_file:
        output_text = output_file.read()
    failing_seeds = sorted(
        {int(seed) for seed in _REPLAY_SEED_PATTERN.findall(output_text)}
    )
    return child.returncode, elapsed, failing_seeds


def main() -> int:
    arguments = parse_arguments()
    suites = list(arguments.suites)
    if arguments.include_soak and _SOAK_SUITE not in suites:
        suites.append(_SOAK_SUITE)

    os.makedirs(arguments.artifacts_dir, exist_ok=True)
    os.makedirs(os.path.dirname(arguments.log) or ".", exist_ok=True)

    commit_hash = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    tree_dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"], capture_output=True, text=True
        ).stdout.strip()
    )

    wait_for_idle_machine(arguments.max_wait_seconds)
    acquire_lock(arguments.max_wait_seconds)
    exit_code = 0
    try:
        for suite in suites:
            batch_stamp = datetime.datetime.now(datetime.UTC).strftime(
                "%Y%m%dT%H%M%SZ"
            )
            suite_slug = suite.rstrip("/").replace("/", "_")
            output_path = os.path.join(
                arguments.artifacts_dir,
                f"swarm_{batch_stamp}_{suite_slug}.out",
            )
            print(
                f"[swarm] {suite}: --sim-vopr-count={arguments.count} "
                f"-> {output_path}",
                flush=True,
            )
            suite_exit, elapsed, failing_seeds = run_suite(
                suite, arguments.count, output_path
            )
            record = {
                "timestamp": batch_stamp,
                "commit": commit_hash,
                "tree_dirty": tree_dirty,
                "suite": suite,
                "sim_vopr_count": arguments.count,
                "exit_code": suite_exit,
                "elapsed_seconds": round(elapsed, 1),
                "failing_seeds": failing_seeds,
                "repro": [
                    f"git checkout {commit_hash} && uv run pytest "
                    f"{suite} --sim-replay={seed}"
                    for seed in failing_seeds
                ],
                "output_log": output_path,
            }
            with open(arguments.log, "a") as log_file:
                log_file.write(json.dumps(record) + "\n")
            print(
                f"[swarm] {suite}: exit={suite_exit} "
                f"elapsed={elapsed:.1f}s failing_seeds={failing_seeds}",
                flush=True,
            )
            if suite_exit != 0:
                exit_code = 1
    finally:
        release_lock()

    print(f"[swarm] done — seed log: {arguments.log}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
