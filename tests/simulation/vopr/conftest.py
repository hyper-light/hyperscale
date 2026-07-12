"""
VOPR pytest options.

``--sim-replay=<seed>`` re-runs exactly the fault schedule that integer
generates — the debugging entry point for any seed the sweep (or a
future long-running fuzz) reports. ``--sim-vopr-count=<n>`` widens the
default sweep corpus for soak runs.
"""


def pytest_addoption(parser):
    parser.addoption(
        "--sim-replay",
        action="store",
        type=int,
        default=None,
        help=(
            "Replay the generated VOPR fault schedule for this seed "
            "(runs it twice and asserts byte-identical results + "
            "invariants; prints the expanded plan)"
        ),
    )
    parser.addoption(
        "--sim-vopr-count",
        action="store",
        type=int,
        default=None,
        help="Number of seeds the VOPR sweep covers (default 4)",
    )
