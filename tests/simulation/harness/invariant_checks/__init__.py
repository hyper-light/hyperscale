"""
The continuous invariant catalog's checks (simulation_framework.md §12-13,
SCENARIOS.md §11). Each module reads node state through the harness's
handles; ``tests/simulation/harness/invariants.py`` wraps each check as a
``SafetyInvariant`` the ``InvariantChecker`` evaluates every tick.
"""
