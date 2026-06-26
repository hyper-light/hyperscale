"""
Lints — static-analysis guard tests that enforce Phase 5+ exit
criteria mechanically in CI.

These tests do not exercise runtime behavior; they AST-walk the
production tree and assert structural invariants that would
otherwise drift back in over time. See
``docs/dev/simulation_framework.md`` for the surrounding design.
"""
