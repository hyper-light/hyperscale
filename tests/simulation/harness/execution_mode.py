"""Execution-mode selector for the simulation harness."""

from enum import StrEnum


class ExecutionMode(StrEnum):
    """Which dependency configuration the harness wires in.

    REAL — production dependencies: real OS clock, real asyncio sockets,
    real OS scheduler. Phase 1+ supports this.

    SIM — virtual clock, in-process transport, deterministic scheduler,
    seeded random. Available once Phases 5–6 land the production-side
    Clock/Random/Transport refactor.
    """

    REAL = "real"
    SIM = "sim"
