"""``ExtensionLedgerConfig`` -- pickled under the namespace
``hyperscale.distributed.health.extension_ledger`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ExtensionLedgerConfig:
    """Configuration for ``ExtensionLedger``."""

    # Cap rolling decision history per workflow at
    # ``max_extensions × 2`` so we keep room for AD-26's full
    # 5-grant schedule plus interleaved denials but never more.
    max_decisions_per_workflow: int = 10
