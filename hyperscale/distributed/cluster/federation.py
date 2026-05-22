"""
DC federation (AD-52 §17).

Gate-side data structure: DatacenterCatalog backed by RegisterDatacenter
log entries applied to the gate cluster's Raft. Generation-aware:
when a per-DC manager cluster suffers catastrophic quorum loss and
rebuilds with a new cluster_uuid_of_dc, the gate cluster sees the new
generation, retains the old for dc_registration_grace_period (default
1h) so in-flight references can fence-validate before failing, then
prunes.

Manager-side: a small helper (RegisterDatacenterProposer) that the
manager's bootstrap-completion callback fires to propose
RegisterDatacenter into the gate cluster's Raft once local bootstrap
finishes (AD-52 §17 bootstrap order: gate → manager → workers).
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .membership_log.register_datacenter import RegisterDatacenter

if TYPE_CHECKING:
    from hyperscale.logging import Logger


_DEFAULT_GRACE_PERIOD_SECONDS: float = 3600.0
_DEFAULT_HISTORY_DEPTH: int = 3


@dataclass(frozen=True, slots=True)
class DatacenterGeneration:
    """One generation of a DC's registration.

    cluster_uuid_of_dc        Distinguishes generations. A change here
                              means the DC's manager cluster was rebuilt.
    region_code               Operator-supplied region label.
    manager_seeds             Locator URIs for the manager cluster.
    advertised_endpoint       Address for direct dispatch (informational).
    registered_at_gate_epoch  Gate cluster's membership_epoch at the
                              moment of registration. Lets downstream
                              consumers fence against pre-generation
                              messages.
    superseded_at_monotonic   When a newer generation arrived, the time
                              the old one was demoted. None for the
                              current generation.
    """

    cluster_uuid_of_dc: str
    region_code: str
    manager_seeds: tuple[str, ...]
    advertised_endpoint: tuple[str, int]
    registered_at_gate_epoch: int
    superseded_at_monotonic: float | None = None


@dataclass(slots=True)
class DatacenterEntry:
    """A DC's full registration history. The most recent generation is
    the operative one; older generations are kept for grace_period."""

    dc_id: str
    generations: list[DatacenterGeneration] = field(default_factory=list)

    @property
    def current(self) -> DatacenterGeneration | None:
        if not self.generations:
            return None
        return self.generations[-1]


class DatacenterCatalog:
    """
    Gate-cluster-only catalog. Mutated by the apply layer when
    RegisterDatacenter commits; consumed by gate routing components
    (AD-36, AD-45, AD-51) via direct reads.

    Outside the deterministic apply layer the catalog uses monotonic
    time for grace-period accounting. The apply layer itself uses the
    gate Raft's epoch as the generation marker.
    """

    __slots__ = (
        "_entries",
        "_grace_period_seconds",
        "_history_depth",
        "_logger",
    )

    def __init__(
        self,
        grace_period_seconds: float = _DEFAULT_GRACE_PERIOD_SECONDS,
        history_depth: int = _DEFAULT_HISTORY_DEPTH,
        logger: "Logger | None" = None,
    ) -> None:
        if history_depth < 1:
            raise ValueError("history_depth must be >= 1")
        self._entries: dict[str, DatacenterEntry] = {}
        self._grace_period_seconds = grace_period_seconds
        self._history_depth = history_depth
        self._logger = logger

    def apply_register(
        self,
        entry: RegisterDatacenter,
    ) -> tuple[str, bool]:
        """
        Apply a RegisterDatacenter log entry. Returns (dc_id, is_new_generation).
        Called from the apply layer.
        """
        existing = self._entries.get(entry.dc_id)
        new_generation = DatacenterGeneration(
            cluster_uuid_of_dc=entry.cluster_uuid_of_dc,
            region_code=entry.region_code,
            manager_seeds=tuple(entry.manager_seeds),
            advertised_endpoint=entry.advertised_endpoint,
            registered_at_gate_epoch=entry.registered_at_gate_epoch,
        )

        if existing is None:
            self._entries[entry.dc_id] = DatacenterEntry(
                dc_id=entry.dc_id,
                generations=[new_generation],
            )
            return entry.dc_id, True

        current = existing.current
        if current is not None and current.cluster_uuid_of_dc == new_generation.cluster_uuid_of_dc:
            # Same generation, treat as no-op refresh — apply
            # advertised_endpoint and manager_seeds updates in place.
            existing.generations[-1] = new_generation
            return entry.dc_id, False

        # Genuinely new generation. Demote the current one with a
        # supersedure timestamp (used for grace-period pruning).
        if current is not None:
            existing.generations[-1] = DatacenterGeneration(
                cluster_uuid_of_dc=current.cluster_uuid_of_dc,
                region_code=current.region_code,
                manager_seeds=current.manager_seeds,
                advertised_endpoint=current.advertised_endpoint,
                registered_at_gate_epoch=current.registered_at_gate_epoch,
                superseded_at_monotonic=time.monotonic(),
            )
        existing.generations.append(new_generation)

        # Bound history depth (oldest pruned even before grace).
        while len(existing.generations) > self._history_depth:
            existing.generations.pop(0)

        return entry.dc_id, True

    def prune_expired_generations(self) -> list[str]:
        """Remove demoted generations whose grace period has elapsed.
        Returns the list of (dc_id, cluster_uuid_of_dc) pruned for
        observability."""
        now_monotonic = time.monotonic()
        pruned_dc_ids: list[str] = []
        for dc_id in sorted(self._entries.keys()):
            entry = self._entries[dc_id]
            kept_generations: list[DatacenterGeneration] = []
            for generation in entry.generations:
                if (
                    generation.superseded_at_monotonic is not None
                    and now_monotonic - generation.superseded_at_monotonic
                    >= self._grace_period_seconds
                ):
                    pruned_dc_ids.append(dc_id)
                    continue
                kept_generations.append(generation)
            entry.generations = kept_generations
        return pruned_dc_ids

    def get_current(self, dc_id: str) -> DatacenterGeneration | None:
        entry = self._entries.get(dc_id)
        if entry is None:
            return None
        return entry.current

    def list_datacenters(self) -> list[str]:
        return sorted(self._entries.keys())

    def history_of(self, dc_id: str) -> list[DatacenterGeneration]:
        entry = self._entries.get(dc_id)
        if entry is None:
            return []
        return list(entry.generations)


class RegisterDatacenterProposer:
    """
    Manager-side helper. Wraps the act of building a RegisterDatacenter
    entry and proposing it into the gate cluster's Raft. The actual
    gate-proposal transport is delegated; this class only owns the
    "what to propose" logic.
    """

    __slots__ = (
        "_dc_id",
        "_region_code",
        "_cluster_uuid_of_dc_provider",
        "_manager_seeds_provider",
        "_advertised_endpoint_provider",
        "_propose_against_gate",
        "_logger",
    )

    def __init__(
        self,
        dc_id: str,
        region_code: str,
        cluster_uuid_of_dc_provider: Callable[[], str],
        manager_seeds_provider: Callable[[], list[str]],
        advertised_endpoint_provider: Callable[[], tuple[str, int]],
        propose_against_gate: Callable[[RegisterDatacenter], Awaitable[int]],
        logger: "Logger | None" = None,
    ) -> None:
        self._dc_id = dc_id
        self._region_code = region_code
        self._cluster_uuid_of_dc_provider = cluster_uuid_of_dc_provider
        self._manager_seeds_provider = manager_seeds_provider
        self._advertised_endpoint_provider = advertised_endpoint_provider
        self._propose_against_gate = propose_against_gate
        self._logger = logger

    async def propose(self, registered_at_gate_epoch: int) -> int:
        """Build and propose the RegisterDatacenter entry. Returns the
        committed gate-Raft LSN."""
        entry = RegisterDatacenter(
            dc_id=self._dc_id,
            region_code=self._region_code,
            cluster_uuid_of_dc=self._cluster_uuid_of_dc_provider(),
            manager_seeds=tuple(self._manager_seeds_provider()),
            advertised_endpoint=self._advertised_endpoint_provider(),
            registered_at_gate_epoch=registered_at_gate_epoch,
        )
        committed_lsn = await self._propose_against_gate(entry)
        if self._logger is not None:
            await self._logger.log({
                "event": "DatacenterRegistered",
                "dc_id": self._dc_id,
                "cluster_uuid_of_dc": entry.cluster_uuid_of_dc,
                "committed_lsn": committed_lsn,
            })
        return committed_lsn
