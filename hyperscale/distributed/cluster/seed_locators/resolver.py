"""
SeedResolver — the entry point for AD-52 §2 locator handling.

Parses a comma-separated list of locator URIs into typed SeedLocator
instances, runs resolution in parallel, dedupes, and caps the result at
--max-seed-candidates using AD-28 WeightedRendezvousHash sampling.

Refresh cadence: jittered interval (default 60s, 0-20% jitter per AD-21).
Triggered ad-hoc by the upstream caller on connection failure
(--seed-refresh-interval semantics).
"""

from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING

from hyperscale.distributed.discovery.selection.rendezvous_hash import (
    WeightedRendezvousHash,
)

from .base import SeedLocator
from .resolved_address import ResolvedAddress
from .dns_locator import DnsLocator
from .dns_srv_locator import DnsSrvLocator
from .exec_locator import ExecLocator
from .file_locator import FileLocator
from .tcp_locator import TcpLocator

if TYPE_CHECKING:
    from hyperscale.logging import Logger


_SCHEME_FACTORIES: dict[str, type] = {
    "tcp://": TcpLocator,
    "dns://": DnsLocator,
    "dns-srv://": DnsSrvLocator,
    "file://": FileLocator,
    "exec://": ExecLocator,
}


def parse_locator_uri(locator_uri: str) -> SeedLocator:
    """
    Construct the right SeedLocator for the given URI. Raises ValueError
    for unsupported schemes or malformed URIs.
    """
    locator_uri = locator_uri.strip()
    if not locator_uri:
        raise ValueError("empty locator URI")

    for scheme_prefix, locator_cls in _SCHEME_FACTORIES.items():
        if locator_uri.startswith(scheme_prefix):
            return locator_cls(locator_uri)

    raise ValueError(
        f"unsupported locator scheme in {locator_uri!r}; "
        f"AD-52 §2 defines tcp://, dns://, dns-srv://, file://, exec://"
    )


class SeedResolver:
    """
    Resolves a list of locator URIs into a deduped, bounded set of
    ResolvedAddresses. Maintains the locator instances across refreshes
    so per-locator caches (mtime, exec interval) work correctly.

    Sampling: when the dedup yields more than max_seed_candidates
    addresses, we use WeightedRendezvousHash with the caller's
    rendezvous_key (typically the caller's node_id) so the sample is
    stable across resolve() calls but spreads load across the cluster.
    """

    __slots__ = (
        "_locators",
        "_max_seed_candidates",
        "_refresh_interval_seconds",
        "_jitter_fraction",
        "_rendezvous_key",
        "_logger",
    )

    DEFAULT_REFRESH_INTERVAL_SECONDS: float = 60.0
    DEFAULT_JITTER_FRACTION: float = 0.20
    DEFAULT_MAX_SEED_CANDIDATES: int = 64

    def __init__(
        self,
        locator_uris: list[str],
        rendezvous_key: str,
        max_seed_candidates: int | None = None,
        refresh_interval_seconds: float | None = None,
        logger: "Logger | None" = None,
    ) -> None:
        if not rendezvous_key:
            raise ValueError("rendezvous_key is required (typically the node_id)")

        self._locators: list[SeedLocator] = [
            parse_locator_uri(uri) for uri in locator_uris
        ]
        self._max_seed_candidates = (
            max_seed_candidates
            if max_seed_candidates is not None
            else self.DEFAULT_MAX_SEED_CANDIDATES
        )
        self._refresh_interval_seconds = (
            refresh_interval_seconds
            if refresh_interval_seconds is not None
            else self.DEFAULT_REFRESH_INTERVAL_SECONDS
        )
        self._jitter_fraction = self.DEFAULT_JITTER_FRACTION
        self._rendezvous_key = rendezvous_key
        self._logger = logger

    @property
    def refresh_interval_seconds(self) -> float:
        """Base refresh interval (before jitter)."""
        return self._refresh_interval_seconds

    def jittered_refresh_interval_seconds(self) -> float:
        """
        Per-AD-21: base interval plus uniform jitter in
        [0, base * jitter_fraction).
        """
        jitter_max = self._refresh_interval_seconds * self._jitter_fraction
        return self._refresh_interval_seconds + random.uniform(0.0, jitter_max)

    async def resolve_all(self) -> list[ResolvedAddress]:
        """
        Resolve every locator in parallel, dedupe by (host, port), and
        cap at max_seed_candidates via AD-28 weighted rendezvous sampling.
        """
        if not self._locators:
            return []

        per_locator_results = await asyncio.gather(
            *[locator.resolve() for locator in self._locators],
            return_exceptions=True,
        )

        # Flatten + handle nested-locator hand-off from file:// and exec://.
        flat_addresses: list[ResolvedAddress] = []
        for per_locator_result in per_locator_results:
            if isinstance(per_locator_result, Exception):
                # Locator-level configuration errors propagate to the
                # caller; transient failures (DNS NXDOMAIN, file missing)
                # are absorbed inside the locator and return [].
                continue
            for resolved_address in per_locator_result:
                if resolved_address.port == 0 and resolved_address.source_scheme in ("file", "exec"):
                    nested_addresses = await self._resolve_nested(
                        resolved_address.host
                    )
                    flat_addresses.extend(nested_addresses)
                else:
                    flat_addresses.append(resolved_address)

        # Dedupe by (host, port). Keep the first source_scheme seen for
        # provenance.
        seen_addresses: dict[tuple[str, int], ResolvedAddress] = {}
        for resolved_address in flat_addresses:
            address_key = (resolved_address.host, resolved_address.port)
            seen_addresses.setdefault(address_key, resolved_address)

        deduped = list(seen_addresses.values())
        if len(deduped) <= self._max_seed_candidates:
            return deduped

        return self._sample_by_rendezvous(deduped)

    async def any_refresh_required(self) -> bool:
        """True if any underlying locator reports a change since last resolve."""
        for locator in self._locators:
            if await locator.refresh_required():
                return True
        return False

    def _sample_by_rendezvous(
        self,
        candidates: list[ResolvedAddress],
    ) -> list[ResolvedAddress]:
        """
        AD-28 weighted rendezvous hashing. Stable selection of
        max_seed_candidates addresses keyed by self._rendezvous_key.
        """
        hasher_instance = WeightedRendezvousHash()
        peer_id_to_address: dict[str, ResolvedAddress] = {}
        for resolved_address in candidates:
            peer_id = f"{resolved_address.host}:{resolved_address.port}"
            peer_id_to_address[peer_id] = resolved_address
            hasher_instance.add_peer(peer_id, weight=max(resolved_address.weight, 0.0001))

        ranked_peer_ids = hasher_instance.select_n(
            self._rendezvous_key,
            n=self._max_seed_candidates,
        )

        return [peer_id_to_address[peer_id] for peer_id in ranked_peer_ids]

    async def _resolve_nested(self, nested_uri: str) -> list[ResolvedAddress]:
        """
        Resolve a locator URI emitted by file:// or exec:// at runtime.
        We instantiate a one-shot locator (not retained, no refresh
        tracking — the parent file:// / exec:// owns the refresh).
        """
        try:
            nested_locator = parse_locator_uri(nested_uri)
        except ValueError:
            return []
        # Nested file:// / exec:// were already rejected by the parent;
        # if one slips through, refuse here too.
        if nested_locator.scheme in ("file", "exec"):
            return []
        try:
            return await nested_locator.resolve()
        except Exception:
            return []
