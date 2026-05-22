"""
dns-srv:// locator — SRV record lookup. Service + port from the record.
"""

from __future__ import annotations

import asyncio

try:
    import aiodns
    HAS_AIODNS = True
except ImportError:
    aiodns = None  # type: ignore[assignment]
    HAS_AIODNS = False

from .resolved_address import ResolvedAddress


class DnsSrvLocator:
    """
    dns-srv://_hyperscale._tcp.svc.cluster.local →
        Looks up SRV record; each result yields a ResolvedAddress with
        the SRV target hostname and port.

    SRV records carry priority and weight. AD-52 §2 doesn't mandate a
    selection policy; we expose the SRV weight on the ResolvedAddress
    so the upstream resolver can feed it into WeightedRendezvousHash.

    aiodns is a core hyperscale dependency (see pyproject.toml). On
    import failure (e.g. broken environment), this locator raises
    immediately rather than silently returning an empty list — a
    misconfigured environment must be surfaced.
    """

    __slots__ = ("_uri", "_query")

    SCHEME_PREFIX = "dns-srv://"

    def __init__(self, uri: str) -> None:
        if not uri.startswith(self.SCHEME_PREFIX):
            raise ValueError(
                f"DnsSrvLocator expects {self.SCHEME_PREFIX} prefix, got {uri!r}"
            )
        if not HAS_AIODNS:
            raise RuntimeError(
                "dns-srv:// locator requires aiodns; install hyperscale's "
                "core dependencies (aiodns is in [project.dependencies])"
            )
        query = uri[len(self.SCHEME_PREFIX):].strip()
        if not query:
            raise ValueError(
                f"dns-srv:// locator {uri!r} has empty SRV query"
            )
        self._uri = uri
        self._query = query

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def scheme(self) -> str:
        return "dns-srv"

    async def resolve(self) -> list[ResolvedAddress]:
        resolver_instance = aiodns.DNSResolver(loop=asyncio.get_running_loop())
        try:
            srv_records = await resolver_instance.query(self._query, "SRV")
        except aiodns.error.DNSError:
            return []
        finally:
            # aiodns DNSResolver does not require explicit close, but we
            # let it fall out of scope here for clarity.
            pass

        resolved: list[ResolvedAddress] = []
        for srv_record in srv_records:
            target_host = getattr(srv_record, "host", None)
            target_port = getattr(srv_record, "port", None)
            target_weight = float(getattr(srv_record, "weight", 1) or 1)
            if not target_host or target_port is None:
                continue
            resolved.append(
                ResolvedAddress(
                    host=target_host,
                    port=int(target_port),
                    source_scheme="dns-srv",
                    weight=target_weight,
                )
            )

        return resolved

    async def refresh_required(self) -> bool:
        return True
