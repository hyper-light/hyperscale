"""
ResolvedAddress — one record returned by SeedLocator.resolve().

Carries enough provenance for the caller to attribute failures, run
weighted rendezvous sampling, and log redacted locator origins per the
AD-52 §2 security note (full paths logged at TRACE only).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ResolvedAddress:
    """
    Fields:
        host           Hostname or IP address (IPv4 or IPv6 literal). Not
                       resolved further — connection-time OS resolver
                       handles late binding for DNS names so DNS changes
                       between resolve() calls are picked up.
        port           Integer port, always set (AD-52 §2 requires it).
        source_scheme  "tcp" | "dns" | "dns-srv" | "file" | "exec" —
                       carries provenance for failure attribution.
        weight         Optional weight for AD-28 WeightedRendezvousHash
                       sampling. 1.0 by default; SRV records expose their
                       numeric weight here.
    """

    host: str
    port: int
    source_scheme: str
    weight: float = 1.0
