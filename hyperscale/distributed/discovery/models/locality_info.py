"""
Locality models for the discovery system.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from dataclasses import dataclass
from enum import IntEnum

from .locality_tier import LocalityTier


@dataclass(slots=True, frozen=True)
class LocalityInfo:
    """
    Locality information for a node.

    Used to determine peer preference based on network topology.
    """

    datacenter_id: str
    """Datacenter identifier (e.g., 'us-east-1a')."""

    region_id: str
    """Region identifier (e.g., 'us-east-1').

    A region typically contains multiple datacenters.
    """

    zone_id: str = ""
    """Availability zone within a datacenter (optional)."""

    rack_id: str = ""
    """Physical rack identifier (optional, for very large deployments)."""

    def get_tier_for_peer(self, peer_dc: str, peer_region: str) -> LocalityTier:
        """
        Determine locality tier for a peer.

        Args:
            peer_dc: Peer's datacenter ID
            peer_region: Peer's region ID

        Returns:
            LocalityTier indicating preference level
        """
        if peer_dc and peer_dc == self.datacenter_id:
            return LocalityTier.SAME_DC
        return self._region_or_global_tier(peer_region)

    def _region_or_global_tier(self, peer_region: str) -> LocalityTier:
        """SAME_REGION for a peer in this node's region, else GLOBAL."""
        if peer_region and peer_region == self.region_id:
            return LocalityTier.SAME_REGION
        return LocalityTier.GLOBAL

    def is_same_datacenter(self, other: "LocalityInfo") -> bool:
        """Check if another node is in the same datacenter."""
        return bool(self.datacenter_id and self.datacenter_id == other.datacenter_id)

    def is_same_region(self, other: "LocalityInfo") -> bool:
        """Check if another node is in the same region."""
        return bool(self.region_id and self.region_id == other.region_id)

    def __str__(self) -> str:
        parts = self._present_parts()
        return ", ".join(parts) if parts else "unknown"

    def _present_parts(self) -> list[str]:
        """``label=value`` for each set locality level, datacenter to zone."""
        return [
            f"{label}={value}"
            for label, value in (
                ("dc", self.datacenter_id),
                ("region", self.region_id),
                ("zone", self.zone_id),
            )
            if value
        ]

_REHOMED = (
    LocalityTier,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
