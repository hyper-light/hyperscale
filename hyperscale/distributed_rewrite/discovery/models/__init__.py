"""Models for the discovery system."""

from hyperscale.distributed_rewrite.discovery.models.discovery_config import (
    DiscoveryConfig as DiscoveryConfig,
)
from hyperscale.distributed_rewrite.discovery.models.peer_info import (
    PeerInfo as PeerInfo,
    PeerHealth as PeerHealth,
)
from hyperscale.distributed_rewrite.discovery.models.locality_info import (
    LocalityInfo as LocalityInfo,
    LocalityTier as LocalityTier,
)
from hyperscale.distributed_rewrite.discovery.models.connection_state import (
    ConnectionState as ConnectionState,
)
