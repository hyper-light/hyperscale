"""``ClusterConnectionState`` -- pickled under the namespace
``hyperscale.distributed.nodes.worker.cluster_connection`` (see that module)."""

from enum import Enum


class ClusterConnectionState(Enum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
