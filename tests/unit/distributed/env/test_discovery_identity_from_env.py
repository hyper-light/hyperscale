"""
Discovery filters peers by the node's own cluster and environment (AD-28).

``Env.get_discovery_config`` (now ``DiscoveryConfig.from_env``) defaulted the discovery identity to the
literals "hyperscale" and "default" and no caller passed the node's
settings, so a node configured with any other CLUSTER_ID or
ENVIRONMENT_ID discovered under the wrong identity.

* the discovery identity is the Env's CLUSTER_ID and ENVIRONMENT_ID.
"""

from hyperscale.distributed.discovery import DiscoveryConfig
from hyperscale.distributed.env import Env


def test_the_discovery_identity_is_the_nodes_own() -> None:
    env = Env(CLUSTER_ID="cluster-a", ENVIRONMENT_ID="staging")

    config = DiscoveryConfig.from_env(env, node_role="worker", allow_dynamic_registration=True)

    assert (config.cluster_id, config.environment_id) == ("cluster-a", "staging")
