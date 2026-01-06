"""
Integration tests for Version Skew Handling (AD-25).

These tests verify that:
1. ProtocolVersion correctly handles major/minor versioning
2. Feature version map accurately tracks feature availability
3. NodeCapabilities properly negotiates common features
4. Handshake messages include version information
5. Backwards compatibility with older nodes

The Version Skew Handling pattern ensures:
- Rolling upgrades without downtime
- Graceful degradation with older nodes
- Feature negotiation between different versions
"""

import pytest

from hyperscale.distributed_rewrite.protocol import (
    ProtocolVersion,
    CURRENT_PROTOCOL_VERSION,
    FEATURE_VERSIONS,
    get_all_features,
    get_features_for_version,
    NodeCapabilities,
    NegotiatedCapabilities,
    negotiate_capabilities,
)
from hyperscale.distributed_rewrite.models import (
    WorkerRegistration,
    ManagerPeerRegistration,
    ManagerPeerRegistrationResponse,
    RegistrationResponse,
    NodeInfo,
    ManagerInfo,
    NodeRole,
)


class TestProtocolVersion:
    """Test ProtocolVersion dataclass."""

    def test_version_creation(self):
        """ProtocolVersion should create with major.minor."""
        version = ProtocolVersion(1, 4)
        assert version.major == 1
        assert version.minor == 4
        assert str(version) == "1.4"

    def test_version_equality(self):
        """Same versions should be equal."""
        v1 = ProtocolVersion(1, 2)
        v2 = ProtocolVersion(1, 2)
        assert v1 == v2

    def test_version_inequality(self):
        """Different versions should not be equal."""
        v1 = ProtocolVersion(1, 2)
        v2 = ProtocolVersion(1, 3)
        v3 = ProtocolVersion(2, 2)
        assert v1 != v2
        assert v1 != v3

    def test_same_major_compatible(self):
        """Same major version should be compatible."""
        v1 = ProtocolVersion(1, 0)
        v2 = ProtocolVersion(1, 5)
        assert v1.is_compatible_with(v2) is True
        assert v2.is_compatible_with(v1) is True

    def test_different_major_incompatible(self):
        """Different major versions should be incompatible."""
        v1 = ProtocolVersion(1, 5)
        v2 = ProtocolVersion(2, 0)
        assert v1.is_compatible_with(v2) is False
        assert v2.is_compatible_with(v1) is False

    def test_supports_feature_base(self):
        """Version 1.0 should support base features."""
        v = ProtocolVersion(1, 0)
        assert v.supports_feature("job_submission") is True
        assert v.supports_feature("cancellation") is True
        assert v.supports_feature("heartbeat") is True

    def test_supports_feature_higher_minor(self):
        """Higher minor versions should support new features."""
        v14 = ProtocolVersion(1, 4)
        assert v14.supports_feature("healthcheck_extensions") is True
        assert v14.supports_feature("rate_limiting") is True

        v10 = ProtocolVersion(1, 0)
        assert v10.supports_feature("healthcheck_extensions") is False
        assert v10.supports_feature("rate_limiting") is False

    def test_supports_unknown_feature(self):
        """Unknown features should return False."""
        v = ProtocolVersion(1, 4)
        assert v.supports_feature("unknown_feature") is False


class TestFeatureVersionMap:
    """Test feature version tracking."""

    def test_feature_versions_exist(self):
        """Feature version map should have entries."""
        assert len(FEATURE_VERSIONS) > 0

    def test_base_features_are_1_0(self):
        """Base features should require version 1.0."""
        assert FEATURE_VERSIONS["job_submission"] == ProtocolVersion(1, 0)
        assert FEATURE_VERSIONS["cancellation"] == ProtocolVersion(1, 0)

    def test_newer_features_require_higher_versions(self):
        """Newer features should require higher minor versions."""
        assert FEATURE_VERSIONS["rate_limiting"] == ProtocolVersion(1, 3)
        assert FEATURE_VERSIONS["healthcheck_extensions"] == ProtocolVersion(1, 4)

    def test_get_all_features(self):
        """get_all_features should return all defined features."""
        features = get_all_features()
        assert "job_submission" in features
        assert "healthcheck_extensions" in features
        assert len(features) == len(FEATURE_VERSIONS)

    def test_get_features_for_version(self):
        """get_features_for_version should filter by version."""
        # Version 1.0 should only have base features
        v10_features = get_features_for_version(ProtocolVersion(1, 0))
        assert "job_submission" in v10_features
        assert "healthcheck_extensions" not in v10_features

        # Version 1.4 should have all features
        v14_features = get_features_for_version(ProtocolVersion(1, 4))
        assert "job_submission" in v14_features
        assert "healthcheck_extensions" in v14_features
        assert "rate_limiting" in v14_features


class TestNodeCapabilities:
    """Test NodeCapabilities negotiation."""

    def test_capabilities_creation(self):
        """NodeCapabilities should create with version and features."""
        caps = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 2),
            capabilities={"job_submission", "cancellation"},
            node_version="hyperscale-1.0.0",
        )
        assert caps.protocol_version == ProtocolVersion(1, 2)
        assert "job_submission" in caps.capabilities
        assert caps.node_version == "hyperscale-1.0.0"

    def test_current_capabilities(self):
        """NodeCapabilities.current() should use current version."""
        caps = NodeCapabilities.current("test-1.0")
        assert caps.protocol_version == CURRENT_PROTOCOL_VERSION
        assert len(caps.capabilities) > 0
        assert caps.node_version == "test-1.0"

    def test_compatible_negotiation(self):
        """Compatible versions should negotiate common features."""
        local = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities={"job_submission", "cancellation", "rate_limiting", "healthcheck_extensions"},
        )
        remote = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 2),
            capabilities={"job_submission", "cancellation", "client_reconnection"},
        )

        common = local.negotiate(remote)

        # Should have intersection of capabilities
        assert "job_submission" in common
        assert "cancellation" in common
        # Features not in both nodes should be excluded
        assert "rate_limiting" not in common  # Only in local
        assert "client_reconnection" not in common  # Only in remote

    def test_incompatible_negotiation_raises(self):
        """Incompatible versions should raise ValueError."""
        local = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 0),
            capabilities={"job_submission"},
        )
        remote = NodeCapabilities(
            protocol_version=ProtocolVersion(2, 0),
            capabilities={"job_submission"},
        )

        with pytest.raises(ValueError, match="Incompatible"):
            local.negotiate(remote)


class TestNegotiateCapabilities:
    """Test the negotiate_capabilities function."""

    def test_successful_negotiation(self):
        """Successful negotiation should return NegotiatedCapabilities."""
        local = NodeCapabilities.current()
        remote = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 2),
            capabilities={"job_submission", "cancellation", "client_reconnection"},
        )

        result = negotiate_capabilities(local, remote)

        assert isinstance(result, NegotiatedCapabilities)
        assert result.compatible is True
        assert result.local_version == local.protocol_version
        assert result.remote_version == remote.protocol_version
        assert len(result.common_features) > 0

    def test_failed_negotiation(self):
        """Incompatible versions should return compatible=False."""
        local = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 0),
            capabilities=set(),
        )
        remote = NodeCapabilities(
            protocol_version=ProtocolVersion(2, 0),
            capabilities=set(),
        )

        result = negotiate_capabilities(local, remote)

        assert result.compatible is False
        assert len(result.common_features) == 0

    def test_supports_check(self):
        """NegotiatedCapabilities.supports() should check common features."""
        local = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities={"job_submission", "rate_limiting"},
        )
        remote = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 2),
            capabilities={"job_submission", "client_reconnection"},
        )

        result = negotiate_capabilities(local, remote)

        assert result.supports("job_submission") is True
        assert result.supports("rate_limiting") is False
        assert result.supports("client_reconnection") is False


class TestHandshakeMessageVersionFields:
    """Test that handshake messages include version fields."""

    def test_worker_registration_has_version_fields(self):
        """WorkerRegistration should have version fields with defaults."""
        reg = WorkerRegistration(
            node=NodeInfo(
                id="worker-1",
                role=NodeRole.WORKER,
                host="localhost",
                port=8000,
            ),
            total_cores=4,
            available_cores=4,
            memory_mb=8192,
            available_memory_mb=8192,
        )

        # Default version should be 1.0
        assert reg.protocol_version_major == 1
        assert reg.protocol_version_minor == 0
        assert reg.capabilities == ""

    def test_worker_registration_with_version(self):
        """WorkerRegistration should accept version fields."""
        reg = WorkerRegistration(
            node=NodeInfo(
                id="worker-1",
                role=NodeRole.WORKER,
                host="localhost",
                port=8000,
            ),
            total_cores=4,
            available_cores=4,
            memory_mb=8192,
            available_memory_mb=8192,
            protocol_version_major=1,
            protocol_version_minor=4,
            capabilities="job_submission,cancellation,rate_limiting",
        )

        assert reg.protocol_version_major == 1
        assert reg.protocol_version_minor == 4
        assert "rate_limiting" in reg.capabilities

    def test_manager_peer_registration_has_version_fields(self):
        """ManagerPeerRegistration should have version fields."""
        reg = ManagerPeerRegistration(
            node=ManagerInfo(
                node_id="manager-1",
                tcp_host="localhost",
                tcp_port=9000,
                udp_host="localhost",
                udp_port=9001,
                datacenter="dc-1",
            ),
            term=1,
            is_leader=False,
        )

        assert reg.protocol_version_major == 1
        assert reg.protocol_version_minor == 0

    def test_registration_response_has_version_fields(self):
        """RegistrationResponse should have version fields."""
        resp = RegistrationResponse(
            accepted=True,
            manager_id="manager-1",
            healthy_managers=[],
        )

        assert resp.protocol_version_major == 1
        assert resp.protocol_version_minor == 0
        assert resp.capabilities == ""

    def test_registration_response_with_negotiated_capabilities(self):
        """RegistrationResponse should include negotiated capabilities."""
        resp = RegistrationResponse(
            accepted=True,
            manager_id="manager-1",
            healthy_managers=[],
            protocol_version_major=1,
            protocol_version_minor=2,
            capabilities="job_submission,cancellation,client_reconnection",
        )

        assert resp.protocol_version_major == 1
        assert resp.protocol_version_minor == 2
        assert "client_reconnection" in resp.capabilities


class TestBackwardsCompatibility:
    """Test backwards compatibility with older nodes."""

    def test_old_message_without_version_fields(self):
        """Messages from older nodes (without version) should use defaults."""
        # Simulate old message by creating without version fields
        reg = WorkerRegistration(
            node=NodeInfo(
                id="old-worker",
                role=NodeRole.WORKER,
                host="localhost",
                port=8000,
            ),
            total_cores=4,
            available_cores=4,
            memory_mb=8192,
            available_memory_mb=8192,
        )

        # Serialize and deserialize
        data = reg.dump()
        restored = WorkerRegistration.load(data)

        # Should have default version
        assert restored.protocol_version_major == 1
        assert restored.protocol_version_minor == 0
        assert restored.capabilities == ""

    def test_new_message_with_version_fields(self):
        """Messages with version fields should preserve them."""
        reg = WorkerRegistration(
            node=NodeInfo(
                id="new-worker",
                role=NodeRole.WORKER,
                host="localhost",
                port=8000,
            ),
            total_cores=4,
            available_cores=4,
            memory_mb=8192,
            available_memory_mb=8192,
            protocol_version_major=1,
            protocol_version_minor=4,
            capabilities="healthcheck_extensions,rate_limiting",
        )

        # Serialize and deserialize
        data = reg.dump()
        restored = WorkerRegistration.load(data)

        assert restored.protocol_version_major == 1
        assert restored.protocol_version_minor == 4
        assert "healthcheck_extensions" in restored.capabilities


class TestVersionNegotiationScenarios:
    """Test realistic version negotiation scenarios."""

    def test_rolling_upgrade_scenario(self):
        """
        Scenario: Rolling upgrade from 1.2 to 1.4.

        1. Old manager (1.2) connects to new worker (1.4)
        2. They negotiate to use 1.2 features only
        3. Both can communicate using common features
        """
        old_manager = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 2),
            capabilities=get_features_for_version(ProtocolVersion(1, 2)),
            node_version="hyperscale-1.2.0",
        )

        new_worker = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
            node_version="hyperscale-1.4.0",
        )

        result = negotiate_capabilities(old_manager, new_worker)

        # Should be compatible
        assert result.compatible is True

        # Should have 1.2 features (not 1.3 or 1.4)
        assert result.supports("job_submission") is True
        assert result.supports("client_reconnection") is True
        assert result.supports("rate_limiting") is False  # 1.3 feature
        assert result.supports("healthcheck_extensions") is False  # 1.4 feature

    def test_same_version_full_features(self):
        """
        Scenario: Same version nodes should have all features.
        """
        node1 = NodeCapabilities.current("node-1")
        node2 = NodeCapabilities.current("node-2")

        result = negotiate_capabilities(node1, node2)

        # Should have all current features
        assert result.compatible is True
        all_current = get_features_for_version(CURRENT_PROTOCOL_VERSION)
        for feature in all_current:
            assert result.supports(feature) is True

    def test_mixed_cluster_degradation(self):
        """
        Scenario: Cluster with mixed versions degrades to lowest common denominator.
        """
        # Three nodes with different versions
        v10_node = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 0),
            capabilities=get_features_for_version(ProtocolVersion(1, 0)),
        )
        v12_node = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 2),
            capabilities=get_features_for_version(ProtocolVersion(1, 2)),
        )
        v14_node = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 4),
            capabilities=get_features_for_version(ProtocolVersion(1, 4)),
        )

        # All should be compatible
        r1 = negotiate_capabilities(v10_node, v12_node)
        r2 = negotiate_capabilities(v12_node, v14_node)
        r3 = negotiate_capabilities(v10_node, v14_node)

        assert r1.compatible is True
        assert r2.compatible is True
        assert r3.compatible is True

        # 1.0 <-> 1.4 should only have 1.0 features
        assert r3.supports("job_submission") is True
        assert r3.supports("client_reconnection") is False
